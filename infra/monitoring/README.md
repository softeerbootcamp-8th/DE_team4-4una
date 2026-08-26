# Monitoring

Project EC2, Spark Streaming EC2, Monitoring EC2에 나눠 배포하는
Prometheus/Grafana 모니터링 구성이다.

## Architecture

```text
[Project EC2]                                   [Monitoring EC2]
- node_exporter    :9100  --- private VPC --->   - Prometheus (scrape)
- cAdvisor         :8081  --- private VPC --->     127.0.0.1:9090 (host 내부 전용)
- Serving API      :8000                         - Grafana :3000 (Prometheus/CloudWatch를
- API metrics      :9101  --- private VPC --->                    datasource로 사용)
- Kafka            :9092  (Project EC2 안에서만 접근)                ↑
- Kafka Exporter   :9308  --- private VPC --->                     |
- Airflow (scheduler/dag-processor/api-server)                     | (Docker network 내부)
    ↓ StatsD UDP 9125 (de4-local 네트워크 내부)                    |
- StatsD Exporter  :9102  --- private VPC --->     - monitoring-node-exporter :9100
                                                    - monitoring-cadvisor      :8081
[Spark Streaming EC2]                              - blackbox-exporter :9115 (Grafana
- node_exporter            :9100 --- private VPC --->  /api/health, Prometheus /-/ready,
- cAdvisor                 :8081 --- private VPC --->  ops-agent /health를 외부 probe)
- Stream Processor metrics :9103 --- private VPC --->  - ops-agent :8080 (webhook)

[EMR Serverless] --- CloudWatch API (AWS) ---> Grafana (CloudWatch datasource, IAM Role)
```

- Prometheus는 Project EC2의 private IP를 통해 node_exporter(9100),
  cAdvisor(8081), Serving API 애플리케이션 metrics(9101), Kafka Exporter
  metrics(9308), StatsD Exporter를 통한 Airflow metrics(9102)를 scrape한다.
- Stream Processor(Spark Structured Streaming) metrics(9103), Spark Streaming
  EC2 자신의 host 메트릭(node_exporter, 9100), 그 위에서 뜨는 컨테이너별
  CPU/Memory(cAdvisor, 8081)는 전용 Spark Streaming EC2의 private IP에서 별도로
  scrape한다.
- Monitoring EC2 자신의 host 메트릭(node_exporter)과 그 위에서 뜨는 컨테이너별
  CPU/Memory(`monitoring-cadvisor`, `infra/compose/monitoring.yaml`)도 수집한다.
  Prometheus/Grafana와 같은 EC2/Docker 네트워크에서 뜨므로 private IP나
  `extra_hosts` 없이 Docker DNS(`monitoring-node-exporter:9100`,
  `monitoring-cadvisor:8080`)로 바로 scrape한다.
- `blackbox-exporter`는 Grafana(`/api/health`)/Prometheus(`/-/ready`)/Ops
  Agent(`/health`)를 외부에서 HTTP로 probe해 Prometheus에 저장한다 — 세 서비스
  모두 자신의 죽음을 스스로 보고할 방법이 마땅치 않아(특히 Grafana는 Alerting
  엔진 자체가 자기 프로세스 안에서 돎) 외부에서 관찰하는 방식을 택했다. 자세한
  내용은 아래 [Monitoring self-health](#monitoring-self-health) 참고.
- EMR Serverless는 Project EC2와 별개로 AWS가 관리하는 서비스라 scrape 대상이
  아니다. Grafana가 CloudWatch datasource로 CloudWatch API를 직접 조회한다.
  자세한 내용은 아래 [EMR Serverless dashboard 지표](#emr-serverless-dashboard-지표)
  참고.
- Serving API는 요청을 처리하는 API port(8000)와 metrics를 노출하는
  port(9101)를 분리한다 — `/metrics`가 공개 API 표면에 섞이지 않는다.
- Kafka Exporter는 Kafka broker(9092) 자체를 외부에 새로 공개하지 않고,
  Project EC2 안에서만 broker에 붙어 metrics를 9308로 노출한다. Monitoring
  EC2는 9308만 스크랩하면 되고 9092에는 접근하지 않는다.
- Airflow(scheduler/dag-processor/api-server)는 `/metrics` endpoint를 직접
  만들지 않는다. 대신 StatsD로 `airflow-statsd-exporter`(같은 `de4-local`
  Docker network, UDP 9125)에 metric을 보내고, exporter가 이를 Prometheus
  형식으로 변환해 9102에서 노출한다. 자세한 내용은 아래
  [Airflow dashboard 지표](#airflow-dashboard-지표) 참고.
- Grafana는 같은 Docker network에서 `http://prometheus:9090`으로 Prometheus에
  접근한다. Datasource는 [grafana/provisioning/datasources/prometheus.yml](grafana/provisioning/datasources/prometheus.yml)로
  자동 등록되고, `System Overview`, `Project Infrastructure`, `Serving API`,
  `Kafka`, `Airflow` dashboard도 provisioning으로 자동 생성된다. 자세한 내용은
  아래 [Grafana Dashboard](#grafana-dashboard) 참고.
- `prometheus.yml`에는 실제 AWS private IP를 하드코딩하지 않는다. 대신
  `PROJECT_EC2_PRIVATE_IP`와 `SPARK_EC2_PRIVATE_IP`를 compose의
  `extra_hosts`로 넘겨 Prometheus 컨테이너 안에서 각각 `project-ec2`와
  `spark-ec2` hostname으로 매핑한다. Monitoring EC2 자신은 같은 Docker network
  안이라 이 매핑조차 필요 없다(위 참고).
- Prometheus job 이름은 역할별로 구분한다 — `project-node`/`spark-node`/
  `monitoring-node`(각 EC2의 node_exporter), `project-containers`/
  `spark-containers`/`monitoring-containers`(각 EC2의 cAdvisor, 컨테이너별
  CPU/Memory). EC2 3대 공통 alert(`infrastructure` rule group, 아래
  [Infrastructure Alert](#infrastructure-alert) 참고)는 host 3개 job을 정규식
  (`project-node|spark-node|monitoring-node`)으로 한 번에 평가한다. System
  Overview의 Container CPU/Memory Usage 패널도 같은 방식으로 컨테이너 job
  3개를 정규식으로 합친다.

## AWS prerequisite

Project EC2, Spark Streaming EC2, Monitoring EC2는 같은 VPC에서 private IP로
통신 가능해야 한다.
Security Group 생성/변경은 이번 작업 범위가 아니므로, 아래 규칙만 별도로 설정해 둔다.
**metrics 포트는 0.0.0.0/0으로 열지 않고 상대 Security Group만 허용한다.**

Project EC2 Security Group (inbound):

| Port | Protocol | Source |
| --- | --- | --- |
| 9100 | TCP | Monitoring EC2 Security Group |
| 8081 | TCP | Monitoring EC2 Security Group |
| 9101 | TCP | Monitoring EC2 Security Group |
| 9308 | TCP | Monitoring EC2 Security Group |
| 9102 | TCP | Monitoring EC2 Security Group |

Spark Streaming EC2 Security Group (inbound):

| Port | Protocol | Source |
| --- | --- | --- |
| 9100 | TCP | Monitoring EC2 Security Group |
| 8081 | TCP | Monitoring EC2 Security Group |
| 9103 | TCP | Monitoring EC2 Security Group |

Kafka broker(9092)는 Monitoring EC2에 새로 공개할 필요가 없다 — Kafka
Exporter가 Project EC2 안에서 `localhost:9092`로 붙어 9308로 metrics를
대신 노출한다. StatsD UDP 9125도 마찬가지로 Docker network(`de4-local`)
내부 통신에만 쓰이므로 Security Group에 열 필요가 없다.

### EMR Serverless / CloudWatch datasource 사전 준비

CloudWatch datasource는 `authType: default`를 쓴다 — Access Key/Secret Key를
`cloudwatch.yml`이나 `.env`에 넣지 않고, Monitoring EC2에 IAM Role(instance
profile)을 붙여 자격증명을 받는다. 이 Role에는 최소한 다음 권한이 필요하다
(EMR Serverless CloudWatch metric 조회 전용이면 `cloudwatch:GetMetricData`,
`cloudwatch:ListMetrics`, `cloudwatch:GetMetricStatistics`로 충분하다).

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "cloudwatch:GetMetricData",
        "cloudwatch:GetMetricStatistics",
        "cloudwatch:ListMetrics"
      ],
      "Resource": "*"
    }
  ]
}
```

이 Role/instance profile 생성은 이번 작업 범위에서 실제로 적용하지 않았다
(Terraform에 Monitoring EC2 리소스 자체가 없다) — Monitoring EC2를 실제
운영할 때 별도로 준비해야 한다.

Monitoring EC2 Security Group (inbound):

| Port | Protocol | Source |
| --- | --- | --- |
| 3000 | TCP | 관리자/팀원 IP |
| 22 | TCP | 0.0.0.0/0 |

22번이 열려 있는 것은 [자동 배포](#자동-배포-cd)가 GitHub 러너에서 SSH로 접속하기
때문이다. 러너는 실행마다 IP가 다르고 GitHub이 공개하는 Actions IP 대역은 수천 개라
Security Group 규칙 수 한도에 걸려 특정 IP만 허용할 수 없다. 비밀번호 인증은 꺼져
있고 키 인증만 허용된다. 노출을 줄이려면 배포할 때만 러너 IP를 규칙에 추가하고 끝나면
지우는 방식이 필요한데, 아직 구현되어 있지 않다.

Project EC2의 22번도 같은 이유로 열려 있다.

Prometheus의 9090은 compose에서 `127.0.0.1:9090:9090`으로 바인딩해 Monitoring
EC2 localhost에서만 접근 가능하므로, Security Group에 별도 inbound 규칙을 열
필요가 없다.

## Project EC2 실행 방법

리포지토리를 clone/pull한 뒤 실행한다.

```bash
docker compose \
  -f infra/compose/exporters.yaml \
  up -d
```

상태 확인:

```bash
docker compose \
  -f infra/compose/exporters.yaml \
  ps
```

node_exporter 확인:

```bash
curl http://localhost:9100/metrics
```

cAdvisor 확인:

```bash
curl http://localhost:8081/metrics
```

Serving API metrics 확인(Serving API 컨테이너가 이 EC2에서 같이 떠 있을 때):

```bash
curl http://localhost:9101/metrics
```

### cAdvisor에 privileged/device 설정을 쓰는 이유

cAdvisor는 컨테이너별 CPU/memory/network 사용량을 읽기 위해 host의 cgroup,
Docker 내부 상태, 커널 메시지 버퍼(`/dev/kmsg`)에 접근해야 한다. 이는 [cAdvisor
공식 문서가 권장하는 실행 방식](https://github.com/google/cadvisor/blob/master/docs/running.md)이며,
특히 Amazon Linux 2023의 cgroup v2 환경에서는 `privileged: true`와
`/dev/kmsg` device 마운트 없이는 일부 cgroup 정보를 읽지 못하는 경우가 있다.
그 외 권한은 추가하지 않았다.

### Kafka Exporter

Kafka와 같은 `infra/compose/kafka.yaml`에서 함께 뜬다.

```bash
docker compose -f infra/compose/kafka.yaml up -d
curl http://localhost:9308/metrics
```

`kafka-exporter`는 `network_mode: host`로 띄운다 — 기본 브리지 네트워크에서는
이 컨테이너 안의 `localhost`가 자기 자신을 가리켜 같은 EC2의 `kafka`
컨테이너에 붙지 못한다. sensor-producer([issue #316](https://github.com/softeerbootcamp-8th/DE_team4-4una/issues/316))와
stream-processor([issue #323](https://github.com/softeerbootcamp-8th/DE_team4-4una/issues/323))가 겪은 것과 같은
문제라 이번에도 `host network + localhost:9092`로 맞췄다(Docker Compose
network에서 `kafka:9092`로 접근하는 방식은 검토했지만 같은 이유로 채택하지
않았다). 그래서 `--kafka.server=localhost:9092`를 쓰고, `9092:9092`처럼 별도
port publish도 필요 없다(host network가 이미 host의 모든 포트를 그대로 쓴다).

### Airflow / StatsD Exporter

Airflow와 같은 `infra/compose/airflow.yaml`에서 `airflow-statsd-exporter`가
함께 뜬다. Kafka와 달리 여기서는 host network를 쓰지 않는다 — Airflow
컴포넌트와 exporter가 원래 같은 compose network(`de4-local`)에 있고, StatsD는
컨테이너가 UDP로 값을 "보내기만" 하는 쪽이라 Kafka client처럼 broker가
알려주는 주소로 다시 접속할 필요가 없기 때문이다(그래서 `localhost` 문제
자체가 발생하지 않는다). Airflow 컴포넌트는
`AIRFLOW__METRICS__STATSD_HOST=airflow-statsd-exporter`로 서비스 이름을
그대로 쓴다.

```bash
docker compose -f infra/compose/airflow.yaml up -d
curl http://localhost:9102/metrics
```

`airflow-statsd-exporter`는 `prom/statsd-exporter:v0.30.0-distroless`를 쓴다
— `v0.30.0`은 plain(non-distroless) 태그가 Docker Hub에 없어서(직접 확인)
같은 버전의 distroless 이미지를 대신 썼다. `command:`가 exec 형태라 shell이
없는 distroless에서도 문제없이 동작하고, linux/arm64도 지원한다(Docker Hub
manifest 확인).

Airflow가 DogStatsD tagged 형식(`AIRFLOW__METRICS__STATSD_DATADOG_ENABLED=True`)을
쓰도록 설정했다 — dag_id/task_id가 metric 이름에 박히는 legacy 방식 대신
label로 남아야 dashboard에서 DAG/task별로 집계할 수 있기 때문이다. 이 경로는
`apache-airflow[statsd]`(패키지 `statsd`)가 아니라 별도 `datadog` 패키지
(`from datadog import DogStatsd`)를 요구한다 — Airflow 소스
(`airflow_shared.observability.metrics.datadog_logger`)로 직접 확인했다.
공식 이미지에는 없어서 `_PIP_ADDITIONAL_REQUIREMENTS`로 설치한다(로컬 개발
전용 방식, `airflow.yaml`의 기존 주석 참고 — 매 기동마다 pip install이 돈다).
STATSD_DATADOG_ENABLED가 공통 env(모든 Airflow 컴포넌트 공유)에 있어서,
`datadog` 패키지도 scheduler뿐 아니라 dag-processor/api-server/`airflow-init`
전부에 설치되게 했다 — 빠뜨리면 그 컴포넌트가 기동 시 `ModuleNotFoundError`로
죽을 위험이 있다.

## Spark Streaming EC2 확인 방법

node_exporter/cAdvisor 실행(host 메트릭 + 컨테이너별 CPU/Memory,
`infra/compose/spark-exporters.yaml`):

```bash
docker compose -f infra/compose/spark-exporters.yaml up -d
curl http://localhost:9100/metrics
curl http://localhost:8081/metrics
```

Stream Processor metrics 확인:

```bash
curl http://localhost:9103/metrics
```

## Monitoring EC2 실행 방법

먼저 `.env`를 준비한다.

```bash
cp infra/monitoring/.env.example infra/monitoring/.env
```

`infra/monitoring/.env`에 실제 값을 채운다.

```env
PROJECT_EC2_PRIVATE_IP=<Project EC2 private IPv4>
SPARK_EC2_PRIVATE_IP=<Spark Streaming EC2 private IPv4>
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=<strong password>
```

`.env`는 `.gitignore`에 의해 커밋되지 않는다(`.env.example`만 허용).

compose 실행:

```bash
docker compose \
  --env-file infra/monitoring/.env \
  -f infra/compose/monitoring.yaml \
  up -d
```

상태 확인:

```bash
docker compose \
  --env-file infra/monitoring/.env \
  -f infra/compose/monitoring.yaml \
  ps
```

## 자동 배포 (CD)

`develop`에 push되면 [.github/workflows/deploy-monitoring.yml](../../.github/workflows/deploy-monitoring.yml)이
설정을 두 EC2에 반영한다. 위 수동 실행 절차는 최초 준비와 문제 확인용으로 남겨둔다.

바뀐 경로에 따라 필요한 호스트만 배포한다.

| 바뀐 경로 | 배포 대상 |
| --- | --- |
| `infra/compose/exporters.yaml` | Project EC2 |
| `infra/compose/spark-exporters.yaml` | Spark Streaming EC2 |
| `infra/compose/monitoring.yaml` | Monitoring EC2 |
| `infra/monitoring/prometheus/**` | Monitoring EC2 |
| `infra/monitoring/grafana/**` | Monitoring EC2 |
| `infra/monitoring/blackbox/**` | Monitoring EC2 |
| `infra/monitoring/statsd/**` | Project EC2 — orchestration 배포가 담당 |

빌드하는 이미지가 없다(ops-agent 제외). 모두 서드파티 이미지를 그대로 쓰므로 ECR과
AWS 자격증명을 사용하지 않고, 설정 파일을 전달한 뒤 compose로 반영하는 것이 전부다.

배포 대상 디렉터리는 `MONITORING_TARGET_DIR`(기본
`/home/ec2-user/de4-monitoring`)이다. `deploy-orchestration`이 저장소 전체를
`--delete`로 미는 경로와 겹치지 않도록 별도 디렉터리를 쓴다.

### GitHub 설정

| 종류 | 이름 | 비고 |
| --- | --- | --- |
| Variables | `MONITORING_EC2_HOST` | Monitoring EC2의 퍼블릭 IP 또는 DNS |
| Secrets | `MONITORING_EC2_SSH_PRIVATE_KEY` | Monitoring EC2 키페어의 개인키 전문 |
| Variables | `SPARK_EC2_HOST` | Spark Streaming EC2의 퍼블릭 IP 또는 DNS(exporter 배포용) |
| Secrets | `SPARK_EC2_SSH_PRIVATE_KEY` | Spark Streaming EC2 키페어의 개인키 전문 |

Project EC2와 키페어가 다르므로 secret을 따로 둔다. `EC2_SSH_PRIVATE_KEY`로 대체하면
`Permission denied (publickey)`만 나와 원인을 찾기 어렵다.

`SPARK_EC2_HOST`/`SPARK_EC2_SSH_PRIVATE_KEY`를 `EC2_HOST`/`EC2_SSH_PRIVATE_KEY`와
별도로 둔 이유: `deploy-stream-processor.yml`은 Spark Streaming EC2 배포에 Project
EC2와 같은 `EC2_HOST`/`EC2_SSH_PRIVATE_KEY`를 쓰고 있는데,
`context/architecture.md`는 둘을 별도 인스턴스로 설명한다 — 이 불일치는 아직
해결되지 않았다(`services/ops-agent/README.md`가 이미 문서화한 known issue). 실제로
같은 EC2로 확인되면 `SPARK_EC2_HOST`/`SPARK_EC2_SSH_PRIVATE_KEY` 값을
`EC2_HOST`/`EC2_SSH_PRIVATE_KEY`와 동일하게 채우면 되고, 그렇지 않다면 별도 값을
채운다 — 어느 쪽이든 이 워크플로 코드는 바꿀 필요가 없다.

선택 항목은 `MONITORING_EC2_USER`(기본 `ec2-user`), `SPARK_EC2_USER`(기본
`ec2-user`), `MONITORING_TARGET_DIR`이다.

### 최초 1회 준비

**1. `.env` 생성.** 배포는 이 파일을 만들지 않는다. 비밀값이 들어가 저장소에 없기
때문이다. 배포 대상 디렉터리에 직접 만든다.

```bash
mkdir -p /home/ec2-user/de4-monitoring/infra/monitoring
vi /home/ec2-user/de4-monitoring/infra/monitoring/.env
```

`.env.example`을 참고해 `PROJECT_EC2_PRIVATE_IP`와 `GRAFANA_ADMIN_PASSWORD`를
채운다. 없으면 배포가 `Check env file on instance` 스텝에서 중단한다.

rsync는 `.env`를 제외하므로 이후 배포가 이 파일을 덮어쓰거나 지우지 않는다.

**2. 기존 수동 컨테이너 정리.** 다른 디렉터리에서 compose를 띄웠다면 프로젝트가 달라
같은 `container_name`을 다시 만들려다 `container name already in use`로 실패한다.

```bash
# Monitoring EC2
docker rm -f prometheus grafana

# Project EC2
docker rm -f node-exporter cadvisor
```

### 배포가 하는 일

Monitoring EC2:

1. `infra/compose/monitoring.yaml`과 `infra/monitoring/`을 전송 (`.env`, `statsd/` 제외)
2. `.env` 존재 확인
3. `docker compose ... up -d`
4. `docker compose ... restart prometheus grafana`
5. `/-/ready`와 `/api/health`로 기동 확인 (최대 90초)

4단계가 필요한 이유는 `prometheus.yml`과 Grafana datasource provisioning이 볼륨으로
마운트된 파일이기 때문이다. 컨테이너 정의가 아니어서 내용만 바뀌면 `up -d`가 아무
일도 하지 않고, 두 프로세스는 기동 시점에만 설정을 읽는다. 재기동하지 않으면 파일만
새 것이고 동작은 예전 설정 그대로다.

Grafana dashboard JSON은 예외다 — `updateIntervalSeconds: 30`으로 재기동 없이 반영된다.

Project EC2:

1. `infra/compose/exporters.yaml` 전송
2. `docker compose ... up -d`
3. `:9100/metrics`, `:8081/metrics`로 기동 확인 (최대 60초)

`exporters.yaml`은 마운트된 설정 파일이 없어 파일이 바뀌면 컨테이너 정의가 바뀌므로
`up -d`가 재생성한다. 별도 재기동이 필요 없다.

### 실패했을 때

워크플로 로그에 인스턴스가 남긴 출력이 그대로 찍힌다. health 실패 시에는 해당 컨테이너
로그도 함께 출력된다.

| 증상 | 원인 |
| --- | --- |
| `필수 설정값이 비어 있습니다` | variables 또는 secret 미설정 |
| SSH 연결 시간 초과 | 22번 인바운드, 또는 퍼블릭 주소 없음 |
| `Permission denied (publickey)` | 키페어 불일치, 또는 `MONITORING_EC2_USER` 틀림 |
| `.env 가 없습니다` | 위 최초 1회 준비 1번 |
| `container name already in use` | 위 최초 1회 준비 2번 |
| health 실패 | 함께 출력되는 컨테이너 로그를 본다 |

## Validation

Prometheus readiness 확인 (Monitoring EC2 안에서):

```bash
curl http://localhost:9090/-/ready
```

Grafana health 확인:

```bash
curl http://localhost:3000/api/health
```

Prometheus Targets(`http://localhost:9090/targets`)에서 다음 job이 모두
`UP`이어야 한다.

- `prometheus`
- `project-node`
- `project-containers`
- `spark-node`
- `spark-containers`
- `monitoring-node`
- `monitoring-containers`
- `serving-api`
- `kafka`
- `airflow`
- `stream-processor`
- `blackbox-self-health`(3개 target 모두 `UP` — target 자체는 항상 UP이다.
  Grafana/Prometheus/Ops Agent가 실제로 정상인지는 `UP` 여부가 아니라 target
  값(`probe_success`)이 1인지로 판단한다 — 아래 [Monitoring
  self-health](#monitoring-self-health) 참고)

Monitoring EC2에서 각 애플리케이션 metrics endpoint에 직접 접근되는지
확인하려면(문제가 생겼을 때만 필요):

```bash
curl http://<PROJECT_EC2_PRIVATE_IP>:9101/metrics
curl http://<PROJECT_EC2_PRIVATE_IP>:9308/metrics
curl http://<PROJECT_EC2_PRIVATE_IP>:9102/metrics
curl http://<SPARK_EC2_PRIVATE_IP>:9103/metrics
```

StatsD Exporter가 Airflow metric을 실제로 받고 있는지는 9102 응답 안에서
확인한다. `statsd_exporter_*`(자기 자신의 internal metric)는 exporter가
살아있기만 하면 항상 보이므로, Airflow가 실제로 값을 보내는지는
`airflow_`로 시작하는 metric이 있는지로 판단한다.

```bash
curl -s http://localhost:9102/metrics | grep '^airflow_'
```

9090은 `127.0.0.1`에만 bind되어 있으므로, Monitoring EC2 밖에서 Prometheus UI를
확인해야 한다면 SSH port forwarding을 사용한다.

```bash
ssh -L 9090:localhost:9090 <user>@<MONITORING_EC2_PUBLIC_IP>
```

이후 로컬 브라우저에서 `http://localhost:9090`으로 접근한다.

Grafana는 3000 포트를 그대로 공개하므로 브라우저에서 바로 접근한다.

```text
http://<MONITORING_EC2_PUBLIC_IP>:3000
```

## Alert rule

`grafana/provisioning/alerting/`의 세 파일이 alert를 정의한다. 파일 기반 프로비저닝이라
Grafana UI에서 수정할 수 없다 — 변경은 이 파일들을 고쳐 배포한다.

| 파일 | 내용 |
| --- | --- |
| `rules.yaml` | alert 룰 (`spark-streaming`, `infrastructure` 그룹) |
| `contact-points.yaml` | `ops-agent`(webhook+Slack), `infra-slack`(Slack 전용) |
| `notification-policies.yaml` | 기본 `ops-agent`, `service=infrastructure`는 `infra-slack` |

Spark Streaming의 Bronze 적재는 세 종류로 나눠 감지한다.

| Alert | 조건 | Severity | 자동 조치 |
| --- | --- | --- | --- |
| `StreamProcessorDown` | Prometheus target 또는 Spark query가 중단된 상태가 1분 지속 | CRITICAL | 기존 Ops Agent 재검증·재시작 대상 |
| `BronzeIngestionStalled` | Kafka 입력이 있는데 마지막 성공 progress가 90초 넘게 없음 | WARNING | 없음 |
| `BronzeIngestionLagGrowing` | Kafka 입력 중 event-time lag가 90초를 넘거나 2분 동안 live offset lag가 증가 | WARNING | 없음 |

`BronzeIngestionStalled`은 Kafka message rate가 0이면 시계열을 반환하지 않고
`noDataState=OK`로 처리한다. 따라서 차량 입력이 없는 정상 상태를 적재 장애로 오인하지
않는다. 여기서 마지막 progress는 sink commit까지 끝난 `QueryProgressEvent`이므로 S3
적재 성공을 나타내는 proxy로 사용한다.

Stream Processor가 보고하는 `stream_processor_kafka_offset_lag`는 micro-batch 종료
시에만 갱신돼 query가 멈추면 값도 멈춘다. 그래서 lag 증가 alert는 Spark가 마지막으로
commit한 end offset 합(`stream_processor_kafka_end_offset_sum`)을 Kafka Exporter의
현재 topic offset 합에서 빼 live backlog를 계산한다.

두 WARNING은 `auto_remediate` 라벨이 없어 컨테이너를 재시작하지 않는다. 기존
`ops-agent` contact point를 통해 Slack에 summary, query/lag 현재 값과 Spark Streaming
dashboard 링크를 보내 사람이 원인을 확인한다.

90초 기준은 `STREAM_MAX_TRIGGER_DELAY`를 30초로 줄이는 #557과 함께 배포하는 것을
전제로 한다. 현재 기본값 5분을 유지한 채 이 규칙만 먼저 배포하면 입력량이
`STREAM_MIN_OFFSETS_PER_TRIGGER`에 도달하지 않은 정상 대기 중에도 경고할 수 있으므로,
#557 배포 전에는 이 규칙을 운영에 적용하지 않는다.

`infrastructure` 그룹은 원래 `ProjectDiskPressure`(#504) 하나였다. 이번에 EC2 3대
공통 host alert와 monitoring self-health alert를 이 그룹에 합치면서
`ProjectDiskPressure`는 지웠다 — 아래 [Infrastructure Alert](#infrastructure-alert)의
`DiskWarning`이 정확히 같은 조건(job=project-node, mountpoint="/", >80%, for 10m)을
포함하는 상위 호환이라, 그대로 두면 Project EC2 disk가 80%를 넘을 때 두 alert가
동시에 울리기 때문이다. 자세한 rule 목록/threshold는 아래
[Infrastructure Alert](#infrastructure-alert) 참고.

발화 시 확인 순서(디스크 alert 기준)는 다음과 같다.

```bash
df -h /
docker system df                     # 이미지/빌드 캐시
docker exec de4-kafka-kafka-1 du -sh /var/lib/kafka/data
du -sh ~/DE_team4-4una/logs 2>/dev/null   # Airflow 로그
```

`for: 10m` + 80% 조건은 평상시(약 50%) 발화하지 않는다. 실제 Slack 수신을 확인하려면
`rules.yaml`의 해당 rule의 `params: [80]`을 현재 사용률 아래로 잠시 낮춰 배포하고,
확인 후 원복한다. Grafana UI의 Alert rules에서 룰이 `Provisioned`로 보이는지도
함께 확인한다.

### CI에서의 자동 검증

`infra/monitoring/**` 또는 `infra/compose/{monitoring,exporters,spark-exporters}.yaml`이
바뀐 PR에서는 `.github/workflows/ci.yml`의 `monitoring-config` job이 아래를
순서대로 검사한다 — 실제 EC2 없이 GitHub Actions 러너 안에서 전부 재현한다.

1. `docker compose config`로 세 compose 파일 문법/필수 변수 검사(placeholder 값 사용)
2. `promtool check config`로 `prometheus.yml` 문법 검사
3. provisioning YAML/dashboard JSON 파싱 + provider가 가리키는 마운트 경로 검사
4. **placeholder 값으로 Prometheus/Grafana를 실제로 띄우고 `/-/ready`,
   `/api/health`가 응답할 때까지 대기**(최근 datasource provisioning 오류로
   Grafana가 기동 실패한 적이 있어 추가함 — 위 1~3번은 문법/정합성만 보고
   기동 자체가 되는지는 못 잡는다) — ops-agent는 저장소 전체를 build context로
   직접 빌드해야 해서 비용이 크고 이 검증과 무관하므로 제외한다(`depends_on`은
   시작 순서일 뿐이라 서비스 이름을 지정하면 그것만 뜬다)
5. Grafana 로그에서 `logger=provisioning`이면서 `level=error`인 줄이 있는지 검사
   (`/api/health`가 200이어도 개별 alert rule 등 일부 provisioning 실패는 로그에만
   남을 수 있어 별도로 본다)
6. `docker compose down -v`로 정리(CI 러너 안의 임시 컨테이너/볼륨만 삭제 — 운영
   Grafana volume과 무관)

## Grafana Dashboard

Grafana가 기동되면 별도 UI 설정 없이 `System Overview`, `Project Infrastructure`,
`Serving API`, `Kafka`, `Airflow`, `Spark Streaming`, `EMR Serverless`, `Service
Status Overview` dashboard가 자동으로 provisioning된다. 여덟 dashboard 모두 같은
provider(`Infrastructure` 폴더, `/var/lib/grafana/dashboards`)가 디렉터리
전체를 읽어서 등록하므로, dashboard를 추가할 때 provider(`dashboards.yml`)를
새로 만들 필요는 없다 — JSON 파일만 그 디렉터리에 추가하면 된다. Datasource는
Prometheus(스크랩 기반)와 CloudWatch(AWS API 직접 조회) 두 개가
[grafana/provisioning/datasources/](grafana/provisioning/datasources/)로
자동 등록된다.

- 접속: `http://<MONITORING_EC2_PUBLIC_IP>:3000`
- 위치: Grafana 좌측 메뉴 **Dashboards → Infrastructure → System Overview** /
  **Project Infrastructure** / **Serving API** / **Kafka** / **Airflow** /
  **Spark Streaming** / **EMR Serverless**
- 구성 파일:
  - `infra/monitoring/grafana/provisioning/dashboards/dashboards.yml` — dashboard
    provider 정의(`Infrastructure` 폴더, `/var/lib/grafana/dashboards`를
    파일 기반으로 읽음)
  - `infra/monitoring/grafana/provisioning/datasources/prometheus.yml` — Prometheus
    datasource 정의
  - `infra/monitoring/grafana/provisioning/datasources/cloudwatch.yml` — CloudWatch
    datasource 정의(`authType: default`로 EC2 IAM Role만 사용, Access/Secret Key
    없음)
  - `infra/monitoring/grafana/dashboards/system-overview.json` — EC2 3대(Project/
    Spark Streaming/Monitoring)와 Kafka/Airflow/Spark Streaming/Serving API/
    EMR Serverless/Prometheus/Grafana/Ops Agent 상태를 한 화면에서 보는
    dashboard 본문. 자세한 내용은 아래
    [System Overview dashboard](#system-overview-dashboard) 참고.
  - `infra/monitoring/grafana/dashboards/project-infrastructure.json` — Project
    EC2 host/container dashboard 본문(패널, PromQL, 임계값). CPU/Memory Usage
    over Time과 짝을 맞춰 `Disk Usage over Time` 그래프도 있다 — Disk %
    stat 패널과 같은 query를 시간축으로 보여줘, DiskWarning(80%)/
    DiskCritical(90%) alert가 왜 울렸는지(또는 서서히 가까워지는지) 추세로
    미리 볼 수 있다.
  - `infra/monitoring/grafana/dashboards/serving-api.json` — Serving API
    dashboard 본문
  - `infra/monitoring/grafana/dashboards/kafka.json` — Kafka dashboard 본문
  - `infra/monitoring/grafana/dashboards/airflow.json` — Airflow dashboard 본문
  - `infra/monitoring/grafana/dashboards/spark-streaming.json` — Stream Processor
    (Spark Streaming EC2에서 Prometheus로 scrape) dashboard 본문
  - `infra/monitoring/grafana/dashboards/emr-serverless.json` — EMR Serverless
    (CloudWatch datasource) dashboard 본문
  - `infra/monitoring/statsd/airflow-mapping.yml` — Airflow timer metric
    3개를 Prometheus histogram으로 바꾸는 statsd_exporter 매핑 설정
  - `infra/monitoring/blackbox/blackbox.yml` — Grafana/Prometheus/Ops Agent
    self-health probe에 쓰는 blackbox_exporter 모듈 설정

### System Overview dashboard

`infra/monitoring/grafana/dashboards/system-overview.json`. "지금 어디가
문제인지 한눈에 보는 화면"이 목적이라 상세 분석용 패널까지 전부 옮기지는
않는다 — 대신 아래 **Trends** row에 서비스별로 가장 핵심적인 그래프 하나씩만
가져와 상태(Service Status)뿐 아니라 추세도 한 화면에서 볼 수 있게 했다. 더
자세히 보려면 각 패널의 링크를 눌러 해당 dashboard(Kafka/Airflow/Spark
Streaming/Serving API/Project Infrastructure)로 이동해 확인한다.

- **Active Alerts**: Grafana 내장 `alertlist` 패널로, 지금 firing/pending인
  alert를 전부 목록으로 보여준다. Overall System Health가 DEGRADED/UNHEALTHY일
  때 "그래서 정확히 뭐가 문제인지"를 바로 이어서 확인할 수 있다 — 요약값
  하나만으로는 원인을 알 수 없다는 피드백을 반영해 추가했다. 이 dashboard
  전용 alert가 아니라 이 Grafana 인스턴스의 모든 alert rule(`spark-streaming`/
  `infrastructure` 그룹)이 대상이다.
- **Host Trends**: EC2 3대의 CPU/Memory/Disk 사용률을 시간 축으로 보여준다.
  위 EC2 Instances의 Gauge는 "지금 값"만 보여주는데, 이 그래프는 "서서히
  차오르는 중인지, 순간 스파이크였는지"를 구분하는 데 쓴다 — 특히 Disk는
  DiskWarning/DiskCritical alert가 왜 울렸는지(또는 곧 울릴지) 미리 보는
  용도로 유용하다. PromQL은 project-infrastructure.json의 CPU Usage over Time
  패널과 동일한 공식을 job 정규식으로 3대 EC2에 확장한 것이다.
- **Overall System Health**: EC2 3대(Project/Spark Streaming/Monitoring)와
  Kafka/Airflow/Spark Streaming/Serving API 4개 서비스를 하나로 합친
  HEALTHY/DEGRADED/UNHEALTHY/UNKNOWN 4단계 상태다. 판정 로직은 아래 [Overall
  System Health 4단계화](#overall-system-health-4단계화) 참고(우선순위:
  UNHEALTHY > UNKNOWN > DEGRADED > HEALTHY).
- **EC2 Instances**: Project/Spark Streaming/Monitoring EC2별로 Status(UP/
  DOWN), CPU/Memory/Disk Gauge, Load, Uptime, Network RX/TX를 보여준다.
  CPU/Memory/Disk의 PromQL과 threshold(70/90, 70/90, 80/90)는
  `project-infrastructure.json`의 CPU %/Memory %/Disk % 패널과 완전히 같다 —
  `job` label만 `project-node`/`spark-node`/`monitoring-node`로 바꿨다.
- **Service Status**: Kafka/Airflow/Spark Streaming/Serving API는 각 상세
  dashboard의 Component Status 패널과 완전히 같은 query/mapping을 재사용한다
  (판정 기준 일치). EMR Serverless는 `emr-serverless.json`의 Component Status와
  완전히 같은 CloudWatch direct
  query(`RunningJobs`, `matchExact: true`, #410)를 재사용해 IDLE/RUNNING/NO
  METRIC DATA로 보여준다 — RunningJobs=0은 정상 idle이라 DOWN으로 취급하지
  않는다. `application_id`/`application_name` 변수는 이 프로젝트가 쓰는 실제
  값(`00g85ljahc0svj2p`/`de4-batch-jobs`)을 기본값으로 채워 뒀다(다른
  application을 보려면 대시보드 상단에서 값을 바꾼다). Prometheus/Grafana/Ops
  Agent는 blackbox_exporter가 probe한 `probe_success`를 UP/DOWN으로 보여준다 —
  자세한 내용과 한계는 아래 [Monitoring self-health](#monitoring-self-health)
  참고.
- **Overall System Health 판정에는 EMR Serverless를 포함하지 않는다** — Prometheus
  기반 컴포넌트(A~G)와 CloudWatch 기반 EMR을 하나의 Grafana expression에서
  결합하는 게 현재 버전(12.4.8)에서 동작이 보장된다고 확신할 수 없고,
  RunningJobs=0(정상 idle)을 장애 신호로 잘못 섞을 위험도 있다. EMR 상태는
  위 Service Status의 독립된 카드로만 확인한다.
- **Trends**: 서비스별 대표 그래프 하나씩 — Kafka Message Rate over Time
  (`kafka.json`과 동일 query), Spark Streaming Rows/sec over Time
  (`spark-streaming.json`의 Rows/sec over Time과 동일), Airflow Task
  Success/Failure Rate(`airflow.json`의 Task Success/Failure Rate over Time과
  동일), Serving API Requests/sec(`serving-api.json`의 Requests/sec stat 패널과
  같은 query를 시간축 그래프로).
  나머지 그래프(latency percentile, DAG별/토픽별 breakdown 등)는 이 dashboard에
  옮기지 않았다 — 전부 옮기면 개별 dashboard를 그대로 복제하는 셈이라, "한눈에
  보는 화면"이라는 목적과 어긋난다.
- **Container Resource Usage**: EC2 3대에서 뜨는 모든 컨테이너의 CPU/Memory를
  두 그래프(Container CPU Usage, Container Memory Usage)로 보여준다.
  `project-infrastructure.json`의 같은 이름 패널과 동일한 query를
  `job=~"project-containers|spark-containers|monitoring-containers"`로 3대
  EC2에 확장했다 — Spark Streaming/Monitoring EC2에는 원래 cAdvisor가 없어서
  이번에 `infra/compose/spark-exporters.yaml`/`infra/compose/monitoring.yaml`에
  추가했다(각각 `cadvisor`/`monitoring-cadvisor` 서비스, Project EC2의 기존
  cAdvisor 설정을 그대로 재사용). legend에 `instance`(어느 EC2인지)와 컨테이너
  이름이 함께 나와 어느 서비스가 자원을 많이 쓰는지 바로 구분된다.

### Overall System Health 4단계화

이 요약 패널은 원래 HEALTHY/DEGRADED/FAILED 3단계였는데, 참조하는 원본 metric이
하나라도 아예 없으면(예: exporter 자체가 배포 전이라 Prometheus job이 없음)
Grafana의 server-side math expression이 계산 자체를 못 해 패널 전체가 큰
"No data"로 표시됐다 — 실제 장애(DOWN)와 metric 부재를 구분할 수 없는 문제였다.
그래서 `UNKNOWN`을 넷째 상태로 분리하고 `FAILED`는 `UNHEALTHY`로 이름을 바꿨다.

지금은 각 원본 query(A~G)를 `(<원래 쿼리>) or vector(99)`로 감싸 metric이 없을 때
"UNKNOWN"을 뜻하는 값 99를 강제로 채운다(PromQL의 `or`가 왼쪽에 결과가 있으면
오른쪽을 무시하는 성질을 이용) — 값이 실제로 있으면 원래 쿼리 결과가 그대로
쓰이므로 기존 판정은 바뀌지 않는다. `I`(down_count), `J`(unknown_count),
`K`(degraded_count)로 나눠 최종 값을 계산한다.

```text
UNHEALTHY(3) = down_count > 0
UNKNOWN(2)   = down_count == 0 && unknown_count > 0
DEGRADED(1)  = down_count == 0 && unknown_count == 0 && degraded_count > 0
HEALTHY(0)   = 그 외
```

A~C는 EC2 3대의 `up`, D~G는 Kafka/Airflow/Spark Streaming/Serving API의
Component Status 코드다. 서비스 4개는 자기 자신이 이미 다단계 코드를 갖고
있어서 "코드가 DOWN 계열 값 이상이면 down", "DEGRADED 계열 값이면 degraded"로
한 번 더 해석하는 단계가 있다 — absent 표시에 `vector(2)`가 아니라
`vector(99)`를 쓰는 이유가 여기 있다(Kafka의 실제 코드값 2, BROKER DOWN과
겹치지 않기 위해).

### Infrastructure Alert

Project/Spark Streaming/Monitoring EC2 3대에 공통으로 적용되는 host alert다
(`infra/monitoring/grafana/provisioning/alerting/rules.yaml`의 `infrastructure`
그룹, `Ops Agent` 폴더 — 이 그룹은 원래 `ProjectDiskPressure`(#504) 하나였고,
이번에 아래 rule들과 self-health rule을 여기에 합쳤다).

| Rule | 조건 | for | severity |
| --- | --- | --- | --- |
| `NodeDown` | `up{job=~"project-node\|spark-node\|monitoring-node"} == 0` | 2m | critical |
| `HighCPU` | CPU 사용률 > 85%(project-infrastructure.json의 CPU % 패널과 같은 공식) | 10m | warning |
| `HighMemory` | Memory 사용률 > 85% | 10m | warning |
| `DiskWarning` | Disk(root filesystem) 사용률 > 80% | 10m | warning |
| `DiskCritical` | Disk 사용률 > 90% | 5m | critical |

모든 rule에 `service: infrastructure` 라벨이 붙는다 — `ProjectDiskPressure`(#504)가
이미 정의해 둔 라벨/route를 그대로 재사용한 것이다. 이 값이
`notification-policies.yaml`에서 `infra-slack` contact point로 라우팅되는
기준이다. `auto_remediate` 라벨은 붙이지 않는다 — CPU/Memory/Disk 문제를 자동
조치(재시작/재부팅/디스크 정리 등)하지 않고 알림만 보낸다.

**`ProjectDiskPressure`를 지운 이유.** `DiskWarning`이 정확히 같은 조건(job=
project-node, mountpoint="/", >80%, for 10m)을 포함하는 상위 호환이라, 둘 다 두면
Project EC2 disk가 80%를 넘을 때 alert가 중복으로 울린다. `DiskWarning`/
`DiskCritical`로 대체하고 `ProjectDiskPressure`는 삭제했다.

**왜 ops-agent의 `ops-agent` contact point를 쓰지 않고 `infra-slack`을
쓰는가(#504가 이미 만든 설계, 그대로 재사용).** `ops-agent`의 webhook을 처리하는
`OpsAgentOrchestrator.handle()`(`services/ops-agent/src/ops_agent/orchestrator.py`)은
어떤 alert가 오든 **항상** `PrometheusClient.stream_processor_status()`로
stream-processor 상태를 재검증하도록 만들어져 있다(#447 설계, stream-processor
전용). EC2 host/self-health alert를 그 contact point로 보내면, stream-processor가
그 순간 우연히 정상이면 "이미 정상, 조치 없음"으로 조용히 넘어가 버리고(실제
진단 메시지가 안 나감), stream-processor가 우연히 비정상이면 엉뚱한 "진단:
Prometheus 상태=..." 메시지가 Slack에 나간다 — 둘 다 기존 ops-agent 동작을
깨뜨리지 않으면서 새 alert를 제대로 다루는 방법이 아니다. `infra-slack`은
webhook 없는 Slack 전용 contact point라 이 문제 자체가 생기지 않는다. 기존
`StreamProcessorDown`과 Bronze 적재 경고들은 `service: infrastructure` 라벨이
없어 그대로 default(`ops-agent`) route로 간다 — **동작 변화 없음**.

Slack Bot Token/채널은 새로 만들지 않고 기존 `$GRAFANA_SLACK_BOT_TOKEN`/
`$GRAFANA_SLACK_ALERT_CHANNEL`(Grafana 컨테이너 env, `OPS_AGENT_SLACK_BOT_TOKEN`/
`OPS_AGENT_SLACK_ALERT_CHANNEL`에서 옴)을 재사용한다 — 새 secret이 필요 없다.

threshold는 초기값이다. 특히 `DiskCritical`의 `for: 5m`(다른 규칙보다 짧음)은
디스크가 가득 차기 직전 상황에 더 빨리 반응하기 위한 의도적 선택이고,
`DiskWarning`의 `for: 10m`은 `ProjectDiskPressure`가 쓰던 값을 그대로 물려받아
다른 규칙과 cadence를 맞췄다 — 둘 다 실제 운영 데이터로 검증된 값은 아니라서,
배포 후 false positive/negative가 관찰되면 조정이 필요하다.

### Monitoring self-health

Grafana가 죽으면 Grafana Alerting도 같이 죽는다 — Alerting 엔진이 Grafana
프로세스 안에서 돌기 때문이다. Grafana 자신의 장애를 Grafana Alerting만으로
감지하는 구조는 근본적으로 순환 의존이라 성립할 수 없다.

**한 만큼만 구현했다 — 새 서비스를 억지로 만들지 않았다.**
`infra/compose/monitoring.yaml`에 `blackbox-exporter`(공식
`prom/blackbox-exporter` 이미지, `monitoring` 네트워크 내부 전용, 포트 미공개)
하나만 추가해서 Grafana(`/api/health`)/Prometheus(`/-/ready`)/Ops
Agent(`/health`) 세 endpoint를 외부에서 HTTP로 주기적으로 probe하고, 그 결과
(`probe_success{instance="..."}`)를 Prometheus에 저장한다
(`infra/monitoring/prometheus/prometheus.yml`의 `blackbox-self-health` job).
ops-agent 코드는 전혀 건드리지 않았고 새 secret도 필요 없다.

이 값으로 할 수 있는 것과 할 수 없는 것을 구분해야 한다.

- **System Overview 대시보드에서 사람이 보는 용도**로는 세 서비스 모두
  충분하다 — Prometheus/Grafana/Ops Agent 패널이 `probe_success`를 그대로
  보여준다.
- **Prometheus/Ops Agent가 죽었을 때 Grafana Alerting이 Slack으로 알림을
  보내는 것**도 가능하다(`rules.yaml`의 `infrastructure` 그룹,
  `PrometheusUnreachable`/`OpsAgentUnreachable`) — 이 두 경우는 Grafana
  자신만 살아있으면 되므로 순환 의존이 아니다. `PrometheusUnreachable`은
  `execErrState`/`noDataState`를 기본값(`Error`/`NoData`, 알림 없이 상태만
  바뀜)이 아니라 `Alerting`으로 명시해서, Prometheus가 완전히 죽어 쿼리
  자체가 실패하거나 데이터가 없는 경우도 "장애"로 취급해 알림이 나가게 했다.
- **Grafana 자신이 죽는 경우는 alert rule을 만들지 않았다** — Grafana
  Alerting이 Grafana 프로세스 안에서 돌기 때문에 이 규칙 자체가 평가되지
  않는다(순환 의존, 해결 불가능한 구조적 한계). `probe_success`는 Prometheus에
  값으로 계속 쌓이므로 **사람이 System Overview를 보면 즉시 알 수 있지만,
  자동 Slack 알림은 나가지 않는다.**

**진짜 해결하려면(이번 범위 밖, 후속 작업으로 남김):**

1. Prometheus Alertmanager(Grafana와 완전히 독립된 별도 alerting 엔진)를 새로
   추가하고, `probe_success{instance=".../api/health"}`에 native Prometheus
   alerting rule을 걸어 Alertmanager가 직접 Slack으로 알림을 보내게 한다.
   Grafana가 죽어도 이 경로는 영향받지 않는다.
2. 또는 ops-agent에 자체 폴링 루프(현재는 Grafana webhook에만 반응하는
   구조 — `services/ops-agent/README.md`의 Incident flow 참고)를 추가해서
   주기적으로 Grafana `/api/health`를 직접 확인하고 죽었으면 Slack에 직접
   알리게 한다.

둘 다 `services/ops-agent/**` 코드 변경 또는 새 인프라 서비스 추가가 필요해
이번 작업 범위(모니터링 설정만, ops-agent remediation 로직 확장 금지) 밖이라
구현하지 않았다.

### Serving API dashboard 지표

`services/serving-api/src/serving_api/metrics.py`가 노출하는 metric 이름을
그대로 PromQL에 쓴다.

| Metric | 종류 | Label |
| --- | --- | --- |
| `serving_api_http_requests_total` | Counter | `method`, `route`, `status` |
| `serving_api_http_request_duration_seconds` | Histogram | `method`, `route` |

`route`는 실제 요청 경로가 아니라 route template(예:
`/api/v1/segments/{segment_id}/comfort-scores/{vehicle_profile_id}`)이다.
매칭되는 route가 없으면 `unmatched` 고정값을 쓴다 — `segment_id` 같은 사용자
입력값이 label에 들어가 카디널리티가 늘어나지 않게 하기 위함이다.

패널별 PromQL:

| Panel | PromQL |
| --- | --- |
| Target Status | `up{job="serving-api"}` |
| Requests/sec | `sum(rate(serving_api_http_requests_total{job="serving-api"}[5m]))` |
| Request Rate over Time | `sum(rate(serving_api_http_requests_total{job="serving-api", status=~"2.."}[5m]))` (4xx/5xx는 `status=~"4.."`/`"5.."`) — 세 status 계열을 한 그래프에 겹쳐 그린다 |
| Latency over Time (p50/p95/p99) | `histogram_quantile(0.50, sum by (le) (rate(serving_api_http_request_duration_seconds_bucket{job="serving-api"}[5m])))` (0.95/0.99는 quantile 값만 교체) |
| Requests by Endpoint | `sum by (route) (rate(serving_api_http_requests_total{job="serving-api"}[5m]))` |
| Latency by Endpoint (p95) | `histogram_quantile(0.95, sum by (le, route) (rate(serving_api_http_request_duration_seconds_bucket{job="serving-api"}[5m])))` |

### Kafka dashboard 지표

[Kafka Exporter](https://github.com/danielqsj/kafka_exporter)(`danielqsj/kafka-exporter:v1.9.0`)가
노출하는 metric을 그대로 쓴다 — 존재하지 않는 metric 이름을 임의로 만들지
않았다.

| Metric | 종류 | Label |
| --- | --- | --- |
| `kafka_brokers` | Gauge | (없음) |
| `kafka_topic_partitions` | Gauge | `topic` |
| `kafka_topic_partition_current_offset` | Gauge | `topic`, `partition` |
| `kafka_topic_partition_under_replicated_partition` | Gauge | `topic`, `partition` |

패널별 PromQL:

| Panel | PromQL |
| --- | --- |
| Target Status | `up{job="kafka"}` |
| Brokers | `kafka_brokers{job="kafka"}` |
| Topics | `count(kafka_topic_partitions{job="kafka", topic!~"__.*"})` |
| Partitions | `sum(kafka_topic_partitions{job="kafka", topic!~"__.*"})` |
| Under Replicated | `sum(kafka_topic_partition_under_replicated_partition{job="kafka"})` |
| Under Replicated Partitions(표) | `kafka_topic_partition_under_replicated_partition{job="kafka"} > 0` |
| Message Rate over Time | `sum by (topic) (rate(kafka_topic_partition_current_offset{job="kafka", topic!~"__.*"}[5m]))` |

**Under Replicated Partitions**는 새 metric 없이 `Under Replicated` 패널이 쓰는
raw metric을 `table` 패널로 그대로 나열한 것이다. `Under Replicated`가 개수만
보여줘서 "몇 개"는 알아도 "어느 topic/partition인지"는 다른 곳(예: broker
로그)을 찾아봐야 했는데, 이 표는 `topic`/`partition`/`instance` label을 그대로
컬럼으로 보여준다. under-replicated partition이 없으면(`> 0` 조건에 걸리는
시계열이 없으면) 표가 비어 있는 게 정상이다.

**이 dashboard에는 consumer lag 패널이 없다.** 이유는 아래 "명확히 해 둘 점"
참고 — lag은 Spark Streaming dashboard의 `Kafka Offset Lag`로 본다.

몇 가지 명확히 해 둘 점:

- **내부 토픽 포함 여부**: `Topics`/`Partitions`/`Message Rate`는 `__`로
  시작하는 내부 토픽(`__consumer_offsets` 등)을 제외한다 — 실제로 운영하는
  `sensor-events` 같은 토픽 상태를 보기 위함이다. 반대로 `Under Replicated`는
  클러스터 건강 신호이므로 내부 토픽을 포함한 전체를 본다.
- **Message Rate는 byte throughput이 아니다**: `kafka_topic_partition_current_offset`의
  증가율로 근사한 message(레코드) 개수 기준 rate다. Kafka broker의 실제
  `Bytes In/Out` JMX metric은 이번 작업 범위에서 제외했다(JMX Exporter 추가
  금지) — dashboard와 이 문서 모두 "Bytes In/Out"이라고 부르지 않는다.
- **consumer lag 패널을 두지 않는다(#586).** `sensor-events`의 유일한
  consumer인 Spark Structured Streaming은 consumer group에 join하지 않고
  offset을 commit하지도 않는다 — 파티션을 `assign()`으로 직접 잡고 offset은
  checkpoint에만 저장한다(`services/stream-processor/src/stream_processor/kafka_source.py`,
  `kafka.group.id` 설정 없음). Kafka Exporter는 committed offset으로 lag을
  계산하므로 `kafka_consumergroup_*` 시계열이 애초에 생성되지 않는다.
  stream-processor가 정상 동작 중이어도 영구 `No data`라서, 한때 있던 네 개의
  Consumer 패널을 지웠다. lag은 Spark Streaming dashboard의 `Kafka Offset Lag`
  (`sum(kafka_topic_partition_current_offset) - stream_processor_kafka_end_offset_sum`)로
  본다 — consumer group lag이 commit 주기에 묶이는 것과 달리 micro-batch commit
  기준이라 이 구조에 더 정확하다.

### Airflow dashboard 지표

Airflow 3.3.1 공식 [Metrics 문서](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/metrics.html)를
근거로 처음 작성했으나, **`scheduler_heartbeat`/`dag_processor_heartbeat`는
실제 배포한 statsd_exporter에 노출되지 않는 metric 이름이었다** — 문서와 실제
동작이 달라서(또는 이 Airflow 3.3.1 버전에서 명칭이 바뀌어서) 두 heartbeat
패널이 항상 `No data`였다. 지금은 실제 exporter(`/metrics`)에서 직접 확인한
metric만 쓴다. wire상 이름은 `AIRFLOW__METRICS__STATSD_PREFIX=airflow`가 붙어
`airflow.<metric>` 형태이고, statsd_exporter가 `.`을 `_`로 바꿔 최종 Prometheus
이름이 된다. DogStatsD tag(`dag_id`, `task_id`, `action` 등)는 매핑 없이도
자동으로 label이 된다.

| Airflow metric | Prometheus 이름 | 종류 | Label |
| --- | --- | --- | --- |
| `scheduler.executor.heartbeat_duration` | `airflow_scheduler_executor_heartbeat_duration_count` | Summary(자동, `_count` suffix) | (없음) |
| `dag_processing.last_run.seconds_ago` | `airflow_dag_processing_last_run_seconds_ago` | Gauge | (없음) |
| `dag_processing.processes` | `airflow_dag_processing_processes` | Counter | `action`(`start` 등) |
| `dag_processing.import_errors` | `airflow_dag_processing_import_errors` | Gauge | (없음) |
| `ti_successes` | `airflow_ti_successes` | Counter | `dag_id`, `task_id` |
| `ti_failures` | `airflow_ti_failures` | Counter | `dag_id`, `task_id` |
| `scheduler.dagruns.running` | `airflow_scheduler_dagruns_running` | Gauge | — |
| `ti.scheduled` | `airflow_ti_scheduled` | Gauge | `queue`, `dag_id`, `task_id` |
| `ti.queued` | `airflow_ti_queued` | Gauge | `queue`, `dag_id`, `task_id` |
| `ti.running` | `airflow_ti_running` | Gauge | `queue`, `dag_id`, `task_id` |
| `task.duration` | `airflow_task_duration_seconds` | Histogram(매핑) | `dag_id`, `task_id` |
| `dagrun.duration.success` | `airflow_dagrun_duration_success_seconds` | Histogram(매핑) | `dag_id` |
| `dagrun.duration.failed` | `airflow_dagrun_duration_failed_seconds` | Histogram(매핑) | `dag_id` |

> **더 이상 쓰지 않는 metric 이름**: `airflow_scheduler_heartbeat`,
> `airflow_dag_processor_heartbeat`. 존재하지 않는 metric이라 항상 `No data`만
> 냈다. 아래 두 패널이 각각의 후속이다.

패널별 PromQL:

| Panel | PromQL |
| --- | --- |
| Target Status | `up{job="airflow"}` |
| Scheduler Heartbeat | `sum(increase(airflow_scheduler_executor_heartbeat_duration_count{job="airflow"}[5m]))` |
| DAG Processor Last Run Age | `max(airflow_dag_processing_last_run_seconds_ago{job="airflow"})` |
| DAG Import Errors | `airflow_dag_processing_import_errors{job="airflow"}` |
| Task Success/Failure Rate over Time | `sum(rate(airflow_ti_successes{job="airflow"}[5m])) or vector(0)` (failure는 `ti_failures`) — 순간값이 아니라 시간축(range query)으로 성공률 추세를 본다 |
| Failures by DAG | `sum by (dag_id) (rate(airflow_ti_failures{job="airflow"}[5m]))` |
| Failures by Task | `sum by (dag_id, task_id) (rate(airflow_ti_failures{job="airflow"}[5m]))` |
| Task/DAG Run Duration (p50/p95/p99) | `histogram_quantile(0.95, sum by (le) (rate(airflow_task_duration_seconds_bucket{job="airflow"}[5m])))` (dagrun success/failed도 같은 형태) |

몇 가지 명확히 해 둘 점:

- **Heartbeat과 Target Status는 다른 걸 본다.** `up{job="airflow"}`는
  statsd_exporter 프로세스가 살아있는지만 의미한다. scheduler/dag-processor가
  실제로 도는지는 `airflow_scheduler_executor_heartbeat_duration_count`/
  `airflow_dag_processing_last_run_seconds_ago`로 따로 확인해야 한다 —
  exporter는 멀쩡한데 scheduler만 죽은 경우 Target Status는 계속 UP으로
  보인다.
- **Scheduler Heartbeat / DAG Processor Last Run Age / DAG Import Errors는
  `No data` 자체가 실제 장애 신호이므로 `or vector(0)`로 감추지 않는다.**
  scheduler나 dag-processor가 죽으면 statsd_exporter가 이 metric들을 아예
  못 받아 시계열이 사라지는데, 이걸 0으로 보정해 버리면 "정상인데 heartbeat가
  0회"인 상태와 "애초에 죽어서 metric이 안 온" 상태를 구분할 수 없게 된다.
  반대로 `Task Success/Failure Rate over Time`은 DAG가 안 도는 것 자체가
  정상적인 0이라 `or vector(0)`로 명시적인 0을 보여준다.
- **duration 단위는 seconds다.** Airflow는 StatsD timer(`|ms`)로 값을
  보내지만, statsd_exporter가 노출할 때 이를 seconds로 자동 변환한다 —
  그래서 mapping의 bucket도 seconds 기준으로 적었고(`[1, 5, 15, 30, ...]`),
  별도 scale 변환은 추가하지 않았다. Grafana panel unit도 `s`로 맞췄다.
- **timer metric은 mapping 설정이 있어야 histogram이 된다.** statsd_exporter
  기본값은 timer를 고정 분위수(0.5/0.9/0.99) Summary로 바꾸는데, p95는 그
  집합에 없다. 그래서 `infra/monitoring/statsd/airflow-mapping.yml`이 이
  3개 metric만 histogram으로 바꾼다 — 그 외 metric(heartbeat duration
  포함)은 매핑 없이 statsd_exporter 기본 변환(Summary, `_count`/`_sum` suffix)을
  그대로 쓴다.
- **`airflow_dag_processing_last_run_seconds_ago`는 DAG 파일별로 별도
  series가 찍힌다** — 실제 exporter에서도 파일별 값이 여러 개 확인됐다.
  raw metric을 stat 패널에 그대로 쓰면 여러 series 중 하나만 보이거나
  표시가 불안정해지므로 `max(...)`로 묶어 "가장 오래 처리 안 된 파일" 기준
  단일 값을 보여준다.
- **DAG Processor Last Run Age의 임계값(60s/300s)은 아직 실제 운영 주기로
  검증하지 못했다.** dag-processor의 실제 파일 스캔 주기에 따라 배포 후
  조정이 필요할 수 있다.
- **DogStatsD tag(즉 `dag_id`/`task_id` label)가 실제로 붙는지는 배포 후
  확인이 필요하다.** `AIRFLOW__METRICS__STATSD_DATADOG_ENABLED=True` +
  statsd_exporter의 기본 DogStatsD tag parsing으로 동작해야 하는 것을
  공식 문서로 확인했지만, 이 조합을 실제 Airflow 3.3.1 + 이 exporter 버전
  조합으로 직접 띄워보지는 못했다(아래 Validation 참고). `Failures by DAG`
  류 패널이 label 없이 뭉뚱그려 나오면 아래 troubleshooting을 따라간다.
- **cardinality**: `job_id`, `run_id`, `file_path`를 label로 쓰는 Airflow
  metric(`local_task_job.task_exit` 등)은 이번 dashboard에서 아예 쓰지 않는다.
  `dag_processing.processes`는 `action` label만 갖고 있어(실제 exporter로
  확인) high-cardinality가 아니라 이번에 새로 썼다. `statsd_disabled_tags`는
  별도로 override하지 않고 Airflow 3.3.1 기본값(`job_id,run_id`)을 그대로
  둔다 — 기본값 자체가 이미 두 high-cardinality tag를 막아주고, 이 dashboard가
  쓰는 metric 중 `job_id`/`run_id`/`file_path` label을 가진 것도 없기
  때문이다.
- **DAG가 아직 한 번도 안 돈 상태에서는 `Task Success/Failure Rate over Time`이
  `No data`가 아니라 `0`인 게 정상이다.** `or vector(0)`로 명시적인 0을
  보여주도록 했다. `Failures by DAG`/`Failures by Task`/
  `Task Duration`/`DAG Run Success/Failed Duration`(timeseries 패널)은
  여전히 `No data`가 정상이다 — 실제로 task/dagrun이 실행돼야 값이 생기고,
  timeseries 그래프에서 빈 구간은 stat 패널의 회색 `No data`처럼 오해를
  주지 않는다. `Target Status`, `Scheduler Heartbeat`,
  `DAG Processor Last Run Age`, `DAG Import Errors`는 scheduler/dag-processor가
  떠 있기만 하면 DAG 실행과 무관하게 값이 나와야 하고, 안 나오면 그 자체가
  장애 신호다.

### Stream Processor dashboard 지표

`services/stream-processor/src/stream_processor/metrics.py`가 노출하는 metric
이름을 그대로 PromQL에 쓴다. `ProgressLogger`가 Spark의
`QueryProgressEvent`/`QueryStartedEvent`/`QueryTerminatedEvent`를 받아 이
metric들을 갱신한다.

| Metric | 종류 | 설명 |
| --- | --- | --- |
| `stream_processor_query_running` | Gauge | 쿼리가 실행 중이면 1, 종료되면 0 |
| `stream_processor_input_rows_total` | Counter | 마이크로배치마다 읽은 `numInputRows` 누적 |
| `stream_processor_input_rows_per_second` | Gauge | 최근 배치의 `inputRowsPerSecond` |
| `stream_processor_processed_rows_per_second` | Gauge | 최근 배치의 `processedRowsPerSecond` |
| `stream_processor_batch_duration_seconds` | Histogram | `durationMs.triggerExecution` 기준 배치 전체 소요 시간(초) |
| `stream_processor_last_progress_timestamp_seconds` | Gauge | 마지막 progress 이벤트의 Unix epoch 초 |
| `stream_processor_query_failures_total` | Counter | 예외로 종료된 횟수(`QueryTerminatedEvent.exception`이 있을 때만 증가) |
| `stream_processor_event_time_lag_seconds` | Gauge | 최근 배치의 최신 event time과 현재 시각 차이(초) |
| `stream_processor_kafka_offset_lag` | Gauge | 최근 micro-batch progress가 보고한 Kafka offset lag |
| `stream_processor_kafka_end_offset_sum` | Gauge | 최근 성공한 micro-batch가 commit한 Kafka end offset의 partition 합 |

패널별 PromQL:

| Panel | PromQL |
| --- | --- |
| Target Status | `up{job="stream-processor"}` |
| Rows/sec over Time | `stream_processor_input_rows_per_second{job="stream-processor"}`와 `stream_processor_processed_rows_per_second{job="stream-processor"}`를 한 그래프에 겹쳐 그린다 |
| Micro-batch Duration (p50/p95) | `histogram_quantile(0.50, sum by (le) (rate(stream_processor_batch_duration_seconds_bucket{job="stream-processor"}[5m])))` (p95는 quantile 값만 교체) |
| Last Progress Age | `(time() - stream_processor_last_progress_timestamp_seconds{job="stream-processor"}) and (stream_processor_last_progress_timestamp_seconds{job="stream-processor"} > 0)` |
| Freshness over Time | 위 Last Progress Age + Event-Time Lag를 시간축(range query)으로 합친 그래프 — 둘 다 초 단위, 같은 330초 threshold를 써서 한 그래프에 둔다 |
| Kafka Offset Lag over Time | Kafka Exporter의 topic offset 합에서 `stream_processor_kafka_end_offset_sum`을 뺀 live backlog를 시간축으로 표시 |
| Query Failures | `stream_processor_query_failures_total{job="stream-processor"}` |

몇 가지 명확히 해 둘 점:

- **Component Status의 `QUERY STOPPED`는 프로세스 생존 여부(`up`)와 다르다.** 컨테이너는
  떠 있지만(`up == 1`) 쿼리가 예외로 죽어 재시작 대기 중인 짧은 순간에는
  `up == 1`, `stream_processor_query_running == 0`일 수 있다.
- **`Last Progress Age`가 `STREAM_MAX_TRIGGER_DELAY`(기본 5분/300초) 근처까지
  올라가는 것은 트래픽이 적을 때 정상이다.** `STREAM_MIN_OFFSETS_PER_TRIGGER`
  (기본 600,000건)가 쌓이기 전에는 이 지연 시간이 지나야 배치가 실행된다.
  Threshold를 300초보다 조금 더 여유 있는 330초로 잡은 것도 이 때문이다 —
  실제로 값이 계속 커지기만 하고 꺾이지 않아야 장애로 본다.
- **아직 한 번도 progress가 발생하지 않은 초기 상태에서는 `Last Progress
  Age`가 `No data`인 게 정상이다.** `stream_processor_last_progress_timestamp_seconds`는
  gauge 초기값이 `0`(Unix epoch)이라, `time() - ...`을 그대로 계산하면
  약 56년(1970-01-01 기준 경과 시간)처럼 잘못된 큰 값이 나온다. 그래서
  `and (stream_processor_last_progress_timestamp_seconds{...} > 0)`로
  timestamp가 실제로 한 번이라도 기록된 경우에만 age를 계산하도록 필터링한다
  — 이 필터 때문에 progress가 아직 없으면 값 자체가 없어(`No data`) 큰
  숫자로 오해할 일이 없다. 이 상태는 첫 마이크로배치가 끝나면 정상적으로
  풀린다.
- **`Micro-batch Duration`은 배치가 5초~5분 간격으로만 발생해 데이터 포인트가
  희소하다.** `rate(...[5m])` 윈도우 안에 샘플이 아예 없으면 `No data`가
  정상이다 — 장애가 아니라 그 구간에 배치가 없었다는 뜻이다.

### Silver/Gold freshness는 아직 측정 불가 (observability gap)

Bronze 적재 신선도는 Spark Streaming dashboard의 `Last Progress Age`/
`Event-Time Lag`로 상시 관측된다. 반면 **Silver/Gold의 실제 적재·갱신 시각을
노출하는 Prometheus/StatsD metric은 아직 없다.** 가장 가까운 기존 신호는
`services/batch-jobs/src/batch_jobs/gold_audit_validation.py`의 Great
Expectations 감사인데(Gold Postgres `standard_segment_comfort_score`/
`current_segment_comfort_score`의 `age_seconds`를 SQL로 계산), Airflow DAG가
돌 때 한 번 계산해 S3 Data Docs로만 남기는 soft-fail 감사라 Grafana가 상시
조회할 수 있는 timeseries가 아니다. Silver 단계는 이런 감사조차 없다.

측정하려면 Postgres exporter를 붙이거나 배치 잡이 완료 시각을 metric으로
내보내야 한다 — 새 exporter/계측을 추가하는 작업이라 아직 하지 않았다. 이
공백은 원래 `service-status-overview.json`의 text 패널에 적혀 있었고, 그
dashboard를 지우면서(#586) 여기로 옮겼다.

### EMR Serverless dashboard 지표

Prometheus가 아니라 CloudWatch datasource로 AWS `AWS/EMRServerless` 네임스페이스
metric을 직접 조회한다 — EMR Serverless 자체를 새로 계측하는 코드는 이번 작업에서
추가하지 않았다(AWS가 1분 주기로 자동 발행하는 값을 그대로 쓴다).

dashboard 상단의 `Application ID`/`Application Name` 변수(텍스트 입력)로
조회할 EMR Serverless application을 지정한다. `Application ID`는 Airflow에서
쓰는 `EMR_SERVERLESS_APPLICATION_ID` 변수와 같은 값이다
(`services/orchestration/dags/emr_serverless.py` 참고). 이 프로젝트에서 쓰는
application이 고정이라 두 변수 모두 실제 값(`00g85ljahc0svj2p`/
`de4-batch-jobs`)을 기본값으로 채워 뒀다 — Grafana를 새로 띄울 때마다 값을
입력할 필요가 없다. 다른 환경/다른 application을 보려면 값을 바꾼다.

> **CloudWatch query는 `ApplicationId`뿐 아니라 `ApplicationName` dimension도
> 함께 넣어야 한다.** `ApplicationId`만으로는 AWS/EMRServerless metric이
> 조회되지 않는 것을 실제 계정(`ApplicationId: 00g85ljahc0svj2p`,
> `ApplicationName: de4-batch-jobs`)에서 직접 확인했다 — 두 dimension을 함께
> 넣었을 때만 `SuccessJobs` 등이 정상적으로 나온다. 처음 이 dashboard를 만들
> 때는 CloudWatch 콘솔에서 두 dimension이 항상 함께 붙어 있는 것을 놓쳐
> `ApplicationId`만 썼었다.

| Metric | Statistic | Dimension | 설명 |
| --- | --- | --- | --- |
| `RunningJobs` / `SuccessJobs` / `FailedJobs` | Maximum | ApplicationId, ApplicationName | 이 metric의 dimension 전부라 direct query 하나로 조회한다(`matchExact: true`) — 1분마다 발행되는 상태값이라 Maximum/Average/Sum이 사실상 같다 |
| `CPUAllocated` / `MemoryAllocated` | Maximum | ApplicationId, ApplicationName, WorkerType, CapacityAllocationType | application에 할당된 capacity, `matchExact: false`+`SUM()`으로 합산 |
| `WorkerCpuUsed` / `WorkerMemoryUsed` | Sum | ApplicationId, ApplicationName, JobId, WorkerType, CapacityAllocationType | AWS 문서가 CPU/Memory 실사용량 합산 시 Statistic Sum + 1분 주기를 명시적으로 권장한다, `matchExact: false`+`SUM()`으로 합산 |

`RunningJobs`/`SuccessJobs`/`FailedJobs`는 ApplicationId+ApplicationName이
dimension 전부이므로(추가 dimension 없음) direct query 하나로 충분하다. 반면
`CPUAllocated`/`MemoryAllocated`/`WorkerCpuUsed`/`WorkerMemoryUsed`는
`WorkerType`/`CapacityAllocationType`(Used는 `JobId`까지)
조합별로 별도 시계열이 발행되므로, CloudWatch 쿼리를 두 개씩 쓴다 —
`matchExact: false`(search)로 실제 dimension 조합을 전부 찾는 숨겨진 쿼리
하나와, 그 결과를 `SUM(...)` 수식(Metric Math)으로 더하는 쿼리 하나. 이렇게
합산해야 application 전체 값이 나온다.

몇 가지 명확히 해 둘 점:

- **`ApplicationName` dimension을 빠뜨리면 모든 패널이 `No data`다.** 위
  경고 참고 — `Application ID`만 채우고 `Application Name`을 비우면 여전히
  조회가 안 된다.
- **hide+`SUM()` Metric Math 쿼리에는 `metricQueryType`/`metricEditorMode`를
  명시해야 한다(#410).** Grafana CloudWatch datasource는 이 두 필드가 없으면
  모든 target을 기본값인 "Search, Code" 모드로 해석한다 — 그러면 직접 작성한
  `namespace`/`metricName`/`dimensions` 필드 대신 Grafana가 자체적으로 합성한
  `SEARCH(...)` 문자열이 실행되는데, 이게 Explore에서 성공한 것과 다른
  쿼리라 `No data`가 났다. Grafana 자체에도 "이 필드들이 빠지면 기본값
  Search/Code로 떨어져 의도한 쿼리와 다르게 동작한다"는 같은 매커니즘의
  버그 리포트가 있다(grafana/grafana#90000) — 다만 그 이슈에서 실제로 터진
  증상은 우리와 달리 파서 크래시(`index out of range`)였고 머지된 수정도
  그 크래시만 고쳤을 뿐, "이 필드들을 항상 채워야 한다"가 Grafana의 공식
  가이드로 명시된 건 아니다. 그래서 direct metric 쿼리에는
  `"metricQueryType": 0, "metricEditorMode": 0`(Search, Builder)을,
  `SUM(...)` 같은 Metric Math 쿼리에는 `"metricQueryType": 0,
  "metricEditorMode": 1`(Search, Code)을 명시적으로 채워 넣었다.
  `RunningJobs`/`SuccessJobs`/`FailedJobs`는 애초에 `SUM()`이 필요 없는
  단일 series라 이 문제를 우회해 direct query로 단순화했다.
- **Statistic 선택은 문서 근거가 있는 것(WorkerCpuUsed/WorkerMemoryUsed의
  Sum)과 그렇지 않은 것(나머지의 Maximum)이 섞여 있다.** 나머지 metric은
  1분마다 한 값만 찍히는 상태값 성격이라 Maximum/Average/Sum이 숫자상
  같아야 정상이지만, 실제로 여러 데이터 포인트가 겹쳐 발행되는 경우를
  발견하면 이 값을 조정해야 한다.
- **Memory 단위는 GB(10진, `decgbytes`)다.** AWS가 `MemoryAllocated`/
  `WorkerMemoryUsed`를 GB 단위로 발행하므로, Grafana에서 원시 byte 값을
  스케일링하는 `bytes` 계열이 아니라 "입력값이 이미 GB"로 가정하는
  `decgbytes`를 그대로 쓴다 — 별도 단위 변환이 필요 없다.
- **CloudWatch datasource가 `authType: default`(EC2 IAM Role)를 쓰므로,
  Monitoring EC2에 위 [사전 준비](#emr-serverless--cloudwatch-datasource-사전-준비)의
  IAM 권한이 없으면 모든 패널이 permission 오류로 `No data`가 된다.**
- **`RunningJobs`가 0인 것은 application이 idle 상태일 뿐 정상이다** — job이
  없으면 0이 나오는 게 맞고, 이걸 장애로 해석하지 않는다. 진짜 조회
  실패(`No data`)와 idle(`0`)은 서로 다른 상태다. Component Status가
  `IDLE`/`RUNNING`/`NO METRIC DATA`로 이 구분을 그대로 보여준다.

### 장애 원인 세분화(Component Status, #437)

기존에는 대부분 `up{job="..."}` 같은 단순 2단계(UP/DOWN)로만 상태를 보여줬다.
#437에서 이미 존재하는 metric만 조합해 컴포넌트별로 더 구체적인 상태
이름(예: Kafka의 `BROKER DOWN`, `UNDER REPLICATED`)을 보여주도록 바꿨다 —
새 metric이나 exporter를 추가하지 않았다.

**우선순위 판정 방식.** 각 `Component Status` 패널은 아래 형태의 PromQL로
여러 조건 중 가장 심각한 것 하나를 골라 숫자 코드로 표현하고, `mappings`로
그 코드를 상태 이름/색으로 바꾼다.

```promql
(3 * (조건A == bool 0) > 0)
  or (2 * (조건B == bool 0) > 0)
  or (1 * (조건C > bool 0) > 0)
  or (0 * up{job="..."})
```

PromQL의 `or`는 왼쪽에 이미 그 label 조합(예: `instance`)의 결과가 있으면
오른쪽 값을 무시하고, 없을 때만 오른쪽 값을 채운다 — 그래서 이 chain은
"가장 심각한 조건부터 순서대로 확인하다가, 처음으로 참인 것의 값을 쓰고,
전부 거짓이면 마지막 fallback(0 * up, 즉 HEALTHY)을 쓴다"는 우선순위
로직이 된다. `== bool 0`/`> bool N` 뒤에 `> 0`을 붙인 이유는 "조건이 거짓인
instance는 아예 결과에서 빠지게(참인 instance만 남게)" 필터링하기 위해서다
— 로컬에 실제 Prometheus를 띄우고 instance 3개(정상/장애/target down)로
직접 검증했다.

이 방식은 `by (instance)`를 붙이면 그대로 instance별로 독립적으로 동작한다
— 아래 multi-instance 절 참고. `state-timeline` 패널은 같은 query를
range query로 그대로 재사용한다(instant 제거).

Kafka/Airflow/Spark Streaming은 **처음에는 각각 `up{job="kafka"}`/
`up{job="airflow"}`/`up{job="stream-processor"}`로 만들었다가 바꿨다** —
`up`은 exporter/endpoint가 스크랩되고 있다는 뜻일 뿐, 그 안의 애플리케이션
로직까지 정상이라는 뜻은 아니기 때문이다. 구체적으로:

- Kafka Exporter는 살아있는데(`up==1`) Kafka broker가 전부 죽어도
  `up{job="kafka"}`는 계속 1이다 — broker 생존은 `kafka_brokers`(Kafka
  Exporter가 admin API로 직접 확인해 보고하는 값)로 봐야 한다.
- Airflow도 statsd_exporter는 살아있는데(`up==1`) scheduler만 죽으면
  `up{job="airflow"}`는 계속 1이다 — scheduler 생존은 heartbeat metric
  증가량으로 봐야 한다(Airflow dashboard와 같은 기준).
- Spark Streaming도 컨테이너는 떠 있지만(`up==1`) 쿼리가 예외로 죽어
  재시작 대기 중인 순간에는 `stream_processor_query_running`이 0이 될 수
  있다 — Spark Streaming dashboard에도 같은 설명이 있다.

Host/Serving API는 이런 "exporter는 살아있는데 그 뒤가 죽는" 시나리오에
해당하는 별도 애플리케이션 health metric이 없어(또는 있어도 이 dashboard
범위를 벗어나) 그대로 `up{job="..."}`을 쓴다 — 이 두 컴포넌트는 여전히
"exporter/endpoint가 스크랩되고 있다"는 뜻으로 읽어야 한다.

**컴포넌트별 상태(우선순위 높은 순).**

| 컴포넌트 | 상태 |
| --- | --- |
| Kafka | `TARGET DOWN`(`up==0`) > `BROKER DOWN`(`kafka_brokers==0`) > `UNDER REPLICATED`(under-replicated partition 존재) > `HEALTHY` |
| Airflow | `METRICS TARGET DOWN`(`up==0`) > `SCHEDULER HEARTBEAT LOST`(최근 5분 heartbeat 0회) > `DAG PROCESSOR STALE`(last run age > 300초, 기존 threshold 재사용) > `DAG IMPORT ERROR` > `HEALTHY` |
| Spark Streaming | `TARGET DOWN` > `QUERY STOPPED`(`query_running==0`) > `PROGRESS STALE`(last progress age > 330초, 기존 threshold 재사용) > `EVENT DATA STALE`(event-time lag > 330초, 같은 threshold) > `RUNNING` |
| Serving API | `TARGET DOWN` > `HEALTHY` (2단계뿐) |
| EMR Serverless | `NO METRIC DATA` / `IDLE` / `RUNNING` (CloudWatch 기반이라 별도 방식, 기존과 동일) |

**의도적으로 뺀 상태 — 임의 threshold를 만들지 않기 위해서다.**

- Kafka `HIGH LAG`: 정상 범위로 합의된 기준이 없고, 애초에
  `kafka_consumergroup_lag` metric 자체가 이 구조에서는 존재하지 않는다
  (위 [Kafka dashboard 지표](#kafka-dashboard-지표) 참고). lag은 Spark
  Streaming 쪽 `Kafka Offset Lag`로 본다.
- Spark `KAFKA BACKLOG` 절대값: 정상 backlog 크기로 합의한 기준이 없어
  임의 threshold를 두지 않는다. 대신 Kafka 입력 중 2분 연속으로
  backlog가 커지는 추세를 `BronzeIngestionLagGrowing`으로 감지한다.
- Serving API `HIGH 5XX`/`HIGH LATENCY`: 5xx rate와 p95 latency 모두 이
  프로젝트에 확립된 SLA가 없다. 실제 운영 기준이 정해지면 이 패널들에
  추가한다.

**Gauge 패널은 두지 않는다(#586).** 한때 Airflow `DAG Processor Last Run Age`와
Spark Streaming `Last Progress Age`/`Event-Time Lag`를 같은 query의 stat과
나란히 Gauge로도 뒀는데, 같은 값을 두 번 보여줄 뿐이라 지웠다 — threshold는
stat 패널에 그대로 살아 있고, 추세는 각각의 over Time 그래프에서 본다.

**Multi-instance 대응.** `sum(...)`/`count(...)`처럼 label 없이 전체를
합치던 쿼리에 `by (instance)`(topic/consumergroup 등 기존 label과 함께)를
추가했다 — 지금은 각 job이 target을 하나씩만 가져서 결과 값은 그대로지만,
나중에 `prometheus.yml`에 같은 job에 target을 하나 더 추가하면(아래
[Prometheus target 구조](#prometheus-target-구조) 참고) 그 즉시 instance별로
분리돼 나온다. 적용한 곳: Kafka(Topics/Partitions/Under Replicated/Message
Rate), Airflow(Failures by DAG/Task), Serving API(Requests/Latency by
Endpoint).

**Airflow의 multi-instance 한계 (해결하지 않음, 알려진 제약).** scheduler/
dag-processor/api-server가 여러 node에 떠도 전부 StatsD UDP로
`airflow-statsd-exporter` 하나에만 값을 보낸다. Prometheus가 붙이는
`instance` label은 이 exporter 자신의 주소(`project-ec2:9102`)이지, 값을
보낸 물리 node가 아니다 — 그래서 `by (instance)`를 아무리 붙여도 "어느
scheduler node가 죽었는지"는 지금 구조로는 구분할 수 없다. 실제로 구분하려면
node마다 별도 exporter를 두거나, exporter 자신에게 어떤 node의 트래픽인지
알려주는 추가 계측(예: StatsD tag로 node id를 함께 보내기)이 필요하다 —
이번 작업 범위 밖이라 구현하지 않았다.

### Prometheus target 구조

`prometheus.yml`의 각 job은 `static_configs.targets`가 문자열 배열이다
— 예를 들어 `stream-processor` job에 두 번째 Spark Streaming node를
추가하려면 그 job 아래에 주소 하나만 더 넣으면 된다(`job_name`을 새로
만들 필요도, dashboard PromQL을 고칠 필요도 없다 — 위 multi-instance
절에서 `by (instance)`를 붙여둔 쿼리는 자동으로 instance별로 나뉜다).
이 구조 자체가 이미 여러 target을 지원하므로 #437에서 `prometheus.yml`
내용은 바꾸지 않았다 — 실재하지 않는 node 주소를 미리 넣어두지 않는다는
원칙(#437 지시사항)도 지켰다.

```yaml
- job_name: stream-processor
  static_configs:
    - targets:
        - spark-ec2:9103
        - spark-ec2-2:9103  # 예시 — 실제로 두 번째 node가 생기면 이렇게 추가
```

### Source of truth와 `allowUiUpdates`

`dashboards.yml`의 `allowUiUpdates: false`는 Grafana UI에서 이 dashboard를
수정해도 디스크(`project-infrastructure.json`)에 저장되지 않게 막는다. 즉
repository의 JSON 파일이 항상 source of truth이고, UI에서 임시로 패널을
조작해 볼 수는 있지만 그 변경은 저장되지 않으며 다음 Grafana 재시작이나
dashboard 새로고침 때 repository 버전으로 되돌아간다. dashboard를 실제로
바꾸려면 `project-infrastructure.json`을 수정하고 재배포해야 한다.
`disableDeletion: true`는 같은 이유로 UI에서 이 dashboard를 삭제하지
못하게 막는다.

### Dashboard가 보이지 않을 때

1. Grafana 컨테이너 로그에 provisioning 에러가 없는지 확인한다.

   ```bash
   docker compose --env-file infra/monitoring/.env -f infra/compose/monitoring.yaml logs grafana
   ```

2. `infra/monitoring/grafana/dashboards/project-infrastructure.json`이
   Monitoring EC2의 `/var/lib/grafana/dashboards`에 실제로 마운트됐는지
   확인한다(`infra/compose/monitoring.yaml`의 volumes 항목).
3. Prometheus target이 모두 `UP`인지 위 [Validation](#validation) 절차로
   확인한다 — datasource 연결 자체가 안 되면 패널에 `No data`만 표시된다.

### 패널에 `No data`가 표시될 때

- Host(node_exporter) 패널: Grafana Explore에서 아래 쿼리를 직접 실행해 값이
  나오는지 확인한다.

  ```promql
  up{job="project-node"}
  ```

- Container(cAdvisor) 패널: cAdvisor 버전/Docker 환경에 따라 `name` label이
  다르게 붙을 수 있다. 특히 Container Network I/O 패널은 컨테이너 네트워크
  구성(공유 네트워크 네임스페이스 등)에 따라 `name` 단위로 값이 안 잡힐 수
  있다. Explore에서 아래 쿼리로 실제 label을 확인한 뒤, dashboard JSON의
  `name` 기준 쿼리를 실제 label에 맞게 조정한다.

  ```promql
  container_cpu_usage_seconds_total{job="project-containers"}
  container_memory_working_set_bytes{job="project-containers"}
  container_network_receive_bytes_total{job="project-containers"}
  container_network_transmit_bytes_total{job="project-containers"}
  ```

- Serving API 패널: `up{job="serving-api"}`가 `0`이거나 값이 아예 없으면
  Prometheus가 Project EC2의 9101 포트에 못 닿는 것이다. 다음을 순서대로
  확인한다.
  1. Serving API 컨테이너가 최신 배포 스크립트로 떠서 `SERVING_API_METRICS_PORT`
     env와 `9101:9101` port publish를 실제로 가지고 있는지
     (`docker inspect <container>`).
  2. Project EC2에서 `curl http://localhost:9101/metrics`가 응답하는지.
  3. Project EC2 Security Group에 9101(source: Monitoring EC2 Security Group)
     inbound 규칙이 있는지.

- Kafka 패널: `up{job="kafka"}`가 `0`이거나 값이 없으면 아래를 순서대로
  확인한다.
  1. `kafka-exporter` 컨테이너 상태 — `docker compose -f infra/compose/kafka.yaml ps`,
     떠 있지 않으면 `docker compose -f infra/compose/kafka.yaml logs kafka-exporter`.
  2. exporter → Kafka 연결 오류 — 로그에 `localhost:9092`로 못 붙는다는
     메시지가 있으면 `kafka` 컨테이너가 떠 있는지, `kafka-exporter`가
     `network_mode: host`로 뜬 게 맞는지 확인한다.
  3. Project EC2에서 `curl http://localhost:9308/metrics`가 응답하는지.
  4. Monitoring EC2에서 `curl http://<PROJECT_EC2_PRIVATE_IP>:9308/metrics`가
     응답하는지(연결 자체가 안 되면 3번은 통과해도 이건 실패할 수 있다).
  5. Project EC2 Security Group에 9308(source: Monitoring EC2 Security Group)
     inbound 규칙이 있는지.
  6. consumer lag이 궁금하다면 이 dashboard가 아니라 Spark Streaming
     dashboard의 `Kafka Offset Lag`를 본다 — 이유는 위
     [Kafka dashboard 지표](#kafka-dashboard-지표)의 consumer lag 항목 참고.

- Airflow 패널: `up{job="airflow"}`가 `0`이거나 값이 없으면, 또는 `up`은
  `1`인데 `airflow_`로 시작하는 metric이 하나도 안 보이면 아래를 순서대로
  확인한다.
  1. Airflow scheduler/dag-processor 컨테이너 상태 —
     `docker compose -f infra/compose/airflow.yaml ps`. 둘 다 떠 있어야
     metric이 나온다.
  2. `airflow-statsd-exporter` 컨테이너 상태 —
     떠 있지 않으면 `docker compose -f infra/compose/airflow.yaml logs airflow-statsd-exporter`.
  3. `airflow-statsd-exporter` 로그에 mapping config 관련 에러(YAML 문법
     오류 등)가 없는지 확인한다 — `--statsd.mapping-config`가 깨지면
     exporter 자체가 기동을 못 할 수 있다.
  4. exporter가 실제로 UDP packet을 받고 있는지 — scheduler/dag-processor
     컨테이너에 `AIRFLOW__METRICS__STATSD_ON`/`STATSD_HOST`/`STATSD_PORT`가
     제대로 들어갔는지 `docker inspect`로 확인하고, `datadog` 패키지가
     설치됐는지 컨테이너 시작 로그에서 `_PIP_ADDITIONAL_REQUIREMENTS` 설치
     로그를 확인한다(빠지면 해당 컴포넌트가 기동 시점에 죽는다 — 위
     [Airflow / StatsD Exporter](#airflow--statsd-exporter) 참고).
  5. Project EC2에서 `curl http://localhost:9102/metrics`가 응답하는지,
     `grep '^airflow_'`로 실제 Airflow metric이 있는지.
  6. Monitoring EC2에서 `curl http://<PROJECT_EC2_PRIVATE_IP>:9102/metrics`가
     응답하는지.
  7. Project EC2 Security Group에 9102(source: Monitoring EC2 Security
     Group) inbound 규칙이 있는지.
  8. Prometheus Targets에서 `airflow`가 `UP`인지.
  9. 위가 다 정상인데 특정 패널만 `No data`라면 PromQL 자체를 의심한다 —
     Grafana Explore에서 해당 metric 이름을 직접 조회해 실제로 존재하는지,
     `dag_id`/`task_id` label이 붙어 있는지 확인한다. 특히 `Scheduler
     Heartbeat`/`DAG Processor Last Run Age`는 각각
     `airflow_scheduler_executor_heartbeat_duration_count`/
     `airflow_dag_processing_last_run_seconds_ago`를 쓴다 — 예전에 쓰던
     `airflow_scheduler_heartbeat`/`airflow_dag_processor_heartbeat`는 이
     exporter에 존재하지 않는 이름이었다.

- Spark Streaming(Stream Processor) 패널: `up{job="stream-processor"}`가 `0`이거나
  값이 없으면 아래를 순서대로 확인한다.
  1. Stream Processor 컨테이너 상태 — `docker inspect stream-processor`,
     떠 있지 않으면 `docker logs stream-processor --tail 100`.
  2. Spark Streaming EC2에서 `curl http://localhost:9103/metrics`가 응답하는지.
  3. Monitoring EC2에서
     `curl http://<SPARK_EC2_PRIVATE_IP>:9103/metrics`가 응답하는지.
  4. Spark Streaming EC2 Security Group에 9103(source: Monitoring EC2 Security
     Group) inbound 규칙이 있는지.
  5. `up`은 `1`인데 특정 패널만 `No data`라면, 해당 metric이 아직 한 번도
     갱신되지 않았을 수 있다 — `stream_processor_*` metric은 Kafka에 실제로
     메시지가 들어와 마이크로배치가 최소 한 번 실행돼야 값이 생긴다.

- EMR Serverless 패널: CloudWatch datasource 자체가 문제인지, 쿼리가
  문제인지부터 구분한다.
  1. dashboard 상단 `Application ID`와 `Application Name` 변수 둘 다 값이
     들어있는지 확인한다 — 둘 중 하나라도 비어 있으면 모든 패널이 `No
     data`다. `ApplicationId`만 넣고 `ApplicationName`을 빠뜨리는 것이
     실제로 겪었던 원인이었다(위 [EMR Serverless dashboard
     지표](#emr-serverless-dashboard-지표) 참고).
  2. Grafana 컨테이너 로그에 `AccessDenied`/`UnrecognizedClientException` 같은
     CloudWatch API 오류가 없는지 확인한다 — 있다면 Monitoring EC2의 IAM
     Role에 위 [사전 준비](#emr-serverless--cloudwatch-datasource-사전-준비)
     권한이 없는 것이다.
  3. Grafana **Connections → Data sources → CloudWatch**에서 **Save & test**를
     눌러 자격증명 자체가 유효한지 확인한다.
  4. 위가 다 정상인데 특정 metric만 `No data`라면, 그 시간대에 실제로 job이나
     worker가 없었을 수 있다(예: `RunningJobs`는 애초에 실행 중인 게 없으면
     0에 가까운 게 정상이다) — CloudWatch 콘솔에서 같은
     namespace/metric/dimension을 직접 조회해 데이터 유무를 먼저 확인한다.
  5. AWS CLI로 직접 확인하려면:

     ```bash
     aws cloudwatch get-metric-statistics \
       --namespace AWS/EMRServerless \
       --metric-name SuccessJobs \
       --dimensions Name=ApplicationId,Value=<application_id> Name=ApplicationName,Value=<application_name> \
       --start-time "$(date -u -d '-1 hour' +%Y-%m-%dT%H:%M:%S)" \
       --end-time "$(date -u +%Y-%m-%dT%H:%M:%S)" \
       --period 60 \
       --statistics Maximum
     ```
  6. **AWS CLI/CloudWatch 콘솔/Grafana Explore에서는 같은 metric이 정상 조회되는데
     이 dashboard의 패널만 `No data`라면**, target JSON에서 `metricQueryType`/
     `metricEditorMode`가 빠지지 않았는지 확인한다(#410) — Explore는 쿼리를
     실행할 때 이 필드를 항상 채워 보내지만, dashboard JSON을 손으로 고칠 때
     빠뜨리기 쉽다. 빠지면 Grafana가 target을 기본값(Search, Code)으로 해석해
     직접 쓴 `namespace`/`metricName`/`dimensions`가 아니라 자체 합성한
     `SEARCH(...)` 문자열을 실행한다. direct metric 쿼리는
     `"metricQueryType": 0, "metricEditorMode": 0`, `SUM(...)` 같은 Metric
     Math 쿼리는 `"metricQueryType": 0, "metricEditorMode": 1`이어야 한다.

- System Overview의 Service Status 패널: 개별 컴포넌트 dashboard에서 이미
  `No data`나 `DOWN`을 확인했다면 그쪽 troubleshooting(Kafka/Airflow/Spark
  Streaming/Serving API 각 절)을 그대로 따라간다. System Overview 자체에서만
  문제가 있어 보이면:
  1. Kafka/Airflow/Spark Streaming/Serving API 카드는 각 컴포넌트
     dashboard의 `Component Status` 패널과 완전히 같은 datasource/PromQL을
     쓴다 — System Overview에서만 다르게 나올 수 없다. 다르게 보이면 브라우저
     캐시나 dashboard 새로고침 문제일 가능성이 높다.
  2. EMR Serverless 패널이 `NO METRIC DATA`면 위 EMR Serverless 패널
     troubleshooting을 따라가되, **`NO METRIC DATA`≠장애**라는 점을 먼저
     염두에 둔다 — `application_id`/`application_name` 변수를 채웠는지부터
     확인하고, 그다음 auto-stop 여부를 `aws emr-serverless
     get-application`으로 확인한다.

### Dashboard 변경 배포

repository에서 dashboard JSON이나 provider 설정을 바꾼 뒤, Monitoring EC2에서
최신 코드를 받고 다시 적용한다.

```bash
docker compose \
  --env-file infra/monitoring/.env \
  -f infra/compose/monitoring.yaml \
  up -d
```

Prometheus나 다른 서비스 상태를 건드리지 않고 Grafana만 다시 띄우고 싶다면:

```bash
docker compose \
  --env-file infra/monitoring/.env \
  -f infra/compose/monitoring.yaml \
  up -d --force-recreate grafana
```

`updateIntervalSeconds: 30`(`dashboards.yml`)로 설정돼 있어, 파일만 바뀌고
컨테이너를 재시작하지 않아도 최대 30초 안에 Grafana가 변경을 감지해
dashboard를 다시 읽는다. 즉시 반영을 확인하고 싶으면 위 `--force-recreate`
명령으로 Grafana를 재시작한다.
