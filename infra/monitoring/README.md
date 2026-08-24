# Monitoring

Project EC2와 Monitoring EC2, 서로 다른 두 AWS EC2에 나눠 배포하는 Prometheus/Grafana
모니터링 구성이다.

## Architecture

```text
[Project EC2]                                   [Monitoring EC2]
- node_exporter    :9100  --- private VPC --->   - Prometheus (scrape)
- cAdvisor         :8081  --- private VPC --->     127.0.0.1:9090 (host 내부 전용)
- Serving API      :8000                         - Grafana :3000 (Prometheus/CloudWatch를
- API metrics      :9101  --- private VPC --->                    datasource로 사용)
- Kafka            :9092  (Project EC2 안에서만 접근)
- Kafka Exporter   :9308  --- private VPC --->
- Airflow (scheduler/dag-processor/api-server)
    ↓ StatsD UDP 9125 (de4-local 네트워크 내부)
- StatsD Exporter  :9102  --- private VPC --->
- Stream Processor
  metrics           :9103  --- private VPC --->

[EMR Serverless] --- CloudWatch API (AWS) ---> Grafana (CloudWatch datasource, IAM Role)
```

- Prometheus는 Project EC2의 private IP를 통해 node_exporter(9100),
  cAdvisor(8081), Serving API 애플리케이션 metrics(9101), Kafka Exporter
  metrics(9308), StatsD Exporter를 통한 Airflow metrics(9102), Stream
  Processor(Spark Structured Streaming) metrics(9103)를 scrape한다.
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
  자동 등록되고, `Project Infrastructure`, `Serving API`, `Kafka`, `Airflow`
  dashboard도 provisioning으로 자동 생성된다. 자세한 내용은 아래
  [Grafana Dashboard](#grafana-dashboard) 참고.
- `prometheus.yml`에는 실제 AWS private IP를 하드코딩하지 않는다. 대신
  `PROJECT_EC2_PRIVATE_IP` 값을 compose의 `extra_hosts`로 넘겨 Prometheus
  컨테이너 안에서 `project-ec2` hostname을 그 IP로 매핑한다.

## AWS prerequisite

Project EC2와 Monitoring EC2는 같은 VPC에서 private IP로 통신 가능해야 한다.
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

Stream Processor metrics 확인(Stream Processor 컨테이너가 이 EC2에서 같이 떠
있을 때):

```bash
curl http://localhost:9103/metrics
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

## Monitoring EC2 실행 방법

먼저 `.env`를 준비한다.

```bash
cp infra/monitoring/.env.example infra/monitoring/.env
```

`infra/monitoring/.env`에 실제 값을 채운다.

```env
PROJECT_EC2_PRIVATE_IP=<Project EC2 private IPv4>
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
| `infra/compose/monitoring.yaml` | Monitoring EC2 |
| `infra/monitoring/prometheus/**` | Monitoring EC2 |
| `infra/monitoring/grafana/**` | Monitoring EC2 |
| `infra/monitoring/statsd/**` | Project EC2 — orchestration 배포가 담당 |

빌드하는 이미지가 없다. 모두 서드파티 이미지를 그대로 쓰므로 ECR과 AWS 자격증명을
사용하지 않고, 설정 파일을 전달한 뒤 compose로 반영하는 것이 전부다.

배포 대상 디렉터리는 `MONITORING_TARGET_DIR`(기본
`/home/ec2-user/de4-monitoring`)이다. `deploy-orchestration`이 저장소 전체를
`--delete`로 미는 경로와 겹치지 않도록 별도 디렉터리를 쓴다.

### GitHub 설정

| 종류 | 이름 | 비고 |
| --- | --- | --- |
| Variables | `MONITORING_EC2_HOST` | Monitoring EC2의 퍼블릭 IP 또는 DNS |
| Secrets | `MONITORING_EC2_SSH_PRIVATE_KEY` | Monitoring EC2 키페어의 개인키 전문 |

Project EC2와 키페어가 다르므로 secret을 따로 둔다. `EC2_SSH_PRIVATE_KEY`로 대체하면
`Permission denied (publickey)`만 나와 원인을 찾기 어렵다.

선택 항목은 `MONITORING_EC2_USER`(기본 `ec2-user`), `MONITORING_TARGET_DIR`이다.

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
- `serving-api`
- `kafka`
- `airflow`
- `stream-processor`

Monitoring EC2에서 Project EC2의 애플리케이션 metrics endpoint에 직접
접근되는지 확인하려면(문제가 생겼을 때만 필요):

```bash
curl http://<PROJECT_EC2_PRIVATE_IP>:9101/metrics
curl http://<PROJECT_EC2_PRIVATE_IP>:9308/metrics
curl http://<PROJECT_EC2_PRIVATE_IP>:9102/metrics
curl http://<PROJECT_EC2_PRIVATE_IP>:9103/metrics
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

## Grafana Dashboard

Grafana가 기동되면 별도 UI 설정 없이 `Project Infrastructure`, `Serving API`,
`Kafka`, `Airflow`, `Spark Streaming`, `EMR Serverless`, `Service Status
Overview` dashboard가 자동으로 provisioning된다. 일곱 dashboard 모두 같은
provider(`Infrastructure` 폴더, `/var/lib/grafana/dashboards`)가 디렉터리
전체를 읽어서 등록하므로, dashboard를 추가할 때 provider(`dashboards.yml`)를
새로 만들 필요는 없다 — JSON 파일만 그 디렉터리에 추가하면 된다. Datasource는
Prometheus(스크랩 기반)와 CloudWatch(AWS API 직접 조회) 두 개가
[grafana/provisioning/datasources/](grafana/provisioning/datasources/)로
자동 등록된다.

- 접속: `http://<MONITORING_EC2_PUBLIC_IP>:3000`
- 위치: Grafana 좌측 메뉴 **Dashboards → Infrastructure → Project Infrastructure**
  / **Serving API** / **Kafka** / **Airflow** / **Spark Streaming** /
  **EMR Serverless** / **Service Status Overview**
- 구성 파일:
  - `infra/monitoring/grafana/provisioning/dashboards/dashboards.yml` — dashboard
    provider 정의(`Infrastructure` 폴더, `/var/lib/grafana/dashboards`를
    파일 기반으로 읽음)
  - `infra/monitoring/grafana/provisioning/datasources/prometheus.yml` — Prometheus
    datasource 정의
  - `infra/monitoring/grafana/provisioning/datasources/cloudwatch.yml` — CloudWatch
    datasource 정의(`authType: default`로 EC2 IAM Role만 사용, Access/Secret Key
    없음)
  - `infra/monitoring/grafana/dashboards/project-infrastructure.json` — host/
    container dashboard 본문(패널, PromQL, 임계값)
  - `infra/monitoring/grafana/dashboards/serving-api.json` — Serving API
    dashboard 본문
  - `infra/monitoring/grafana/dashboards/kafka.json` — Kafka dashboard 본문
  - `infra/monitoring/grafana/dashboards/airflow.json` — Airflow dashboard 본문
  - `infra/monitoring/grafana/dashboards/spark-streaming.json` — Stream Processor
    (Project EC2에서 Prometheus로 scrape) dashboard 본문
  - `infra/monitoring/grafana/dashboards/emr-serverless.json` — EMR Serverless
    (CloudWatch datasource) dashboard 본문
  - `infra/monitoring/grafana/dashboards/service-status-overview.json` — 주요
    컴포넌트(Project EC2/Host, Kafka, Airflow, Spark Streaming, Serving API,
    EMR Serverless) 상태를 한 화면에서 확인하는 통합 dashboard 본문. 자세한
    내용은 아래 [Service Status Overview dashboard](#service-status-overview-dashboard)
    참고.
  - `infra/monitoring/statsd/airflow-mapping.yml` — Airflow timer metric
    3개를 Prometheus histogram으로 바꾸는 statsd_exporter 매핑 설정

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
| 2xx / 4xx / 5xx | `sum(rate(serving_api_http_requests_total{job="serving-api", status=~"2.."}[5m]))` (4xx/5xx는 `status=~"4.."`/`"5.."`) |
| p50 / p95 / p99 latency | `histogram_quantile(0.50, sum by (le) (rate(serving_api_http_request_duration_seconds_bucket{job="serving-api"}[5m])))` (0.95/0.99는 quantile 값만 교체) |
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
| `kafka_consumergroup_lag` | Gauge | `consumergroup`, `topic`, `partition` |
| `kafka_consumergroup_lag_sum` | Gauge | `consumergroup`, `topic` (모든 partition 합산) |
| `kafka_consumergroup_members` | Gauge | `consumergroup` |

패널별 PromQL:

| Panel | PromQL |
| --- | --- |
| Target Status | `up{job="kafka"}` |
| Brokers | `kafka_brokers{job="kafka"}` |
| Topics | `count(kafka_topic_partitions{job="kafka", topic!~"__.*"})` |
| Partitions | `sum(kafka_topic_partitions{job="kafka", topic!~"__.*"})` |
| Under Replicated | `sum(kafka_topic_partition_under_replicated_partition{job="kafka"})` |
| Message Rate over Time | `sum by (topic) (rate(kafka_topic_partition_current_offset{job="kafka", topic!~"__.*"}[5m]))` |
| Consumer Lag | `sum(kafka_consumergroup_lag{job="kafka"})` |
| Consumer Lag by Topic | `sum by (topic) (kafka_consumergroup_lag_sum{job="kafka"})` |
| Consumer Group Members | `sum by (consumergroup) (kafka_consumergroup_members{job="kafka"})` |

몇 가지 명확히 해 둘 점:

- **내부 토픽 포함 여부**: `Topics`/`Partitions`/`Message Rate`는 `__`로
  시작하는 내부 토픽(`__consumer_offsets` 등)을 제외한다 — 실제로 운영하는
  `sensor-events` 같은 토픽 상태를 보기 위함이다. 반대로 `Under Replicated`는
  클러스터 건강 신호이므로 내부 토픽을 포함한 전체를 본다.
- **Message Rate는 byte throughput이 아니다**: `kafka_topic_partition_current_offset`의
  증가율로 근사한 message(레코드) 개수 기준 rate다. Kafka broker의 실제
  `Bytes In/Out` JMX metric은 이번 작업 범위에서 제외했다(JMX Exporter 추가
  금지) — dashboard와 이 문서 모두 "Bytes In/Out"이라고 부르지 않는다.
- **Consumer 관련 패널(`Consumer Lag`, `Consumer Lag by Topic`,
  `Consumer Group Members`)은 consumer group이 하나도 없으면 `No data`가
  정상이다.** Kafka Exporter는 실행 중인 consumer group이 없으면 이 metric
  자체를 아예 내보내지 않는다 — Prometheus나 exporter 장애가 아니다. 검증할
  때는 먼저 실제 consumer(예: `stream-processor`)가 그 토픽에 붙어 있는지
  확인한다.

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
| DAG Processor Process Starts (5m) | `sum(increase(airflow_dag_processing_processes{job="airflow", action="start"}[5m])) or vector(0)` |
| DAG Import Errors | `airflow_dag_processing_import_errors{job="airflow"}` |
| Running DAG Runs / Scheduled / Queued / Running Tasks | `sum(airflow_scheduler_dagruns_running{job="airflow"}) or vector(0)` (나머지도 같은 형태로 `airflow_ti_scheduled`/`airflow_ti_queued`/`airflow_ti_running`) |
| Task Success/Failure Rate | `sum(rate(airflow_ti_successes{job="airflow"}[5m])) or vector(0)` (failure는 `ti_failures`) |
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
  반대로 `Running DAG Runs`/`Scheduled`/`Queued`/`Running Tasks`/
  `Task Success/Failure Rate`/`DAG Processor Process Starts`는 DAG가 안
  돌거나 재시작이 없는 것 자체가 정상적인 0이라 `or vector(0)`로 명시적인
  0을 보여준다.
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
- **`DAG Processor Process Starts`가 높다고 곧바로 crash loop로 단정하지
  않는다.** dag-processor는 정상적으로도 파일 파싱 subprocess를 반복
  생성할 수 있어서, 이 패널은 최근 DAG 파일 처리 activity 정도로 보고
  `DAG Processor Last Run Age`/`DAG Import Errors`와 함께 판단해야 한다.
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
- **DAG가 아직 한 번도 안 돈 상태에서는 다음 패널이 `0`(`No data`가 아니라)인
  게 정상이다**: `Task Success/Failure Rate`, `Running DAG Runs`/`Scheduled`/
  `Queued`/`Running Tasks`, `DAG Processor Process Starts`. `or vector(0)`로
  명시적인 0을 보여주도록 바꿨다. `Failures by DAG`/`Failures by Task`/
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

패널별 PromQL:

| Panel | PromQL |
| --- | --- |
| Target Status | `up{job="stream-processor"}` |
| Query Running | `stream_processor_query_running{job="stream-processor"}` |
| Input Rows/sec | `stream_processor_input_rows_per_second{job="stream-processor"}` |
| Processed Rows/sec | `stream_processor_processed_rows_per_second{job="stream-processor"}` |
| Micro-batch Duration (p50/p95) | `histogram_quantile(0.50, sum by (le) (rate(stream_processor_batch_duration_seconds_bucket{job="stream-processor"}[5m])))` (p95는 quantile 값만 교체) |
| Last Progress Age | `(time() - stream_processor_last_progress_timestamp_seconds{job="stream-processor"}) and (stream_processor_last_progress_timestamp_seconds{job="stream-processor"} > 0)` |
| Total Input Rows | `stream_processor_input_rows_total{job="stream-processor"}` |
| Query Failures | `stream_processor_query_failures_total{job="stream-processor"}` |

몇 가지 명확히 해 둘 점:

- **`Query Running`은 프로세스 생존 여부(`up`)와 다르다.** 컨테이너는
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
  정상이다 — Kafka Exporter의 `Consumer Lag`류 패널과 같은 이유다.

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
| `RunningWorkerCount` | Maximum | ApplicationId, ApplicationName, WorkerType, CapacityAllocationType | worker 조합별로 별도 series라 `matchExact: false`(search)+`SUM()` 식으로 합산 |
| `CPUAllocated` / `MemoryAllocated` | Maximum | ApplicationId, ApplicationName, WorkerType, CapacityAllocationType | application에 할당된 capacity, `matchExact: false`+`SUM()`으로 합산 |
| `WorkerCpuUsed` / `WorkerMemoryUsed` | Sum | ApplicationId, ApplicationName, JobId, WorkerType, CapacityAllocationType | AWS 문서가 CPU/Memory 실사용량 합산 시 Statistic Sum + 1분 주기를 명시적으로 권장한다, `matchExact: false`+`SUM()`으로 합산 |

`RunningJobs`/`SuccessJobs`/`FailedJobs`는 ApplicationId+ApplicationName이
dimension 전부이므로(추가 dimension 없음) direct query 하나로 충분하다. 반면
`RunningWorkerCount`/`CPUAllocated`/`MemoryAllocated`/`WorkerCpuUsed`/
`WorkerMemoryUsed`는 `WorkerType`/`CapacityAllocationType`(Used는 `JobId`까지)
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
- **`RunningJobs`/`RunningWorkerCount`가 0인 것은 application이 idle 상태일 뿐
  정상이다** — job이 없으면 0이 나오는 게 맞고, 이걸 장애로 해석하지 않는다.
  진짜 조회 실패(`No data`)와 idle(`0`)은 서로 다른 상태다. 아래
  [Service Status Overview dashboard](#service-status-overview-dashboard)도
  같은 원칙으로 설계했다.

### Service Status Overview dashboard

`infra/monitoring/grafana/dashboards/service-status-overview.json`. 개별
dashboard를 하나씩 열어보지 않고 Project EC2/Host, Kafka, Airflow, Spark
Streaming, Serving API, EMR Serverless 여섯 컴포넌트 상태를 한 화면에서 먼저
확인하기 위한 dashboard다. 상세 metric은 각 컴포넌트 dashboard로 넘어가서
본다 — 이 dashboard 자체는 패널을 최소화했다.

| 컴포넌트 | 판단 기준 | 비고 |
| --- | --- | --- |
| Project EC2 / Host | `up{job=~"project-node\|project-containers"}` | node_exporter + cAdvisor 스크랩 여부만 본다 |
| Kafka | `kafka_brokers{job="kafka"} > bool 0` | exporter up이 아니라 실제 broker 수 기준. exporter만 살아있고 broker가 전부 죽은 경우까지 DOWN으로 잡는다 |
| Airflow | `sum(increase(airflow_scheduler_executor_heartbeat_duration_count{job="airflow"}[5m])) > bool 0` | statsd_exporter up이 아니라 scheduler heartbeat 기준. exporter만 살아있고 scheduler가 죽은 경우까지 DOWN으로 잡는다 |
| Spark Streaming | `stream_processor_query_running{job="stream-processor"}` | 프로세스 up이 아니라 실제 streaming query 실행 여부 기준 |
| Serving API | `up{job="serving-api"}` | metrics endpoint 스크랩 여부만 본다 |
| EMR Serverless | CloudWatch `AWS/EMRServerless` `RunningJobs`(최근 6시간, `matchExact: false`) | 아래 설명 참고 |

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

**EMR Serverless는 Prometheus scrape 대상이 아니라서 `up{}`이 없다.** 대신
CloudWatch `RunningJobs`를 최근 6시간 창(`matchExact: false`로 실제 dimension
조합 검색)으로 조회해서, 그 창 안에 metric이 한 번이라도 있으면(0 running
이어도) `OK`로, 6시간 내내 metric이 전혀 없으면 `NO METRIC DATA`로 표시한다.
"job이 0개"와 "metric 자체가 없음"을 구분하기 위한 설계다 — job이 0개인
idle 상태를 `OK`로, `ApplicationId`/`ApplicationName`이 비어있거나 틀렸거나
IAM 권한이 없거나(또는 오래 idle이라 auto-stop된 경우도 포함) `NO METRIC
DATA`로 나눈다. `NO METRIC DATA`가 곧바로 장애를 의미하지는 않는다 —
auto-stop은 EMR Serverless의 정상 동작이다. 실제 application 상태는
아래 명령으로 직접 확인해야 정확하다.

```bash
aws emr-serverless get-application --application-id <application_id>
```

이 dashboard도 EMR Serverless dashboard와 같은 `application_id`/
`application_name` 변수를 쓴다(별도 dashboard라 변수를 공유하지 않지만,
둘 다 이 프로젝트에서 쓰는 실제 값 `00g85ljahc0svj2p`/`de4-batch-jobs`를
기본값으로 채워 뒀다). 다른 application을 보려면 값을 바꾼다.

이 dashboard는 실제 AWS 계정으로 EMR Serverless가 idle→active를 오가는
전이 상황까지 검증하지 못했다 — 6시간이라는 조회 창 길이가 실제 auto-stop
주기에 비해 적절한지, `OK`/`NO METRIC DATA` 판정이 실제 장애 상황에서
기대대로 동작하는지는 배포 후 관찰이 필요하다.

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
  6. 위가 다 정상인데 `Consumer Lag`류 패널만 `No data`라면, 위
     [Kafka dashboard 지표](#kafka-dashboard-지표) 마지막 항목대로 consumer
     group이 아직 없는 것뿐일 수 있다 — 장애가 아니다.

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
  2. Project EC2에서 `curl http://localhost:9103/metrics`가 응답하는지.
  3. Monitoring EC2에서
     `curl http://<PROJECT_EC2_PRIVATE_IP>:9103/metrics`가 응답하는지.
  4. Project EC2 Security Group에 9103(source: Monitoring EC2 Security
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
     worker가 없었을 수 있다(예: `RunningJobs`/`RunningWorkerCount`는 애초에
     실행 중인 게 없으면 0에 가까운 게 정상이다) — CloudWatch 콘솔에서 같은
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

- Service Status Overview 패널: 개별 컴포넌트 dashboard에서 이미 `No data`나
  `DOWN`을 확인했다면 그쪽 troubleshooting(Kafka/Airflow/Spark
  Streaming/Serving API 각 절)을 그대로 따라간다. Overview 자체에서만
  문제가 있어 보이면:
  1. Host/Kafka/Airflow/Spark Streaming/Serving API 패널은 각 컴포넌트
     dashboard의 `Target Status` 패널과 완전히 같은 datasource/PromQL을
     쓴다 — Overview에서만 다르게 나올 수 없다. 다르게 보이면 브라우저
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
