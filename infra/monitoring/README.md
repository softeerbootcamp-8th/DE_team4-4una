# Monitoring

Project EC2와 Monitoring EC2, 서로 다른 두 AWS EC2에 나눠 배포하는 Prometheus/Grafana
모니터링 구성이다.

## Architecture

```text
[Project EC2]                                   [Monitoring EC2]
- node_exporter :9100     --- private VPC --->   - Prometheus (scrape)
- cAdvisor      :8081     --- private VPC --->     127.0.0.1:9090 (host 내부 전용)
                                                  - Grafana :3000 (Prometheus를 datasource로 사용)
```

- Prometheus는 Project EC2의 private IP를 통해 node_exporter(9100)와
  cAdvisor(8081)의 metrics를 scrape한다.
- Grafana는 같은 Docker network에서 `http://prometheus:9090`으로 Prometheus에
  접근한다. Datasource는 [grafana/provisioning/datasources/prometheus.yml](grafana/provisioning/datasources/prometheus.yml)로
  자동 등록되고, `Project Infrastructure` dashboard도 provisioning으로 자동
  생성된다. 자세한 내용은 아래 [Grafana Dashboard](#grafana-dashboard) 참고.
- `prometheus.yml`에는 실제 AWS private IP를 하드코딩하지 않는다. 대신
  `PROJECT_EC2_PRIVATE_IP` 값을 compose의 `extra_hosts`로 넘겨 Prometheus
  컨테이너 안에서 `project-ec2` hostname을 그 IP로 매핑한다.

## AWS prerequisite

Project EC2와 Monitoring EC2는 같은 VPC에서 private IP로 통신 가능해야 한다.
Security Group 생성/변경은 이번 작업 범위가 아니므로, 아래 규칙만 별도로
설정해 둔다. **0.0.0.0/0으로 여는 규칙은 사용하지 않는다.**

Project EC2 Security Group (inbound):

| Port | Protocol | Source |
| --- | --- | --- |
| 9100 | TCP | Monitoring EC2 Security Group |
| 8081 | TCP | Monitoring EC2 Security Group |

Monitoring EC2 Security Group (inbound):

| Port | Protocol | Source |
| --- | --- | --- |
| 3000 | TCP | 관리자/팀원 IP |
| 22 | TCP | 관리자 IP |

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

### cAdvisor에 privileged/device 설정을 쓰는 이유

cAdvisor는 컨테이너별 CPU/memory/network 사용량을 읽기 위해 host의 cgroup,
Docker 내부 상태, 커널 메시지 버퍼(`/dev/kmsg`)에 접근해야 한다. 이는 [cAdvisor
공식 문서가 권장하는 실행 방식](https://github.com/google/cadvisor/blob/master/docs/running.md)이며,
특히 Amazon Linux 2023의 cgroup v2 환경에서는 `privileged: true`와
`/dev/kmsg` device 마운트 없이는 일부 cgroup 정보를 읽지 못하는 경우가 있다.
그 외 권한은 추가하지 않았다.

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

## Validation

Prometheus readiness 확인 (Monitoring EC2 안에서):

```bash
curl http://localhost:9090/-/ready
```

Grafana health 확인:

```bash
curl http://localhost:3000/api/health
```

Prometheus Targets(`http://localhost:9090/targets`)에서 다음 세 job이 모두
`UP`이어야 한다.

- `prometheus`
- `project-node`
- `project-containers`

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

Grafana가 기동되면 별도 UI 설정 없이 `Project Infrastructure` dashboard가
자동으로 provisioning된다.

- 접속: `http://<MONITORING_EC2_PUBLIC_IP>:3000`
- 위치: Grafana 좌측 메뉴 **Dashboards → Infrastructure → Project Infrastructure**
- 구성 파일:
  - `infra/monitoring/grafana/provisioning/dashboards/dashboards.yml` — dashboard
    provider 정의(`Infrastructure` 폴더, `/var/lib/grafana/dashboards`를
    파일 기반으로 읽음)
  - `infra/monitoring/grafana/dashboards/project-infrastructure.json` — dashboard
    본문(패널, PromQL, 임계값)

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
