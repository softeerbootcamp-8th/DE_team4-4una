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
  자동 등록되며, 이번 작업에서 dashboard는 만들지 않는다.
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
