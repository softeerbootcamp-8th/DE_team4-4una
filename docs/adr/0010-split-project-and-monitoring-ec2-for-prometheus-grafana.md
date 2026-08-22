---
status: proposed
date: 2026-08-22
supersedes:
superseded_by:
---

# 0010. Project EC2와 Monitoring EC2를 분리한 Prometheus/Grafana 모니터링

## 배경

`infra/compose/`에는 지금까지 로컬 개발용 Airflow/Kafka/Postgres 스택만 있고,
AWS에 배포된 서비스의 host 수준(CPU/memory/filesystem/network) 및 컨테이너
수준 메트릭을 관측할 수단이 없다. AWS에는 실제 서비스가 도는 EC2(Project EC2)와
모니터링 전용 EC2(Monitoring EC2)가 별도로 존재하며, 이 둘을 잇는 관측
파이프라인을 코드로 관리해야 한다. AWS Security Group 등 리소스 자체의 생성은
이 결정과 이번 변경 범위 밖이며, 코드는 두 EC2가 같은 VPC에서 private IP로
통신 가능하다는 전제 위에서만 동작한다.

## 결정

`infra/compose/exporters.yaml`(Project EC2: node_exporter 9100, cAdvisor 8081)과
`infra/compose/monitoring.yaml`(Monitoring EC2: Prometheus, Grafana)을 별도 compose
파일로 분리한다. Prometheus는 `infra/monitoring/.env`의 `PROJECT_EC2_PRIVATE_IP`를
compose의 `extra_hosts`로 받아 컨테이너 안에서 `project-ec2` hostname을 그 IP로
매핑하고, `prometheus.yml`은 실제 IP 대신 `project-ec2:9100`/`project-ec2:8081`만
target으로 사용한다 — 코드나 설정 파일에 실제 AWS private IP를 하드코딩하지 않는다.
Prometheus의 9090은 `127.0.0.1:9090:9090`으로만 바인딩해 Monitoring EC2 localhost
밖에는 노출하지 않고, Grafana(3000)는 admin 계정을 `GRAFANA_ADMIN_USER`/
`GRAFANA_ADMIN_PASSWORD` 환경 변수로만 주입하며 Prometheus datasource를 provisioning
파일로 자동 등록해 수동 설정을 없앤다. Prometheus TSDB는 7일 보존의 named volume으로
영속화한다.

## 대안

| 대안 | 장점 | 단점 | 기각 이유 |
| --- | --- | --- | --- |
| Project EC2 한 대에 서비스와 모니터링을 함께 배치 | 네트워크 설정과 EC2 대수가 단순해짐 | 모니터링 컴포넌트의 부하가 서비스에 영향을 주고, 해당 EC2 자체에 장애가 나면 그 순간의 메트릭을 못 봄 | 관측 대상과 관측 도구가 같은 장애 지점을 공유하면 안 됨 |
| Amazon Managed Service for Prometheus/Grafana 사용 | 운영 부담이 줄고 HA를 관리형으로 확보 | 비용이 추가되고, `terraform/envs/`가 아직 비어 있어 관리형 서비스 채택 자체가 미확정 | 이번 프로젝트 규모에 과함, IaC 채택 여부는 별도 결정 사항이며 나머지 인프라(kafka/airflow/postgres)도 self-host Compose로 일관되게 관리 중 |
| `prometheus.yml`에 Project EC2 private IP를 직접 기록 | 설정이 한 파일로 단순해짐 | private IP가 코드에 남고, IP가 바뀔 때마다 커밋이 필요함 | AGENTS.md의 민감/환경 정보 비노출 원칙과 충돌 |
| Prometheus 9090을 공개 포트로 바인딩 | 팀원이 브라우저로 바로 Prometheus UI 접근 가능 | 인증 없는 Prometheus UI/API가 인터넷에 노출됨 | 불필요한 공격 표면 — Grafana(3000)만 공개하고 Prometheus는 SSH port forwarding으로 접근 |

## 결과

**긍정**: private IP와 Grafana admin 비밀번호가 커밋되지 않는다(`infra/monitoring/.env`,
`.gitignore`로 제외). Monitoring 스택은 로컬 개발용 compose 스택과 별도 Docker
network(`de4-monitoring`)로 격리된다. Grafana datasource 자동 provisioning으로
팀원이 UI에서 수동 설정을 하지 않아도 된다.

**부정**: Security Group 등 AWS 리소스 자체는 아직 코드화되지 않아 두 EC2 사이
통신 허용은 계속 수동 설정에 의존한다(후속 IaC 이슈 필요). TSDB 보존 기간을 7일로
짧게 잡아 장기 트렌드 분석에는 쓸 수 없다. 이번 범위에는 Grafana dashboard/alert,
FastAPI·Kafka·Spark·Airflow 자체 메트릭 수집이 포함되지 않아 필요 시 별도 이슈로
확장해야 한다.

## 영향 범위

- `infra/compose/monitoring.yaml`, `infra/compose/exporters.yaml` 신규
- `infra/monitoring/prometheus/prometheus.yml`,
  `infra/monitoring/grafana/provisioning/datasources/prometheus.yml`,
  `infra/monitoring/.env.example`, `infra/monitoring/README.md` 신규
- `context/architecture.md`, `context/services.md` (모니터링 컴포넌트 반영)

## 참고

- 관련 이슈: #298
