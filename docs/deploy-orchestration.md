# Orchestration(Airflow) 배포

`develop`에 머지된 커밋으로 EC2의 Airflow docker compose 스택
(`airflow-dag-processor`/`airflow-scheduler`/`airflow-webserver`)을 기동하는
절차와 사전 조건을 정리한다. 파이프라인은
[.github/workflows/deploy-orchestration.yml](../.github/workflows/deploy-orchestration.yml)과
[infra/compose/airflow.yaml](../infra/compose/airflow.yaml) 두 파일로 구성된다.

AWS OIDC/배포 Role/EC2 인스턴스 자체의 계정 단위 설정은
[docs/deploy-serving-api.md](deploy-serving-api.md)에서 이미 다뤘고 같은 계정·같은
인스턴스를 재사용한다. 이 문서는 orchestration에서 추가로 필요한 부분만 적는다.

## 흐름

```
develop에 머지 (경로 감지) → repository variables 확인
  → rsync로 러너가 checkout한 저장소를 EC2 고정 경로에 동기화
  → SSH로 EC2에서 docker compose --env-file ... up -d
  → /api/v2/monitor/health 폴링 (metadatabase+scheduler 모두 healthy일 때까지)
       타임아웃되면 컨테이너 로그를 출력하고 실패 처리 (자동 롤백 없음)
  → job summary에 commit, repo dir, health 엔드포인트 기록
```

독립 워크플로다. `develop` push 중 아래 경로가 바뀌었을 때만 실행되고, Actions
탭에서 `Run workflow`로 수동 실행할 수도 있다.

```
services/orchestration/**   libs/de4-core/**
infra/compose/airflow.yaml  infra/monitoring/statsd/**
.github/workflows/deploy-orchestration.yml
```

경로 목록이 "orchestration 코드"보다 넓은 이유는 아래 rsync가 저장소 트리를 통째로
밀기 때문이다. 기준은 **`airflow.yaml`이 bind mount하는 것 전부**다.

| mount | 경로 필터 |
| --- | --- |
| `services/orchestration/{dags,jobs}` | 포함 |
| `libs/de4-core/src/de4_core` | 포함 |
| `infra/monitoring/statsd/airflow-mapping.yml` | 포함 |
| `data/{reference,processed,local-lake}` | rsync가 `data/`를 제외하므로 미포함 |

statsd 매핑은 `infra/monitoring/` 아래 있지만 `deploy-monitoring.yml`이 아니라 이
워크플로가 담당한다. bind mount라 내용만 바뀌면 `up -d`가 아무 일도 하지 않으므로,
compose 스텝이 `airflow-statsd-exporter`를 명시적으로 재기동한다.

**CI를 기다리지 않는다.** `develop` 병합은 branch protection의 required status
check(`CI Passed`)을 통과해야만 가능하므로, `develop`에 올라온 시점에 이미 검증된
커밋이다.

**커스텀 이미지를 만들지 않는다.** `infra/compose/airflow.yaml`이 공식
`apache/airflow` 이미지를 직접 쓰고 DAG·jobs·`de4_core`는 rsync + bind mount로
전달하므로, 예전에 `services/orchestration/Dockerfile`로 빌드해 ECR에 올리던
이미지는 어디서도 실행되지 않았다. 그래서 Dockerfile과 ECR 관련 스텝을 함께
제거했다. 이 워크플로는 이제 AWS 자격증명 없이 SSH만으로 동작한다.

저장소 동기화에 `git clone/pull`이 아니라 `rsync`를 쓴 이유는, 러너가 이미
`actions/checkout`으로 받아온 트리를 기존 SSH 연결(`EC2_SSH_PRIVATE_KEY`)로 그대로
밀어넣을 수 있어 EC2에 별도 GitHub 자격증명(deploy key 등)을 새로 등록할 필요가
없기 때문이다.

## GitHub 설정

`Settings > Secrets and variables > Actions`에서 등록한다.

### Variables — 필수

| 변수 | 비고 |
| --- | --- |
| `AWS_REGION`, `EC2_HOST`, `EC2_SSH_PRIVATE_KEY`(secret) | serving-api/stream-processor와 공유 (같은 계정, 같은 인스턴스) |

`AWS_REGION`은 compose에 넘겨 컨테이너가 쓰는 리전 값이다. 워크플로 자체는 AWS를
호출하지 않으므로 `AWS_DEPLOY_ROLE_ARN`이 필요 없다.

### Variables — 선택

기본값이 있어 비워두어도 동작한다.

| 변수 | 기본값 |
| --- | --- |
| `EC2_USER` | `ec2-user` |
| `ORCHESTRATION_REPO_DIR` | `/home/ec2-user/DE_team4-4una` (stream-processor와 같은 인스턴스라 경로도 재사용) |
| `ORCHESTRATION_ENV_FILE` | `/etc/orchestration/orchestration.env` |

## AWS 사전 준비

orchestration 배포를 위해 추가로 설정할 것은 없다. 워크플로가 ECR을 쓰지 않으므로
배포 Role도, 전용 ECR 리포지토리도 필요 없다.

EC2 인스턴스 프로파일은 [docs/deploy-serving-api.md](deploy-serving-api.md#aws-사전-준비)에서
설정한 것을 그대로 쓴다. Airflow 컨테이너가 S3·EMR Serverless를 호출할 때 쓰는 것이고,
배포 절차와는 무관하다.

## EC2 사전 조건

serving-api/stream-processor와 같은 인스턴스를 재사용하므로 docker, AWS CLI,
curl은 이미 준비돼 있다. 추가로 필요한 것은 다음과 같다.

| 항목 | 쓰는 곳 | 확인/설치 |
| --- | --- | --- |
| rsync | 저장소 동기화 | `rsync --version`으로 확인, 없으면 `sudo dnf install -y rsync` |
| python3 | 헬스체크 응답(JSON) 파싱 | Amazon Linux 2023 기본 포함 |
| `ORCHESTRATION_REPO_DIR` 상위 디렉터리 쓰기 권한 | rsync 대상 | `EC2_USER`가 홈 디렉터리 아래에 쓸 수 있으면 충분 |
| env 파일 | 아래 참고 | 사람이 인스턴스에 직접 만든다 |

## env 파일

`docker compose --env-file`로 넘기는 파일이다. 사람이 인스턴스에 직접 만들고,
값은 저장소에 기록하지 않는다. 기본 경로는 `/etc/orchestration/orchestration.env`다.
CD는 GitHub Repository Variable의 `AWS_REGION`을 원격 Compose 프로세스에
`AWS_REGION`과 `AWS_DEFAULT_REGION`으로 전달한다. 수동으로 Compose를 실행할
때에는 같은 두 키를 env 파일에 직접 설정해야 EMR Serverless 클라이언트가 호출할
리전을 결정할 수 있다.

필요한 키 전체 목록과 각 값의 의미는
[services/orchestration/README.md의 "준비" 절](../services/orchestration/README.md#준비)을
참고한다 — 로컬 개발용 `.env`와 같은 키 집합이며, `AIRFLOW_POSTGRES_HOST`/
`AIRFLOW_POSTGRES_PORT`만 로컬(빈 값 → `postgres:5432`)과 달리 실제 RDS 엔드포인트를
채운다. RDS에 `airflow` 스키마/유저 생성과 권한 부여는 사람이 사전에 수행하며 이
워크플로는 자동화하지 않는다.

DB 비밀번호가 들어가므로 소유자와 권한을 제한한다.

```bash
sudo install -d -m 700 /etc/orchestration
sudo chown root:root /etc/orchestration/orchestration.env
sudo chmod 600 /etc/orchestration/orchestration.env
```

## 배포 동작

컨테이너를 지우지 않고 `docker compose up -d`로 변경된 서비스만 recreate한다.
`airflow-init`이 `airflow db migrate`를 먼저 실행해 완료돼야(`depends_on:
condition: service_completed_successfully`) 나머지 세 컨테이너가 뜨므로, compose
스텝 자체가 마이그레이션 완료를 기다리는 지점이기도 하다.

헬스체크는 `/api/v2/monitor/health`의 JSON 바디에서 `metadatabase.status`와
`scheduler.status`가 모두 `healthy`인지 5초 간격으로 확인한다. HTTP 상태 코드만으로는
컴포넌트 단위 unhealthy를 구분할 수 없어 바디를 직접 파싱한다. 기본 타임아웃은
180초이며 `ORCHESTRATION_ENV_FILE`과 달리 워크플로 자체의 `HEALTH_TIMEOUT` 값으로
고정돼 있다(필요하면 워크플로 파일을 수정한다). 타임아웃되면 4개 컨테이너의
최근 로그(`docker compose logs --tail=200`)를 출력하고 워크플로를 실패로 끝낸다 —
serving-api와 달리 직전 상태로 자동 롤백하지 않는다.

### 이미지 정리

배포가 이미지를 만들지 않으므로 정리할 것도 없다.

다만 **이 변경 이전에 쌓인 이미지는 남아 있다.** 배포마다 ECR 태그
(`<registry>/<repo>:<sha>`)와 로컬 태그(`orchestration:<sha>`)가 하나씩 쌓였고,
이제 그것을 지우는 스텝이 없다. 한 번은 직접 정리해야 한다.

```bash
docker images --filter 'reference=orchestration:*'
docker rmi <태그>
```

`docker image prune -af`는 쓰지 않는다 — 이 EC2에는 Kafka, Airflow, exporter 등
다른 서비스 이미지가 함께 있다.

```bash
docker images --filter "reference=orchestration:*" --format '{{.Repository}}:{{.Tag}}'
```

## 실패했을 때

| 증상 | 원인 |
| --- | --- |
| `repository variables가 비어 있습니다` | 위 필수 variables 미설정 |
| rsync 연결 실패/hang | 보안그룹 22번 인바운드 없음, 또는 known_hosts 갱신 실패 |
| rsync는 성공했는데 compose가 파일을 못 찾음 | `ORCHESTRATION_REPO_DIR` 값이 실제 경로와 불일치 |
| `env 파일이 없습니다`류 compose 에러 | 인스턴스에 env 파일 미생성, 또는 `ORCHESTRATION_ENV_FILE` 경로 불일치 |
| `airflow-init`에서 멈춤 | RDS 접속 정보(`AIRFLOW_POSTGRES_*`) 오류, 또는 RDS 보안그룹이 EC2발 인바운드를 막음 |
| health 타임아웃, `metadatabase` unhealthy | RDS 접속 실패 — env 파일의 `AIRFLOW_POSTGRES_*` 확인 |
| health 타임아웃, `scheduler` unhealthy | `airflow-scheduler` 컨테이너 로그(워크플로 출력) 확인 — DAG import 오류 등 |
| `rsync: command not found` (EC2) | 위 "EC2 사전 조건"의 rsync 설치 필요 |

인스턴스를 재생성해 호스트 키가 바뀌어도 워크플로가 매번 `ssh-keyscan`으로 받으므로
따로 손댈 것은 없다. 대신 `EC2_HOST`는 새 주소로 갱신해야 한다.
