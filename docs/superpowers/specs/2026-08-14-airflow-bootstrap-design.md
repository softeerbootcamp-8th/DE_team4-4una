# Airflow LocalExecutor 부트스트랩 설계

- 관련 이슈: [#70 feat: bootstrap Airflow with LocalExecutor](https://github.com/softeerbootcamp-8th/DE_team4-4una/issues/70)
- 브랜치: `feat/70-bootstrap-airflow-with-localexecutor`
- 작성일: 2026-08-14

## 배경

로컬 개발 환경에서 Apache Airflow를 LocalExecutor로 처음 구축한다. 브랜치는
`develop`과 동일한 상태였고, 이슈 #70에 대한 실제 작업은 아직 없었다. 이슈 본문은
"Gold/서빙 데이터가 있는 Postgres 컨테이너 안에 Airflow 메타데이터용 별도
database를 만든다"고 전제하지만, 저장소에는 그런 Postgres 컨테이너가 아직 없다
(`infra/postgres/`는 빈 디렉터리, Gold/서빙 DB 선정은 `context/open-questions.md`의
`OQ-004`로 아직 열려 있음). 이 설계는 그 간극을 포함해 실제 구현 전에 필요한
결정들을 정리한다.

## 확정된 결정

브레인스토밍 과정에서 사용자와 합의한 사항:

1. **Postgres 범위**: 이번 작업에서는 Airflow 전용 Postgres 컨테이너만 만든다.
   Gold/서빙용 DB는 이슈 범위 밖이며, 나중에 필요해지면 같은 컨테이너에 database를
   추가하는 방향으로 확장한다 (컨테이너를 새로 만들지 않음).
2. **Airflow 실행 방식**: 호스트에서 `uv run`으로 띄우는 대신, Airflow
   webserver/scheduler도 docker-compose 컨테이너로 띄운다. 재현성(완료 조건 중
   "다른 팀원도 동일하게 로컬에서 재현 가능")을 우선한다.
3. **베이스 이미지**: 공식 `apache/airflow:3.3.1-python3.12` 이미지를 그대로
   사용한다. `services/orchestration/Dockerfile`(uv 기반 placeholder)을 확장해
   커스텀 이미지를 빌드하지 않는다.
4. **`services/orchestration/pyproject.toml`**: `airflow`를 의존성으로 추가하지
   않는다. 실행은 공식 이미지가 전담하므로 워크스페이스 전체 `uv sync`에 영향을
   주지 않는 쪽을 택했다. (이슈 본문의 "pyproject.toml에 의존성 추가"라는 문구는
   호스트 실행을 전제로 한 것이라 이번 컨테이너 방식에서는 적용하지 않는다.)

## 구성 요소

### 1. `infra/compose/postgres.yaml` (신규)

`Makefile`의 `up-postgres` 타겟이 이미 이 경로를 참조하도록 준비되어 있다.
Airflow 메타데이터 DB 하나만 가진 Postgres 컨테이너를 정의한다.

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: ${AIRFLOW_POSTGRES_USER}
      POSTGRES_PASSWORD: ${AIRFLOW_POSTGRES_PASSWORD}
      POSTGRES_DB: ${AIRFLOW_POSTGRES_DB}
    ports:
      - "5432:5432"
    volumes:
      - postgres-data:/var/lib/postgresql/data
    networks:
      default:
        name: de4-local

volumes:
  postgres-data:
```

### 2. `infra/compose/airflow.yaml` (신규)

공식 이미지, LocalExecutor. `airflow-init`(1회성 `db migrate`) →
`airflow-scheduler` / `airflow-webserver` 구성. `services/orchestration/dags`를
DAG 폴더로 볼륨 마운트한다.

```yaml
x-airflow-common: &airflow-common
  image: apache/airflow:3.3.1-python3.12
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${AIRFLOW_POSTGRES_USER}:${AIRFLOW_POSTGRES_PASSWORD}@postgres:5432/${AIRFLOW_POSTGRES_DB}
    AIRFLOW_HOME: /opt/airflow
  volumes:
    - ../../services/orchestration/dags:/opt/airflow/dags
  networks:
    default:
      name: de4-local

services:
  airflow-init:
    <<: *airflow-common
    entrypoint: ["airflow", "db", "migrate"]

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8080:8080"
    depends_on:
      airflow-init:
        condition: service_completed_successfully
```

`postgres.yaml`과 `airflow.yaml` 양쪽 모두 `networks.default.name: de4-local`을
지정해, `make up-postgres`와 `make up-airflow`처럼 파일을 따로 실행해도 같은
네트워크에 붙어 `postgres`라는 서비스명으로 서로를 찾을 수 있게 한다.

### 3. `Makefile`

기존 `up-kafka up-postgres up-monitoring:` 타겟 목록에 `up-airflow`를 추가한다
(규칙 자체는 이미 범용적이라 파일만 있으면 동작한다).

### 4. `.env.example` 추가 키

기존 스타일(키만, 값 없음)을 유지한다.

```
AIRFLOW_HOME=
AIRFLOW_POSTGRES_DB=
AIRFLOW_POSTGRES_USER=
AIRFLOW_POSTGRES_PASSWORD=
```

`AIRFLOW__CORE__EXECUTOR`, `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`은 compose 파일
안에서 위 값들을 조합해 구성하므로 `.env`에 별도 키를 두지 않는다. DB 접속
정보나 `AIRFLOW_HOME`을 바꾸고 싶으면 `.env`의 `AIRFLOW_POSTGRES_*` 값만
바꾸면 되고, compose/코드 수정은 필요 없다.

### 5. 예시 DAG

`services/orchestration/dags/hello_world.py` — `BashOperator`로
`echo hello world`를 실행하는 최소 DAG. 실제 파이프라인 DAG는 이슈 범위 밖(다음
이슈)이다.

### 6. 문서화

`services/orchestration/README.md`에 재현 절차를 기록한다:
`.env` 설정 → `make up-postgres` → `make up-airflow` → 웹 UI(`localhost:8080`)
또는 CLI로 `hello_world` DAG 트리거 → success 확인.

## 데이터 흐름 / 설정 전달

```
.env (AIRFLOW_POSTGRES_*, AIRFLOW_HOME)
  └─ docker compose --env-file .env -f infra/compose/postgres.yaml up -d
  │     └─ postgres 컨테이너: POSTGRES_USER/PASSWORD/DB로 초기화
  └─ docker compose --env-file .env -f infra/compose/airflow.yaml up -d
        └─ airflow-init: AIRFLOW__DATABASE__SQL_ALCHEMY_CONN으로 postgres에 접속해 db migrate
        └─ airflow-scheduler / airflow-webserver: 같은 접속정보, LocalExecutor로 DAG 실행
              └─ services/orchestration/dags/hello_world.py를 스케줄러가 로드
```

## 에러 처리

- `airflow-init`이 실패하면(`db migrate` 실패) `depends_on: service_completed_successfully`
  조건 때문에 scheduler/webserver가 뜨지 않는다 — 실패가 조용히 넘어가지 않는다.
- Postgres 컨테이너가 없거나 접속정보가 틀리면 `airflow-init`이 재시도 없이
  바로 실패하므로, `.env` 설정 오류를 빠르게 알 수 있다.

## 테스트 / 검증 계획 (완료 조건 매핑)

| 완료 조건 | 검증 방법 |
| --- | --- |
| 고정된 버전의 Airflow, `airflow version`으로 확인 | `docker compose -f infra/compose/airflow.yaml exec airflow-webserver airflow version` → `3.3.1` 출력 |
| LocalExecutor로 스케줄러 정상 기동 | 스케줄러 컨테이너 로그에서 executor 확인 |
| 메타데이터 DB가 Gold/서빙 데이터와 분리된 별도 database로 존재 | 이번 범위에서는 Airflow 전용 컨테이너/DB이므로 자명하게 분리됨; 후속 이슈에서 같은 컨테이너에 Gold/서빙 DB를 추가할 때 재검증 |
| 예시 DAG가 CLI/웹 UI에서 success로 완료 | `airflow dags trigger hello_world` 또는 웹 UI 트리거 후 상태 확인 |
| 코드 수정 없이 환경변수만으로 DB/`AIRFLOW_HOME` 변경 가능 | `.env`의 `AIRFLOW_POSTGRES_*` 값만 바꿔 재기동 후 정상 동작 확인 |
| `.env.example`에 Airflow 관련 키 추가 | 파일 diff로 확인 |
| 재현 절차 문서화 | `services/orchestration/README.md` 존재 및 단계별 커맨드 확인 |

## 범위 밖 (이번 작업에서 하지 않음)

- 실제 파이프라인 DAG(Kafka→Bronze, batch-jobs, gold-loader 스케줄링)
- CeleryExecutor/KubernetesExecutor 등 분산 실행 지원
- 운영 배포, 인증/RBAC 설정
- `services/orchestration/Dockerfile`(uv 기반) 변경 — 이번 컨테이너 전략과
  무관하므로 그대로 둔다
- Gold/서빙용 Postgres database 실제 추가 (컨테이너 구조만 확장 가능하게 준비)
