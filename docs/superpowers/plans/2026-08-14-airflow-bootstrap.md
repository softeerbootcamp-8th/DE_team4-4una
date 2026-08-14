# Airflow LocalExecutor 부트스트랩 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 로컬 개발 환경에 Apache Airflow 3.3.1을 LocalExecutor로 부트스트랩하고, 전용 Postgres 메타데이터 DB와 최소 예시 DAG로 정상 동작을 확인한다.

**Architecture:** 공식 `apache/airflow:3.3.1-python3.12` 이미지를 docker-compose로 띄운다. Postgres 메타데이터 DB는 별도 compose 파일(`infra/compose/postgres.yaml`)의 컨테이너 하나로 관리하고, Airflow 스택(`infra/compose/airflow.yaml`)은 `airflow-init`(1회성 `db migrate`) → `airflow-scheduler`/`airflow-webserver` 순서로 기동한다. 두 compose 파일은 같은 이름의 external-style 네트워크(`de4-local`)를 공유해 서비스명으로 서로를 찾는다.

**Tech Stack:** Docker Compose, 공식 `apache/airflow:3.3.1-python3.12` 이미지, `postgres:16`, `make`.

**Spec:** `docs/superpowers/specs/2026-08-14-airflow-bootstrap-design.md`

## Global Constraints

- Airflow 버전은 `apache/airflow:3.3.1-python3.12` 이미지 태그로만 고정한다. `services/orchestration/pyproject.toml`에는 `airflow`를 의존성으로 추가하지 않는다.
- executor는 `LocalExecutor`로 설정한다 (기본 `SequentialExecutor` 아님).
- Postgres는 이번 범위에서 Airflow 메타데이터 전용 컨테이너/DB만 만든다. Gold/서빙 DB는 범위 밖이며, 이 컨테이너에 나중에 DB를 추가하는 방향으로 확장 가능하게만 남겨둔다.
- `infra/compose/postgres.yaml`과 `infra/compose/airflow.yaml`은 둘 다 `networks.default.name: de4-local`을 지정한다.
- `.env.example`에는 키 이름만 추가하고 값은 채우지 않는다 (기존 파일 스타일 유지).
- 커밋 메시지는 `CONTRIBUTING.md` 컨벤션을 따른다: `<type>: <subject>`(영어 소문자, 명령형, 마침표 없음), 이슈 참조는 footer에 `Refs #70`.
- 이 브랜치의 PR은 `develop`을 대상으로 하고, 변경 라인(추가+삭제) 합계가 500줄을 넘지 않게 유지한다(현재 계획상 전체 변경분은 500줄 이내로 예상됨).
- 코드에 비밀값을 하드코딩하지 않는다 — Postgres 계정 정보는 항상 `${AIRFLOW_POSTGRES_*}` 환경변수로 참조한다.

---

### Task 1: `.env.example`에 Airflow 관련 키 추가

**Files:**
- Modify: `.env.example`

**Interfaces:**
- Produces: `AIRFLOW_HOME`, `AIRFLOW_POSTGRES_DB`, `AIRFLOW_POSTGRES_USER`, `AIRFLOW_POSTGRES_PASSWORD` — Task 2, 3에서 compose 파일이 `${VAR}` 형태로 참조하는 키 이름.

- [ ] **Step 1: 현재 파일 확인**

Run: `cat .env.example`

기존 내용(참고용, 이미 존재):
```
KAFKA_BOOTSTRAP_SERVERS=
KAFKA_SENSOR_TOPIC=
POSTGRES_HOST=
POSTGRES_PORT=
POSTGRES_DB=
POSTGRES_USER=
POSTGRES_PASSWORD=
AWS_REGION=
CENSUS_API_KEY=
```

- [ ] **Step 2: Airflow 키 추가**

`POSTGRES_PASSWORD=` 다음, `AWS_REGION=` 앞에 아래 4줄을 추가한다 (기존 `POSTGRES_*`는 향후 Gold/서빙용으로 예약된 것이므로 이름을 겹치지 않게 `AIRFLOW_POSTGRES_*`로 둔다):

```
AIRFLOW_HOME=
AIRFLOW_POSTGRES_DB=
AIRFLOW_POSTGRES_USER=
AIRFLOW_POSTGRES_PASSWORD=
```

`AIRFLOW_HOME`은 호스트 경로가 아니라 **Airflow 컨테이너 내부 경로**로 쓰인다
(Task 3의 `airflow.yaml`이 이 값을 컨테이너 환경변수와 볼륨 마운트 대상 경로로
그대로 사용한다). 실제 로컬 실행 시 `.env`에는 공식 이미지의 기본값인
`/opt/airflow`를 채우는 것을 권장한다고 README(Task 5)에 명시한다.

- [ ] **Step 3: 값이 비어 있는지, 실제 비밀값이 섞여 들어가지 않았는지 확인**

Run: `grep -E "^AIRFLOW" .env.example`
Expected:
```
AIRFLOW_HOME=
AIRFLOW_POSTGRES_DB=
AIRFLOW_POSTGRES_USER=
AIRFLOW_POSTGRES_PASSWORD=
```
(모든 줄이 `=`로 끝나고 값이 없어야 한다.)

- [ ] **Step 4: Commit**

```bash
git add .env.example
git commit -m "docs: add airflow env keys to .env.example

Refs #70"
```

---

### Task 2: Airflow 메타데이터 전용 Postgres compose 파일

**Files:**
- Create: `infra/compose/postgres.yaml`

**Interfaces:**
- Consumes: `.env`의 `AIRFLOW_POSTGRES_DB`, `AIRFLOW_POSTGRES_USER`, `AIRFLOW_POSTGRES_PASSWORD` (Task 1).
- Produces: 네트워크 `de4-local` 위에 서비스명 `postgres`, 포트 `5432`로 접속 가능한 Postgres — Task 3의 Airflow 컨테이너가 호스트명 `postgres`로 참조.

- [ ] **Step 1: `infra/compose/postgres.yaml` 작성**

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

- [ ] **Step 2: compose 파일 문법 검증**

`.env`에 임시로 `AIRFLOW_POSTGRES_DB=airflow`, `AIRFLOW_POSTGRES_USER=airflow`, `AIRFLOW_POSTGRES_PASSWORD=airflow`를 채운 뒤(로컬 전용 임시값, 커밋하지 않음):

Run: `docker compose --env-file .env -f infra/compose/postgres.yaml config`
Expected: 에러 없이 렌더링된 compose 설정이 출력됨 (변수가 빈 문자열로 치환되지 않고 실제 값으로 채워져 있는지 확인).

- [ ] **Step 3: 컨테이너 기동 및 준비 상태 확인**

Run:
```bash
make up-postgres
docker compose -f infra/compose/postgres.yaml exec postgres pg_isready -U "$AIRFLOW_POSTGRES_USER" -d "$AIRFLOW_POSTGRES_DB"
```
Expected: `... accepting connections` 출력.

- [ ] **Step 4: 정리**

Run: `docker compose -f infra/compose/postgres.yaml down`

- [ ] **Step 5: Commit**

```bash
git add infra/compose/postgres.yaml
git commit -m "feat: add airflow metadata postgres compose service

Refs #70"
```

---

### Task 3: Airflow LocalExecutor compose 스택 + Makefile 타겟

**Files:**
- Create: `infra/compose/airflow.yaml`
- Modify: `Makefile`

**Interfaces:**
- Consumes: Task 2의 `postgres` 서비스(네트워크 `de4-local`, 포트 5432), Task 1의 `AIRFLOW_HOME`/`AIRFLOW_POSTGRES_*` 환경변수.
- Produces: 포트 `8080`에서 접속 가능한 `airflow-webserver`, `de4-local` 네트워크 위의 `airflow-scheduler` — Task 4에서 DAG를 로드/트리거할 대상.

- [ ] **Step 1: `infra/compose/airflow.yaml` 작성**

```yaml
x-airflow-common: &airflow-common
  image: apache/airflow:3.3.1-python3.12
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${AIRFLOW_POSTGRES_USER}:${AIRFLOW_POSTGRES_PASSWORD}@postgres:5432/${AIRFLOW_POSTGRES_DB}
    AIRFLOW_HOME: ${AIRFLOW_HOME}
  volumes:
    - ../../services/orchestration/dags:${AIRFLOW_HOME}/dags
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

- [ ] **Step 2: `Makefile`에 `up-airflow` 타겟 추가**

기존:
```makefile
.PHONY: help sync lock lint test package-jobs migrate up-kafka up-postgres up-monitoring
```
```makefile
up-kafka up-postgres up-monitoring:
	@test -f "$(COMPOSE_DIR)/$(@:up-%=%).yaml" || { echo "$(COMPOSE_DIR)/$(@:up-%=%).yaml 파일이 필요합니다."; exit 1; }
	$(COMPOSE) -f "$(COMPOSE_DIR)/$(@:up-%=%).yaml" up -d
```

수정 후 (두 줄 모두에 `up-airflow` 추가):
```makefile
.PHONY: help sync lock lint test package-jobs migrate up-kafka up-postgres up-airflow up-monitoring
```
```makefile
up-kafka up-postgres up-airflow up-monitoring:
	@test -f "$(COMPOSE_DIR)/$(@:up-%=%).yaml" || { echo "$(COMPOSE_DIR)/$(@:up-%=%).yaml 파일이 필요합니다."; exit 1; }
	$(COMPOSE) -f "$(COMPOSE_DIR)/$(@:up-%=%).yaml" up -d
```

`help` 타겟의 안내 문구는 `up-<component>`로 이미 일반화돼 있으므로 수정하지 않는다.

- [ ] **Step 3: 두 스택을 함께 기동해 버전/executor 확인**

`.env`에 Task 2에서 쓴 임시값이 그대로 있는 상태에서:

Run:
```bash
make up-postgres
make up-airflow
docker compose -f infra/compose/airflow.yaml logs airflow-init
```
Expected: `airflow-init` 로그에 마이그레이션 성공 메시지가 보이고 컨테이너가 exit code 0으로 종료됨(`docker compose -f infra/compose/airflow.yaml ps airflow-init`로 `Exited (0)` 확인).

Run: `docker compose -f infra/compose/airflow.yaml exec airflow-webserver airflow version`
Expected: `3.3.1` 포함된 출력.

Run: `docker compose -f infra/compose/airflow.yaml exec airflow-scheduler airflow config get-value core executor`
Expected: `LocalExecutor`

- [ ] **Step 4: 정리**

Run:
```bash
docker compose -f infra/compose/airflow.yaml down
docker compose -f infra/compose/postgres.yaml down
```

- [ ] **Step 5: Commit**

```bash
git add infra/compose/airflow.yaml Makefile
git commit -m "feat: bootstrap airflow with localexecutor via compose

Refs #70"
```

---

### Task 4: 최소 예시 DAG로 스케줄러 동작 확인

**Files:**
- Create: `services/orchestration/dags/hello_world.py`

**Interfaces:**
- Consumes: Task 3의 `airflow-scheduler`/`airflow-webserver` (DAG 폴더 볼륨 마운트로 자동 로드됨).
- Produces: `dag_id="hello_world"` — Task 5 README의 재현 절차가 트리거 대상으로 참조.

- [ ] **Step 1: DAG 파일 작성**

```python
"""Airflow LocalExecutor 부트스트랩(#70) 동작 확인용 최소 DAG.

BashOperator로 "hello world"를 출력하는 것 외에는 아무 일도 하지 않는다.
실제 파이프라인 DAG(Kafka -> Bronze, batch-jobs, gold-loader 스케줄링)는
후속 이슈에서 추가한다.
"""

from __future__ import annotations

import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="hello_world",
    description="Airflow LocalExecutor 부트스트랩 동작 확인용 최소 DAG",
    schedule=None,
    start_date=datetime.datetime(2026, 8, 14),
    catchup=False,
    tags=["bootstrap"],
) as dag:
    hello = BashOperator(
        task_id="hello",
        bash_command="echo hello world",
    )
```

- [ ] **Step 2: 두 스택을 다시 기동하고 DAG가 파싱 오류 없이 로드되는지 확인**

Run:
```bash
make up-postgres
make up-airflow
docker compose -f infra/compose/airflow.yaml exec airflow-webserver airflow dags list-import-errors
```
Expected: 출력이 비어 있음(`No data found` 또는 빈 표). `hello_world.py` 관련 에러가 보이면 다음 서브스텝으로 진행하기 전에 import 경로를 수정한다.

- 만약 `from airflow import DAG` 또는 `from airflow.operators.bash import BashOperator`가 이 이미지 버전에서 실패한다면(구버전 호환 shim이 제거된 경우), 아래로 교체한다:
  ```python
  from airflow.sdk import DAG
  from airflow.providers.standard.operators.bash import BashOperator
  ```
  교체 후 Step 2를 다시 실행해 import 오류가 사라지는지 확인한다.

- [ ] **Step 3: DAG 등록 확인**

Run: `docker compose -f infra/compose/airflow.yaml exec airflow-webserver airflow dags list`
Expected: 출력에 `hello_world`가 포함됨.

- [ ] **Step 4: DAG를 언폴즈하고 트리거**

Run:
```bash
docker compose -f infra/compose/airflow.yaml exec airflow-webserver airflow dags unpause hello_world
docker compose -f infra/compose/airflow.yaml exec airflow-webserver airflow dags trigger hello_world
```

- [ ] **Step 5: 실행 상태가 success로 끝나는지 확인**

Run (몇 초 대기 후):
```bash
docker compose -f infra/compose/airflow.yaml exec airflow-webserver airflow dags list-runs -d hello_world
```
Expected: 가장 최근 run의 `state` 컬럼이 `success`. `running`이면 몇 초 더 기다린 뒤 다시 실행하고, `failed`이면 `airflow tasks logs hello_world hello <run_id>`로 로그를 확인해 원인을 파악한다(로그 확인 없이 추측성 수정을 하지 않는다 — `AGENTS.md` 원칙).

- [ ] **Step 6: 정리**

Run:
```bash
docker compose -f infra/compose/airflow.yaml down
docker compose -f infra/compose/postgres.yaml down
```

- [ ] **Step 7: Commit**

```bash
git add services/orchestration/dags/hello_world.py
git commit -m "feat: add hello world dag to verify scheduler

Refs #70"
```

---

### Task 5: 재현 절차 문서화

**Files:**
- Create: `services/orchestration/README.md`

**Interfaces:**
- Consumes: Task 1-4에서 확정된 `.env` 키, `make up-postgres`/`make up-airflow` 커맨드, `hello_world` DAG.

- [ ] **Step 1: README 작성**

```markdown
# orchestration

Apache Airflow(LocalExecutor)를 로컬 개발 환경에서 부트스트랩하는 서비스다.
실제 파이프라인 DAG는 아직 없고, 예시 DAG(`hello_world`)로 스케줄러/웹서버
동작만 확인한다.

## 준비

저장소 루트의 `.env`에 다음 키를 채운다 (`.env.example` 참고). 값은 로컬
개발용으로 자유롭게 정하면 된다.

- `AIRFLOW_HOME` — Airflow 컨테이너 내부 경로다. 공식 이미지의 기본값인
  `/opt/airflow`를 그대로 쓰는 것을 권장한다(호스트 경로가 아니다).
- `AIRFLOW_POSTGRES_DB`, `AIRFLOW_POSTGRES_USER`, `AIRFLOW_POSTGRES_PASSWORD`

## 로컬에서 실행하기

1. Airflow 메타데이터 DB용 Postgres를 띄운다.

   ```bash
   make up-postgres
   ```

2. Airflow(스케줄러/웹서버)를 띄운다. `airflow-init`이 `airflow db migrate`를
   먼저 실행하고 종료하면 나머지 두 서비스가 뜬다.

   ```bash
   make up-airflow
   ```

3. 버전과 executor를 확인한다.

   ```bash
   docker compose -f infra/compose/airflow.yaml exec airflow-webserver airflow version
   docker compose -f infra/compose/airflow.yaml exec airflow-scheduler airflow config get-value core executor
   ```

4. 예시 DAG를 트리거하고 success로 끝나는지 확인한다.

   ```bash
   docker compose -f infra/compose/airflow.yaml exec airflow-webserver airflow dags unpause hello_world
   docker compose -f infra/compose/airflow.yaml exec airflow-webserver airflow dags trigger hello_world
   docker compose -f infra/compose/airflow.yaml exec airflow-webserver airflow dags list-runs -d hello_world
   ```

   웹 UI는 `http://localhost:8080`에서 확인할 수 있다.

## 종료

```bash
docker compose -f infra/compose/airflow.yaml down
docker compose -f infra/compose/postgres.yaml down
```

## 범위 밖

- 실제 파이프라인 DAG(Kafka -> Bronze, batch-jobs, gold-loader 스케줄링)
- CeleryExecutor/KubernetesExecutor 등 분산 실행 지원
- 운영 배포, 인증/RBAC 설정
```

- [ ] **Step 2: 문서에 적힌 커맨드를 그대로 따라가며 재현되는지 최종 확인**

Run: README의 "로컬에서 실행하기" 1~4단계를 처음부터 그대로 실행.
Expected: Task 2~4에서 이미 확인한 것과 동일한 결과(버전 `3.3.1`, executor `LocalExecutor`, `hello_world` run이 `success`).

- [ ] **Step 3: 전체 정리 및 임시로 채워둔 `.env` 값 원복 여부 확인**

Run: `git status .env` (`.env`는 `.gitignore` 대상이라 추적되지 않아야 한다 — 추적되고 있다면 즉시 중단하고 보고한다).

- [ ] **Step 4: Commit**

```bash
git add services/orchestration/README.md
git commit -m "docs: document airflow local reproduction steps

Refs #70"
```

---

## 이 계획에서 하지 않는 것 (스펙의 범위 밖 항목과 동일)

- 실제 파이프라인 DAG, Celery/K8s executor, 운영 배포/인증
- `services/orchestration/Dockerfile`(uv 기반 placeholder) 변경
- Gold/서빙용 Postgres database 실제 추가
