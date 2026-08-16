# segment_comfort_score Gold PostgreSQL 적재 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `#127(comfort_score/formula.py)`이 계산한 segment × vehicle_profile
comfort_score/confidence_score를 PostgreSQL `segment_comfort_score` Gold
테이블에 UPSERT로 적재하는 Spark 배치 Job과, 이를 지원하는 최초의 DB
마이그레이션 체계를 만든다.

**Architecture:** `db/migrations/*.sql`(raw SQL, 번호순)로 `vehicle_profile`/
`segment_comfort_score`/staging 테이블을 만들고, `services/batch-jobs`에
체크섬 기반 마이그레이션 실행기(`batch_jobs.migrate`)와 Gold Job
(`comfort_score.gold_job` + `comfort_score.gold_writer`)을 추가한다. Gold
Job은 기존 `loader.py`/`formula.py`(#117/#127, 무수정)를 그대로 소비하고,
Spark JDBC로 staging 테이블에 쓴 뒤 advisory lock으로 보호되는 단일 SQL
MERGE로 실제 테이블에 UPSERT한다.

**Tech Stack:** Python 3.12, `uv` 워크스페이스, PySpark 4.1(JDBC), PostgreSQL
16, `psycopg2-binary`(신규 의존성), pytest(로컬 `local[1]` Spark 세션 +
fake DB-API 커넥션 — 이 저장소에 실제 외부 서비스에 붙는 테스트가 없어서
mocking 라이브러리 대신 손으로 만든 fake를 그대로 따른다).

**Spec:** `docs/superpowers/specs/2026-08-16-segment-comfort-score-gold-load-design.md`
(이 계획의 모든 결정은 이 문서의 근거를 그대로 따른다 — 실행자는 두 문서를
같이 읽는다)

## Global Constraints

- Python 3.12, `uv` 사용. 의존성 추가 후 반드시 `uv lock`(또는 `uv sync
  --all-packages`)으로 `uv.lock` 갱신 (AGENTS.md).
- `segment_comfort_score`/staging 컬럼 타입은 정확히: `segment_id TEXT`,
  `vehicle_profile_id INTEGER`, `comfort_score DOUBLE PRECISION`,
  `confidence_score DOUBLE PRECISION`, `sample_count BIGINT`,
  `score_version TEXT`, `calculated_at TIMESTAMPTZ` (스펙 §1).
- 이미 적용된 마이그레이션 파일은 절대 수정하지 않는다 — 새 파일을
  추가한다 (AGENTS.md, 스펙 §2에서 체크섬으로 코드 강제).
- `segment_comfort_score`(서빙 테이블)에는 최초 `CREATE TABLE` 이후 어떤
  실행 경로에서도 DDL/TRUNCATE를 하지 않는다 — 일반 `SELECT`가 항상
  즉시 반환돼야 한다(스펙 §5 "읽기 가용성 보장").
- 통합 테스트는 `RUN_INTEGRATION` 미설정 시 skip, `RUN_INTEGRATION=1`인데
  Postgres 접속 실패 시 **skip이 아니라 fail**한다 (스펙 §"테스트 전략").
- 새 의존성은 `psycopg2-binary`(pip) + `org.postgresql:postgresql:42.7.4`
  (Spark `spark.jars.packages`, pip 의존성 아님) 둘뿐이다. 그 외 의존성을
  추가하지 않는다.
- `db/`는 순수 `.sql` 데이터만 두고, 실행 로직은 전부
  `services/batch-jobs`에 둔다(루트가 workspace member가 아니라서 —
  스펙 §2).

## Prerequisites (로컬 환경, 코드 변경 아님)

이 플랜의 모든 수동 검증과 통합 테스트는 로컬 Postgres가 떠 있어야 한다.
이슈 #129가 "당장은 Airflow용 Postgres를 함께 사용"이라고 명시했으므로,
별도 DB를 새로 만들지 않고 Airflow가 쓰는 것과 **같은** database를
재사용한다:

1. 저장소 루트에 `.env`가 없다면 `.env.example`을 복사한다.
2. `.env`에 다음을 채운다(로컬 개발용 임의 값):
   ```
   AIRFLOW_POSTGRES_DB=airflow
   AIRFLOW_POSTGRES_USER=airflow
   AIRFLOW_POSTGRES_PASSWORD=airflow
   POSTGRES_HOST=localhost
   POSTGRES_PORT=5432
   POSTGRES_DB=airflow
   POSTGRES_USER=airflow
   POSTGRES_PASSWORD=airflow
   ```
   (`POSTGRES_*`는 `AIRFLOW_POSTGRES_*`와 동일한 값 — 같은 인스턴스의 같은
   database를 가리킨다.)
3. `make up-postgres`로 Postgres를 띄운다.

---

### Task 1: DB 마이그레이션 파일 (`vehicle_profile`, `segment_comfort_score`, staging, sentinel seed)

**Files:**
- Create: `db/migrations/0001_create_vehicle_profile.sql`
- Create: `db/migrations/0002_create_segment_comfort_score.sql`
- Create: `db/migrations/0003_seed_vehicle_profile_agnostic.sql`
- Create: `db/README.md`

**Interfaces:**
- Consumes: 없음(이 Task는 순수 SQL이며 어떤 코드도 import하지 않는다)
- Produces: 3개 테이블(`vehicle_profile`, `segment_comfort_score`,
  `segment_comfort_score_staging`)의 스키마. 이후 모든 Task가 이 정확한
  컬럼명/타입을 그대로 참조한다 — Task 4/5/7이 이 DDL과 반드시 일치해야
  하는 `EXPECTED_STAGING_COLUMNS` 딕셔너리를 만든다.

- [ ] **Step 1: `0001_create_vehicle_profile.sql` 작성**

```sql
-- vehicle_profile: Bronze 참조 테이블 (context/data/schema-catalog.md 145~162행).
--
-- vehicle_profile_id는 GENERATED ALWAYS AS IDENTITY/SERIAL이 아닌 plain
-- INTEGER다 — 실제 차량 프로필은 Bronze 소스가 부여한 ID를 그대로 쓰고,
-- vehicle_profile_id=0(vehicle-agnostic sentinel,
-- 0003_seed_vehicle_profile_agnostic.sql)이 OVERRIDING SYSTEM VALUE 없이
-- 들어가야 하기 때문이다.
CREATE TABLE vehicle_profile (
    vehicle_profile_id INTEGER NOT NULL PRIMARY KEY,
    profile_name TEXT NOT NULL,
    vehicle_class TEXT NOT NULL,
    manufacturer TEXT,
    model_name TEXT,
    mass_kg DOUBLE PRECISION,
    wheelbase_mm DOUBLE PRECISION,
    suspension_type TEXT,
    vertical_weight DOUBLE PRECISION NOT NULL,
    longitudinal_weight DOUBLE PRECISION NOT NULL,
    lateral_weight DOUBLE PRECISION NOT NULL,
    is_active BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
```

- [ ] **Step 2: `0002_create_segment_comfort_score.sql` 작성**

```sql
-- segment_comfort_score: Gold 테이블 (#129). 컬럼 범위는 #127(formula.py)
-- 출력과 정확히 일치하는 MVP 범위다. 근거:
-- docs/superpowers/specs/2026-08-16-segment-comfort-score-gold-load-design.md §1
CREATE TABLE segment_comfort_score (
    segment_id TEXT NOT NULL,
    vehicle_profile_id INTEGER NOT NULL REFERENCES vehicle_profile (vehicle_profile_id),
    comfort_score DOUBLE PRECISION NOT NULL CHECK (comfort_score BETWEEN 0 AND 100),
    confidence_score DOUBLE PRECISION NOT NULL CHECK (confidence_score BETWEEN 0 AND 1),
    sample_count BIGINT NOT NULL CHECK (sample_count >= 0),
    score_version TEXT NOT NULL,
    calculated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (segment_id, vehicle_profile_id)
);

-- Spark JDBC write의 staging 대상. 본 테이블과 동일 타입으로 명시 생성해서
-- Spark의 타입 추론에 맡기지 않는다 (overwrite 모드는 테이블이 없으면
-- 그냥 CREATE해버리고, 그 순간 타입은 Spark 추론값이 된다). 의도적으로
-- PK/UNIQUE가 없다 — 중복 유입은 여기서 막지 않고 MERGE 직전
-- 애플리케이션 단(comfort_score/gold_writer.py)에서 검증한다.
CREATE TABLE segment_comfort_score_staging (
    segment_id TEXT NOT NULL,
    vehicle_profile_id INTEGER NOT NULL,
    comfort_score DOUBLE PRECISION NOT NULL,
    confidence_score DOUBLE PRECISION NOT NULL,
    sample_count BIGINT NOT NULL,
    score_version TEXT NOT NULL,
    calculated_at TIMESTAMPTZ NOT NULL
);
```

- [ ] **Step 3: `0003_seed_vehicle_profile_agnostic.sql` 작성**

```sql
-- vehicle_profile_id=0은 "차량 구분 없음" sentinel이다 (OQ-038, accepted
-- 2026-08-16). formula.py::_combined_hourly_score가 두 grain(per-vehicle/
-- vehicle-agnostic) 모두에 동일한 comfort_score.yaml 전역 가중치를 적용하고
-- vehicle-agnostic 경로는 차량별 보정을 하지 않으므로, 이 행의 가중치는
-- 정확히 그 전역 가중치(0.5/0.3/0.2)와 같아야 한다.
--
-- 주의(drift 위험): 이 값은 services/batch-jobs/config/comfort_score.yaml과
-- 별개 파일에 하드코딩된 중복 값이다. comfort_score.yaml의 가중치가
-- 바뀌면 이 파일도 같이 갱신해야 하며 자동 동기화 장치는 없다.
--
-- vehicle_class='ALL'은 실제 차량 등급과 겹치지 않는 sentinel 전용 값이다.
--
-- 이 행은 "샘플 데이터"가 아니라 스키마 무결성의 일부라 db/seeds/가 아닌
-- 마이그레이션에 둔다 — make migrate 한 번으로 스키마와 함께 반드시
-- 적용되고, formula.py의 vehicle-agnostic 행이 FK 위반 없이 들어간다.
INSERT INTO vehicle_profile (
    vehicle_profile_id, profile_name, vehicle_class,
    vertical_weight, longitudinal_weight, lateral_weight,
    is_active, created_at, updated_at
) VALUES (
    0, 'ALL_VEHICLES', 'ALL',
    0.5, 0.3, 0.2,
    TRUE, '2026-08-16T00:00:00Z', '2026-08-16T00:00:00Z'
)
ON CONFLICT (vehicle_profile_id) DO NOTHING;
```

- [ ] **Step 4: `db/README.md` 작성**

```markdown
# db

이 디렉터리는 순수 `.sql` 데이터만 담는다 — 실행 로직은
`services/batch-jobs`의 `batch_jobs.migrate` 모듈에 있다 (루트는 uv
workspace member가 아니라 여기 직접 의존성/테스트를 둘 수 없기 때문).

## migrations/

번호 붙인 raw SQL 파일(`NNNN_설명.sql`). 파일명 순서대로 한 번씩만
적용되고, 적용 이력은 DB 안의 `schema_migrations` 테이블에 기록된다.

**이미 적용된 파일은 절대 수정하지 않는다.** 스키마를 바꾸려면 새 번호의
파일을 추가한다 — 내용이 바뀐 걸 실행기가 체크섬으로 감지해 하드 실패한다.

## 실행

```bash
make migrate
```

내부적으로 `uv run --package batch-jobs batch-jobs migrate-database`를
실행한다. 접속 정보는 저장소 루트 `.env`의 `POSTGRES_HOST`/`PORT`/`DB`/
`USER`/`PASSWORD`를 사용한다.
```

- [ ] **Step 5: 수동 검증 (자동 테스트는 Task 2에서)**

Prerequisites의 `.env` 설정 후:

```bash
make up-postgres
docker compose -f infra/compose/postgres.yaml exec postgres \
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /dev/stdin < db/migrations/0001_create_vehicle_profile.sql
docker compose -f infra/compose/postgres.yaml exec postgres \
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /dev/stdin < db/migrations/0002_create_segment_comfort_score.sql
docker compose -f infra/compose/postgres.yaml exec postgres \
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /dev/stdin < db/migrations/0003_seed_vehicle_profile_agnostic.sql
docker compose -f infra/compose/postgres.yaml exec postgres \
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
  "SELECT vehicle_profile_id, profile_name FROM vehicle_profile WHERE vehicle_profile_id = 0;"
```

Expected: 마지막 쿼리가 `0 | ALL_VEHICLES` 한 행을 반환한다. 셋 다 에러 없이
끝나면 SQL 구문은 유효하다는 뜻이다(체크섬 이력 관리는 Task 2에서 검증).

- [ ] **Step 6: 정리 후 커밋**

이 수동 검증에서 만든 테이블은 Task 2에서 `schema_migrations` 없이 만든
것이므로 지운다(Task 2의 자동 실행기 테스트가 처음부터 다시 만들어야 이력
관리가 검증됨):

```bash
docker compose -f infra/compose/postgres.yaml exec postgres \
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
  "DROP TABLE segment_comfort_score, segment_comfort_score_staging, vehicle_profile CASCADE;"
git add db/migrations/ db/README.md
git commit -m "feat: add segment_comfort_score gold migrations (#129)"
```

---

### Task 2: 마이그레이션 실행기 (`batch_jobs.migrate`)

**Files:**
- Create: `services/batch-jobs/src/batch_jobs/db_lock_keys.py`
- Create: `services/batch-jobs/src/batch_jobs/migrate.py`
- Create: `services/batch-jobs/tests/test_migrate.py`
- Modify: `services/batch-jobs/pyproject.toml` (psycopg2-binary 추가)
- Modify: `uv.lock` (via `uv lock`)

**Interfaces:**
- Consumes: 없음
- Produces:
  - `batch_jobs.db_lock_keys.MIGRATION_LOCK_KEY: int`,
    `batch_jobs.db_lock_keys.GOLD_JOB_STAGING_LOCK_KEY: int`
  - `batch_jobs.migrate.MigrationConfig` (dataclass: `migrations_dir: Path`,
    `postgres_host: str`, `postgres_port: int`, `postgres_db: str`,
    `postgres_user: str`, `postgres_password: str`, `.from_env(env=None)`)
  - `batch_jobs.migrate.MigrationResult` (dataclass: `applied: tuple[str,
    ...]`, `skipped: tuple[str, ...]`)
  - `batch_jobs.migrate.run_migrations(migrations_dir: Path, connection) ->
    MigrationResult` — `connection`은 DB-API 커넥션(psycopg2 실물 또는 Task
    내 fake). Task 3의 CLI가 이 함수를 호출한다.

- [ ] **Step 1: `psycopg2-binary` 의존성 추가**

`services/batch-jobs/pyproject.toml`의 `dependencies` 배열에 알파벳 순서로
삽입 (기존 목록이 알파벳순이므로 `pandas`와 `pyarrow` 사이):

```toml
dependencies = [
    "de4-core",
    "duckdb>=1.4.3",
    "numpy>=2.0.0",
    "pandas>=2.0.0",
    "psycopg2-binary>=2.9,<3.0",
    "pyarrow>=25.0.1",
    "pyproj>=3.7.2",
    "pyshp>=3.0.2",
    "pyspark>=4.1.3,<5.0.0",
    "pyyaml>=6.0,<7.0",
    "shapely>=2.1.2",
]
```

Run: `cd /Users/yong/PycharmProjects/DE_team4-4una && uv lock`
Expected: `uv.lock`이 갱신되고 명령이 에러 없이 끝난다.

- [ ] **Step 2: `db_lock_keys.py` 작성**

```python
"""Postgres advisory-lock 키 레지스트리.

이 저장소에서 pg_advisory_lock을 쓰는 곳은 여기 상수를 통해서만 키를
가져온다. 직접 정수를 하드코딩하면 다른 사용처와 우연히 같은 값을 골라
서로 다른 락이 같은 자원인 것처럼 충돌할 수 있다.
"""

from __future__ import annotations

# batch_jobs.migrate가 마이그레이션 적용 중 동시 실행을 막는 데 쓴다.
MIGRATION_LOCK_KEY = 1001

# comfort_score.gold_writer가 staging 테이블 write~MERGE 구간을 보호하는 데 쓴다.
GOLD_JOB_STAGING_LOCK_KEY = 1002
```

- [ ] **Step 3: 실패하는 테스트 작성 (`test_migrate.py`)**

```python
"""Tests for batch_jobs/migrate.py (#129)."""

from __future__ import annotations

from pathlib import Path

import pytest
from batch_jobs.migrate import run_migrations


class FakeCursor:
    def __init__(self, connection: "FakeConnection") -> None:
        self.connection = connection
        self._current: tuple | None = None

    def execute(self, sql: str, params: tuple = ()) -> None:
        self.connection.executed.append(sql.strip())
        normalized = sql.strip()
        if normalized.startswith("SELECT pg_try_advisory_lock"):
            self._current = (True,)
        elif normalized.startswith("SELECT pg_advisory_unlock"):
            self._current = None
        elif normalized.startswith("SELECT checksum FROM schema_migrations"):
            (filename,) = params
            checksum = self.connection.applied.get(filename)
            self._current = (checksum,) if checksum is not None else None
        elif normalized.startswith("INSERT INTO schema_migrations"):
            filename, checksum = params
            self.connection.applied[filename] = checksum
            self._current = None
        else:
            # CREATE TABLE IF NOT EXISTS schema_migrations, 각 마이그레이션
            # 파일 본문 등 — 실행됐다는 사실만 기록하고 결과행은 없다.
            self._current = None

    def fetchone(self) -> tuple | None:
        return self._current

    def close(self) -> None:
        pass


class FakeConnection:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.applied: dict[str, str] = {}
        self.committed = 0
        self.rolled_back = 0

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.committed += 1

    def rollback(self) -> None:
        self.rolled_back += 1


def write_migration(directory: Path, name: str, sql: str) -> None:
    (directory / name).write_text(sql)


def test_applies_new_migration_files_in_order(tmp_path):
    write_migration(tmp_path, "0001_a.sql", "CREATE TABLE a (id INT);")
    write_migration(tmp_path, "0002_b.sql", "CREATE TABLE b (id INT);")
    connection = FakeConnection()

    result = run_migrations(tmp_path, connection)

    assert result.applied == ("0001_a.sql", "0002_b.sql")
    assert result.skipped == ()
    assert set(connection.applied) == {"0001_a.sql", "0002_b.sql"}


def test_skips_already_applied_migration_with_matching_checksum(tmp_path):
    write_migration(tmp_path, "0001_a.sql", "CREATE TABLE a (id INT);")
    connection = FakeConnection()
    run_migrations(tmp_path, connection)

    result = run_migrations(tmp_path, connection)

    assert result.applied == ()
    assert result.skipped == ("0001_a.sql",)


def test_raises_when_an_applied_migration_file_is_modified(tmp_path):
    write_migration(tmp_path, "0001_a.sql", "CREATE TABLE a (id INT);")
    connection = FakeConnection()
    run_migrations(tmp_path, connection)
    write_migration(tmp_path, "0001_a.sql", "CREATE TABLE a (id INT, extra INT);")

    with pytest.raises(ValueError, match="0001_a.sql"):
        run_migrations(tmp_path, connection)
```

- [ ] **Step 4: 테스트 실행해서 실패 확인**

Run: `cd /Users/yong/PycharmProjects/DE_team4-4una && uv run --package batch-jobs pytest services/batch-jobs/tests/test_migrate.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'batch_jobs.migrate'`

- [ ] **Step 5: `migrate.py` 최소 구현**

```python
"""Apply db/migrations/*.sql to Postgres, tracked in schema_migrations (#129)."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from batch_jobs.db_lock_keys import MIGRATION_LOCK_KEY

DEFAULT_MIGRATIONS_DIR = Path("db/migrations")


@dataclass(frozen=True, slots=True)
class MigrationConfig:
    migrations_dir: Path
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> MigrationConfig:
        source = env if env is not None else os.environ
        return cls(
            migrations_dir=Path(
                source.get("DB_MIGRATIONS_DIR") or DEFAULT_MIGRATIONS_DIR
            ),
            postgres_host=_require(source, "POSTGRES_HOST"),
            postgres_port=int(_require(source, "POSTGRES_PORT")),
            postgres_db=_require(source, "POSTGRES_DB"),
            postgres_user=_require(source, "POSTGRES_USER"),
            postgres_password=_require(source, "POSTGRES_PASSWORD"),
        )


def _require(source: Mapping[str, str], key: str) -> str:
    value = source.get(key)
    if not value:
        raise ValueError(f"{key} must be set")
    return value


@dataclass(frozen=True, slots=True)
class MigrationResult:
    applied: tuple[str, ...]
    skipped: tuple[str, ...]


def run_migrations(migrations_dir: Path, connection) -> MigrationResult:
    """migrations_dir의 *.sql을 파일명 순으로 적용한다.

    connection: psycopg2 커넥션(또는 테스트용 fake). 커밋/롤백 책임은 이
    함수가 진다 — 호출자는 connection.close()만 하면 된다.
    """
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT pg_try_advisory_lock(%s)", (MIGRATION_LOCK_KEY,))
        acquired = cursor.fetchone()
        if acquired is None or not acquired[0]:
            raise RuntimeError("another migration run holds the migration lock")

        cursor.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            "filename TEXT PRIMARY KEY, checksum TEXT NOT NULL, "
            "applied_at TIMESTAMPTZ NOT NULL)"
        )
        connection.commit()

        applied: list[str] = []
        skipped: list[str] = []
        for path in sorted(migrations_dir.glob("*.sql")):
            checksum = hashlib.sha256(path.read_bytes()).hexdigest()
            cursor.execute(
                "SELECT checksum FROM schema_migrations WHERE filename = %s",
                (path.name,),
            )
            existing = cursor.fetchone()
            if existing is None:
                cursor.execute(path.read_text())
                cursor.execute(
                    "INSERT INTO schema_migrations (filename, checksum, applied_at) "
                    "VALUES (%s, %s, now())",
                    (path.name, checksum),
                )
                connection.commit()
                applied.append(path.name)
            elif existing[0] != checksum:
                raise ValueError(
                    f"{path.name} has been modified after being applied "
                    "(checksum mismatch) — migrations must never be edited "
                    "after being applied; add a new migration file instead"
                )
            else:
                skipped.append(path.name)
        return MigrationResult(tuple(applied), tuple(skipped))
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.execute("SELECT pg_advisory_unlock(%s)", (MIGRATION_LOCK_KEY,))
        cursor.close()
```

- [ ] **Step 6: 테스트 실행해서 통과 확인**

Run: `cd /Users/yong/PycharmProjects/DE_team4-4una && uv run --package batch-jobs pytest services/batch-jobs/tests/test_migrate.py -v`
Expected: PASS (3 passed)

- [ ] **Step 7: lint + 커밋**

```bash
uv run --all-packages ruff check services/batch-jobs/src/batch_jobs/migrate.py services/batch-jobs/src/batch_jobs/db_lock_keys.py services/batch-jobs/tests/test_migrate.py
git add services/batch-jobs/pyproject.toml uv.lock services/batch-jobs/src/batch_jobs/db_lock_keys.py services/batch-jobs/src/batch_jobs/migrate.py services/batch-jobs/tests/test_migrate.py
git commit -m "feat: add checksum-tracked migration runner (#129)"
```

---

### Task 3: CLI `migrate-database` 서브커맨드 + Makefile + `.env.example`

**Files:**
- Modify: `services/batch-jobs/src/batch_jobs/cli.py`
- Modify: `Makefile`
- Modify: `.env.example`

**Interfaces:**
- Consumes: `batch_jobs.migrate.MigrationConfig`, `batch_jobs.migrate.run_migrations`
  (Task 2)
- Produces: `batch-jobs migrate-database` CLI 커맨드, `make migrate` 기본
  동작. 이후 Task는 이 커맨드에 의존하지 않는다(터미널 Task).

- [ ] **Step 1: `cli.py`에 서브커맨드 정의 추가**

`build_parser()` 함수 안, `score_parser` 정의 다음에 추가:

```python
    migrate_parser = subparsers.add_parser("migrate-database")
```

- [ ] **Step 2: 실행 함수 추가**

`run_hourly_scoring` 함수 다음에 추가:

```python
def run_migrate_database() -> None:
    import psycopg2
    from batch_jobs.migrate import MigrationConfig, run_migrations

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    config = MigrationConfig.from_env()
    connection = psycopg2.connect(
        host=config.postgres_host,
        port=config.postgres_port,
        dbname=config.postgres_db,
        user=config.postgres_user,
        password=config.postgres_password,
    )
    try:
        result = run_migrations(config.migrations_dir, connection)
    finally:
        connection.close()
    print(
        json.dumps(
            {"applied": list(result.applied), "skipped": list(result.skipped)},
            sort_keys=True,
        )
    )
```

- [ ] **Step 3: `main()` 디스패치에 분기 추가**

`if arguments.command == "score-hourly-comfort":` 블록 다음에:

```python
    if arguments.command == "migrate-database":
        run_migrate_database()
        return
```

- [ ] **Step 4: `Makefile`에 기본값 한 줄 추가**

`COMPOSE_DIR := infra/compose` 다음 줄에:

```makefile
MIGRATE_CMD ?= uv run --package batch-jobs batch-jobs migrate-database
```

(기존에 `MIGRATE_CMD`를 override해서 쓰던 사용자는 그대로 유지된다.)

- [ ] **Step 5: `.env.example`에 `DB_MIGRATIONS_DIR` 추가**

`HOURLY_COMFORT_RUN_ID=` 다음 줄에:

```
DB_MIGRATIONS_DIR=
```

- [ ] **Step 6: 동작 확인** (Prerequisites의 `.env`/`make up-postgres` 필요)

Run: `cd /Users/yong/PycharmProjects/DE_team4-4una && make migrate`
Expected: `{"applied": ["0001_create_vehicle_profile.sql", "0002_create_segment_comfort_score.sql", "0003_seed_vehicle_profile_agnostic.sql"], "skipped": []}` 출력.

Run: `make migrate` (다시 한 번)
Expected: `{"applied": [], "skipped": ["0001_create_vehicle_profile.sql", "0002_create_segment_comfort_score.sql", "0003_seed_vehicle_profile_agnostic.sql"]}` — 재실행이 안전함을 확인.

- [ ] **Step 7: lint + 커밋**

```bash
uv run --all-packages ruff check services/batch-jobs/src/batch_jobs/cli.py
git add services/batch-jobs/src/batch_jobs/cli.py Makefile .env.example
git commit -m "feat: wire migrate-database CLI command (#129)"
```

---

### Task 4: `comfort_score/gold_writer.py` (staging → 검증 → MERGE)

**Files:**
- Create: `services/batch-jobs/src/comfort_score/gold_writer.py`
- Create: `services/batch-jobs/tests/test_gold_writer.py`

**Interfaces:**
- Consumes: `batch_jobs.db_lock_keys.GOLD_JOB_STAGING_LOCK_KEY` (Task 2)
- Produces:
  - `comfort_score.gold_writer.STAGING_TABLE: str`,
    `comfort_score.gold_writer.TARGET_TABLE: str`,
    `comfort_score.gold_writer.EXPECTED_STAGING_COLUMNS: dict[str, str]`
  - `comfort_score.gold_writer.WriteSummary` (dataclass: `staging_count:
    int`, `inserted_count: int`, `updated_count: int`)
  - `comfort_score.gold_writer.write_segment_comfort_scores(df:
    DataFrame, jdbc_url: str, postgres_user: str, postgres_password: str,
    connection) -> WriteSummary` — Task 5가 이 함수를 호출한다.
  - private 헬퍼(단위 테스트 대상, `loader.py`의 `_validate_schema`처럼
    직접 import해서 테스트한다): `_acquire_lock`, `_release_lock`,
    `_validate_staging_table_shape`, `_validate_no_duplicates_or_nan`,
    `_merge`, `_write_staging`, `_truncate_staging`.

- [ ] **Step 1: 실패하는 단위 테스트 작성 (`test_gold_writer.py`)**

```python
"""Tests for comfort_score/gold_writer.py (#129)."""

from __future__ import annotations

import pytest
from comfort_score.gold_writer import (
    EXPECTED_STAGING_COLUMNS,
    _acquire_lock,
    _merge,
    _validate_no_duplicates_or_nan,
    _validate_staging_table_shape,
)


class FakeCursor:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self._queued: list[object] = []

    def queue(self, result) -> None:
        """다음 execute() 이후 fetchone()이 반환할 값을 예약한다."""
        self._queued.append(result)

    def execute(self, sql: str, params: tuple = ()) -> None:
        self.executed.append(sql.strip())
        self._current = self._queued.pop(0) if self._queued else None

    def fetchone(self):
        return self._current

    def fetchall(self):
        return self._current or []


def test_acquire_lock_raises_when_already_held():
    cursor = FakeCursor()
    cursor.queue((False,))

    with pytest.raises(RuntimeError, match="lock"):
        _acquire_lock(cursor)


def test_acquire_lock_succeeds_when_available():
    cursor = FakeCursor()
    cursor.queue((True,))

    _acquire_lock(cursor)  # must not raise


def test_validate_staging_table_shape_raises_when_table_missing():
    cursor = FakeCursor()
    cursor.queue([])

    with pytest.raises(RuntimeError, match="make migrate"):
        _validate_staging_table_shape(cursor)


def test_validate_staging_table_shape_raises_on_type_mismatch():
    cursor = FakeCursor()
    wrong_columns = dict(EXPECTED_STAGING_COLUMNS)
    wrong_columns["sample_count"] = "integer"  # 실제는 bigint여야 함
    cursor.queue(list(wrong_columns.items()))

    with pytest.raises(RuntimeError, match="sample_count"):
        _validate_staging_table_shape(cursor)


def test_validate_staging_table_shape_passes_when_columns_match():
    cursor = FakeCursor()
    cursor.queue(list(EXPECTED_STAGING_COLUMNS.items()))

    _validate_staging_table_shape(cursor)  # must not raise


def test_validate_no_duplicates_raises_on_duplicate_keys():
    cursor = FakeCursor()
    cursor.queue((3, 2))  # 3 rows, 2 distinct keys -> 1 duplicate

    with pytest.raises(ValueError, match="duplicate"):
        _validate_no_duplicates_or_nan(cursor)


def test_validate_no_duplicates_raises_on_nan_or_infinity_scores():
    cursor = FakeCursor()
    cursor.queue((2, 2))  # no duplicates
    cursor.queue((1,))  # 1 row with NaN/Infinity

    with pytest.raises(ValueError, match="NaN"):
        _validate_no_duplicates_or_nan(cursor)


def test_validate_no_duplicates_passes_when_clean():
    cursor = FakeCursor()
    cursor.queue((2, 2))
    cursor.queue((0,))

    _validate_no_duplicates_or_nan(cursor)  # must not raise


def test_merge_returns_inserted_and_updated_counts():
    cursor = FakeCursor()
    cursor.queue((7, 3))

    inserted, updated = _merge(cursor)

    assert (inserted, updated) == (7, 3)
    assert "ON CONFLICT" in cursor.executed[-1]
```

- [ ] **Step 2: 테스트 실행해서 실패 확인**

Run: `cd /Users/yong/PycharmProjects/DE_team4-4una && uv run --package batch-jobs pytest services/batch-jobs/tests/test_gold_writer.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'comfort_score.gold_writer'`

- [ ] **Step 3: `gold_writer.py` 구현**

```python
"""Write segment_comfort_score results to PostgreSQL via a staging table +
SQL MERGE (#129).

Spark JDBC는 네이티브 UPSERT를 지원하지 않으므로, staging 테이블에
overwrite로 쓴 뒤 같은 커넥션에서 SQL MERGE(INSERT ... ON CONFLICT)를 한 번
실행한다. staging 보호를 위해 advisory lock을 Spark write보다 먼저 잡는다
— 잠그지 않은 채 write를 먼저 하면 두 실행이 겹칠 때 "검증한 데이터와
적재하는 데이터가 다른" 레이스가 생기기 때문이다. 자세한 설계 근거는
docs/superpowers/specs/2026-08-16-segment-comfort-score-gold-load-design.md 참고.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame

from batch_jobs.db_lock_keys import GOLD_JOB_STAGING_LOCK_KEY

STAGING_TABLE = "segment_comfort_score_staging"
TARGET_TABLE = "segment_comfort_score"

# db/migrations/0002_create_segment_comfort_score.sql의 staging DDL과
# 정확히 일치해야 한다. information_schema.columns.data_type이 실제로
# 반환하는 문자열 그대로다.
EXPECTED_STAGING_COLUMNS = {
    "segment_id": "text",
    "vehicle_profile_id": "integer",
    "comfort_score": "double precision",
    "confidence_score": "double precision",
    "sample_count": "bigint",
    "score_version": "text",
    "calculated_at": "timestamp with time zone",
}

_MERGE_SQL = f"""
WITH upserted AS (
    INSERT INTO {TARGET_TABLE}
      (segment_id, vehicle_profile_id, comfort_score, confidence_score,
       sample_count, score_version, calculated_at)
    SELECT segment_id, vehicle_profile_id, comfort_score, confidence_score,
           sample_count, score_version, calculated_at
    FROM {STAGING_TABLE}
    ON CONFLICT (segment_id, vehicle_profile_id) DO UPDATE SET
      comfort_score = EXCLUDED.comfort_score,
      confidence_score = EXCLUDED.confidence_score,
      sample_count = EXCLUDED.sample_count,
      score_version = EXCLUDED.score_version,
      calculated_at = EXCLUDED.calculated_at
    RETURNING (xmax = 0) AS inserted
)
SELECT count(*) FILTER (WHERE inserted)     AS inserted_count,
       count(*) FILTER (WHERE NOT inserted) AS updated_count
FROM upserted;
"""


@dataclass(frozen=True, slots=True)
class WriteSummary:
    staging_count: int
    inserted_count: int
    updated_count: int


def write_segment_comfort_scores(
    df: DataFrame,
    jdbc_url: str,
    postgres_user: str,
    postgres_password: str,
    connection,
) -> WriteSummary:
    """connection: 대상 Postgres에 대한 DB-API 커넥션(psycopg2 또는 테스트 fake).

    호출자(comfort_score.gold_job)가 connection을 열고 닫는다 — 이 함수는
    commit/rollback만 책임진다.
    """
    cursor = connection.cursor()
    try:
        _acquire_lock(cursor)
        _validate_staging_table_shape(cursor)
        _write_staging(df, jdbc_url, postgres_user, postgres_password)
        _validate_no_duplicates_or_nan(cursor)
        inserted_count, updated_count = _merge(cursor)
        connection.commit()
        _truncate_staging(cursor)
        connection.commit()
        return WriteSummary(
            staging_count=inserted_count + updated_count,
            inserted_count=inserted_count,
            updated_count=updated_count,
        )
    except Exception:
        connection.rollback()
        raise
    finally:
        _release_lock(cursor)
        cursor.close()


def _acquire_lock(cursor) -> None:
    cursor.execute("SELECT pg_try_advisory_lock(%s)", (GOLD_JOB_STAGING_LOCK_KEY,))
    acquired = cursor.fetchone()
    if acquired is None or not acquired[0]:
        raise RuntimeError(
            "another segment_comfort_score gold job run holds the staging lock"
        )


def _release_lock(cursor) -> None:
    cursor.execute("SELECT pg_advisory_unlock(%s)", (GOLD_JOB_STAGING_LOCK_KEY,))


def _validate_staging_table_shape(cursor) -> None:
    cursor.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = %s",
        (STAGING_TABLE,),
    )
    actual = dict(cursor.fetchall())
    if not actual:
        raise RuntimeError(f"{STAGING_TABLE} does not exist — run `make migrate` first")
    mismatched = {
        column: (expected, actual.get(column))
        for column, expected in EXPECTED_STAGING_COLUMNS.items()
        if actual.get(column) != expected
    }
    if mismatched:
        raise RuntimeError(
            f"{STAGING_TABLE} schema mismatch (expected vs actual): {mismatched} "
            "— run `make migrate` first"
        )


def _write_staging(
    df: DataFrame, jdbc_url: str, postgres_user: str, postgres_password: str
) -> None:
    (
        df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", STAGING_TABLE)
        .option("user", postgres_user)
        .option("password", postgres_password)
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )


def _validate_no_duplicates_or_nan(cursor) -> None:
    cursor.execute(
        f"SELECT count(*), count(DISTINCT (segment_id, vehicle_profile_id)) "
        f"FROM {STAGING_TABLE}"
    )
    total, distinct = cursor.fetchone()
    if total != distinct:
        raise ValueError(
            f"{STAGING_TABLE} has {total - distinct} duplicate "
            "(segment_id, vehicle_profile_id) rows — formula.py should never "
            "produce these; refusing to merge"
        )
    cursor.execute(
        f"SELECT count(*) FROM {STAGING_TABLE} "
        "WHERE comfort_score = 'NaN' OR confidence_score = 'NaN' "
        "OR comfort_score = 'Infinity' OR confidence_score = 'Infinity'"
    )
    (bad_count,) = cursor.fetchone()
    if bad_count:
        raise ValueError(
            f"{STAGING_TABLE} has {bad_count} row(s) with NaN/Infinity scores"
        )


def _merge(cursor) -> tuple[int, int]:
    cursor.execute(_MERGE_SQL)
    inserted_count, updated_count = cursor.fetchone()
    return inserted_count, updated_count


def _truncate_staging(cursor) -> None:
    cursor.execute(f"TRUNCATE {STAGING_TABLE}")
```

- [ ] **Step 4: 테스트 실행해서 통과 확인**

Run: `cd /Users/yong/PycharmProjects/DE_team4-4una && uv run --package batch-jobs pytest services/batch-jobs/tests/test_gold_writer.py -v`
Expected: PASS (8 passed)

- [ ] **Step 5: lint + 커밋**

```bash
uv run --all-packages ruff check services/batch-jobs/src/comfort_score/gold_writer.py services/batch-jobs/tests/test_gold_writer.py
git add services/batch-jobs/src/comfort_score/gold_writer.py services/batch-jobs/tests/test_gold_writer.py
git commit -m "feat: add staging+MERGE writer for segment_comfort_score (#129)"
```

---

### Task 5: `comfort_score/gold_job.py` (Spark 파이프라인 오케스트레이션)

**Files:**
- Create: `services/batch-jobs/src/comfort_score/gold_job.py`
- Create: `services/batch-jobs/tests/test_gold_job.py`
- Modify: `.env.example`

**Interfaces:**
- Consumes:
  - `comfort_score.loader.load_hourly_comfort_score_for_gold(spark,
    data_lake_uri, as_of, window_hours) -> DataFrame` (기존, #117)
  - `comfort_score.config.load_comfort_score_config(path) ->
    ComfortScoreConfig`, `comfort_score.config.DEFAULT_COMFORT_SCORE_CONFIG_PATH`
    (기존)
  - `comfort_score.formula.compute_segment_comfort_scores(hourly_df,
    config) -> DataFrame` (기존, #127)
  - `comfort_score.gold_writer.write_segment_comfort_scores(df, jdbc_url,
    postgres_user, postgres_password, connection) -> WriteSummary` (Task 4)
- Produces:
  - `comfort_score.gold_job.SegmentComfortScoreJobConfig` (dataclass:
    `data_lake_uri: str`, `window_hours: int`, `comfort_score_config_path:
    Path`, `postgres_host: str`, `postgres_port: int`, `postgres_db: str`,
    `postgres_user: str`, `postgres_password: str`, property `jdbc_url:
    str`, `.from_env(env=None)`)
  - `comfort_score.gold_job.SegmentComfortScoreJobSummary` (dataclass:
    `scored_count: int`, `merged_count: int`, `inserted_count: int`,
    `updated_count: int`)
  - `comfort_score.gold_job.build_spark_session() -> SparkSession`
  - `comfort_score.gold_job.run_segment_comfort_score_job(spark, config,
    as_of: datetime, connection) -> SegmentComfortScoreJobSummary` — Task 6의
    CLI가 이 함수를 호출한다.

- [ ] **Step 1: 실패하는 테스트 작성 (`test_gold_job.py`)**

```python
"""Tests for comfort_score/gold_job.py (#129)."""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime

import pytest
from batch_jobs.schemas import HOURLY_COMFORT_SCORE_SCHEMA
from comfort_score.gold_job import (
    SegmentComfortScoreJobConfig,
    SegmentComfortScoreJobSummary,
    _attach_calculated_at,
    _validate_as_of,
    run_segment_comfort_score_job,
)
from comfort_score.config import DEFAULT_COMFORT_SCORE_CONFIG_PATH
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("gold-job-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def make_config(tmp_path, data_lake_uri: str) -> SegmentComfortScoreJobConfig:
    return SegmentComfortScoreJobConfig(
        data_lake_uri=data_lake_uri,
        window_hours=168,
        comfort_score_config_path=DEFAULT_COMFORT_SCORE_CONFIG_PATH,
        postgres_host="unused",
        postgres_port=5432,
        postgres_db="unused",
        postgres_user="unused",
        postgres_password="unused",
    )


def test_validate_as_of_raises_on_naive_datetime():
    with pytest.raises(ValueError, match="timezone-aware"):
        _validate_as_of(datetime(2026, 8, 16, 0, 0))  # noqa: DTZ001


def test_validate_as_of_accepts_aware_datetime():
    _validate_as_of(datetime(2026, 8, 16, 0, 0, tzinfo=UTC))  # must not raise


def test_attach_calculated_at_uses_the_same_as_of_literal_for_every_row(spark):
    as_of = datetime(2026, 8, 16, 3, 0, tzinfo=UTC)
    df = spark.createDataFrame(
        [("seg-1", 1), ("seg-2", 2)], "segment_id string, vehicle_profile_id int"
    )

    result = _attach_calculated_at(df, as_of)

    epochs = [row[0] for row in result.select(F.unix_timestamp("calculated_at")).collect()]
    assert epochs == [int(as_of.timestamp())] * 2


def test_returns_zero_merged_count_and_skips_write_when_window_has_no_rows(
    spark, tmp_path
):
    input_path = tmp_path / "silver" / "hourly_comfort_score"
    spark.createDataFrame([], HOURLY_COMFORT_SCORE_SCHEMA).write.parquet(str(input_path))
    config = make_config(tmp_path, str(tmp_path))
    as_of = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)

    summary = run_segment_comfort_score_job(spark, config, as_of, connection=None)

    assert summary == SegmentComfortScoreJobSummary(0, 0, 0, 0)
```

- [ ] **Step 2: 테스트 실행해서 실패 확인**

Run: `cd /Users/yong/PycharmProjects/DE_team4-4una && uv run --package batch-jobs pytest services/batch-jobs/tests/test_gold_job.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'comfort_score.gold_job'`

- [ ] **Step 3: `gold_job.py` 구현**

```python
"""Spark batch entry point for Gold segment_comfort_score loading (#129)."""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from comfort_score.config import DEFAULT_COMFORT_SCORE_CONFIG_PATH, load_comfort_score_config
from comfort_score.formula import compute_segment_comfort_scores
from comfort_score.gold_writer import write_segment_comfort_scores
from comfort_score.loader import DEFAULT_WINDOW_HOURS, load_hourly_comfort_score_for_gold

logger = logging.getLogger(__name__)

# 운영 배포 이미지에는 미리 구워 넣는 걸 후속 과제로 남긴다 — 로컬 개발에서는
# Spark가 Maven에서 자동으로 받는다. 버전은 정확히 고정한다.
POSTGRES_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.4"


@dataclass(frozen=True, slots=True)
class SegmentComfortScoreJobConfig:
    data_lake_uri: str
    window_hours: int
    comfort_score_config_path: Path
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> SegmentComfortScoreJobConfig:
        source = env if env is not None else os.environ
        return cls(
            data_lake_uri=source.get("SEGMENT_COMFORT_SCORE_DATA_LAKE_URI")
            or "data/local-lake",
            window_hours=int(
                source.get("SEGMENT_COMFORT_SCORE_WINDOW_HOURS") or DEFAULT_WINDOW_HOURS
            ),
            comfort_score_config_path=Path(
                source.get("SEGMENT_COMFORT_SCORE_CONFIG_PATH")
                or DEFAULT_COMFORT_SCORE_CONFIG_PATH
            ),
            postgres_host=_require(source, "POSTGRES_HOST"),
            postgres_port=int(_require(source, "POSTGRES_PORT")),
            postgres_db=_require(source, "POSTGRES_DB"),
            postgres_user=_require(source, "POSTGRES_USER"),
            postgres_password=_require(source, "POSTGRES_PASSWORD"),
        )


def _require(source: Mapping[str, str], key: str) -> str:
    value = source.get(key)
    if not value:
        raise ValueError(f"{key} must be set")
    return value


@dataclass(frozen=True, slots=True)
class SegmentComfortScoreJobSummary:
    scored_count: int
    merged_count: int
    inserted_count: int
    updated_count: int


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("segment-comfort-score-gold-load")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", POSTGRES_JDBC_PACKAGE)
        .getOrCreate()
    )


def run_segment_comfort_score_job(
    spark: SparkSession,
    config: SegmentComfortScoreJobConfig,
    as_of: datetime,
    connection,
) -> SegmentComfortScoreJobSummary:
    """168h 윈도우를 읽어 Gold 집계 후 PostgreSQL에 UPSERT한다.

    connection: 대상 Postgres에 대한 DB-API 커넥션. 0행이면 이 함수는 이
    connection을 전혀 건드리지 않고 반환한다(락도 잡지 않음).
    """
    _validate_as_of(as_of)
    hourly_df = load_hourly_comfort_score_for_gold(
        spark, config.data_lake_uri, as_of, config.window_hours
    )
    scoring_config = load_comfort_score_config(config.comfort_score_config_path)
    scored = _attach_calculated_at(
        compute_segment_comfort_scores(hourly_df, scoring_config), as_of
    ).persist(StorageLevel.MEMORY_AND_DISK)

    try:
        scored_count = scored.count()
        if scored_count == 0:
            logger.warning(
                "segment comfort score gold job produced 0 rows; skipping merge"
            )
            return SegmentComfortScoreJobSummary(0, 0, 0, 0)

        write_summary = write_segment_comfort_scores(
            scored, config.jdbc_url, config.postgres_user, config.postgres_password,
            connection,
        )
        summary = SegmentComfortScoreJobSummary(
            scored_count=scored_count,
            merged_count=write_summary.staging_count,
            inserted_count=write_summary.inserted_count,
            updated_count=write_summary.updated_count,
        )
        logger.info(
            "segment comfort score gold job finished scored=%d inserted=%d updated=%d",
            summary.scored_count,
            summary.inserted_count,
            summary.updated_count,
        )
        return summary
    finally:
        scored.unpersist()


def _attach_calculated_at(df: DataFrame, as_of: datetime) -> DataFrame:
    # F.current_timestamp()가 아니라 as_of 리터럴을 드라이버에서 한 번만
    # 계산해 고정한다 — 태스크마다 다른 값이 나오면 같은 실행 안에서도
    # calculated_at이 흔들려 stale 판정/재실행 비교가 어려워진다.
    return df.withColumn("calculated_at", F.lit(as_of))


def _validate_as_of(as_of: datetime) -> None:
    if as_of.utcoffset() is None:
        raise ValueError("as_of must be timezone-aware")
```

- [ ] **Step 4: 테스트 실행해서 통과 확인**

Run: `cd /Users/yong/PycharmProjects/DE_team4-4una && uv run --package batch-jobs pytest services/batch-jobs/tests/test_gold_job.py -v`
Expected: PASS (4 passed)

- [ ] **Step 5: `.env.example`에 env var 추가**

`HOURLY_COMFORT_SCORING_CONFIG_PATH=` 다음 줄에:

```
SEGMENT_COMFORT_SCORE_DATA_LAKE_URI=
SEGMENT_COMFORT_SCORE_WINDOW_HOURS=
SEGMENT_COMFORT_SCORE_CONFIG_PATH=
```

- [ ] **Step 6: lint + 커밋**

```bash
uv run --all-packages ruff check services/batch-jobs/src/comfort_score/gold_job.py services/batch-jobs/tests/test_gold_job.py
git add services/batch-jobs/src/comfort_score/gold_job.py services/batch-jobs/tests/test_gold_job.py .env.example
git commit -m "feat: add segment_comfort_score gold job orchestration (#129)"
```

---

### Task 6: CLI `load-segment-comfort-score` 서브커맨드

**Files:**
- Modify: `services/batch-jobs/src/batch_jobs/cli.py`

**Interfaces:**
- Consumes: `comfort_score.gold_job.SegmentComfortScoreJobConfig`,
  `comfort_score.gold_job.build_spark_session`,
  `comfort_score.gold_job.run_segment_comfort_score_job` (Task 5)
- Produces: `batch-jobs load-segment-comfort-score --as-of <ISO8601>` CLI
  커맨드. 터미널 Task — 이후 아무도 이 CLI 함수를 import하지 않는다.

- [ ] **Step 1: 서브커맨드 정의 추가**

`build_parser()`의 `migrate_parser` 정의 다음:

```python
    gold_parser = subparsers.add_parser("load-segment-comfort-score")
    gold_parser.add_argument("--as-of", required=True)
```

- [ ] **Step 2: 실행 함수 추가**

`run_migrate_database` 다음:

```python
def run_segment_comfort_score_loading(arguments: argparse.Namespace) -> None:
    import psycopg2
    from comfort_score.gold_job import (
        SegmentComfortScoreJobConfig,
        build_spark_session,
        run_segment_comfort_score_job,
    )

    as_of = datetime.fromisoformat(arguments.as_of)
    if as_of.utcoffset() is None:
        raise ValueError(
            "--as-of must include a UTC offset, e.g. 2026-08-16T00:00:00+00:00"
        )

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    config = SegmentComfortScoreJobConfig.from_env()
    spark = build_spark_session()
    connection = psycopg2.connect(
        host=config.postgres_host,
        port=config.postgres_port,
        dbname=config.postgres_db,
        user=config.postgres_user,
        password=config.postgres_password,
    )
    try:
        summary = run_segment_comfort_score_job(spark, config, as_of, connection)
        print(
            json.dumps(
                {
                    "scored_count": summary.scored_count,
                    "merged_count": summary.merged_count,
                    "inserted_count": summary.inserted_count,
                    "updated_count": summary.updated_count,
                },
                sort_keys=True,
            )
        )
    finally:
        connection.close()
        spark.stop()
```

- [ ] **Step 3: `main()` 디스패치에 분기 추가**

`if arguments.command == "migrate-database":` 블록 다음:

```python
    if arguments.command == "load-segment-comfort-score":
        run_segment_comfort_score_loading(arguments)
        return
```

- [ ] **Step 4: 동작 확인** (Prerequisites + Task 3의 `make migrate` 완료 필요)

Run:
```bash
uv run --package batch-jobs batch-jobs load-segment-comfort-score --as-of 2026-08-16T00:00:00+00:00
```
Expected: 로컬 `data/local-lake`에 `silver/hourly_comfort_score`가 없으면
`scored_count=0`으로 정상 종료(에러 아님). 있다면 실제 적재 결과 JSON 출력.

- [ ] **Step 5: lint + 커밋**

```bash
uv run --all-packages ruff check services/batch-jobs/src/batch_jobs/cli.py
git add services/batch-jobs/src/batch_jobs/cli.py
git commit -m "feat: wire load-segment-comfort-score CLI command (#129)"
```

---

### Task 7: 통합 테스트 (실제 Postgres)

**Files:**
- Create: `services/batch-jobs/tests/test_segment_comfort_score_integration.py`

**Interfaces:**
- Consumes: 이전 모든 Task의 실물 코드 — mock/fake 없이 실제 psycopg2 +
  `make up-postgres`로 띄운 실제 Postgres에 붙는다.
- Produces: 없음 (터미널 Task, 이 계획의 마지막 검증 단계)

- [ ] **Step 1: 테스트 작성**

```python
"""Integration tests for segment_comfort_score gold loading (#129).

RUN_INTEGRATION 미설정 시 skip(로컬 편의). RUN_INTEGRATION=1인데 Postgres
접속이 실패하면 skip이 아니라 fail한다 — "접속 안 되면 조용히 스킵"은
CI에서 영원히 초록불이 켜지는 결과를 낳으므로 채택하지 않는다.
"""

from __future__ import annotations

import os
import threading
import time
from datetime import UTC, datetime, timedelta

import psycopg2
import pytest
from batch_jobs.db_lock_keys import GOLD_JOB_STAGING_LOCK_KEY
from batch_jobs.migrate import MigrationConfig, run_migrations
from batch_jobs.schemas import HOURLY_COMFORT_SCORE_SCHEMA
from comfort_score.config import DEFAULT_COMFORT_SCORE_CONFIG_PATH
from comfort_score.gold_job import (
    SegmentComfortScoreJobConfig,
    build_spark_session,
    run_segment_comfort_score_job,
)
from comfort_score.gold_writer import STAGING_TABLE, TARGET_TABLE
from pyspark.sql import SparkSession

RUN_INTEGRATION = os.environ.get("RUN_INTEGRATION") == "1"

pytestmark = pytest.mark.skipif(
    not RUN_INTEGRATION, reason="set RUN_INTEGRATION=1 to run against a real Postgres"
)


def _connect():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


@pytest.fixture(scope="module")
def spark():
    session = build_spark_session()
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def clean_tables():
    # RUN_INTEGRATION=1인데 접속이 안 되면 여기서 바로 fail한다(skip 아님) —
    # 이 fixture가 실패하면 pytest가 해당 테스트를 error로 보고하지 skip으로
    # 보고하지 않는다.
    connection = _connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"TRUNCATE {TARGET_TABLE}, {STAGING_TABLE}")
            cursor.execute(
                "DELETE FROM vehicle_profile WHERE vehicle_profile_id != 0"
            )
        connection.commit()
    finally:
        connection.close()
    yield


@pytest.fixture(scope="module", autouse=True)
def migrated():
    connection = _connect()
    try:
        run_migrations(MigrationConfig.from_env().migrations_dir, connection)
    finally:
        connection.close()


def hourly_row(**overrides):
    row = {
        "segment_id": "seg-1",
        "vehicle_profile_id": 1,
        "data_period_start": datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
        "data_period_end": datetime(2026, 8, 15, 13, 0, tzinfo=UTC),
        "road_snapshot_date": datetime(2026, 8, 1).date(),
        "vertical_score": 80.0,
        "longitudinal_score": 80.0,
        "lateral_score": 80.0,
        "scoring_version": "hourly-comfort-v1",
        "sample_count": 100,
        "trip_count": 10,
        "_run_id": "test-run",
        "_processed_at": datetime(2026, 8, 15, 13, 5, tzinfo=UTC),
    }
    row.update(overrides)
    return tuple(row[field.name] for field in HOURLY_COMFORT_SCORE_SCHEMA)


def write_hourly_scores(spark, tmp_path, *rows) -> str:
    data_lake_uri = str(tmp_path)
    (
        spark.createDataFrame(list(rows), HOURLY_COMFORT_SCORE_SCHEMA)
        .write.parquet(str(tmp_path / "silver" / "hourly_comfort_score"))
    )
    return data_lake_uri


def make_config(data_lake_uri: str) -> SegmentComfortScoreJobConfig:
    env = os.environ
    return SegmentComfortScoreJobConfig(
        data_lake_uri=data_lake_uri,
        window_hours=168,
        comfort_score_config_path=DEFAULT_COMFORT_SCORE_CONFIG_PATH,
        postgres_host=env["POSTGRES_HOST"],
        postgres_port=int(env["POSTGRES_PORT"]),
        postgres_db=env["POSTGRES_DB"],
        postgres_user=env["POSTGRES_USER"],
        postgres_password=env["POSTGRES_PASSWORD"],
    )


def fetch_rows(connection) -> list[tuple]:
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT segment_id, vehicle_profile_id, comfort_score, calculated_at "
            f"FROM {TARGET_TABLE} ORDER BY segment_id, vehicle_profile_id"
        )
        return cursor.fetchall()


def test_loads_and_upserts_on_rerun_without_duplicating_rows(spark, tmp_path):
    as_of = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
    data_lake_uri = write_hourly_scores(spark, tmp_path, hourly_row())
    config = make_config(data_lake_uri)
    connection = _connect()
    try:
        first = run_segment_comfort_score_job(spark, config, as_of, connection)
        assert first.inserted_count >= 1
        first_rows = fetch_rows(connection)

        # 같은 조합을 다른 값으로 재실행 -> 행 수는 그대로, 값만 갱신
        later_as_of = as_of + timedelta(hours=1)
        write_hourly_scores(
            spark, tmp_path, hourly_row(vertical_score=0.0, longitudinal_score=0.0)
        )
        second = run_segment_comfort_score_job(spark, config, later_as_of, connection)
        second_rows = fetch_rows(connection)

        assert second.updated_count >= 1
        assert len(second_rows) == len(first_rows)
        assert second_rows != first_rows  # comfort_score/calculated_at이 바뀜
    finally:
        connection.close()


def test_fk_violation_rejected_for_unknown_vehicle_profile(spark, tmp_path):
    as_of = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
    data_lake_uri = write_hourly_scores(
        spark, tmp_path, hourly_row(vehicle_profile_id=999)  # vehicle_profile에 없는 ID
    )
    config = make_config(data_lake_uri)
    connection = _connect()
    try:
        with pytest.raises(psycopg2.errors.ForeignKeyViolation):
            run_segment_comfort_score_job(spark, config, as_of, connection)
    finally:
        connection.rollback()
        connection.close()


def test_staging_shape_check_fails_clearly_when_staging_table_is_missing(
    spark, tmp_path
):
    data_lake_uri = write_hourly_scores(spark, tmp_path, hourly_row())
    config = make_config(data_lake_uri)
    connection = _connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE {STAGING_TABLE}")
        connection.commit()

        with pytest.raises(RuntimeError, match="make migrate"):
            run_segment_comfort_score_job(
                spark, config, datetime(2026, 8, 16, 0, 0, tzinfo=UTC), connection
            )
    finally:
        connection.rollback()
        connection.close()
        # 다음 테스트를 위해 staging을 되살린다
        restore = _connect()
        try:
            run_migrations(MigrationConfig.from_env().migrations_dir, restore)
        finally:
            restore.close()


def test_concurrent_run_fails_fast_on_advisory_lock(spark, tmp_path):
    as_of = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
    data_lake_uri = write_hourly_scores(spark, tmp_path, hourly_row())
    config = make_config(data_lake_uri)

    holder = _connect()
    blocker_cursor = holder.cursor()
    blocker_cursor.execute(
        "SELECT pg_try_advisory_lock(%s)", (GOLD_JOB_STAGING_LOCK_KEY,)
    )
    assert blocker_cursor.fetchone()[0] is True

    connection = _connect()
    try:
        with pytest.raises(RuntimeError, match="staging lock"):
            run_segment_comfort_score_job(spark, config, as_of, connection)
    finally:
        connection.rollback()
        connection.close()
        blocker_cursor.execute(
            "SELECT pg_advisory_unlock(%s)", (GOLD_JOB_STAGING_LOCK_KEY,)
        )
        holder.close()


def test_reads_are_never_blocked_while_merge_runs(spark, tmp_path):
    as_of = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
    rows = tuple(
        hourly_row(segment_id=f"seg-{i}") for i in range(200)
    )  # MERGE를 조금이라도 오래 걸리게
    data_lake_uri = write_hourly_scores(spark, tmp_path, *rows)
    config = make_config(data_lake_uri)
    connection = _connect()

    read_durations: list[float] = []
    stop = threading.Event()

    def read_loop():
        reader = _connect()
        try:
            while not stop.is_set():
                started = time.monotonic()
                with reader.cursor() as cursor:
                    cursor.execute(f"SELECT count(*) FROM {TARGET_TABLE}")
                    cursor.fetchone()
                read_durations.append(time.monotonic() - started)
                time.sleep(0.01)
        finally:
            reader.close()

    reader_thread = threading.Thread(target=read_loop)
    reader_thread.start()
    try:
        run_segment_comfort_score_job(spark, config, as_of, connection)
    finally:
        stop.set()
        reader_thread.join(timeout=5)
        connection.close()

    assert read_durations  # 읽기 스레드가 실제로 최소 한 번은 실행됨
    assert max(read_durations) < 1.0  # 어떤 읽기도 1초 이상 블록되지 않음
```

- [ ] **Step 2: 로컬에서 실행**

Prerequisites 완료 + Task 3의 `make migrate` 실행 후:

```bash
cd /Users/yong/PycharmProjects/DE_team4-4una
RUN_INTEGRATION=1 uv run --package batch-jobs pytest services/batch-jobs/tests/test_segment_comfort_score_integration.py -v
```

Expected: 5 passed (RUN_INTEGRATION 미설정 시 이 파일 전체가 skip되는 것도
`uv run --package batch-jobs pytest services/batch-jobs/tests/test_segment_comfort_score_integration.py -v`
로 별도 확인).

- [ ] **Step 3: 전체 워크스페이스 검증**

```bash
uv sync --all-packages
uv run --all-packages ruff check .
uv run --all-packages pytest
RUN_INTEGRATION=1 uv run --package batch-jobs pytest services/batch-jobs/tests/test_segment_comfort_score_integration.py -v
```

Expected: 전부 통과.

- [ ] **Step 4: 커밋**

```bash
git add services/batch-jobs/tests/test_segment_comfort_score_integration.py
git commit -m "test: add integration tests for segment_comfort_score gold loading (#129)"
```
