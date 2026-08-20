# data_quality_audit DAG — at-rest Gold 감시 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a new `data_quality_audit` Airflow DAG that audits the full
`standard_segment_comfort_score` and `current_segment_comfort_score` Gold
Postgres tables once a day (range checks, freshness, `vehicle_profile_id`
referential integrity), writes a human-viewable Great Expectations Data Docs
report to S3, and soft-fails (signals via a red Airflow task, blocks nothing
downstream) when checks fail.

**Architecture:** `services/batch-jobs` owns a new `gold_audit_validation`
module + 4 Expectation Suite JSON files + a new `audit-gold` CLI subcommand.
It uses GX's `SqlAlchemyExecutionEngine` against Postgres, runs validation
through a `ValidationDefinition`+`Checkpoint` (not the simpler
`batch.validate()` used by existing in-flight checks — that path does not
persist results, so Data Docs would render empty; verified locally against
`great-expectations==1.21.0`), renders Data Docs to a local temp dir via
`TupleFilesystemStoreBackend` (the real `TupleS3StoreBackend` no longer
exists in this GX version — also verified locally), and uploads that
directory to S3 with `boto3`. `services/orchestration` adds a new DAG with
two independent `BashOperator` tasks (one per table) that run the existing
`batch-jobs` Docker image, following the same `docker run` pattern as
`validate_standard_score`.

**Tech Stack:** Python 3.12, `great-expectations==1.21.0` (`[spark,postgresql]`
extras), `boto3`, `psycopg2`, Apache Airflow 3.3.1 (`BashOperator`,
docker-outside-of-docker), PostgreSQL.

**Spec:** `docs/superpowers/specs/2026-08-20-data-quality-audit-dag-at-rest-gold-design.md`

## Global Constraints

- Freshness threshold: **10800 seconds (3 hours)**, identical for both
  tables (spec §4).
- `vehicle_profile_id` orphan count must be exactly `0` for both tables
  (spec §4).
- Range checks: `comfort_score`/`vertical_score`/`longitudinal_score`/
  `lateral_score` must each be in `[0, 100]` (spec §4).
- Table names are restricted to the literal set
  `{"standard_segment_comfort_score", "current_segment_comfort_score"}` —
  any other value is a hard `ValueError`, never interpolated into SQL
  without this check (spec §4, components).
- S3 bucket default: `de4-data-quality-docs`, override via
  `GOLD_AUDIT_S3_BUCKET` env var. S3 key prefix:
  `data-quality-audit/gold/<table>/...` (spec §1).
- Every run overwrites the same S3 keys (latest-only, no dated history —
  spec §1 "보존 정책").
- Do not use `batch.validate(suite)` directly for this module — it does not
  persist to `validation_results_store`, so Data Docs render empty. Use
  `ValidationDefinition` + `Checkpoint(actions=[UpdateDataDocsAction(...)])`
  (spec §1, §4 — verified locally).
- `services/orchestration` must not import `batch_jobs` (existing repo
  boundary — spec §2). All GX/Postgres logic stays in `services/batch-jobs`;
  the DAG only shells out via `BashOperator`.
- Never write real secrets into `.env.example`, code, logs, or tests
  (AGENTS.md).

---

## Task 1: Add `great-expectations[postgresql]` and `boto3` dependencies

**Files:**
- Modify: `services/batch-jobs/pyproject.toml`

**Interfaces:**
- Produces: `boto3`, `psycopg2` (already present), and GX's postgres SQL
  dialect support become importable in `services/batch-jobs`. No Python
  symbols yet — this task only prepares dependencies for later tasks.

- [ ] **Step 1: Edit `pyproject.toml`**

Change the `great-expectations` line and add `boto3`:

```toml
dependencies = [
    "de4-core",
    "duckdb>=1.4.3",
    "boto3>=1.40,<2.0",
    "great-expectations[spark,postgresql]>=1.21.0",
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

Keep the list alphabetically ordered as it already is (insert `boto3` after
`duckdb`, before `great-expectations`).

- [ ] **Step 2: Update the lockfile**

Run: `uv lock` (from repo root)
Expected: `uv.lock` changes to add `boto3` and its transitive dependencies
(`botocore`, `s3transfer`, `jmespath`, `urllib3`, etc.), and picks up the
`postgresql` extra's SQL dialect dependencies (e.g. `sqlalchemy` bounds may
tighten — this is expected and fine, same as what #249's branch already did
for this exact extra change).

- [ ] **Step 3: Sync and verify imports**

Run: `uv sync --package batch-jobs`
Run: `uv run --package batch-jobs python -c "import boto3; import great_expectations as gx; from great_expectations.checkpoint.actions import UpdateDataDocsAction; print('ok')"`
Expected: prints `ok` with no import errors.

- [ ] **Step 4: Commit**

```bash
git add services/batch-jobs/pyproject.toml uv.lock
git commit -m "chore(batch-jobs): add boto3 and great-expectations postgresql extra for gold audit (#253)"
```

---

## Task 2: Add the 4 Expectation Suite JSON files

**Files:**
- Create: `services/batch-jobs/src/batch_jobs/resources/expectations/standard_segment_comfort_score_audit_range_suite.json`
- Create: `services/batch-jobs/src/batch_jobs/resources/expectations/standard_segment_comfort_score_audit_summary_suite.json`
- Create: `services/batch-jobs/src/batch_jobs/resources/expectations/current_segment_comfort_score_audit_range_suite.json`
- Create: `services/batch-jobs/src/batch_jobs/resources/expectations/current_segment_comfort_score_audit_summary_suite.json`
- Test: `services/batch-jobs/tests/test_gold_audit_validation.py`

**Interfaces:**
- Produces: 4 suite JSON files loadable by
  `great_expectations.ExpectationSuite(**json.loads(path.read_text()))`
  (same loading pattern as `sensor_processing_validation.load_expectation_suite`).
  Task 4 will add the module-level `load_expectation_suite` function that
  reads these.

- [ ] **Step 1: Write the failing test**

Create `services/batch-jobs/tests/test_gold_audit_validation.py` with:

```python
"""Tests for batch_jobs/gold_audit_validation.py (#253, ADR-0004 at-rest audit)."""

from __future__ import annotations

import json
from pathlib import Path

RESOURCE_DIR = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "batch_jobs"
    / "resources"
    / "expectations"
)


class TestAuditSuiteFiles:
    def test_standard_range_suite_has_four_score_range_expectations(self) -> None:
        payload = json.loads(
            (RESOURCE_DIR / "standard_segment_comfort_score_audit_range_suite.json").read_text()
        )

        assert payload["name"] == "standard_segment_comfort_score_audit_range_suite"
        assert len(payload["expectations"]) == 4
        columns = {e["kwargs"]["column"] for e in payload["expectations"]}
        assert columns == {
            "comfort_score",
            "vertical_score",
            "longitudinal_score",
            "lateral_score",
        }
        for expectation in payload["expectations"]:
            assert expectation["type"] == "expect_column_values_to_be_between"
            assert expectation["kwargs"]["min_value"] == 0.0
            assert expectation["kwargs"]["max_value"] == 100.0

    def test_current_range_suite_has_four_score_range_expectations(self) -> None:
        payload = json.loads(
            (RESOURCE_DIR / "current_segment_comfort_score_audit_range_suite.json").read_text()
        )

        assert payload["name"] == "current_segment_comfort_score_audit_range_suite"
        assert len(payload["expectations"]) == 4

    def test_standard_summary_suite_checks_freshness_and_orphan_count(self) -> None:
        payload = json.loads(
            (RESOURCE_DIR / "standard_segment_comfort_score_audit_summary_suite.json").read_text()
        )

        assert payload["name"] == "standard_segment_comfort_score_audit_summary_suite"
        assert len(payload["expectations"]) == 2
        by_column = {e["kwargs"]["column"]: e for e in payload["expectations"]}
        assert by_column["age_seconds"]["kwargs"]["min_value"] == 0
        assert by_column["age_seconds"]["kwargs"]["max_value"] == 10800
        assert by_column["orphan_vehicle_profile_count"]["kwargs"]["min_value"] == 0
        assert by_column["orphan_vehicle_profile_count"]["kwargs"]["max_value"] == 0

    def test_current_summary_suite_checks_freshness_and_orphan_count(self) -> None:
        payload = json.loads(
            (RESOURCE_DIR / "current_segment_comfort_score_audit_summary_suite.json").read_text()
        )

        assert payload["name"] == "current_segment_comfort_score_audit_summary_suite"
        assert len(payload["expectations"]) == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --package batch-jobs pytest services/batch-jobs/tests/test_gold_audit_validation.py -v`
Expected: FAIL with `FileNotFoundError` (suite files don't exist yet).

- [ ] **Step 3: Create the 4 suite files**

`services/batch-jobs/src/batch_jobs/resources/expectations/standard_segment_comfort_score_audit_range_suite.json`:

```json
{
  "expectations": [
    {
      "kwargs": {
        "column": "comfort_score",
        "min_value": 0.0,
        "max_value": 100.0
      },
      "meta": {},
      "severity": "critical",
      "type": "expect_column_values_to_be_between"
    },
    {
      "kwargs": {
        "column": "vertical_score",
        "min_value": 0.0,
        "max_value": 100.0
      },
      "meta": {},
      "severity": "critical",
      "type": "expect_column_values_to_be_between"
    },
    {
      "kwargs": {
        "column": "longitudinal_score",
        "min_value": 0.0,
        "max_value": 100.0
      },
      "meta": {},
      "severity": "critical",
      "type": "expect_column_values_to_be_between"
    },
    {
      "kwargs": {
        "column": "lateral_score",
        "min_value": 0.0,
        "max_value": 100.0
      },
      "meta": {},
      "severity": "critical",
      "type": "expect_column_values_to_be_between"
    }
  ],
  "id": null,
  "meta": {
    "great_expectations_version": "1.21.0"
  },
  "name": "standard_segment_comfort_score_audit_range_suite",
  "notes": null
}
```

`services/batch-jobs/src/batch_jobs/resources/expectations/current_segment_comfort_score_audit_range_suite.json`
(identical expectations, only `"name"` differs):

```json
{
  "expectations": [
    {
      "kwargs": {
        "column": "comfort_score",
        "min_value": 0.0,
        "max_value": 100.0
      },
      "meta": {},
      "severity": "critical",
      "type": "expect_column_values_to_be_between"
    },
    {
      "kwargs": {
        "column": "vertical_score",
        "min_value": 0.0,
        "max_value": 100.0
      },
      "meta": {},
      "severity": "critical",
      "type": "expect_column_values_to_be_between"
    },
    {
      "kwargs": {
        "column": "longitudinal_score",
        "min_value": 0.0,
        "max_value": 100.0
      },
      "meta": {},
      "severity": "critical",
      "type": "expect_column_values_to_be_between"
    },
    {
      "kwargs": {
        "column": "lateral_score",
        "min_value": 0.0,
        "max_value": 100.0
      },
      "meta": {},
      "severity": "critical",
      "type": "expect_column_values_to_be_between"
    }
  ],
  "id": null,
  "meta": {
    "great_expectations_version": "1.21.0"
  },
  "name": "current_segment_comfort_score_audit_range_suite",
  "notes": null
}
```

`services/batch-jobs/src/batch_jobs/resources/expectations/standard_segment_comfort_score_audit_summary_suite.json`:

```json
{
  "expectations": [
    {
      "kwargs": {
        "column": "age_seconds",
        "min_value": 0,
        "max_value": 10800
      },
      "meta": {},
      "severity": "critical",
      "type": "expect_column_values_to_be_between"
    },
    {
      "kwargs": {
        "column": "orphan_vehicle_profile_count",
        "min_value": 0,
        "max_value": 0
      },
      "meta": {},
      "severity": "critical",
      "type": "expect_column_values_to_be_between"
    }
  ],
  "id": null,
  "meta": {
    "great_expectations_version": "1.21.0"
  },
  "name": "standard_segment_comfort_score_audit_summary_suite",
  "notes": null
}
```

`services/batch-jobs/src/batch_jobs/resources/expectations/current_segment_comfort_score_audit_summary_suite.json`
(identical expectations, only `"name"` differs):

```json
{
  "expectations": [
    {
      "kwargs": {
        "column": "age_seconds",
        "min_value": 0,
        "max_value": 10800
      },
      "meta": {},
      "severity": "critical",
      "type": "expect_column_values_to_be_between"
    },
    {
      "kwargs": {
        "column": "orphan_vehicle_profile_count",
        "min_value": 0,
        "max_value": 0
      },
      "meta": {},
      "severity": "critical",
      "type": "expect_column_values_to_be_between"
    }
  ],
  "id": null,
  "meta": {
    "great_expectations_version": "1.21.0"
  },
  "name": "current_segment_comfort_score_audit_summary_suite",
  "notes": null
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run --package batch-jobs pytest services/batch-jobs/tests/test_gold_audit_validation.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add services/batch-jobs/src/batch_jobs/resources/expectations/*_audit_*_suite.json services/batch-jobs/tests/test_gold_audit_validation.py
git commit -m "feat(batch-jobs): add gold at-rest audit expectation suites (#253)"
```

---

## Task 3: `GoldAuditValidationConfig` + table/column constants

**Files:**
- Create: `services/batch-jobs/src/batch_jobs/gold_audit_validation.py`
- Modify: `services/batch-jobs/tests/test_gold_audit_validation.py`

**Interfaces:**
- Consumes: `batch_jobs.resources.RESOURCE_DIR` (existing, `Path`).
- Produces:
  - `TABLES: tuple[str, str]` = `("standard_segment_comfort_score", "current_segment_comfort_score")`
  - `FRESHNESS_THRESHOLD_SECONDS: int` = `10800`
  - `_validate_table(table: str) -> None` (raises `ValueError` for unknown tables)
  - `GoldAuditValidationConfig` frozen dataclass with fields
    `postgres_host: str`, `postgres_port: int`, `postgres_db: str`,
    `postgres_user: str`, `postgres_password: str`, `s3_bucket: str`,
    `range_suite_paths: Mapping[str, Path]`,
    `summary_suite_paths: Mapping[str, Path]`, property `connection_string: str`,
    classmethod `from_env(env: Mapping[str, str] | None = None) -> GoldAuditValidationConfig`.

- [ ] **Step 1: Write the failing tests**

Append to `services/batch-jobs/tests/test_gold_audit_validation.py`:

```python
from batch_jobs.gold_audit_validation import (
    DEFAULT_RANGE_SUITE_PATHS,
    DEFAULT_SUMMARY_SUITE_PATHS,
    TABLES,
    GoldAuditValidationConfig,
    _validate_table,
)


class TestValidateTable:
    def test_accepts_known_tables(self) -> None:
        for table in TABLES:
            _validate_table(table)  # must not raise

    def test_rejects_unknown_table(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="standard_segment_comfort_score"):
            _validate_table("segment_comfort_score; DROP TABLE vehicle_profile")


class TestGoldAuditValidationConfig:
    def test_from_env_reads_postgres_and_s3_vars(self) -> None:
        config = GoldAuditValidationConfig.from_env(
            {
                "POSTGRES_HOST": "db.local",
                "POSTGRES_PORT": "5433",
                "POSTGRES_DB": "de4",
                "POSTGRES_USER": "app",
                "POSTGRES_PASSWORD": "secret",
                "GOLD_AUDIT_S3_BUCKET": "custom-bucket",
            }
        )

        assert config.postgres_host == "db.local"
        assert config.postgres_port == 5433
        assert config.postgres_db == "de4"
        assert config.postgres_user == "app"
        assert config.postgres_password == "secret"
        assert config.s3_bucket == "custom-bucket"
        assert config.range_suite_paths == DEFAULT_RANGE_SUITE_PATHS
        assert config.summary_suite_paths == DEFAULT_SUMMARY_SUITE_PATHS

    def test_from_env_defaults_s3_bucket(self) -> None:
        config = GoldAuditValidationConfig.from_env(
            {
                "POSTGRES_HOST": "db.local",
                "POSTGRES_PORT": "5433",
                "POSTGRES_DB": "de4",
                "POSTGRES_USER": "app",
                "POSTGRES_PASSWORD": "secret",
            }
        )

        assert config.s3_bucket == "de4-data-quality-docs"

    def test_from_env_requires_postgres_vars(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="POSTGRES_HOST"):
            GoldAuditValidationConfig.from_env({})

    def test_connection_string_uses_sqlalchemy_postgres_dialect(self) -> None:
        config = GoldAuditValidationConfig(
            postgres_host="db.local",
            postgres_port=5433,
            postgres_db="de4",
            postgres_user="app",
            postgres_password="secret",
            s3_bucket="de4-data-quality-docs",
            range_suite_paths=DEFAULT_RANGE_SUITE_PATHS,
            summary_suite_paths=DEFAULT_SUMMARY_SUITE_PATHS,
        )

        assert config.connection_string == "postgresql+psycopg2://app:secret@db.local:5433/de4"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run --package batch-jobs pytest services/batch-jobs/tests/test_gold_audit_validation.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'batch_jobs.gold_audit_validation'`.

- [ ] **Step 3: Write the implementation**

Create `services/batch-jobs/src/batch_jobs/gold_audit_validation.py`:

```python
"""Gold at-rest 감사 (#253, ADR-0004 롤아웃 ④번).

`standard_segment_comfort_score`/`current_segment_comfort_score`(둘 다
Postgres) 전체 범위를 매일 감사한다. in-flight 검증(#220, #249)과 달리
"이번 실행분"이 아니라 이미 적재된 전체 이력을 대상으로 하고, 실패해도
파이프라인을 막지 않는다(soft fail). Gold는 `SqlAlchemyExecutionEngine`으로
직접 조회한다(ADR-0004: Gold/Postgres는 Spark가 아니라 SqlAlchemy 경로).

Data Docs를 S3에서 열람 가능해야 하므로(완료 조건), 다른 GX 검증 모듈들과
달리 `batch.validate(suite)`를 직접 호출하지 않는다 — 그 경로는
`validation_results_store`에 기록을 남기지 않아 Data Docs가 빈 채로
렌더링된다(로컬 재현으로 확인). 대신 `ValidationDefinition` +
`Checkpoint`(`UpdateDataDocsAction` 포함) 경로로 실행한다.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from batch_jobs.resources import RESOURCE_DIR

TABLES: tuple[str, str] = (
    "standard_segment_comfort_score",
    "current_segment_comfort_score",
)

# 두 테이블 모두 최신 행이 이 값(초)보다 오래되면 stale로 본다(spec §4).
FRESHNESS_THRESHOLD_SECONDS = 10800

# freshness를 판정할 기준 컬럼 — current_segment_comfort_score엔 score_as_of가 없다.
_FRESHNESS_COLUMN: dict[str, str] = {
    "standard_segment_comfort_score": "score_as_of",
    "current_segment_comfort_score": "calculated_at",
}

DEFAULT_RANGE_SUITE_PATHS: dict[str, Path] = {
    table: RESOURCE_DIR / "expectations" / f"{table}_audit_range_suite.json"
    for table in TABLES
}
DEFAULT_SUMMARY_SUITE_PATHS: dict[str, Path] = {
    table: RESOURCE_DIR / "expectations" / f"{table}_audit_summary_suite.json"
    for table in TABLES
}

DEFAULT_S3_BUCKET = "de4-data-quality-docs"
S3_PREFIX = "data-quality-audit/gold"


def _validate_table(table: str) -> None:
    if table not in TABLES:
        raise ValueError(f"table must be one of {TABLES}, got {table!r}")


def _require(source: Mapping[str, str], key: str) -> str:
    value = source.get(key)
    if not value:
        raise ValueError(f"{key} must be set")
    return value


@dataclass(frozen=True, slots=True)
class GoldAuditValidationConfig:
    """`load-standard-segment-comfort-score`(StandardComfortScoreJobConfig)와
    같은 POSTGRES_* env var를 재사용한다."""

    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    s3_bucket: str
    range_suite_paths: Mapping[str, Path]
    summary_suite_paths: Mapping[str, Path]

    @property
    def connection_string(self) -> str:
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> GoldAuditValidationConfig:
        source = env if env is not None else os.environ
        return cls(
            postgres_host=_require(source, "POSTGRES_HOST"),
            postgres_port=int(_require(source, "POSTGRES_PORT")),
            postgres_db=_require(source, "POSTGRES_DB"),
            postgres_user=_require(source, "POSTGRES_USER"),
            postgres_password=_require(source, "POSTGRES_PASSWORD"),
            s3_bucket=source.get("GOLD_AUDIT_S3_BUCKET") or DEFAULT_S3_BUCKET,
            range_suite_paths=DEFAULT_RANGE_SUITE_PATHS,
            summary_suite_paths=DEFAULT_SUMMARY_SUITE_PATHS,
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run --package batch-jobs pytest services/batch-jobs/tests/test_gold_audit_validation.py -v`
Expected: PASS (all tests so far).

- [ ] **Step 5: Commit**

```bash
git add services/batch-jobs/src/batch_jobs/gold_audit_validation.py services/batch-jobs/tests/test_gold_audit_validation.py
git commit -m "feat(batch-jobs): add GoldAuditValidationConfig (#253)"
```

---

## Task 4: Query builders (`build_range_query`, `build_summary_query`)

**Files:**
- Modify: `services/batch-jobs/src/batch_jobs/gold_audit_validation.py`
- Modify: `services/batch-jobs/tests/test_gold_audit_validation.py`

**Interfaces:**
- Consumes: `TABLES`, `_validate_table`, `_FRESHNESS_COLUMN` (Task 3).
- Produces: `build_range_query(table: str) -> str`,
  `build_summary_query(table: str) -> str`. Both raise `ValueError` for
  unknown tables (via `_validate_table`). Task 6 (`run_gold_audit`) calls
  these to build the two GX query assets per table.

- [ ] **Step 1: Write the failing tests**

Append to `services/batch-jobs/tests/test_gold_audit_validation.py`:

```python
from batch_jobs.gold_audit_validation import build_range_query, build_summary_query


class TestBuildRangeQuery:
    def test_selects_the_full_table(self) -> None:
        query = build_range_query("standard_segment_comfort_score")

        assert query == "SELECT * FROM standard_segment_comfort_score"

    def test_rejects_unknown_table(self) -> None:
        import pytest

        with pytest.raises(ValueError):
            build_range_query("not_a_real_table")


class TestBuildSummaryQuery:
    def test_standard_table_uses_score_as_of_for_freshness(self) -> None:
        query = build_summary_query("standard_segment_comfort_score")

        assert "MAX(score_as_of)" in query
        assert "age_seconds" in query
        assert "orphan_vehicle_profile_count" in query
        assert "LEFT JOIN vehicle_profile vp" in query
        assert "FROM standard_segment_comfort_score" in query

    def test_current_table_uses_calculated_at_for_freshness(self) -> None:
        query = build_summary_query("current_segment_comfort_score")

        assert "MAX(calculated_at)" in query
        assert "FROM current_segment_comfort_score" in query

    def test_rejects_unknown_table(self) -> None:
        import pytest

        with pytest.raises(ValueError):
            build_summary_query("not_a_real_table")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run --package batch-jobs pytest services/batch-jobs/tests/test_gold_audit_validation.py -v`
Expected: FAIL with `ImportError: cannot import name 'build_range_query'`.

- [ ] **Step 3: Add the implementation**

Append to `services/batch-jobs/src/batch_jobs/gold_audit_validation.py`
(after the `GoldAuditValidationConfig` class):

```python
def build_range_query(table: str) -> str:
    _validate_table(table)
    return f"SELECT * FROM {table}"


def build_summary_query(table: str) -> str:
    """freshness(초)와 orphan `vehicle_profile_id` 수를 한 행으로 묶어 낸다.

    `current_segment_comfort_score.vehicle_profile_id`엔 DB FK가 없어(0006
    마이그레이션) 이 anti-join이 실제로 의미가 있다. `standard_segment_comfort_score`는
    이미 FK로 이 위반이 불가능하지만, 검증 비용이 저렴해 안전망으로 그대로 둔다.
    """
    _validate_table(table)
    freshness_column = _FRESHNESS_COLUMN[table]
    return (
        f"SELECT EXTRACT(EPOCH FROM (now() - MAX({freshness_column})))"
        f"::double precision AS age_seconds, "
        f"(SELECT count(*) FROM {table} t "
        f"LEFT JOIN vehicle_profile vp "
        f"ON t.vehicle_profile_id = vp.vehicle_profile_id "
        f"WHERE vp.vehicle_profile_id IS NULL) AS orphan_vehicle_profile_count "
        f"FROM {table}"
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run --package batch-jobs pytest services/batch-jobs/tests/test_gold_audit_validation.py -v`
Expected: PASS (all tests so far).

- [ ] **Step 5: Commit**

```bash
git add services/batch-jobs/src/batch_jobs/gold_audit_validation.py services/batch-jobs/tests/test_gold_audit_validation.py
git commit -m "feat(batch-jobs): add gold audit range/summary query builders (#253)"
```

---

## Task 5: `count_rows`, `load_expectation_suite`, `upload_data_docs_to_s3`

**Files:**
- Modify: `services/batch-jobs/src/batch_jobs/gold_audit_validation.py`
- Modify: `services/batch-jobs/tests/test_gold_audit_validation.py`

**Interfaces:**
- Consumes: `_validate_table` (Task 3).
- Produces:
  - `count_rows(connection, table: str) -> int`
  - `load_expectation_suite(path: Path) -> gx.ExpectationSuite`
  - `upload_data_docs_to_s3(local_dir: Path, bucket: str, prefix: str, s3_client) -> int`
    (returns number of files uploaded)

  Task 6 (`run_gold_audit`) calls all three.

- [ ] **Step 1: Write the failing tests**

Append to `services/batch-jobs/tests/test_gold_audit_validation.py`:

```python
from batch_jobs.gold_audit_validation import (
    count_rows,
    load_expectation_suite,
    upload_data_docs_to_s3,
)


class _FakeCursor:
    def __init__(self, row: tuple) -> None:
        self._row = row

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None

    def execute(self, sql: str) -> None:
        self.executed_sql = sql

    def fetchone(self) -> tuple:
        return self._row


class _FakeConnection:
    def __init__(self, row: tuple) -> None:
        self._row = row
        self.cursor_used: _FakeCursor | None = None

    def cursor(self) -> _FakeCursor:
        self.cursor_used = _FakeCursor(self._row)
        return self.cursor_used


class TestCountRows:
    def test_returns_the_count_from_the_query(self) -> None:
        connection = _FakeConnection((42,))

        assert count_rows(connection, "standard_segment_comfort_score") == 42
        assert "COUNT(*)" in connection.cursor_used.executed_sql
        assert "standard_segment_comfort_score" in connection.cursor_used.executed_sql

    def test_rejects_unknown_table(self) -> None:
        import pytest

        with pytest.raises(ValueError):
            count_rows(_FakeConnection((0,)), "not_a_real_table")


class TestLoadExpectationSuite:
    def test_loads_the_committed_range_suite(self) -> None:
        from batch_jobs.gold_audit_validation import DEFAULT_RANGE_SUITE_PATHS

        suite = load_expectation_suite(
            DEFAULT_RANGE_SUITE_PATHS["standard_segment_comfort_score"]
        )

        assert suite.name == "standard_segment_comfort_score_audit_range_suite"
        assert len(suite.expectations) == 4


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def put_object(self, **kwargs: object) -> None:
        self.objects[(str(kwargs["Bucket"]), str(kwargs["Key"]))] = kwargs["Body"]  # type: ignore[assignment]


class TestUploadDataDocsToS3:
    def test_uploads_every_file_with_relative_path_as_key(self, tmp_path: Path) -> None:
        (tmp_path / "index.html").write_text("<html></html>")
        nested = tmp_path / "expectations"
        nested.mkdir()
        (nested / "suite.html").write_text("<html>suite</html>")
        client = FakeS3Client()

        uploaded = upload_data_docs_to_s3(
            tmp_path, "de4-data-quality-docs", "data-quality-audit/gold/standard_segment_comfort_score", client
        )

        assert uploaded == 2
        assert client.objects[
            ("de4-data-quality-docs", "data-quality-audit/gold/standard_segment_comfort_score/index.html")
        ] == b"<html></html>"
        assert client.objects[
            (
                "de4-data-quality-docs",
                "data-quality-audit/gold/standard_segment_comfort_score/expectations/suite.html",
            )
        ] == b"<html>suite</html>"
```

Add `from pathlib import Path` to the test file's imports if not already
present (it is, from Task 2's `RESOURCE_DIR` import).

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run --package batch-jobs pytest services/batch-jobs/tests/test_gold_audit_validation.py -v`
Expected: FAIL with `ImportError: cannot import name 'count_rows'`.

- [ ] **Step 3: Add the implementation**

Add `import json` to the top of
`services/batch-jobs/src/batch_jobs/gold_audit_validation.py` (next to
`import os`), and add `import great_expectations as gx` after the stdlib
imports. Then append:

```python
def count_rows(connection, table: str) -> int:
    _validate_table(table)
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")  # table validated above
        return cursor.fetchone()[0]


def load_expectation_suite(path: Path) -> gx.ExpectationSuite:
    payload = json.loads(Path(path).read_text())
    return gx.ExpectationSuite(**payload)


def upload_data_docs_to_s3(local_dir: Path, bucket: str, prefix: str, s3_client) -> int:
    """렌더된 Data Docs 임시 디렉터리를 S3에 업로드한다.

    GX의 `TupleS3StoreBackend`가 great-expectations==1.21.0엔 없어(#253 스펙
    §1) GX는 로컬에만 렌더링하고, 이 함수가 그 결과물을 boto3로 직접 옮긴다.
    """
    local_dir = Path(local_dir)
    uploaded = 0
    for path in sorted(local_dir.rglob("*")):
        if path.is_file():
            relative = path.relative_to(local_dir).as_posix()
            key = f"{prefix}/{relative}"
            s3_client.put_object(Bucket=bucket, Key=key, Body=path.read_bytes())
            uploaded += 1
    return uploaded
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run --package batch-jobs pytest services/batch-jobs/tests/test_gold_audit_validation.py -v`
Expected: PASS (all tests so far).

- [ ] **Step 5: Commit**

```bash
git add services/batch-jobs/src/batch_jobs/gold_audit_validation.py services/batch-jobs/tests/test_gold_audit_validation.py
git commit -m "feat(batch-jobs): add count_rows/load_expectation_suite/upload_data_docs_to_s3 (#253)"
```

---

## Task 6: `run_gold_audit` — the Checkpoint orchestration

**Files:**
- Modify: `services/batch-jobs/src/batch_jobs/gold_audit_validation.py`
- Modify: `services/batch-jobs/tests/test_gold_audit_validation.py`

**Interfaces:**
- Consumes: everything from Tasks 3-5, plus real `psycopg2` connections and
  an injected S3 client (test doubles use `FakeS3Client` from Task 5; the
  CLI in Task 7 passes a real `boto3.client("s3")`).
- Produces:
  - `GoldAuditSummary` frozen dataclass: `table: str`, `row_count: int`,
    `success: bool`.
  - `GoldAuditValidationFailed(Exception)`.
  - `run_gold_audit(config: GoldAuditValidationConfig, connection, table: str, s3_client) -> GoldAuditSummary`
    — raises `GoldAuditValidationFailed` if the table is empty or checks
    fail. Task 7's CLI calls this directly.

This task's automated tests are gated behind `RUN_INTEGRATION=1` against the
already-running local Postgres (`docker compose` in `infra/compose/postgres.yaml`,
already up in this environment) — same convention as
`test_current_score_signature_migration.py` /
`test_latest_zone_weather_migration.py`. S3 is always faked via `FakeS3Client`
(Task 5) so the test never needs real AWS credentials.

- [ ] **Step 1: Write the failing tests**

Append to `services/batch-jobs/tests/test_gold_audit_validation.py`:

```python
import os
from datetime import UTC, datetime, timedelta

import psycopg2
import pytest
from batch_jobs.gold_audit_validation import (
    DEFAULT_RANGE_SUITE_PATHS,
    DEFAULT_SUMMARY_SUITE_PATHS,
    GoldAuditSummary,
    GoldAuditValidationFailed,
    run_gold_audit,
)

RUN_INTEGRATION = os.environ.get("RUN_INTEGRATION") == "1"


def _pg_connect():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def _config() -> GoldAuditValidationConfig:
    from batch_jobs.gold_audit_validation import GoldAuditValidationConfig

    return GoldAuditValidationConfig(
        postgres_host=os.environ["POSTGRES_HOST"],
        postgres_port=int(os.environ["POSTGRES_PORT"]),
        postgres_db=os.environ["POSTGRES_DB"],
        postgres_user=os.environ["POSTGRES_USER"],
        postgres_password=os.environ["POSTGRES_PASSWORD"],
        s3_bucket="de4-data-quality-docs",
        range_suite_paths=DEFAULT_RANGE_SUITE_PATHS,
        summary_suite_paths=DEFAULT_SUMMARY_SUITE_PATHS,
    )


def _standard_row(**overrides: object) -> dict[str, object]:
    now = datetime.now(UTC)
    row = {
        "segment_id": "seg-1",
        "vehicle_profile_id": 0,
        "score_as_of": now,
        "data_period_start": now - timedelta(hours=168),
        "data_period_end": now,
        "vertical_score": 50.0,
        "longitudinal_score": 50.0,
        "lateral_score": 50.0,
        "comfort_score": 50.0,
        "sample_count": 10,
        "confidence_score": 0.5,
        "score_version": "1.0.0",
        "calculated_at": now,
    }
    row.update(overrides)
    return row


def _insert(connection, table: str, rows: list[dict[str, object]]) -> None:
    columns = list(rows[0].keys())
    placeholders = ", ".join(["%s"] * len(columns))
    with connection.cursor() as cursor:
        for row in rows:
            cursor.execute(
                f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
                [row[column] for column in columns],
            )
    connection.commit()


@pytest.mark.skipif(
    not RUN_INTEGRATION, reason="set RUN_INTEGRATION=1 to run against a real Postgres"
)
class TestRunGoldAuditAgainstStandardSegmentComfortScore:
    TABLE = "standard_segment_comfort_score"

    @pytest.fixture(autouse=True)
    def _clean_table(self):
        connection = _pg_connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"DELETE FROM {self.TABLE}")
            connection.commit()
        finally:
            connection.close()
        yield

    def test_succeeds_with_fresh_in_range_data(self) -> None:
        connection = _pg_connect()
        try:
            _insert(connection, self.TABLE, [_standard_row()])
            s3_client = FakeS3Client()

            summary = run_gold_audit(_config(), connection, self.TABLE, s3_client)

            assert summary == GoldAuditSummary(table=self.TABLE, row_count=1, success=True)
            assert len(s3_client.objects) > 0
            assert any(
                key.startswith(f"data-quality-audit/gold/{self.TABLE}/")
                for _, key in s3_client.objects
            )
        finally:
            connection.close()

    def test_fails_on_out_of_range_comfort_score(self) -> None:
        connection = _pg_connect()
        try:
            _insert(connection, self.TABLE, [_standard_row(comfort_score=150.0)])
            s3_client = FakeS3Client()

            with pytest.raises(GoldAuditValidationFailed):
                run_gold_audit(_config(), connection, self.TABLE, s3_client)

            # Data Docs는 실패해도 업로드돼 있어야 한다(soft fail, spec §6).
            assert len(s3_client.objects) > 0
        finally:
            connection.close()

    def test_fails_on_stale_score_as_of(self) -> None:
        connection = _pg_connect()
        try:
            stale_time = datetime.now(UTC) - timedelta(hours=10)
            _insert(
                connection,
                self.TABLE,
                [_standard_row(score_as_of=stale_time, data_period_end=stale_time)],
            )
            s3_client = FakeS3Client()

            with pytest.raises(GoldAuditValidationFailed):
                run_gold_audit(_config(), connection, self.TABLE, s3_client)
        finally:
            connection.close()

    def test_fails_when_table_is_empty(self) -> None:
        connection = _pg_connect()
        try:
            with pytest.raises(GoldAuditValidationFailed, match="no rows"):
                run_gold_audit(_config(), connection, self.TABLE, FakeS3Client())
        finally:
            connection.close()
```

Note: `vehicle_profile_id=0` is the pre-seeded vehicle-agnostic sentinel row
(migration `0003_seed_vehicle_profile_agnostic.sql`), so it always exists —
no orphan `vehicle_profile_id` test case is included here because
`standard_segment_comfort_score` has a hard DB `FOREIGN KEY` that makes an
orphaning insert fail at the `INSERT` itself, not at audit time (spec §4).
`current_segment_comfort_score` is where the orphan check matters; add the
same 4-test class for it in Step 3 below, using the FK-less table so the
orphan case can actually be inserted:

```python
@pytest.mark.skipif(
    not RUN_INTEGRATION, reason="set RUN_INTEGRATION=1 to run against a real Postgres"
)
class TestRunGoldAuditAgainstCurrentSegmentComfortScore:
    TABLE = "current_segment_comfort_score"

    @pytest.fixture(autouse=True)
    def _clean_table(self):
        connection = _pg_connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"DELETE FROM {self.TABLE}")
            connection.commit()
        finally:
            connection.close()
        yield

    def _row(self, **overrides: object) -> dict[str, object]:
        now = datetime.now(UTC)
        row = {
            "segment_id": "seg-1",
            "vehicle_profile_id": 0,
            "location_id": 1,
            "standard_score_as_of": now,
            "weather_time": None,
            "data_period_start": now - timedelta(hours=168),
            "vertical_score": 50.0,
            "longitudinal_score": 50.0,
            "lateral_score": 50.0,
            "comfort_score": 50.0,
            "sample_count": 10,
            "confidence_score": 0.5,
            "standard_score_version": "1.0.0",
            "weather_rule_version": None,
            "calculated_at": now,
        }
        row.update(overrides)
        return row

    def test_succeeds_with_fresh_in_range_data(self) -> None:
        connection = _pg_connect()
        try:
            standard_as_of = datetime.now(UTC)
            with connection.cursor() as cursor:
                cursor.execute(f"DELETE FROM standard_segment_comfort_score")
                cursor.execute(
                    "INSERT INTO standard_segment_comfort_score "
                    "(segment_id, vehicle_profile_id, score_as_of, data_period_start, "
                    "data_period_end, vertical_score, longitudinal_score, lateral_score, "
                    "comfort_score, sample_count, confidence_score, score_version, calculated_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        "seg-1", 0, standard_as_of, standard_as_of, standard_as_of,
                        50.0, 50.0, 50.0, 50.0, 10, 0.5, "1.0.0", standard_as_of,
                    ),
                )
            connection.commit()
            _insert(
                connection,
                self.TABLE,
                [self._row(standard_score_as_of=standard_as_of)],
            )
            s3_client = FakeS3Client()

            summary = run_gold_audit(_config(), connection, self.TABLE, s3_client)

            assert summary == GoldAuditSummary(table=self.TABLE, row_count=1, success=True)
        finally:
            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM current_segment_comfort_score")
                cursor.execute("DELETE FROM standard_segment_comfort_score")
            connection.commit()
            connection.close()

    def test_fails_when_table_is_empty(self) -> None:
        connection = _pg_connect()
        try:
            with pytest.raises(GoldAuditValidationFailed, match="no rows"):
                run_gold_audit(_config(), connection, self.TABLE, FakeS3Client())
        finally:
            connection.close()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run --package batch-jobs pytest services/batch-jobs/tests/test_gold_audit_validation.py -v`
Expected: `ImportError: cannot import name 'run_gold_audit'` (non-integration
collection error); with `RUN_INTEGRATION=1` also set, the integration tests
themselves would additionally fail on the same import error.

- [ ] **Step 3: Add the implementation**

Add these imports to the top of
`services/batch-jobs/src/batch_jobs/gold_audit_validation.py` (with the
other `great_expectations` imports):

```python
import tempfile

from great_expectations.checkpoint.actions import UpdateDataDocsAction
from great_expectations.checkpoint.checkpoint import Checkpoint
from great_expectations.core.validation_definition import ValidationDefinition
```

Append the rest of the module:

```python
@dataclass(frozen=True, slots=True)
class GoldAuditSummary:
    table: str
    row_count: int
    success: bool


class GoldAuditValidationFailed(Exception):
    """검증 실패 시 발생시켜 Airflow task를 soft fail시킨다(ADR-0004, spec §6)."""


def run_gold_audit(
    config: GoldAuditValidationConfig,
    connection,
    table: str,
    s3_client,
) -> GoldAuditSummary:
    """`table` 전체를 range/freshness/참조무결성 기준으로 감사한다(at-rest,
    이번 실행분이 아니라 전체 이력).

    성공/실패와 무관하게 Data Docs를 항상 렌더링 후 S3에 업로드한 다음에야
    실패 여부를 판정한다(spec §6) — 단, 테이블이 아예 비어 있으면 GX를
    실행할 대상 자체가 없으므로 그 전에 바로 실패시킨다(#249의
    `run_standard_score_validation`과 동일한 선례).
    """
    _validate_table(table)
    row_count = count_rows(connection, table)
    if row_count == 0:
        raise GoldAuditValidationFailed(f"no rows found in {table}")

    with tempfile.TemporaryDirectory(prefix="gold-audit-data-docs-") as tmp_dir:
        context = gx.get_context(mode="ephemeral")
        context.add_data_docs_site(
            site_name="gold_audit_site",
            site_config={
                "class_name": "SiteBuilder",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": tmp_dir,
                },
                "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
            },
        )

        datasource = context.data_sources.add_postgres(
            name=f"{table}_datasource", connection_string=config.connection_string
        )

        range_asset = datasource.add_query_asset(
            name=f"{table}_range", query=build_range_query(table)
        )
        range_batch_definition = range_asset.add_batch_definition_whole_table(
            f"{table}_range_batch"
        )
        range_suite = context.suites.add(
            load_expectation_suite(config.range_suite_paths[table])
        )
        range_vdef = context.validation_definitions.add(
            ValidationDefinition(
                name=f"{table}_range_vdef", data=range_batch_definition, suite=range_suite
            )
        )

        summary_asset = datasource.add_query_asset(
            name=f"{table}_summary", query=build_summary_query(table)
        )
        summary_batch_definition = summary_asset.add_batch_definition_whole_table(
            f"{table}_summary_batch"
        )
        summary_suite = context.suites.add(
            load_expectation_suite(config.summary_suite_paths[table])
        )
        summary_vdef = context.validation_definitions.add(
            ValidationDefinition(
                name=f"{table}_summary_vdef", data=summary_batch_definition, suite=summary_suite
            )
        )

        checkpoint = context.checkpoints.add(
            Checkpoint(
                name=f"{table}_audit_checkpoint",
                validation_definitions=[range_vdef, summary_vdef],
                actions=[UpdateDataDocsAction(name="update_data_docs")],
            )
        )
        result = checkpoint.run()

        upload_data_docs_to_s3(
            Path(tmp_dir), config.s3_bucket, f"{S3_PREFIX}/{table}", s3_client
        )

    summary = GoldAuditSummary(table=table, row_count=row_count, success=result.success)
    if not summary.success:
        raise GoldAuditValidationFailed(f"gold audit failed for table={table}: {summary}")
    return summary
```

- [ ] **Step 4: Run tests to verify they pass**

Run (unit-only first, should still pass/skip cleanly without a DB):
`uv run --package batch-jobs pytest services/batch-jobs/tests/test_gold_audit_validation.py -v`
Expected: PASS, with the 8 new integration tests reported as SKIPPED.

Then run against the real local Postgres (already running in this
environment via `infra/compose/postgres.yaml`):
`RUN_INTEGRATION=1 POSTGRES_HOST=localhost POSTGRES_PORT=5432 POSTGRES_DB=<from .env> POSTGRES_USER=<from .env> POSTGRES_PASSWORD=<from .env> uv run --package batch-jobs pytest services/batch-jobs/tests/test_gold_audit_validation.py -v`
(fill in `POSTGRES_DB`/`POSTGRES_USER`/`POSTGRES_PASSWORD` from the repo's
`.env`, not from this plan — don't hardcode secrets in commands committed
anywhere).
Expected: PASS (all 8 integration tests, including the two
`GoldAuditValidationFailed` cases and the empty-table cases).

- [ ] **Step 5: Commit**

```bash
git add services/batch-jobs/src/batch_jobs/gold_audit_validation.py services/batch-jobs/tests/test_gold_audit_validation.py
git commit -m "feat(batch-jobs): add run_gold_audit Checkpoint orchestration (#253)"
```

---

## Task 7: `audit-gold` CLI subcommand

**Files:**
- Modify: `services/batch-jobs/src/batch_jobs/cli.py`
- Modify: `services/batch-jobs/tests/test_cli_dispatch.py`

**Interfaces:**
- Consumes: `batch_jobs.gold_audit_validation.GoldAuditValidationConfig`,
  `run_gold_audit` (Task 6).
- Produces: `batch-jobs audit-gold --table <table>` CLI command. Task 9's
  DAG shells out to this exact command.

- [ ] **Step 1: Write the failing test**

Append to `services/batch-jobs/tests/test_cli_dispatch.py`:

```python
def test_audit_gold_command_requires_table() -> None:
    import pytest

    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(["audit-gold"])


def test_audit_gold_command_accepts_standard_table() -> None:
    arguments = cli.build_parser().parse_args(
        ["audit-gold", "--table", "standard_segment_comfort_score"]
    )

    assert arguments.table == "standard_segment_comfort_score"


def test_audit_gold_command_accepts_current_table() -> None:
    arguments = cli.build_parser().parse_args(
        ["audit-gold", "--table", "current_segment_comfort_score"]
    )

    assert arguments.table == "current_segment_comfort_score"


def test_audit_gold_command_rejects_unknown_table() -> None:
    import pytest

    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(["audit-gold", "--table", "not_a_real_table"])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --package batch-jobs pytest services/batch-jobs/tests/test_cli_dispatch.py -k audit_gold -v`
Expected: FAIL — `argparse.ArgumentError: invalid choice: 'audit-gold'`
(subparser doesn't exist yet).

- [ ] **Step 3: Add the CLI wiring**

In `services/batch-jobs/src/batch_jobs/cli.py`, inside `build_parser()`,
add this right after the existing
`validate_standard_score_parser.add_argument("--as-of", required=True)`
block (near line 65 in the current file):

```python
    audit_gold_parser = subparsers.add_parser("audit-gold")
    audit_gold_parser.add_argument(
        "--table",
        required=True,
        choices=["standard_segment_comfort_score", "current_segment_comfort_score"],
    )
```

Add a new run function near `run_sensor_processing_validation_cli` (after
`run_standard_comfort_score_loading`, matching the existing file's ordering
of "run function per subcommand"):

```python
def run_gold_audit_cli(arguments: argparse.Namespace) -> None:
    import boto3
    import psycopg2

    from batch_jobs.gold_audit_validation import (
        GoldAuditValidationConfig,
        run_gold_audit,
    )

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    config = GoldAuditValidationConfig.from_env()
    connection = psycopg2.connect(
        host=config.postgres_host,
        port=config.postgres_port,
        dbname=config.postgres_db,
        user=config.postgres_user,
        password=config.postgres_password,
    )
    s3_client = boto3.client("s3")
    try:
        summary = run_gold_audit(config, connection, arguments.table, s3_client)
        print(
            json.dumps(
                {
                    "table": summary.table,
                    "row_count": summary.row_count,
                    "success": summary.success,
                },
                sort_keys=True,
            )
        )
    finally:
        connection.close()
```

In `main()`, add the dispatch branch right after the
`validate-standard-score` branch:

```python
    if arguments.command == "audit-gold":
        run_gold_audit_cli(arguments)
        return
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run --package batch-jobs pytest services/batch-jobs/tests/test_cli_dispatch.py -v`
Expected: PASS (all tests, including the 4 new `audit_gold` ones).

Also run the full batch-jobs unit suite to make sure nothing else broke:
Run: `uv run --package batch-jobs pytest services/batch-jobs/tests -v -m "not integration"`
(or, if there's no `integration` marker configured, just rely on the
`RUN_INTEGRATION` env-var skip already in place — run without setting
`RUN_INTEGRATION`)
Expected: PASS, integration tests skipped.

- [ ] **Step 5: Commit**

```bash
git add services/batch-jobs/src/batch_jobs/cli.py services/batch-jobs/tests/test_cli_dispatch.py
git commit -m "feat(batch-jobs): add audit-gold CLI subcommand (#253)"
```

---

## Task 8: `data_quality_audit` DAG

**Files:**
- Create: `services/orchestration/dags/data_quality_audit.py`
- Create: `services/orchestration/tests/test_data_quality_audit_dag.py`

**Interfaces:**
- Consumes: nothing from other orchestration modules (no asset imports,
  unlike `standard_score_pipeline.py` — this DAG has no outlets).
- Produces: `dag_id="data_quality_audit"` with two independent
  `BashOperator` tasks. This is the final user-facing deliverable — no
  later task depends on this module.

- [ ] **Step 1: Write the failing test**

Create `services/orchestration/tests/test_data_quality_audit_dag.py`:

```python
"""data_quality_audit DAG의 구조를 docker 없이 검증하는 테스트 (#253).

실제 task 실행(batch-jobs 컨테이너 기동, S3 업로드)은 로컬 Airflow에서
수동으로 확인한다(spec의 완료 조건). 여기서는 DAG가 정상 파싱되고, 두
task가 서로 독립적(병렬)이며, outlet이 없는지를 확인한다.
"""

from __future__ import annotations

import datetime
import importlib.util
import sys
from pathlib import Path

DAG_PATH = Path(__file__).resolve().parents[1] / "dags" / "data_quality_audit.py"


def _load_dag_module():
    spec = importlib.util.spec_from_file_location("data_quality_audit", DAG_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_dag_parses_with_expected_schedule():
    module = _load_dag_module()

    assert module.dag.dag_id == "data_quality_audit"
    assert module.dag.schedule_interval == "0 3 * * *"
    assert module.dag.catchup is False


def test_dag_contains_one_task_per_gold_table():
    module = _load_dag_module()

    task_ids = {task.task_id for task in module.dag.tasks}
    assert task_ids == {
        "audit_standard_segment_comfort_score",
        "audit_current_segment_comfort_score",
    }


def test_tasks_have_no_upstream_or_downstream_dependencies():
    module = _load_dag_module()

    for task in module.dag.tasks:
        assert task.upstream_task_ids == set()
        assert task.downstream_task_ids == set()


def test_tasks_have_no_outlets():
    module = _load_dag_module()

    for task in module.dag.tasks:
        assert task.outlets == []


def test_audit_standard_task_targets_standard_table():
    module = _load_dag_module()

    task = module.dag.get_task("audit_standard_segment_comfort_score")
    assert "audit-gold" in task.bash_command
    assert "--table=standard_segment_comfort_score" in task.bash_command


def test_audit_current_task_targets_current_table():
    module = _load_dag_module()

    task = module.dag.get_task("audit_current_segment_comfort_score")
    assert "audit-gold" in task.bash_command
    assert "--table=current_segment_comfort_score" in task.bash_command


def test_tasks_pass_postgres_and_aws_env_vars():
    module = _load_dag_module()

    for task_id in (
        "audit_standard_segment_comfort_score",
        "audit_current_segment_comfort_score",
    ):
        command = module.dag.get_task(task_id).bash_command
        for env_var in (
            "POSTGRES_HOST",
            "POSTGRES_PORT",
            "POSTGRES_DB",
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_REGION",
            "GOLD_AUDIT_S3_BUCKET",
        ):
            assert env_var in command


def test_dag_preserves_retry_policy():
    module = _load_dag_module()

    for task in module.dag.tasks:
        assert task.retries == 1
        assert task.retry_delay == datetime.timedelta(minutes=5)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --package orchestration pytest services/orchestration/tests/test_data_quality_audit_dag.py -v`
Expected: FAIL with `FileNotFoundError`/module load error (DAG file doesn't
exist yet).

- [ ] **Step 3: Write the DAG**

Create `services/orchestration/dags/data_quality_audit.py`:

```python
"""at-rest Gold 감시용 신규 DAG (#253, ADR-0004 롤아웃 ④번).

`standard_segment_comfort_score`/`current_segment_comfort_score`(Postgres)
전체 범위·freshness·`vehicle_profile_id` 참조 무결성을 매일 1회 독립
스케줄로 감사한다. in-flight 검증(#220, #249)과 달리 파이프라인 게이트가
아니라 완전히 독립된 DAG라 outlet이 없고, task가 실패해도 다른 DAG를
막지 않는다(soft fail — ADR-0004: "task 실패로 신호만 주고 다른 DAG는 막지
않음").

## 로컬 실행 방식 (임시, EMR Serverless 전환 시 사라짐)

`standard_score_pipeline.py`와 동일하게 BashOperator로 batch-jobs 컨테이너를
docker-outside-of-docker로 띄운다(`infra/compose/airflow.yaml`의 docker
socket 마운트 필요). local-lake 마운트는 필요 없다 — Postgres만 조회한다.
"""

from __future__ import annotations

import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

_AUDIT_GOLD_ENV_FLAGS = (
    "-e POSTGRES_HOST -e POSTGRES_PORT -e POSTGRES_DB -e POSTGRES_USER -e POSTGRES_PASSWORD "
    "-e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_REGION -e GOLD_AUDIT_S3_BUCKET "
)


def _audit_gold_bash_command(table: str) -> str:
    return (
        "docker run --rm --network de4-local "
        + _AUDIT_GOLD_ENV_FLAGS
        + "batch-jobs:${BATCH_JOBS_IMAGE_TAG:?BATCH_JOBS_IMAGE_TAG must be set} "
        "uv run --no-sync --package batch-jobs batch-jobs "
        f"audit-gold --table={table}"
    )


with DAG(
    dag_id="data_quality_audit",
    description="Gold(standard/current_segment_comfort_score) at-rest 품질 감시 — 매일 1회, soft fail",
    schedule="0 3 * * *",
    start_date=datetime.datetime(2026, 8, 20, tzinfo=datetime.UTC),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=5),
    },
    tags=["data-quality-audit", "comfort-score"],
) as dag:
    # 두 task는 서로 독립이라(의존관계 없음) 병렬로 실행된다. outlet이 없어
    # 이 DAG의 성공/실패는 어떤 다른 DAG도 깨우거나 막지 않는다.
    audit_standard_segment_comfort_score = BashOperator(
        task_id="audit_standard_segment_comfort_score",
        bash_command=_audit_gold_bash_command("standard_segment_comfort_score"),
    )
    audit_current_segment_comfort_score = BashOperator(
        task_id="audit_current_segment_comfort_score",
        bash_command=_audit_gold_bash_command("current_segment_comfort_score"),
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run --package orchestration pytest services/orchestration/tests/test_data_quality_audit_dag.py -v`
Expected: PASS (all tests).

- [ ] **Step 5: Commit**

```bash
git add services/orchestration/dags/data_quality_audit.py services/orchestration/tests/test_data_quality_audit_dag.py
git commit -m "feat(orchestration): add data_quality_audit DAG (#253)"
```

---

## Task 9: Wire Postgres/AWS env vars through `airflow.yaml` and `.env.example`

**Files:**
- Modify: `infra/compose/airflow.yaml`
- Modify: `.env.example`

**Interfaces:**
- Consumes: nothing (infra config only).
- Produces: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `GOLD_AUDIT_S3_BUCKET`
  become available inside the `airflow-scheduler` container's environment
  (so the `-e AWS_ACCESS_KEY_ID` etc. flags in Task 8's `docker run`
  commands can pass them through — matching how `POSTGRES_PASSWORD` already
  flows: host `.env` → docker-compose `${...}` interpolation →
  `x-airflow-env` → scheduler container env → `docker run -e VAR` inherits
  from the calling shell's env).

This task has no automated test (it's compose/env wiring, verified manually
in Task 10's local Airflow check) — but it must not be skipped, or the DAG
tasks will fail with empty AWS credentials at runtime.

- [ ] **Step 1: Add AWS/S3 env vars to `x-airflow-env`**

In `infra/compose/airflow.yaml`, inside the `x-airflow-env: &airflow-env`
block, add these lines right after the existing `POSTGRES_PASSWORD:
${POSTGRES_PASSWORD}` line:

```yaml
    # data_quality_audit DAG(#253)가 audit-gold task에 전달하는 값. GX Data
    # Docs를 S3에 업로드하는 데 쓴다(ADR-0004, spec §1).
    AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
    AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
    GOLD_AUDIT_S3_BUCKET: ${GOLD_AUDIT_S3_BUCKET}
```

(`AWS_REGION` is already referenced elsewhere in this repo's env
conventions and boto3 reads it directly from the process environment — add
it too for consistency since it isn't in this block yet:)

```yaml
    AWS_REGION: ${AWS_REGION}
```

- [ ] **Step 2: Add placeholders to `.env.example`**

In `.env.example`, add these lines right after the existing `AWS_REGION=`
line (do not fill in real values — this file is committed to git):

```
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
GOLD_AUDIT_S3_BUCKET=de4-data-quality-docs
```

Also change the existing blank `AWS_REGION=` line to
`AWS_REGION=ap-northeast-2` — this isn't a secret (it's the bucket's
region, spec §1) and gives new contributors a working default.

- [ ] **Step 3: Verify compose config parses**

Run: `docker compose -f infra/compose/postgres.yaml -f infra/compose/kafka.yaml -f infra/compose/airflow.yaml config --quiet`
Expected: exits `0` with no output (valid YAML, no interpolation errors —
missing `.env` vars just resolve to empty strings, which is fine for a
`config` syntax check).

- [ ] **Step 4: Commit**

```bash
git add infra/compose/airflow.yaml .env.example
git commit -m "chore(infra): wire AWS/S3 env vars for data_quality_audit DAG (#253)"
```

---

## Task 10: ADR-0004 amendment note + `context/data/quality-rules.md` update

**Files:**
- Modify: `docs/adr/0004-data-quality-validation-with-great-expectations.md`
- Modify: `context/data/quality-rules.md`

**Interfaces:**
- Consumes: nothing (documentation only).
- Produces: durable record of the `TupleS3StoreBackend`→local-render+`boto3`
  correction and the newly-implemented Gold at-rest audit rules, per
  AGENTS.md ("요구사항이나 아키텍처가 바뀌면 관련 코드와 함께 context/를
  갱신한다").

This task has no automated test — it's documentation. Follow the existing
`## 수정 노트 (#249)` pattern already used in
`docs/adr/0007-split-comfort-score-pipeline-into-three-dags.md` (see commit
`f22e2ca`): append a new section, never rewrite the original decision text.

- [ ] **Step 1: Add the amendment note to ADR-0004**

Read `docs/adr/0004-data-quality-validation-with-great-expectations.md`
first to find the exact heading text of its last two sections (`## 영향
범위` and `## 참고`). Insert a new `## 수정 노트 (#253)` section between
them, with this content:

```markdown
## 수정 노트 (#253)

`data_quality_audit` DAG(at-rest 감사, 이 ADR의 롤아웃 ④번) 구현 착수
직전 실측 검증 결과, 이 문서가 전제한 GX의 `TupleS3StoreBackend`가
`great-expectations==1.21.0`(이 repo에 고정된 버전)엔 더 이상 존재하지
않는다는 것을 확인했다 — GX 1.x가 self-hosted 클라우드 스토어(S3/GCS/Azure)를
걷어내고 `ephemeral`/`file`/`cloud`(유료 GX Cloud SaaS) 세 컨텍스트 모드로
단순화하면서 사라진 것으로 보인다. 이 문서의 "GX의 S3 store backend
(`TupleS3StoreBackend`, 레거시 GX부터 있던 기능)로 Data Docs를 S3에
호스팅한다"는 결정 자체(Data Docs는 S3에 쓴다)는 그대로 유효하지만, 구현
메커니즘은 다음으로 바뀐다: `TupleFilesystemStoreBackend`로 로컬 임시
디렉터리에 렌더링 → `boto3`로 그 디렉터리를 직접 S3에 업로드. GX가 S3에
직접 쓰는 대신 우리 코드가 렌더 결과물을 옮기는 역할을 맡는다. 상세 근거는
`docs/superpowers/specs/2026-08-20-data-quality-audit-dag-at-rest-gold-design.md`
§1을 참고한다.
```

- [ ] **Step 2: Update `context/data/quality-rules.md`**

Read the existing `## Gold quality` section first (it currently ends with
"A result that fails the accepted minimum coverage rule must not appear as
an ordinary high-confidence score."). Append a new subsection right after
it:

```markdown
### Gold at-rest audit (implemented, #253)

`standard_segment_comfort_score` and `current_segment_comfort_score` are
audited in full once a day by the independent `data_quality_audit` DAG
(`0 3 * * *`, soft fail — a failing task signals via a red Airflow task and
a Great Expectations Data Docs report in S3, but blocks no other DAG).
Implemented as `batch_jobs.gold_audit_validation` (GX `SqlAlchemyExecutionEngine`
against Postgres, ADR-0004):

- **Range**: `comfort_score`/`vertical_score`/`longitudinal_score`/
  `lateral_score` must each be in `[0, 100]`, checked across every row in
  the table (not just the latest run).
- **Freshness**: the newest row (`score_as_of` for
  `standard_segment_comfort_score`, `calculated_at` for
  `current_segment_comfort_score`, since the latter has no `score_as_of`
  column) must be no older than 10800 seconds (3 hours).
- **`vehicle_profile_id` referential integrity**: zero rows may reference a
  `vehicle_profile_id` absent from `vehicle_profile`.
  `standard_segment_comfort_score` already enforces this with a database
  `FOREIGN KEY` (migration `0006`), so this check is a no-op safety net
  there; `current_segment_comfort_score.vehicle_profile_id` has no such FK,
  so this is the only place that violation would be caught.

Schema/PK/required-column invariants remain the writers' responsibility
(`standard_writer.py`, `jobs/current_score.py`) at write time — this audit
does not duplicate them (ADR-0004: hard invariants stay in code, not GX).
```

- [ ] **Step 3: Commit**

```bash
git add docs/adr/0004-data-quality-validation-with-great-expectations.md context/data/quality-rules.md
git commit -m "docs: sync ADR-0004 and quality-rules with gold at-rest audit implementation (#253)"
```

---

## After all tasks: manual local Airflow verification (spec's completion criteria)

Not a code task — the spec's actual acceptance criteria require confirming
this by hand in the local Airflow UI (per this repo's project-memory
convention: after any Airflow-related change, bring up/check the local web
UI):

1. `make build-batch-jobs-image` (or repo's equivalent) to rebuild the
   `batch-jobs` image with the new `audit-gold` command.
2. Bring up the local stack (`docker compose ... up`), unpause
   `data_quality_audit` in the Airflow UI, trigger it manually.
3. Confirm both tasks succeed against normal seeded data.
4. Intentionally break data (insert an out-of-range `comfort_score`, an
   orphaned `vehicle_profile_id` in `current_segment_comfort_score`, or a
   stale `score_as_of`/`calculated_at`), re-trigger, confirm the
   corresponding task actually goes red (soft fail) while
   `current_score_pipeline` and other DAGs remain unaffected.
5. Confirm the Data Docs report is actually visible in the
   `de4-data-quality-docs` S3 bucket under
   `data-quality-audit/gold/<table>/index.html` for both the pass and fail
   runs.
6. Report back the local Airflow UI URL and admin credentials so the user
   can verify directly.
