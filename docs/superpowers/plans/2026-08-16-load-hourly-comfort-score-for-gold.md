# Load hourly_comfort_score for Gold Aggregation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a Spark batch load step that reads the last 168 hours of Silver3
`hourly_comfort_score`, joined with `hourly_segment_features.trip_count`, as a
DataFrame ready for the follow-up "데이터 연산" sub-issue (#101) to consume.

**Architecture:** A single pure function in a new `comfort_score/loader.py` module
builds Parquet paths via the existing `de4_core.join_uri` (local `file://` and
`s3://` share the same call), validates the actual on-disk schema against an
explicit contract before touching the data, filters to a caller-supplied 168-hour
window, collapses duplicate `scoring_version` rows to the highest semantic
version per key, and left-joins in `trip_count`. No write, no aggregation, no
orchestration wiring — those are explicitly out of scope (see Global
Constraints).

**Tech Stack:** Python 3.12, PySpark 4.1 (`services/batch-jobs` already depends
on it — no new dependency), pytest.

**Spec:** GitHub issue #117 (`feat: load hourly comfort score data for gold
aggregation`), `context/comfort-score.md`, `context/data/schema-catalog.md`
(`hourly_comfort_score`, `hourly_segment_features` sections — updated in this
same change to drop the retired `hourly_comfort_score.comfort_score` column),
`context/open-questions.md` OQ-039.

## Global Constraints

- Read the last 168 hours only; **re-read the full window every run** — no
  incremental/rolling aggregation (explicitly out of scope for #117).
- Read `hourly_comfort_score` against its **explicit** schema; missing columns
  or type mismatches must **fail clearly** (no silent null-fill, no
  quarantine — quarantine is out of scope for this issue).
- The data-lake location must never be hardcoded; it flows in through a plain
  function parameter (`data_lake_uri`) and is turned into per-table URIs with
  `de4_core.join_uri`, exactly as `join_uri`'s existing `file://`/`s3://`
  dispatch already supports — swapping local Parquet for S3 requires changing
  only the argument value, never the loader code.
- `hourly_segment_features.trip_count` must be joined in on
  `(segment_id, vehicle_profile_id, data_period_start)` — **left join**, so a
  comfort-score hour with no matching traffic-feature row keeps `trip_count`
  as `null` instead of silently disappearing (OQ-039 is joined-in as the
  currently-proposed direction, but stays `Open` in
  `context/open-questions.md` — do not mark it Accepted).
- When multiple `scoring_version` values exist for the same
  `(segment_id, vehicle_profile_id, data_period_start)`, keep only the row
  with the highest version, compared **semver-style** (split on `.`, cast
  each part to an integer, compare element-wise) — a plain string comparison
  would wrongly rank `"10.0.0"` below `"9.0.0"`.
- Output is a Spark `DataFrame` returned from a pure function — no write, no
  Gold-table load (out of scope, belongs to the next sub-issue), no real S3
  auth wiring (out of scope, local Parquet only for now).
- `hourly_comfort_score` no longer carries a combined `comfort_score` column —
  only `vertical_score` / `longitudinal_score` / `lateral_score` (this plan
  updates `context/data/schema-catalog.md` accordingly; the follow-up formula
  sub-issue derives `c_h` itself from the three directional scores).
- New test/fixture data must be built by hand from the schema contract —
  `hourly_comfort_score` (#88/#89) and `hourly_segment_features` (#116)
  producers are not implemented on this branch, so no real Parquet exists to
  borrow from.
- No new dependency: `pyspark` is already a `services/batch-jobs` dependency.

---

## File Structure

```
services/batch-jobs/src/comfort_score/
  schemas.py    (new)  — HOURLY_COMFORT_SCORE_SCHEMA, HOURLY_SEGMENT_FEATURES_JOIN_SCHEMA
  loader.py     (new)  — load_hourly_comfort_score_for_gold(...) + private helpers
services/batch-jobs/tests/
  test_comfort_score_loader.py  (new) — all tests for the two files above
```

No existing file is modified except `context/data/schema-catalog.md` (already
updated as part of this planning session — see the Global Constraints note
above; nothing further to do there in the tasks below).

---

## Task 1: Explicit Silver3 schemas

**Files:**
- Create: `services/batch-jobs/src/comfort_score/schemas.py`
- Test: `services/batch-jobs/tests/test_comfort_score_loader.py`

**Interfaces:**
- Produces: `HOURLY_COMFORT_SCORE_SCHEMA: StructType`,
  `HOURLY_SEGMENT_FEATURES_JOIN_SCHEMA: StructType` — consumed by Task 2's
  `_validate_schema` and Task 5's `load_hourly_comfort_score_for_gold`.

- [x] **Step 1: Write the failing test**

Create `services/batch-jobs/tests/test_comfort_score_loader.py` with this
opening content:

```python
from comfort_score.schemas import (
    HOURLY_COMFORT_SCORE_SCHEMA,
    HOURLY_SEGMENT_FEATURES_JOIN_SCHEMA,
)


def test_hourly_comfort_score_schema_matches_the_silver_contract():
    fields = {field.name: field.dataType.typeName() for field in HOURLY_COMFORT_SCORE_SCHEMA}
    assert fields == {
        "segment_id": "string",
        "vehicle_profile_id": "integer",
        "data_period_start": "timestamp",
        "data_period_end": "timestamp",
        "road_snapshot_date": "date",
        "vertical_score": "double",
        "longitudinal_score": "double",
        "lateral_score": "double",
        "scoring_version": "string",
        "sample_count": "long",
        "_run_id": "string",
        "_processed_at": "timestamp",
    }
    assert all(not field.nullable for field in HOURLY_COMFORT_SCORE_SCHEMA)


def test_hourly_segment_features_join_schema_covers_join_keys_and_trip_count():
    fields = {
        field.name: field.dataType.typeName() for field in HOURLY_SEGMENT_FEATURES_JOIN_SCHEMA
    }
    assert fields == {
        "segment_id": "string",
        "vehicle_profile_id": "integer",
        "data_period_start": "timestamp",
        "trip_count": "long",
    }
    assert all(not field.nullable for field in HOURLY_SEGMENT_FEATURES_JOIN_SCHEMA)
```

- [x] **Step 2: Run test to verify it fails**

Run: `uv run --package batch-jobs pytest services/batch-jobs/tests/test_comfort_score_loader.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'comfort_score.schemas'`

- [x] **Step 3: Write minimal implementation**

Create `services/batch-jobs/src/comfort_score/schemas.py`:

```python
"""Spark schemas for the Silver3 tables `comfort_score.loader` reads (#117)."""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# context/data/schema-catalog.md의 `hourly_comfort_score` 정의를 그대로 코드화한다.
# 이 테이블은 방향별 점수만 가지고 있고, 합산된 comfort_score는 Gold에만 있다.
HOURLY_COMFORT_SCORE_SCHEMA = StructType(
    [
        StructField("segment_id", StringType(), nullable=False),
        StructField("vehicle_profile_id", IntegerType(), nullable=False),
        StructField("data_period_start", TimestampType(), nullable=False),
        StructField("data_period_end", TimestampType(), nullable=False),
        StructField("road_snapshot_date", DateType(), nullable=False),
        StructField("vertical_score", DoubleType(), nullable=False),
        StructField("longitudinal_score", DoubleType(), nullable=False),
        StructField("lateral_score", DoubleType(), nullable=False),
        StructField("scoring_version", StringType(), nullable=False),
        StructField("sample_count", LongType(), nullable=False),
        StructField("_run_id", StringType(), nullable=False),
        StructField("_processed_at", TimestampType(), nullable=False),
    ]
)

# hourly_segment_features 전체가 아니라, 이 로더가 join에 실제로 쓰는 컬럼만
# 명시한다 (조인 키 세 개 + trip_count). 전체 스키마는 그 테이블을 직접
# 다루는 코드(#116)가 소유한다.
HOURLY_SEGMENT_FEATURES_JOIN_SCHEMA = StructType(
    [
        StructField("segment_id", StringType(), nullable=False),
        StructField("vehicle_profile_id", IntegerType(), nullable=False),
        StructField("data_period_start", TimestampType(), nullable=False),
        StructField("trip_count", LongType(), nullable=False),
    ]
)
```

- [x] **Step 4: Run test to verify it passes**

Run: `uv run --package batch-jobs pytest services/batch-jobs/tests/test_comfort_score_loader.py -v`
Expected: PASS (2 tests)

- [x] **Step 5: Commit**

```bash
git add services/batch-jobs/src/comfort_score/schemas.py services/batch-jobs/tests/test_comfort_score_loader.py
git commit -m "feat: define explicit Silver3 schemas for the comfort-score loader"
```

---

## Task 2: Schema validation helper

**Files:**
- Create: `services/batch-jobs/src/comfort_score/loader.py`
- Test: `services/batch-jobs/tests/test_comfort_score_loader.py`

**Interfaces:**
- Consumes: `HOURLY_COMFORT_SCORE_SCHEMA`, `HOURLY_SEGMENT_FEATURES_JOIN_SCHEMA` (Task 1)
- Produces: `_validate_schema(actual: StructType, expected: StructType, source: str) -> None`
  — raises `ValueError` naming every missing column and every type mismatch;
  consumed by Task 5's `_read_validated_parquet`.

- [x] **Step 1: Write the failing test**

Append to `services/batch-jobs/tests/test_comfort_score_loader.py`:

```python
import pytest
from comfort_score.loader import _validate_schema
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

EXPECTED = StructType(
    [
        StructField("segment_id", StringType(), nullable=False),
        StructField("trip_count", IntegerType(), nullable=False),
    ]
)


def test_validate_schema_passes_when_all_columns_and_types_match():
    actual = StructType(
        [
            StructField("segment_id", StringType(), nullable=False),
            StructField("trip_count", IntegerType(), nullable=False),
            StructField("extra_column", StringType(), nullable=True),
        ]
    )

    _validate_schema(actual, EXPECTED, source="test-source")  # must not raise


def test_validate_schema_raises_with_missing_column_names():
    actual = StructType([StructField("segment_id", StringType(), nullable=False)])

    with pytest.raises(ValueError, match="trip_count"):
        _validate_schema(actual, EXPECTED, source="test-source")


def test_validate_schema_raises_with_type_mismatch_detail():
    actual = StructType(
        [
            StructField("segment_id", StringType(), nullable=False),
            StructField("trip_count", StringType(), nullable=False),
        ]
    )

    with pytest.raises(ValueError, match="trip_count"):
        _validate_schema(actual, EXPECTED, source="test-source")
```

- [x] **Step 2: Run test to verify it fails**

Run: `uv run --package batch-jobs pytest services/batch-jobs/tests/test_comfort_score_loader.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'comfort_score.loader'`

- [x] **Step 3: Write minimal implementation**

Create `services/batch-jobs/src/comfort_score/loader.py`:

```python
"""Load the last N hours of `hourly_comfort_score` for Gold aggregation (#117).

다음 서브 이슈("데이터 연산", #101)가 바로 이어받을 수 있도록, 최근 168시간
윈도우의 hourly_comfort_score를 hourly_segment_features.trip_count와 결합해
Spark DataFrame으로 반환한다. 실제 집계/Shrinkage 연산과 Gold 적재는 이 함수의
범위 밖이다 (services/batch-jobs/src/comfort_score/formula.py, 후속 이슈).
"""

from __future__ import annotations

from pyspark.sql.types import StructType


def _validate_schema(actual: StructType, expected: StructType, source: str) -> None:
    """Fail clearly when `actual` is missing a column or has the wrong type.

    `spark.read.schema(...).parquet(...)`은 누락된 컬럼을 조용히 NULL로 채워서
    "명확히 실패"라는 완료 조건을 못 지키기 때문에, 실제로 읽은 스키마를 먼저
    비교한다. 여기 없는 추가 컬럼은 문제 삼지 않는다.
    """
    actual_fields = {field.name: field.dataType for field in actual.fields}

    missing = [field.name for field in expected.fields if field.name not in actual_fields]
    if missing:
        raise ValueError(f"{source}: missing required column(s): {', '.join(missing)}")

    mismatched = [
        f"{field.name} (expected {field.dataType}, got {actual_fields[field.name]})"
        for field in expected.fields
        if actual_fields[field.name] != field.dataType
    ]
    if mismatched:
        raise ValueError(f"{source}: column type mismatch: {', '.join(mismatched)}")
```

- [x] **Step 4: Run test to verify it passes**

Run: `uv run --package batch-jobs pytest services/batch-jobs/tests/test_comfort_score_loader.py -v`
Expected: PASS (5 tests)

- [x] **Step 5: Commit**

```bash
git add services/batch-jobs/src/comfort_score/loader.py services/batch-jobs/tests/test_comfort_score_loader.py
git commit -m "feat: fail fast on hourly_comfort_score schema mismatches"
```

---

## Task 3: 168-hour window filter

**Files:**
- Modify: `services/batch-jobs/src/comfort_score/loader.py`
- Test: `services/batch-jobs/tests/test_comfort_score_loader.py`

**Interfaces:**
- Produces: `_filter_window_hours(df: DataFrame, as_of: datetime, window_hours: int) -> DataFrame`
  — keeps rows with `window_start <= data_period_start < as_of`; consumed by
  Task 5's `load_hourly_comfort_score_for_gold`.

- [x] **Step 1: Write the failing test**

Append to `services/batch-jobs/tests/test_comfort_score_loader.py`:

```python
import os
import time
from datetime import datetime, timedelta, timezone

from comfort_score.loader import _filter_window_hours
from pyspark.sql import SparkSession

os.environ["TZ"] = "UTC"
time.tzset()

AS_OF = datetime(2026, 8, 16, 0, 0, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="session")
def spark():
    # 세션 전체에서 재사용: SparkSession 기동에 몇 초가 걸린다 (cleansing/test_reader.py와 동일 패턴).
    session = (
        SparkSession.builder.appName("batch-jobs-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_filter_window_hours_keeps_only_the_half_open_168_hour_window(spark):
    rows = spark.createDataFrame(
        [
            (AS_OF - timedelta(hours=169),),  # window 시작 1시간 전 — 제외
            (AS_OF - timedelta(hours=168),),  # window 시작 정각 — 포함
            (AS_OF - timedelta(hours=1),),  # window 안 — 포함
            (AS_OF,),  # as_of 자신 — 제외 (배타적 상한)
        ],
        "data_period_start timestamp",
    )

    kept = {row["data_period_start"] for row in _filter_window_hours(rows, AS_OF, 168).collect()}

    assert kept == {AS_OF - timedelta(hours=168), AS_OF - timedelta(hours=1)}
```

- [x] **Step 2: Run test to verify it fails**

Run: `uv run --package batch-jobs pytest services/batch-jobs/tests/test_comfort_score_loader.py -v`
Expected: FAIL with `ImportError: cannot import name '_filter_window_hours'`

- [x] **Step 3: Write minimal implementation**

Add to `services/batch-jobs/src/comfort_score/loader.py` (below the existing
`from __future__ import annotations` import block, adding new imports at the
top):

```python
from datetime import datetime, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
```

and the function itself:

```python
def _filter_window_hours(df: DataFrame, as_of: datetime, window_hours: int) -> DataFrame:
    """Keep rows with `data_period_start` in `[as_of - window_hours, as_of)`.

    상한을 배타적으로 둔다: as_of는 "지금"을 뜻하고, 그 시각에 시작하는 시간은
    아직 끝나지 않았으므로 이번 윈도우에 포함하지 않는다.
    """
    window_start = as_of - timedelta(hours=window_hours)
    return df.filter(
        (F.col("data_period_start") >= F.lit(window_start))
        & (F.col("data_period_start") < F.lit(as_of))
    )
```

- [x] **Step 4: Run test to verify it passes**

Run: `uv run --package batch-jobs pytest services/batch-jobs/tests/test_comfort_score_loader.py -v`
Expected: PASS (6 tests)

- [x] **Step 5: Commit**

```bash
git add services/batch-jobs/src/comfort_score/loader.py services/batch-jobs/tests/test_comfort_score_loader.py
git commit -m "feat: filter hourly_comfort_score to the 168-hour reload window"
```

---

## Task 4: Latest-`scoring_version` selection

**Files:**
- Modify: `services/batch-jobs/src/comfort_score/loader.py`
- Test: `services/batch-jobs/tests/test_comfort_score_loader.py`

**Interfaces:**
- Produces: `_select_latest_scoring_version(df: DataFrame) -> DataFrame` — keeps
  one row per `(segment_id, vehicle_profile_id, data_period_start)`, the one
  with the semver-highest `scoring_version`; consumed by Task 5's
  `load_hourly_comfort_score_for_gold`.

- [x] **Step 1: Write the failing test**

Append to `services/batch-jobs/tests/test_comfort_score_loader.py`:

```python
from comfort_score.loader import _select_latest_scoring_version


def test_select_latest_scoring_version_compares_semver_not_strings(spark):
    # 문자열 그대로 비교하면 "10.0.0" < "9.0.0"으로 잘못 판정된다 — 이 케이스가 그걸 잡는다.
    rows = spark.createDataFrame(
        [
            ("seg-1", 1, AS_OF, "9.0.0", 10),
            ("seg-1", 1, AS_OF, "10.0.0", 20),
            ("seg-2", 1, AS_OF, "1.1.1", 30),
        ],
        "segment_id string, vehicle_profile_id int, data_period_start timestamp, "
        "scoring_version string, sample_count long",
    )

    result = {
        (row["segment_id"], row["vehicle_profile_id"], row["data_period_start"]): row[
            "sample_count"
        ]
        for row in _select_latest_scoring_version(rows).collect()
    }

    assert result == {
        ("seg-1", 1, AS_OF): 20,  # "10.0.0"이 이겨야 한다
        ("seg-2", 1, AS_OF): 30,
    }
```

- [x] **Step 2: Run test to verify it fails**

Run: `uv run --package batch-jobs pytest services/batch-jobs/tests/test_comfort_score_loader.py -v`
Expected: FAIL with `ImportError: cannot import name '_select_latest_scoring_version'`

- [x] **Step 3: Write minimal implementation**

Add to the imports at the top of `services/batch-jobs/src/comfort_score/loader.py`:

```python
from pyspark.sql.window import Window
```

and a module-level constant plus the function:

```python
# 조인 키: 두 테이블 스키마 모두에 공통으로 존재한다 (schema-catalog.md).
JOIN_KEYS = ["segment_id", "vehicle_profile_id", "data_period_start"]


def _select_latest_scoring_version(df: DataFrame) -> DataFrame:
    """Keep only the semver-highest `scoring_version` row per `JOIN_KEYS`.

    "1.1.1" < "3.1.1" < "10.0.0"처럼 세미버전 규칙으로 비교해야 하므로, 점으로
    나눈 각 자리를 정수 배열로 캐스팅해 사전식으로 비교한다.
    """
    version_rank = F.split(F.col("scoring_version"), r"\.").cast("array<int>")
    ranking_window = Window.partitionBy(*JOIN_KEYS).orderBy(version_rank.desc())
    return (
        df.withColumn("_version_rank", F.row_number().over(ranking_window))
        .filter(F.col("_version_rank") == 1)
        .drop("_version_rank")
    )
```

- [x] **Step 4: Run test to verify it passes**

Run: `uv run --package batch-jobs pytest services/batch-jobs/tests/test_comfort_score_loader.py -v`
Expected: PASS (7 tests)

- [x] **Step 5: Commit**

```bash
git add services/batch-jobs/src/comfort_score/loader.py services/batch-jobs/tests/test_comfort_score_loader.py
git commit -m "feat: keep only the semver-highest scoring_version per key"
```

---

## Task 5: Full loader — happy path

**Files:**
- Modify: `services/batch-jobs/src/comfort_score/loader.py`
- Test: `services/batch-jobs/tests/test_comfort_score_loader.py`

**Interfaces:**
- Consumes: `HOURLY_COMFORT_SCORE_SCHEMA`, `HOURLY_SEGMENT_FEATURES_JOIN_SCHEMA`
  (Task 1), `_validate_schema` (Task 2), `_filter_window_hours` (Task 3),
  `_select_latest_scoring_version` (Task 4), `JOIN_KEYS` (Task 4),
  `de4_core.join_uri`.
- Produces: `load_hourly_comfort_score_for_gold(spark: SparkSession, data_lake_uri: str, as_of: datetime, window_hours: int = 168) -> DataFrame`
  — the public entry point this whole module exists for.

- [x] **Step 1: Write the failing test**

Append to `services/batch-jobs/tests/test_comfort_score_loader.py`:

```python
from pathlib import Path

from comfort_score.loader import load_hourly_comfort_score_for_gold
from pyspark.sql.types import StructType


def _write_rows(spark, path: Path, schema: StructType, rows: list[dict]) -> None:
    data = [tuple(row[field.name] for field in schema.fields) for row in rows]
    spark.createDataFrame(data, schema).write.parquet(str(path))


def _comfort_score_row(**overrides: object) -> dict:
    base = {
        "segment_id": "seg-1",
        "vehicle_profile_id": 1,
        "data_period_start": AS_OF - timedelta(hours=1),
        "data_period_end": AS_OF,
        "road_snapshot_date": (AS_OF - timedelta(hours=1)).date(),
        "vertical_score": 80.0,
        "longitudinal_score": 80.0,
        "lateral_score": 80.0,
        "scoring_version": "1.0.0",
        "sample_count": 10,
        "_run_id": "run-1",
        "_processed_at": AS_OF,
    }
    return base | overrides


def _segment_features_row(**overrides: object) -> dict:
    base = {
        "segment_id": "seg-1",
        "vehicle_profile_id": 1,
        "data_period_start": AS_OF - timedelta(hours=1),
        "trip_count": 5,
    }
    return base | overrides


def test_load_hourly_comfort_score_for_gold_windows_and_joins_trip_count(spark, tmp_path):
    _write_rows(
        spark,
        tmp_path / "silver" / "hourly_comfort_score",
        HOURLY_COMFORT_SCORE_SCHEMA,
        [
            _comfort_score_row(segment_id="in-window"),
            _comfort_score_row(
                segment_id="out-of-window", data_period_start=AS_OF - timedelta(hours=200)
            ),
            _comfort_score_row(segment_id="no-traffic-features", vehicle_profile_id=2),
        ],
    )
    _write_rows(
        spark,
        tmp_path / "silver" / "hourly_segment_features",
        HOURLY_SEGMENT_FEATURES_JOIN_SCHEMA,
        [_segment_features_row(segment_id="in-window", trip_count=7)],
    )

    result = {
        row["segment_id"]: row["trip_count"]
        for row in load_hourly_comfort_score_for_gold(spark, str(tmp_path), AS_OF).collect()
    }

    # out-of-window 행은 아예 빠지고, trip_count가 매칭 안 되면 null로 남는다 (left join).
    assert result == {"in-window": 7, "no-traffic-features": None}
```

Also add the two schema-module names to the existing `from comfort_score.schemas
import (...)` import at the top of the test file (they were only imported
inside Task 1's test functions' module scope already via the top-level
import — confirm it reads exactly):

```python
from comfort_score.schemas import (
    HOURLY_COMFORT_SCORE_SCHEMA,
    HOURLY_SEGMENT_FEATURES_JOIN_SCHEMA,
)
```

- [x] **Step 2: Run test to verify it fails**

Run: `uv run --package batch-jobs pytest services/batch-jobs/tests/test_comfort_score_loader.py -v`
Expected: FAIL with `ImportError: cannot import name 'load_hourly_comfort_score_for_gold'`

- [x] **Step 3: Write minimal implementation**

Add to the imports at the top of `services/batch-jobs/src/comfort_score/loader.py`:

```python
from comfort_score.schemas import (
    HOURLY_COMFORT_SCORE_SCHEMA,
    HOURLY_SEGMENT_FEATURES_JOIN_SCHEMA,
)
from de4_core import join_uri
from pyspark.sql import SparkSession
```

and the public function plus its private read helper:

```python
DEFAULT_WINDOW_HOURS = 168


def load_hourly_comfort_score_for_gold(
    spark: SparkSession,
    data_lake_uri: str,
    as_of: datetime,
    window_hours: int = DEFAULT_WINDOW_HOURS,
) -> DataFrame:
    """Load `hourly_comfort_score`, windowed and joined with `trip_count`.

    - `data_lake_uri`는 로컬 Parquet 루트(예: `file://.../data/local-lake`)와
      운영 S3 루트(`s3://...`)를 함수 수정 없이 그대로 바꿔 끼울 수 있는
      파라미터다 (join_uri가 두 스킴을 모두 처리한다).
    - 매 호출마다 [as_of - window_hours, as_of) 구간 전체를 다시 읽는다 (증분 없음).
    - trip_count는 comfort-score.md가 제안하는 T_h 소스(OQ-039, 아직 Open)를
      left join으로 가져온다 — 매칭 안 되면 null로 남기고 행을 버리지 않는다.
    """
    comfort_score_uri = join_uri(data_lake_uri, "silver", "hourly_comfort_score")
    segment_features_uri = join_uri(data_lake_uri, "silver", "hourly_segment_features")

    comfort_score_df = _read_validated_parquet(
        spark, comfort_score_uri, HOURLY_COMFORT_SCORE_SCHEMA
    )
    segment_features_df = _read_validated_parquet(
        spark, segment_features_uri, HOURLY_SEGMENT_FEATURES_JOIN_SCHEMA
    )

    windowed = _filter_window_hours(comfort_score_df, as_of, window_hours)
    latest = _select_latest_scoring_version(windowed)
    trip_counts = segment_features_df.select(*JOIN_KEYS, "trip_count")

    return latest.join(trip_counts, on=JOIN_KEYS, how="left")


def _read_validated_parquet(spark: SparkSession, uri: str, expected: StructType) -> DataFrame:
    df = spark.read.parquet(uri)
    _validate_schema(df.schema, expected, uri)
    return df
```

- [x] **Step 4: Run test to verify it passes**

Run: `uv run --package batch-jobs pytest services/batch-jobs/tests/test_comfort_score_loader.py -v`
Expected: PASS (8 tests)

- [x] **Step 5: Commit**

```bash
git add services/batch-jobs/src/comfort_score/loader.py services/batch-jobs/tests/test_comfort_score_loader.py
git commit -m "feat: load hourly_comfort_score windowed and joined with trip_count"
```

---

## Task 6: Fail-fast integration coverage

**Files:**
- Test: `services/batch-jobs/tests/test_comfort_score_loader.py`

**Interfaces:**
- Consumes: `load_hourly_comfort_score_for_gold` (Task 5) — exercised
  end-to-end, not just the `_validate_schema` unit in Task 2, to prove the
  wiring itself surfaces the failure and not just the helper in isolation.

- [x] **Step 1: Write the failing test**

Append to `services/batch-jobs/tests/test_comfort_score_loader.py`:

```python
def test_load_raises_clearly_when_hourly_comfort_score_is_missing_a_column(spark, tmp_path):
    incomplete_schema = StructType(
        [field for field in HOURLY_COMFORT_SCORE_SCHEMA.fields if field.name != "sample_count"]
    )
    _write_rows(
        spark, tmp_path / "silver" / "hourly_comfort_score", incomplete_schema, [
            {k: v for k, v in _comfort_score_row().items() if k != "sample_count"}
        ]
    )
    _write_rows(
        spark,
        tmp_path / "silver" / "hourly_segment_features",
        HOURLY_SEGMENT_FEATURES_JOIN_SCHEMA,
        [_segment_features_row()],
    )

    with pytest.raises(ValueError, match="sample_count"):
        load_hourly_comfort_score_for_gold(spark, str(tmp_path), AS_OF)


def test_load_raises_clearly_when_hourly_comfort_score_has_a_type_mismatch(spark, tmp_path):
    mismatched_schema = StructType(
        [
            StructField("sample_count", StringType(), nullable=False)
            if field.name == "sample_count"
            else field
            for field in HOURLY_COMFORT_SCORE_SCHEMA.fields
        ]
    )
    _write_rows(
        spark,
        tmp_path / "silver" / "hourly_comfort_score",
        mismatched_schema,
        [_comfort_score_row(sample_count="10")],
    )
    _write_rows(
        spark,
        tmp_path / "silver" / "hourly_segment_features",
        HOURLY_SEGMENT_FEATURES_JOIN_SCHEMA,
        [_segment_features_row()],
    )

    with pytest.raises(ValueError, match="sample_count"):
        load_hourly_comfort_score_for_gold(spark, str(tmp_path), AS_OF)
```

Add `StringType` to the existing `pyspark.sql.types` import line at the top of
the test file (alongside `StructType`).

- [x] **Step 2: Run test to verify it fails**

Run: `uv run --package batch-jobs pytest services/batch-jobs/tests/test_comfort_score_loader.py -v`
Expected: FAIL — both new tests error before reaching the `pytest.raises`
block (or raise a different exception type / no exception), because nothing
about Task 5's code changes for this task; confirm this by reading the
failure output, since this task only adds coverage.

Actually, given Task 5 already validates schema on read, these two tests are
expected to **pass immediately** once written — this step exists to build
confidence, not to drive new production code. If either test fails, that's a
real bug in Task 2/5's implementation to fix before continuing (most likely:
`spark.read.parquet(uri)` inferring a schema for the type-mismatch fixture
that happens to already look like `StringType`, in which case adjust the
fixture to use a genuinely incompatible type such as `IntegerType`).

- [x] **Step 3: Confirm passing, no implementation change expected**

Run: `uv run --package batch-jobs pytest services/batch-jobs/tests/test_comfort_score_loader.py -v`
Expected: PASS (10 tests)

- [x] **Step 4: Run the full batch-jobs suite and lint**

Run:
```bash
uv run --all-packages ruff check services/batch-jobs
uv run --package batch-jobs pytest services/batch-jobs/tests -v
```
Expected: all pass, no lint errors.

- [x] **Step 5: Commit**

```bash
git add services/batch-jobs/tests/test_comfort_score_loader.py
git commit -m "test: cover fail-fast schema errors end-to-end through the loader"
```

---

## Self-Review

**1. Spec coverage** — issue #117 acceptance criteria mapped to tasks:
- "168시간 윈도우 행만 로드" → Task 3 (unit), Task 5 (integration)
- "trip_count 결합" → Task 5
- "경로 하드코딩 없음, S3 교체 가능" → Task 5's `data_lake_uri` param +
  `join_uri`; no loader-internal path logic exists to change on swap
- "스키마 다른 입력 시 명확히 실패" → Task 2 (unit), Task 6 (integration)
- "fixture 데이터로 유닛 테스트 통과" → all tasks build fixtures inline via
  `tmp_path`, matching `cleansing/tests/test_reader.py`'s convention
- `scoring_version` 정책 결정 → Task 4
- "다음 서브 이슈가 바로 쓸 수 있는 형태" → Task 5 returns a plain `DataFrame`,
  no write

**2. Placeholder scan** — no TBD/TODO, no "add appropriate handling" left in
any step; every step has literal code or an exact `pytest`/`ruff` command.

**3. Type consistency** — `_validate_schema`, `_filter_window_hours`,
`_select_latest_scoring_version`, `_read_validated_parquet`, and
`load_hourly_comfort_score_for_gold` are named and typed identically every
place they're referenced across Tasks 2–6; `JOIN_KEYS` defined once in Task 4
and reused as-is in Task 5.

---

## Execution Log (2026-08-16)

All 6 tasks executed inline on `feat/117-load-hourly-comfort-score-for-gold`,
one commit per task, exactly as listed above. Two small deviations from the
literal step text above, both discovered only while running the tests:

- **Task 3's test timestamps**: the plan text above shows `AS_OF` built with
  `tzinfo=timezone.utc`. In practice, `spark.collect()` returns naive
  `datetime` values for `TimestampType` columns (confirmed against the
  existing `test_hourly_aggregation.py` / `test_road_segment_persist.py`
  convention in this same test directory), so a tz-aware `AS_OF` compared
  against collected rows never matches. Fixed by defining `AS_OF` as a plain
  naive `datetime` with `# noqa: DTZ001` and an explanatory comment — the
  same idiom already used in `test_road_segment_persist.py:110` and
  `test_kafka_source.py:32`. No production code was affected.
- **Import ordering**: `uv run ruff check --fix` reordered the import blocks
  in `loader.py` and the test file (`de4_core`/`pyspark` before the local
  `comfort_score` package) — cosmetic only.

Final verification (repo root):

```
uv run --all-packages ruff check .     # All checks passed!
uv run --all-packages pytest           # 248 passed
```

**Not done, by design** (per the user's explicit instruction not to touch
integration/release surfaces while unsupervised): no PR opened, no push, no
merge, no rebase. Six commits sit locally on
`feat/117-load-hourly-comfort-score-for-gold`, ready for review:

```
829356f test: cover fail-fast schema errors end-to-end through the loader
3fbd6af feat: load hourly_comfort_score windowed and joined with trip_count
3a344cb feat: keep only the semver-highest scoring_version per key
a644bb2 feat: filter hourly_comfort_score to the 168-hour reload window
8fd2fb2 feat: fail fast on hourly_comfort_score schema mismatches
98e10fa feat: define explicit Silver3 schemas for the comfort-score loader
```

Plus the earlier, separately-committed `context/data/schema-catalog.md` fix
(dropping the retired `hourly_comfort_score.comfort_score` column) made
during the planning conversation, already on this branch.

**Left for the user to decide before merging (not addressed by this
issue's scope):**
- Whether/how `data_lake_uri` gets wired to an env-driven config + CLI entry
  point for a real orchestration run (issue #117 only asked for the loader
  function itself).
- OQ-039 remains formally **Open** in `context/open-questions.md` — this
  loader implements the currently-proposed join direction but does not
  close the question.

---

## Post-merge rework (2026-08-16, before PR)

While drafting the PR, diffing against `origin/develop` surfaced that this
branch was 7 commits behind, including `3336573` ("feat: retain trip count in
hourly comfort scores", merged the night before via #113/#118). That commit
made `hourly_comfort_score` carry its own `trip_count` — copied straight
through from `hourly_segment_features` at Silver3-compute time
(`batch_jobs/hourly_comfort.py`) — which made this loader's
`hourly_segment_features` join redundant, and made the loader's own
`comfort_score/schemas.py` a second, now-incorrect (missing `trip_count`)
definition of a schema that already exists as
`batch_jobs.schemas.HOURLY_COMFORT_SCORE_SCHEMA`.

Surfaced this to the user (who was awake again by this point) instead of
silently reworking the design; they chose "drop the join, reuse the existing
schema" (the option this log records).

Changes made after merging `origin/develop`:
- `git merge origin/develop` (merge commit, not rebase — this repo bans force
  push, which a rebase would have required to publish).
- Deleted `services/batch-jobs/src/comfort_score/schemas.py` entirely.
- `loader.py` now imports `HOURLY_COMFORT_SCORE_SCHEMA` from
  `batch_jobs.schemas` (the pre-existing, already-tested contract) instead of
  redefining it; `hourly_segment_features` is no longer read at all;
  `JOIN_KEYS` renamed to `PRIMARY_KEY` (no join left to name after).
- Tests rewritten to match: no `hourly_segment_features` fixture, `trip_count`
  asserted to pass straight through from `hourly_comfort_score`.
- Re-ran `uv run --all-packages ruff check .` and
  `uv run --all-packages pytest` after the rework — both clean (276 passed).

Not changed, left for the PR reviewer to weigh in on: `OQ-039` in
`context/open-questions.md` is still marked **Open**, even though
`3336573` already resolved it in practice (`hourly_comfort_score` growing its
own traffic-count column, not the Gold job joining `hourly_segment_features`
for it). Flagging this rather than flipping its status myself, since that
question belongs to whoever owns it.

