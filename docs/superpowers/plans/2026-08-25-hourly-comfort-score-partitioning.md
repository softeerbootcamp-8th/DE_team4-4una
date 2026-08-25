# `hourly_comfort_score` 시간 파티셔닝 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `hourly_comfort_score`를 시간별로 파티셔닝해, `standard_score_pipeline`의 세 task가 전체 이력이 아니라 필요한 시간대만 읽고 쓰게 한다.

**Architecture:** Silver2와 같은 `data_period_date=YYYY-MM-DD/hour=HH` 2단 파티션을 쓴다. 쓰기는 저장소에 이미 두 벌 있는 staging → read-back 검증 → backup rename 교체 패턴을 따르고, 읽기는 검증이 해당 파티션만, 168시간 롤업이 파티션 컬럼 프루닝으로 정확히 168개만 읽는다. 기존 평면 데이터는 재파티션하지 않고 아카이브로 옮긴다.

**Tech Stack:** Python 3.12, PySpark 4.x, Great Expectations 1.21, pyarrow, Airflow 3.3, uv 워크스페이스

**Spec:** `docs/superpowers/specs/2026-08-25-hourly-comfort-score-partitioning-design.md`

## Global Constraints

- Python 3.12, 의존성은 `uv`로 관리한다. 이 계획은 새 의존성을 추가하지 않는다.
- Spark 테스트는 **JDK 21**이 필요하다. JDK 24 이상은 `SparkSession` 생성에서 `UnsupportedOperationException: getSubject is not supported`로 실패한다.
- 검증 명령은 저장소 루트에서 실행한다.
  ```bash
  uv run --all-packages ruff check .
  JAVA_HOME=/opt/homebrew/opt/openjdk@21 uv run --all-packages pytest
  ```
- 커밋 메시지는 `<type>: <subject>` 형식이고 본문은 한국어다. `Refs #469`를 푸터에 넣는다. **`Co-Authored-By: Claude`나 "Generated with Claude Code" 푸터를 넣지 않는다.**
- 브랜치는 `perf/469-partition-hourly-comfort-score`(이미 생성됨, `origin/develop` 기준)를 쓴다.
- 코드 주석은 한국어 인라인 `#`로 쓰고, docstring은 기존 스타일대로 영어를 허용한다. 왜 그렇게 했는지가 자명하지 않은 곳에만 단다.
- backup 디렉터리 이름은 반드시 `_`로 시작한다. Spark 파티션 탐색이 `_`/`.` 로 시작하는 디렉터리를 무시하기 때문이다.
- 시각 인자는 전부 UTC-aware `datetime`이고 정시로 잘려 있어야 한다.

---

## File Structure

| 파일 | 책임 |
| --- | --- |
| `services/batch-jobs/src/batch_jobs/hourly_comfort_storage.py` (신규) | `hourly_comfort_score`/`rejected`의 한 시간 파티션을 안전하게 교체한다. 경로 조립 + staging/rename 절차 |
| `services/batch-jobs/src/batch_jobs/hourly_comfort_job.py` | Silver2의 한 시간 파티션을 읽어 채점하고, 위 모듈로 한 파티션만 쓴다 |
| `services/batch-jobs/src/batch_jobs/hourly_scoring_validation.py` | 이번 실행이 쓴 파티션만 GX로 검증한다 |
| `services/batch-jobs/src/batch_jobs/comfort_score/loader.py` | 파티션 컬럼으로 정확히 168시간만 읽는다 |
| `services/batch-jobs/src/batch_jobs/cli.py` | 두 명령에 `--target-hour`를 받는다 |
| `services/orchestration/dags/standard_score_pipeline.py` | 두 task에 `--target-hour`를 넘긴다 |
| `services/orchestration/jobs/pipeline_counts.py` | 파티션 경로로 건수를 센다 |

**의도적으로 하지 않는 것**: `hourly_comfort_storage.py`는 Hadoop FS 헬퍼(`_path_exists`/`_delete_path`/`_rename_path`)를 `cleansing/hourly_storage.py`, `hourly_segment_feature_storage.py`와 중복해 갖는다. 세 벌을 공용 모듈로 합치는 것은 이미 동작 중인 두 모듈을 건드리게 되므로 이 이슈 범위 밖이다. 후속 이슈 후보로 남긴다.

---

### Task 1: 파티션 저장 모듈

**Files:**
- Create: `services/batch-jobs/src/batch_jobs/hourly_comfort_storage.py`
- Test: `services/batch-jobs/tests/test_hourly_comfort_storage.py`

**Interfaces:**
- Consumes: 없음 (신규 모듈)
- Produces:
  - `hour_output_path(output_root: str, target_hour: datetime) -> str`
  - `write_hourly_comfort_partition(spark: SparkSession, frame: DataFrame, output_root: str, target_hour: datetime, run_id: str, expected_schema: StructType) -> HourlyComfortWriteResult`
  - `HourlyComfortWriteResult(output_path: str, row_count: int)`

- [ ] **Step 1: 경로 헬퍼의 실패 테스트를 쓴다**

`services/batch-jobs/tests/test_hourly_comfort_storage.py`:

```python
"""Tests for batch_jobs/hourly_comfort_storage.py (#469)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from batch_jobs.hourly_comfort_storage import (
    _backup_path,
    hour_output_path,
)


def test_hour_output_path_matches_the_silver2_layout():
    target_hour = datetime(2026, 8, 25, 9, tzinfo=UTC)

    path = hour_output_path("s3://de4-lake/silver/hourly_comfort_score", target_hour)

    assert path == (
        "s3://de4-lake/silver/hourly_comfort_score"
        "/data_period_date=2026-08-25/hour=09"
    )


def test_hour_output_path_strips_a_trailing_slash():
    target_hour = datetime(2026, 8, 25, 9, tzinfo=UTC)

    path = hour_output_path("data/local-lake/silver/hourly_comfort_score/", target_hour)

    assert path.endswith("/data_period_date=2026-08-25/hour=09")
    assert "//data_period_date" not in path


def test_backup_path_starts_with_an_underscore_so_spark_ignores_it():
    # `hour=09.bak`처럼 쓰면 Spark 파티션 탐색이 hour="09.bak" 값으로 읽어
    # 컬럼 타입 추론이 int에서 string으로 바뀐다.
    final_path = (
        "s3://de4-lake/silver/hourly_comfort_score"
        "/data_period_date=2026-08-25/hour=09"
    )

    backup = _backup_path(final_path)

    assert backup == (
        "s3://de4-lake/silver/hourly_comfort_score"
        "/data_period_date=2026-08-25/_backup_hour=09"
    )
```

- [ ] **Step 2: 테스트가 실패하는지 확인한다**

Run: `uv run --package batch-jobs pytest services/batch-jobs/tests/test_hourly_comfort_storage.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'batch_jobs.hourly_comfort_storage'`

- [ ] **Step 3: 경로 헬퍼를 구현한다**

`services/batch-jobs/src/batch_jobs/hourly_comfort_storage.py`:

```python
"""Stage, validate, and replace one hourly comfort-score partition (#469)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

_STAGING_DIRNAME = "_staging"
_SAFE_RUN_ID = re.compile(r"^[A-Za-z0-9_.:+-]+$")


@dataclass(frozen=True, slots=True)
class HourlyComfortWriteResult:
    output_path: str
    row_count: int


def hour_output_path(output_root: str, target_hour: datetime) -> str:
    """Silver2(`hourly_segment_feature_storage.hour_output_path`)와 같은 레이아웃을 쓴다."""
    return (
        f"{output_root.rstrip('/')}/data_period_date={target_hour.date().isoformat()}"
        f"/hour={target_hour.hour:02d}"
    )


def _backup_path(final_path: str) -> str:
    # 반드시 `_`로 시작해야 Spark 파티션 탐색이 무시한다. `hour=09.bak`으로 쓰면
    # hour="09.bak" 값으로 인식돼 컬럼 타입 추론이 int에서 string으로 바뀐다.
    parent, name = final_path.rsplit("/", maxsplit=1)
    return f"{parent}/_backup_{name}"
```

- [ ] **Step 4: 테스트가 통과하는지 확인한다**

Run: `uv run --package batch-jobs pytest services/batch-jobs/tests/test_hourly_comfort_storage.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: 쓰기 절차의 실패 테스트를 쓴다**

같은 파일 상단 import에 다음을 추가한다.

```python
from batch_jobs.hourly_comfort_storage import (
    _backup_path,
    hour_output_path,
    write_hourly_comfort_partition,
)
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
```

파일 끝에 다음을 추가한다.

```python
SCHEMA = StructType(
    [
        StructField("segment_id", StringType(), nullable=False),
        StructField("vehicle_profile_id", IntegerType(), nullable=False),
        StructField("data_period_start", TimestampType(), nullable=False),
    ]
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("batch-jobs-tests")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def _frame(spark: SparkSession, target_hour: datetime, segment_ids: list[str]):
    return spark.createDataFrame(
        [(segment_id, 1, target_hour) for segment_id in segment_ids], SCHEMA
    )


def test_write_creates_the_target_hour_partition(spark, tmp_path):
    root = str(tmp_path / "hourly_comfort_score")
    target_hour = datetime(2026, 8, 25, 9, tzinfo=UTC)

    result = write_hourly_comfort_partition(
        spark, _frame(spark, target_hour, ["a", "b"]), root, target_hour, "run-1", SCHEMA
    )

    assert result.row_count == 2
    assert result.output_path == hour_output_path(root, target_hour)
    assert spark.read.schema(SCHEMA).parquet(result.output_path).count() == 2


def test_rewriting_the_same_hour_replaces_it_instead_of_appending(spark, tmp_path):
    root = str(tmp_path / "hourly_comfort_score")
    target_hour = datetime(2026, 8, 25, 9, tzinfo=UTC)
    write_hourly_comfort_partition(
        spark, _frame(spark, target_hour, ["a", "b"]), root, target_hour, "run-1", SCHEMA
    )

    result = write_hourly_comfort_partition(
        spark, _frame(spark, target_hour, ["c"]), root, target_hour, "run-2", SCHEMA
    )

    assert result.row_count == 1
    rows = spark.read.schema(SCHEMA).parquet(result.output_path).collect()
    assert [row.segment_id for row in rows] == ["c"]


def test_writing_one_hour_leaves_other_hours_untouched(spark, tmp_path):
    root = str(tmp_path / "hourly_comfort_score")
    first = datetime(2026, 8, 25, 9, tzinfo=UTC)
    second = datetime(2026, 8, 25, 10, tzinfo=UTC)
    write_hourly_comfort_partition(
        spark, _frame(spark, first, ["a"]), root, first, "run-1", SCHEMA
    )

    write_hourly_comfort_partition(
        spark, _frame(spark, second, ["b"]), root, second, "run-2", SCHEMA
    )

    assert spark.read.schema(SCHEMA).parquet(hour_output_path(root, first)).count() == 1
    assert spark.read.schema(SCHEMA).parquet(hour_output_path(root, second)).count() == 1


def test_an_empty_result_removes_the_existing_partition(spark, tmp_path):
    # rejected 출력은 정상 실행에서 비어 있는 것이 기본이다.
    root = str(tmp_path / "rejected")
    target_hour = datetime(2026, 8, 25, 9, tzinfo=UTC)
    write_hourly_comfort_partition(
        spark, _frame(spark, target_hour, ["a"]), root, target_hour, "run-1", SCHEMA
    )

    result = write_hourly_comfort_partition(
        spark, _frame(spark, target_hour, []), root, target_hour, "run-2", SCHEMA
    )

    assert result.row_count == 0
    assert not Path(hour_output_path(root, target_hour)).exists()


def test_staging_directory_is_cleaned_up(spark, tmp_path):
    root = str(tmp_path / "hourly_comfort_score")
    target_hour = datetime(2026, 8, 25, 9, tzinfo=UTC)

    write_hourly_comfort_partition(
        spark, _frame(spark, target_hour, ["a"]), root, target_hour, "run-1", SCHEMA
    )

    assert not (Path(root) / "_staging").exists()


def test_a_non_utc_target_hour_is_rejected(spark, tmp_path):
    root = str(tmp_path / "hourly_comfort_score")
    naive_hour = datetime(2026, 8, 25, 9)

    with pytest.raises(ValueError, match="UTC"):
        write_hourly_comfort_partition(
            spark, _frame(spark, datetime(2026, 8, 25, 9, tzinfo=UTC), ["a"]),
            root, naive_hour, "run-1", SCHEMA,
        )


def test_an_unsafe_run_id_is_rejected(spark, tmp_path):
    root = str(tmp_path / "hourly_comfort_score")
    target_hour = datetime(2026, 8, 25, 9, tzinfo=UTC)

    with pytest.raises(ValueError, match="unsafe path characters"):
        write_hourly_comfort_partition(
            spark, _frame(spark, target_hour, ["a"]), root, target_hour, "../escape", SCHEMA
        )
```

파일 상단 import에 `from pathlib import Path`를 추가한다.

- [ ] **Step 6: 테스트가 실패하는지 확인한다**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk@21 uv run --package batch-jobs pytest services/batch-jobs/tests/test_hourly_comfort_storage.py -v`
Expected: FAIL — `ImportError: cannot import name 'write_hourly_comfort_partition'`

- [ ] **Step 7: 쓰기 절차를 구현한다**

`hourly_comfort_storage.py`에 이어서 추가한다.

```python
def write_hourly_comfort_partition(
    spark: SparkSession,
    frame: DataFrame,
    output_root: str,
    target_hour: datetime,
    run_id: str,
    expected_schema: StructType,
) -> HourlyComfortWriteResult:
    """Replace only the requested UTC-hour partition after a read-back check.

    `cleansing/hourly_storage.py`, `hourly_segment_feature_storage.py`와 같은 절차다 —
    staging에 쓰고, 다시 읽어 행 수를 확인한 뒤에만 대상 파티션과 교체한다.
    """
    _require_safe_run_id(run_id)
    _require_utc_hour(target_hour)
    _require_schema(frame, expected_schema)

    final_path = hour_output_path(output_root, target_hour)
    staging_path = f"{output_root.rstrip('/')}/{_STAGING_DIRNAME}/{run_id}"
    expected_count = frame.count()

    try:
        staged_path: str | None = None
        if expected_count:
            # 직전 실행이 죽어 staging에 잔여물이 남아 있을 수 있다(#380). mode("overwrite")
            # 없이 쓰므로 미리 지워야 PATH_ALREADY_EXISTS로 막히지 않는다.
            _delete_path(spark, staging_path)
            frame.write.parquet(staging_path)
            staged = spark.read.schema(expected_schema).parquet(staging_path)
            if staged.count() != expected_count:
                raise ValueError("staged row count does not match the computed result")
            staged_path = staging_path
        # expected_count가 0이면 staged_path를 None으로 둬, 기존 파티션을 지우기만 한다.
        _replace_partition(spark, final_path, staged_path)
    finally:
        _delete_path(spark, staging_path)

    return HourlyComfortWriteResult(output_path=final_path, row_count=expected_count)


def _require_safe_run_id(run_id: str) -> None:
    if not _SAFE_RUN_ID.fullmatch(run_id):
        raise ValueError(f"run_id contains unsafe path characters: {run_id!r}")


def _require_utc_hour(target_hour: datetime) -> None:
    # tzinfo를 떼면 호스트 OS 타임존에 따라 다른 시각으로 재해석된다.
    if target_hour.utcoffset() != timedelta(0):
        raise ValueError("target_hour must be UTC timezone-aware")
    if (target_hour.minute, target_hour.second, target_hour.microsecond) != (0, 0, 0):
        raise ValueError("target_hour must be truncated to the hour")


def _require_schema(frame: DataFrame, expected: StructType) -> None:
    actual_fields = {field.name: field.dataType for field in frame.schema.fields}
    expected_fields = {field.name: field.dataType for field in expected.fields}
    if actual_fields != expected_fields:
        raise ValueError(
            f"schema mismatch: expected {expected.simpleString()}, "
            f"got {frame.schema.simpleString()}"
        )


def _replace_partition(
    spark: SparkSession, final_path: str, staged_path: str | None
) -> None:
    """백업 후 rename으로 스왑하고, 실패 시 백업에서 되돌린다.

    **S3(EMRFS) 주의**: EMRFS의 `FileSystem.rename()`은 디렉터리의 각 객체를 copy 후
    delete하는 방식이라 원자적이지 않다. `cleansing/hourly_storage.py::_replace_partition`과
    동일한 위험을 그대로 감수한다(#290).
    """
    backup_path = _backup_path(final_path)
    had_existing = False
    promoted = False
    try:
        _recover_backup(spark, final_path, backup_path)
        if _path_exists(spark, final_path):
            _rename_path(spark, final_path, backup_path)
            had_existing = True

        if staged_path is not None:
            _make_parent_directory(spark, final_path)
            _rename_path(spark, staged_path, final_path)
            promoted = True
    except Exception:
        if promoted:
            _delete_path(spark, final_path)
        if had_existing:
            _rename_path(spark, backup_path, final_path)
        raise
    else:
        if had_existing:
            _delete_path(spark, backup_path)


def _recover_backup(spark: SparkSession, final_path: str, backup_path: str) -> None:
    # 직전 실행이 final -> backup 이동 직후 죽었다면 backup이 유일한 정상본이다.
    if not _path_exists(spark, backup_path):
        return
    if _path_exists(spark, final_path):
        _delete_path(spark, backup_path)
    else:
        _rename_path(spark, backup_path, final_path)


def _hadoop_path(spark: SparkSession, path: str):
    return spark._jvm.org.apache.hadoop.fs.Path(path)


def _filesystem(spark: SparkSession, path: str):
    hadoop_path = _hadoop_path(spark, path)
    return hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())


def _path_exists(spark: SparkSession, path: str) -> bool:
    return bool(_filesystem(spark, path).exists(_hadoop_path(spark, path)))


def _delete_path(spark: SparkSession, path: str) -> None:
    filesystem = _filesystem(spark, path)
    hadoop_path = _hadoop_path(spark, path)
    if filesystem.exists(hadoop_path):
        filesystem.delete(hadoop_path, True)


def _rename_path(spark: SparkSession, source: str, destination: str) -> None:
    filesystem = _filesystem(spark, source)
    renamed = filesystem.rename(
        _hadoop_path(spark, source), _hadoop_path(spark, destination)
    )
    if not renamed:
        raise OSError(f"failed to rename {source!r} to {destination!r}")


def _make_parent_directory(spark: SparkSession, path: str) -> None:
    parent = _hadoop_path(spark, path).getParent()
    filesystem = parent.getFileSystem(spark._jsc.hadoopConfiguration())
    if not filesystem.mkdirs(parent) and not filesystem.exists(parent):
        raise OSError(f"failed to create output directory {parent}")
```

- [ ] **Step 8: 테스트가 통과하는지 확인한다**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk@21 uv run --package batch-jobs pytest services/batch-jobs/tests/test_hourly_comfort_storage.py -v`
Expected: PASS (10 passed)

- [ ] **Step 9: 커밋한다**

```bash
git add services/batch-jobs/src/batch_jobs/hourly_comfort_storage.py \
        services/batch-jobs/tests/test_hourly_comfort_storage.py
git commit -F - <<'MSG'
feat: add hourly comfort score partition writer

hourly_comfort_score를 시간별로 교체하려면 staging에 쓰고 다시 읽어 확인한 뒤
대상 파티션만 스왑하는 절차가 필요하다. cleansing/hourly_storage.py와
hourly_segment_feature_storage.py가 쓰는 것과 같은 패턴이다.

backup 디렉터리 이름은 _backup_ 접두어를 쓴다. hourly_segment_feature_storage는
hour=09.bak으로 만드는데, 이러면 Spark 파티션 탐색이 hour="09.bak" 값으로 읽어
컬럼 타입 추론이 int에서 string으로 바뀐다.

빈 결과는 기존 파티션을 지우기만 하고 새로 쓰지 않는다 — rejected 출력은 정상
실행에서 비어 있는 것이 기본이다.

Refs #469
MSG
```

---

### Task 2: 채점 job을 한 시간 파티션으로 좁힌다

**Files:**
- Modify: `services/batch-jobs/src/batch_jobs/hourly_comfort_job.py`
- Modify: `services/batch-jobs/src/batch_jobs/cli.py:66-71,200-235`
- Modify: `services/orchestration/dags/standard_score_pipeline.py:258-271`
- Test: `services/batch-jobs/tests/test_hourly_comfort.py`, `services/orchestration/tests/test_standard_score_pipeline_dag.py`

**Interfaces:**
- Consumes: Task 1의 `write_hourly_comfort_partition`, `HourlyComfortWriteResult`
- Produces:
  - `run_hourly_comfort_job(spark, config, run_id, processed_at, target_hour) -> HourlyComfortJobSummary` — `target_hour: datetime` 인자가 **새로 추가된다**
  - CLI: `score-hourly-comfort --target-hour <ISO8601 UTC>` (필수)

- [ ] **Step 1: 실패 테스트를 쓴다**

`services/batch-jobs/tests/test_hourly_comfort.py` 끝에 추가한다.

```python
def test_job_reads_only_the_target_hour_partition_of_silver2(spark, tmp_path):
    """Silver2 루트가 아니라 target_hour 파티션만 읽는다 (#469)."""
    from batch_jobs.hourly_comfort_job import (
        HourlyComfortJobConfig,
        run_hourly_comfort_job,
    )
    from batch_jobs.hourly_segment_feature_storage import hour_output_path

    feature_root = str(tmp_path / "features")
    target_hour = datetime(2026, 8, 25, 9, tzinfo=UTC)
    other_hour = datetime(2026, 8, 25, 10, tzinfo=UTC)
    # 두 시간대를 각각 자기 파티션에 넣는다. job이 전체를 읽으면 2행이 나온다.
    _write_feature_partition(spark, feature_root, target_hour, ["seg-a"])
    _write_feature_partition(spark, feature_root, other_hour, ["seg-b"])

    config = HourlyComfortJobConfig(
        feature_input_path=feature_root,
        score_output_path=str(tmp_path / "score"),
        rejected_output_path=str(tmp_path / "rejected"),
        scoring_config_path=DEFAULT_HOURLY_SCORING_CONFIG_PATH,
    )

    summary = run_hourly_comfort_job(
        spark, config, "run-1", PROCESSED_AT, target_hour
    )

    assert summary.scored_count == 1
    assert hour_output_path(feature_root, other_hour)  # 다른 시간대는 그대로 남아 있다


def test_job_writes_only_the_target_hour_partition_of_silver3(spark, tmp_path):
    from batch_jobs.hourly_comfort_job import (
        HourlyComfortJobConfig,
        run_hourly_comfort_job,
    )
    from batch_jobs.hourly_comfort_storage import hour_output_path as score_hour_path

    feature_root = str(tmp_path / "features")
    score_root = str(tmp_path / "score")
    first = datetime(2026, 8, 25, 9, tzinfo=UTC)
    second = datetime(2026, 8, 25, 10, tzinfo=UTC)
    _write_feature_partition(spark, feature_root, first, ["seg-a"])
    _write_feature_partition(spark, feature_root, second, ["seg-b"])
    config = HourlyComfortJobConfig(
        feature_input_path=feature_root,
        score_output_path=score_root,
        rejected_output_path=str(tmp_path / "rejected"),
        scoring_config_path=DEFAULT_HOURLY_SCORING_CONFIG_PATH,
    )

    run_hourly_comfort_job(spark, config, "run-1", PROCESSED_AT, first)
    run_hourly_comfort_job(spark, config, "run-2", PROCESSED_AT, second)

    # 두 번째 실행이 첫 번째 파티션을 덮어쓰지 않는다.
    assert spark.read.parquet(score_hour_path(score_root, first)).count() == 1
    assert spark.read.parquet(score_hour_path(score_root, second)).count() == 1
```

이 테스트가 쓰는 `_write_feature_partition` 헬퍼를 같은 파일에 추가한다. 기존 테스트가 이미 쓰고 있는 feature 행 생성 헬퍼가 있으면 그것을 재사용하고, 없으면 아래를 쓴다.

```python
def _write_feature_partition(spark, feature_root, target_hour, segment_ids):
    """HOURLY_SEGMENT_FEATURE_SCHEMA를 만족하는 최소 행을 해당 시간 파티션에 쓴다."""
    from batch_jobs.hourly_segment_feature_storage import hour_output_path

    rows = [
        {
            "segment_id": segment_id,
            "vehicle_profile_id": 1,
            "data_period_start": target_hour,
            "data_period_end": target_hour + timedelta(hours=1),
            "road_snapshot_date": target_hour.date(),
            "avg_speed_mps": 8.0,
            "rms_accel_x": 0.2, "rms_accel_y": 0.2, "rms_accel_z": 0.2,
            "p95_abs_accel_x": 0.5, "p95_abs_accel_y": 0.5, "p95_abs_accel_z": 0.5,
            "rms_jerk_x": 1.0, "rms_jerk_y": 1.0, "rms_jerk_z": 1.0,
            "p95_abs_jerk_x": 2.0, "p95_abs_jerk_y": 2.0, "p95_abs_jerk_z": 2.0,
            "hard_brake_count": 0, "hard_accel_count": 0,
            "sharp_steer_count": 0, "steer_reversal_count": 0,
            "rms_steering_rate": 0.1, "rms_steering_vibration": 0.1,
            "sample_count": 100, "trip_count": 10,
            "feature_version": "hourly-features-v1",
            "_processed_at": PROCESSED_AT,
            "_run_id": "seed",
        }
        for segment_id in segment_ids
    ]
    frame = spark.createDataFrame(rows, HOURLY_SEGMENT_FEATURE_SCHEMA)
    frame.write.parquet(hour_output_path(feature_root, target_hour))
```

필요한 import(`timedelta`, `HOURLY_SEGMENT_FEATURE_SCHEMA`, `DEFAULT_HOURLY_SCORING_CONFIG_PATH`)를 파일 상단에 추가한다.

- [ ] **Step 2: 테스트가 실패하는지 확인한다**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk@21 uv run --package batch-jobs pytest services/batch-jobs/tests/test_hourly_comfort.py -k target_hour -v`
Expected: FAIL — `TypeError: run_hourly_comfort_job() takes 4 positional arguments but 5 were given`

- [ ] **Step 3: job을 구현한다**

`hourly_comfort_job.py`의 `run_hourly_comfort_job`을 아래로 바꾼다.

```python
def run_hourly_comfort_job(
    spark: SparkSession,
    config: HourlyComfortJobConfig,
    run_id: str,
    processed_at: datetime,
    target_hour: datetime,
) -> HourlyComfortJobSummary:
    """Score one Silver2 hour partition and replace the matching Silver3 partition."""
    _validate_job_config(config)
    features = spark.read.schema(HOURLY_SEGMENT_FEATURE_SCHEMA).parquet(
        feature_hour_path(config.feature_input_path, target_hour)
    )
    scoring_config = load_hourly_scoring_config(config.scoring_config_path)
    result = calculate_hourly_comfort_scores(
        features, run_id, processed_at, scoring_config
    )
    scored = _select_declared_score_schema(result.scored).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    rejected = result.rejected.persist(StorageLevel.MEMORY_AND_DISK)

    try:
        score_result = write_hourly_comfort_partition(
            spark, scored, config.score_output_path, target_hour, run_id,
            HOURLY_COMFORT_SCORE_SCHEMA,
        )
        rejected_result = write_hourly_comfort_partition(
            spark, rejected, config.rejected_output_path, target_hour, run_id,
            rejected.schema,
        )
        summary = HourlyComfortJobSummary(
            score_result.row_count, rejected_result.row_count
        )
    finally:
        scored.unpersist()
        rejected.unpersist()

    _log_summary(
        config, run_id=run_id, processed_at=processed_at,
        target_hour=target_hour, summary=summary,
    )
    return summary
```

import에 다음을 추가한다.

```python
from batch_jobs.hourly_comfort_storage import write_hourly_comfort_partition
from batch_jobs.hourly_segment_feature_storage import (
    hour_output_path as feature_hour_path,
)
```

`_log_summary`의 docstring(`:112-118`)이 사실과 다르므로 함께 고치고 `target_hour`를 로그에 넣는다.

```python
def _log_summary(
    config: HourlyComfortJobConfig,
    *,
    run_id: str,
    processed_at: datetime,
    target_hour: datetime,
    summary: HourlyComfortJobSummary,
) -> None:
    """이번 실행이 어느 시간대를, 어느 경로에서 처리했는지 한 줄로 남긴다(#406, #469).

    이전에는 target_hour 인자가 없어 "대상 시간대는 feature_input_path에 들어 있다"고
    설명했지만 사실이 아니었다 — DAG는 시간 템플릿이 없는 고정 Variable을 넘겼다.
    이제 target_hour를 직접 받아 그대로 남긴다. S3 경로는 자격증명이 아니라 로그에
    남겨도 안전하다.
    """
    logger.info(
        "hourly comfort scoring finished run_id=%s target_hour=%s processed_at=%s "
        "input=%s score_output=%s rejected_output=%s scored=%d rejected=%d",
        run_id,
        target_hour.isoformat(),
        processed_at.isoformat(),
        config.feature_input_path,
        config.score_output_path,
        config.rejected_output_path,
        summary.scored_count,
        summary.rejected_count,
    )
```

- [ ] **Step 4: 테스트가 통과하는지 확인한다**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk@21 uv run --package batch-jobs pytest services/batch-jobs/tests/test_hourly_comfort.py -v`
Expected: PASS. 기존 테스트가 `run_hourly_comfort_job`을 4인자로 부르고 있다면 `target_hour`를 넘기도록 함께 고친다.

- [ ] **Step 5: CLI에 `--target-hour`를 추가한다**

`cli.py:66-71`의 파서에 추가한다.

```python
    score_parser.add_argument(
        "--target-hour", type=datetime.fromisoformat, required=True
    )
```

`cli.py:200-227`의 `run_hourly_scoring`에서 job 호출을 바꾼다.

```python
            summary = run_hourly_comfort_job(
                spark, config, run_id, datetime.now(UTC), arguments.target_hour
            )
```

- [ ] **Step 6: DAG가 `--target-hour`를 넘기게 한다**

`standard_score_pipeline.py:258-271`의 `run_hourly_scoring`에 인자를 추가한다.

```python
        run_hourly_scoring = submit_batch_jobs_command(
            task_id="run_hourly_scoring",
            entry_point_arguments=[
                "score-hourly-comfort",
                "--run-id",
                "{{ run_id }}",
                "--target-hour",
                "{{ data_interval_start.isoformat() }}",
                "--input-path",
                _HOURLY_COMFORT_INPUT_PATH,
                "--output-path",
                _HOURLY_COMFORT_OUTPUT_PATH,
                "--rejected-output-path",
                _HOURLY_COMFORT_REJECTED_OUTPUT_PATH,
            ],
        )
```

`services/orchestration/tests/test_standard_score_pipeline_dag.py`에 테스트를 추가한다.

```python
def test_run_hourly_scoring_invokes_scoring_with_the_target_hour():
    module = _load_dag_module()

    args = _entry_point_arguments(
        module.dag.get_task("hourly_scoring.run_hourly_scoring")
    )
    assert args[0] == "score-hourly-comfort"
    assert "--target-hour" in args
    assert args[args.index("--target-hour") + 1] == (
        "{{ data_interval_start.isoformat() }}"
    )
```

`_load_dag_module()`과 `_entry_point_arguments(task)`는 같은 파일의 기존 헬퍼다
(`test_standard_score_pipeline_dag.py:44-56`). `_entry_point_arguments`는
`task.job_driver["sparkSubmit"]["entryPointArguments"]`를 꺼내므로, 연산자 속성에
직접 접근하지 않는다.

- [ ] **Step 7: 전체 테스트를 돌린다**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk@21 uv run --all-packages pytest -q`
Expected: 전부 통과

- [ ] **Step 8: 커밋한다**

```bash
git add services/batch-jobs/src/batch_jobs/hourly_comfort_job.py \
        services/batch-jobs/src/batch_jobs/cli.py \
        services/batch-jobs/tests/test_hourly_comfort.py \
        services/orchestration/dags/standard_score_pipeline.py \
        services/orchestration/tests/test_standard_score_pipeline_dag.py
git commit -F - <<'MSG'
perf: score only the target hour partition

run_hourly_scoring이 매 실행 Silver2 전체를 읽고 Silver3 전체를 다시 썼다.
데이터가 쌓일수록 선형으로 늘어나는데, 실제로 필요한 건 그 시간대 하나다.

score-hourly-comfort에 --target-hour를 추가하고 DAG가 data_interval_start를
넘긴다. 읽기는 Silver2의 해당 파티션만, 쓰기는 Task 1의 파티션 writer를 쓴다.

_log_summary의 docstring도 고친다. "대상 시간대는 feature_input_path에 들어
있다"고 적혀 있었으나 DAG는 시간 템플릿이 없는 고정 Variable을 넘겼다.

Refs #469
MSG
```

---

### Task 3: 검증을 파티션 스코프로 좁히고 `zero_sample_rate`를 뺀다

**Files:**
- Modify: `services/batch-jobs/src/batch_jobs/hourly_scoring_validation.py`
- Delete: `services/batch-jobs/src/batch_jobs/resources/expectations/hourly_comfort_score_zero_sample_rate_suite.json`
- Modify: `services/batch-jobs/src/batch_jobs/cli.py:85-86,396-428`
- Modify: `services/orchestration/dags/standard_score_pipeline.py:272-284`
- Test: `services/batch-jobs/tests/test_hourly_scoring_validation.py`, `services/orchestration/tests/test_standard_score_pipeline_dag.py`

**Interfaces:**
- Consumes: Task 1의 `hour_output_path`
- Produces:
  - `run_hourly_scoring_validation(spark, config, target_hour) -> HourlyScoringValidationSummary`
  - `HourlyScoringValidationSummary(target_hour: datetime, row_count: int, score_ranges_success: bool)` — `zero_sample_count`/`zero_sample_rate`/`zero_sample_rate_success` 필드가 **사라진다**
  - `HourlyScoringValidationConfig(score_output_path, score_ranges_suite_path)` — `zero_sample_rate_suite_path`가 **사라진다**
  - CLI: `validate-hourly-scoring --target-hour <ISO8601 UTC>` (필수)

- [ ] **Step 1: 실패 테스트를 쓴다**

`services/batch-jobs/tests/test_hourly_scoring_validation.py` 끝에 추가한다.

```python
def test_validation_reads_only_the_target_hour_partition(spark, tmp_path):
    """다른 시간대의 잘못된 값에 영향받지 않는다 (#469)."""
    from batch_jobs.hourly_comfort_storage import hour_output_path

    root = str(tmp_path / "hourly_comfort_score")
    good_hour = datetime(2026, 8, 25, 9, tzinfo=UTC)
    bad_hour = datetime(2026, 8, 25, 10, tzinfo=UTC)
    # 범위를 벗어난 점수(999)를 다른 시간대에 심는다. 전체를 읽으면 실패해야 한다.
    _write_score_partition(spark, root, good_hour, vertical_score=50.0)
    _write_score_partition(spark, root, bad_hour, vertical_score=999.0)

    config = HourlyScoringValidationConfig(
        score_output_path=root,
        score_ranges_suite_path=DEFAULT_SCORE_RANGES_SUITE_PATH,
    )

    summary = run_hourly_scoring_validation(spark, config, good_hour)

    assert summary.target_hour == good_hour
    assert summary.row_count == 1
    assert summary.success


def test_validation_fails_when_the_target_hour_partition_is_missing(spark, tmp_path):
    root = str(tmp_path / "hourly_comfort_score")
    config = HourlyScoringValidationConfig(
        score_output_path=root,
        score_ranges_suite_path=DEFAULT_SCORE_RANGES_SUITE_PATH,
    )

    with pytest.raises(HourlyScoringValidationFailed, match="no hourly_comfort_score"):
        run_hourly_scoring_validation(
            spark, config, datetime(2026, 8, 25, 9, tzinfo=UTC)
        )
```

`_write_score_partition` 헬퍼를 같은 파일에 추가한다. 기존 테스트에 점수 행 생성 헬퍼가 있으면 재사용하고, 없으면 아래를 쓴다.

```python
def _write_score_partition(spark, root, target_hour, vertical_score):
    from batch_jobs.hourly_comfort_storage import hour_output_path

    rows = [
        {
            "segment_id": "seg-a",
            "vehicle_profile_id": 1,
            "data_period_start": target_hour,
            "data_period_end": target_hour + timedelta(hours=1),
            "road_snapshot_date": target_hour.date(),
            "vertical_score": vertical_score,
            "longitudinal_score": 50.0,
            "lateral_score": 50.0,
            "scoring_version": "1.0.0",
            "sample_count": 100,
            "trip_count": 10,
            "_run_id": "seed",
            "_processed_at": PROCESSED_AT,
        }
    ]
    frame = spark.createDataFrame(rows, HOURLY_COMFORT_SCORE_SCHEMA)
    frame.write.parquet(hour_output_path(root, target_hour))
```

기존의 `zero_sample_rate` 관련 테스트(`compute_zero_sample_rate`, `DEFAULT_ZERO_SAMPLE_RATE_SUITE_PATH`, 설정 override 검증의 해당 줄)를 삭제한다.

- [ ] **Step 2: 테스트가 실패하는지 확인한다**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk@21 uv run --package batch-jobs pytest services/batch-jobs/tests/test_hourly_scoring_validation.py -v`
Expected: FAIL — `TypeError: HourlyScoringValidationConfig.__init__() missing 1 required positional argument: 'zero_sample_rate_suite_path'`

- [ ] **Step 3: 검증 모듈을 구현한다**

`hourly_scoring_validation.py`에서 다음을 삭제한다.

- 모듈 docstring의 "풀 리컴퓨트" 문단(`:8-11`)
- `DEFAULT_ZERO_SAMPLE_RATE_SUITE_PATH` (`:32-34`)
- `compute_zero_sample_rate` (`:89-93`)
- `HourlyScoringValidationConfig.zero_sample_rate_suite_path`와 `from_env`의 해당 분기
- `HourlyScoringValidationSummary`의 `zero_sample_count`/`zero_sample_rate`/`zero_sample_rate_success`
- `run_hourly_scoring_validation`의 zero-sample 계산과 rate suite 검증

모듈 docstring을 아래로 바꾼다.

```python
"""`hourly_scoring` TaskGroup 산출물을 Great Expectations로 검증한다 (#249, ADR-0004).

`hourly_comfort_score`의 방향별 점수 범위(0~100)와 `scoring_version` 형식(SemVer)을
GX Suite로 검증한다. 스키마/필수값 같은 하드 인바리언트는
`HOURLY_COMFORT_SCORE_SCHEMA`(nullable=False 필드)가 쓰기 시점에 이미 강제하므로
여기서 다시 다루지 않는다(ADR-0004: 하드 인바리언트는 GX로 옮기지 않는다).

`run_hourly_scoring`이 target_hour 파티션 하나만 쓰므로(#469), 검증도 그 파티션만
대상으로 한다 — `validate_sensor_processing`과 같은 방식이다.

이전에 있던 zero-sample 비율 검증은 제거했다. `hourly_comfort.py:72`의 `eligible`
조건이 `sample_count > 0`인 행만 출력에 넣어 비율의 분자가 항상 0이었고, 구조적으로
실패할 수 없는 검증이었다.
"""
```

`Summary`와 실행 함수를 아래로 바꾼다.

```python
@dataclass(frozen=True, slots=True)
class HourlyScoringValidationSummary:
    target_hour: datetime
    row_count: int
    score_ranges_success: bool

    @property
    def success(self) -> bool:
        return self.score_ranges_success


def read_hourly_comfort_score_partition(
    spark: SparkSession, score_output_path: str, target_hour: datetime
) -> DataFrame | None:
    path = hour_output_path(score_output_path, target_hour)
    if not _path_exists(spark, path):
        return None
    return spark.read.parquet(path)


def run_hourly_scoring_validation(
    spark: SparkSession,
    config: HourlyScoringValidationConfig,
    target_hour: datetime,
) -> HourlyScoringValidationSummary:
    """`hourly_comfort_score`의 target_hour 파티션만 검증한다(in-flight, 전체 이력 아님)."""
    scores_df = read_hourly_comfort_score_partition(
        spark, config.score_output_path, target_hour
    )
    if scores_df is None:
        raise HourlyScoringValidationFailed(
            f"no hourly_comfort_score partition found for "
            f"target_hour={target_hour.isoformat()} under {config.score_output_path}"
        )

    row_count = scores_df.count()
    if row_count == 0:
        raise HourlyScoringValidationFailed(
            f"hourly_comfort_score partition for target_hour={target_hour.isoformat()} "
            "has zero rows"
        )

    ranges_suite = load_expectation_suite(config.score_ranges_suite_path)
    ranges_result = validate_dataframe(scores_df, ranges_suite, "hourly_comfort_score")

    summary = HourlyScoringValidationSummary(
        target_hour=target_hour,
        row_count=row_count,
        score_ranges_success=ranges_result.success,
    )
    if not summary.success:
        raise HourlyScoringValidationFailed(
            f"hourly_scoring validation failed for {config.score_output_path}: {summary}"
        )
    return summary
```

import에 `from batch_jobs.hourly_comfort_storage import hour_output_path`를 추가하고, 더 이상 쓰지 않는 `from pyspark.sql import functions as F`를 제거한다.

- [ ] **Step 4: suite 파일을 삭제한다**

```bash
git rm services/batch-jobs/src/batch_jobs/resources/expectations/hourly_comfort_score_zero_sample_rate_suite.json
```

- [ ] **Step 5: CLI와 DAG를 배선한다**

`cli.py:85-86`:

```python
    validate_hourly_scoring_parser.add_argument(
        "--target-hour", type=datetime.fromisoformat, required=True
    )
```

`cli.py:396-428`의 `run_hourly_scoring_validation_cli`에서 config 생성과 결과 출력을 바꾼다.

```python
    config = HourlyScoringValidationConfig(
        score_output_path=arguments.output_path or defaults.score_output_path,
        score_ranges_suite_path=defaults.score_ranges_suite_path,
    )
    ...
            summary = run_hourly_scoring_validation(spark, config, arguments.target_hour)
        print(
            json.dumps(
                {
                    "target_hour": summary.target_hour.isoformat(),
                    "row_count": summary.row_count,
                    "success": summary.success,
                },
                sort_keys=True,
            )
        )
```

`standard_score_pipeline.py:272-284`의 주석과 인자를 바꾼다.

```python
        # validate_hourly_scoring은 run_hourly_scoring이 방금 쓴 target_hour 파티션만
        # 읽으므로(#469), 같은 Airflow Variable(HOURLY_COMFORT_OUTPUT_PATH)을 재사용해
        # 항상 같은 경로를 가리키게 한다. validate_sensor_processing과 같은 방식이다.
        validate_hourly_scoring = submit_batch_jobs_command(
            task_id="validate_hourly_scoring",
            entry_point_arguments=[
                "validate-hourly-scoring",
                "--target-hour",
                "{{ data_interval_start.isoformat() }}",
                "--output-path",
                _HOURLY_COMFORT_OUTPUT_PATH,
            ],
        )
```

`test_standard_score_pipeline_dag.py`에 테스트를 추가한다.

```python
def test_validate_hourly_scoring_scopes_to_the_target_hour():
    module = _load_dag_module()

    args = _entry_point_arguments(
        module.dag.get_task("hourly_scoring.validate_hourly_scoring")
    )
    assert args[0] == "validate-hourly-scoring"
    assert "--target-hour" in args
    assert args[args.index("--target-hour") + 1] == (
        "{{ data_interval_start.isoformat() }}"
    )
```

- [ ] **Step 6: 전체 테스트를 돌린다**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk@21 uv run --all-packages pytest -q`
Expected: 전부 통과. `test_cli_dispatch.py`/`test_cli_perf_logging.py`가 옛 시그니처를 쓰고 있으면 함께 고친다.

- [ ] **Step 7: 커밋한다**

```bash
git add -A services/batch-jobs services/orchestration
git commit -F - <<'MSG'
perf: validate only the target hour partition of hourly scoring

validate_hourly_scoring이 hourly_comfort_score 전체를 세 번 스캔했다.
run_hourly_scoring이 이제 한 파티션만 쓰므로 검증도 그 파티션만 본다 —
validate_sensor_processing이 이미 쓰는 방식과 같다.

zero-sample 비율 검증은 이식하지 않고 뺀다. hourly_comfort.py:72의 eligible
조건이 sample_count > 0인 행만 출력에 넣어, 비율의 분자가 항상 0이고 임계값
0.05를 언제나 통과했다. 구조적으로 실패할 수 없는 검증이었다.

남는 검증은 방향별 점수 범위와 scoring_version SemVer 형식 두 가지다.

Refs #469
MSG
```

---

### Task 4: 168시간 파티션 프루닝

**Files:**
- Modify: `services/batch-jobs/src/batch_jobs/comfort_score/loader.py:29-48`
- Test: `services/batch-jobs/tests/test_standard_comfort_score.py`

**Interfaces:**
- Consumes: Task 1이 만든 파티션 레이아웃
- Produces: `load_hourly_comfort_score_for_gold` 시그니처는 그대로. 내부에 `_filter_window_partitions(df, as_of, window_hours) -> DataFrame` 추가

- [ ] **Step 1: 실패 테스트를 쓴다**

`services/batch-jobs/tests/test_standard_comfort_score.py` 끝에 추가한다.

```python
def test_loader_reads_exactly_the_window_partitions(spark, tmp_path):
    """169시간치를 심어두고 168개만 읽는지 확인한다 (#469)."""
    from batch_jobs.comfort_score.loader import load_hourly_comfort_score_for_gold

    data_lake_uri = str(tmp_path)
    as_of = datetime(2026, 8, 25, 0, tzinfo=UTC)
    # 윈도우는 [as_of - 168h, as_of) = [2026-08-18 00:00, 2026-08-25 00:00)
    out_of_window = as_of - timedelta(hours=169)   # 포함되면 안 됨
    first_in_window = as_of - timedelta(hours=168) # 포함돼야 함
    last_in_window = as_of - timedelta(hours=1)    # 포함돼야 함
    at_as_of = as_of                               # 상한 배타이므로 제외돼야 함
    for hour in (out_of_window, first_in_window, last_in_window, at_as_of):
        _write_gold_input_partition(spark, data_lake_uri, hour)

    frame = load_hourly_comfort_score_for_gold(spark, data_lake_uri, as_of, 168)

    starts = {row.data_period_start for row in frame.select("data_period_start").collect()}
    assert starts == {first_in_window, last_in_window}


def test_loader_tolerates_a_missing_partition_inside_the_window(spark, tmp_path):
    from batch_jobs.comfort_score.loader import load_hourly_comfort_score_for_gold

    data_lake_uri = str(tmp_path)
    as_of = datetime(2026, 8, 25, 0, tzinfo=UTC)
    # 윈도우 안에 두 시간만 존재하고 나머지 166시간은 파티션이 없다.
    _write_gold_input_partition(spark, data_lake_uri, as_of - timedelta(hours=100))
    _write_gold_input_partition(spark, data_lake_uri, as_of - timedelta(hours=2))

    frame = load_hourly_comfort_score_for_gold(spark, data_lake_uri, as_of, 168)

    assert frame.count() == 2
```

`_write_gold_input_partition` 헬퍼를 같은 파일에 추가한다.

```python
def _write_gold_input_partition(spark, data_lake_uri, target_hour):
    """`silver/hourly_comfort_score` 아래 해당 시간 파티션에 한 행을 쓴다."""
    from pathlib import Path

    from batch_jobs.hourly_comfort_storage import hour_output_path

    root = str(Path(data_lake_uri) / "silver" / "hourly_comfort_score")
    rows = [
        {
            "segment_id": "seg-a",
            "vehicle_profile_id": 1,
            "data_period_start": target_hour,
            "data_period_end": target_hour + timedelta(hours=1),
            "road_snapshot_date": target_hour.date(),
            "vertical_score": 50.0,
            "longitudinal_score": 50.0,
            "lateral_score": 50.0,
            "scoring_version": "1.0.0",
            "sample_count": 100,
            "trip_count": 10,
            "_run_id": "seed",
            "_processed_at": datetime(2026, 8, 25, 0, tzinfo=UTC),
        }
    ]
    frame = spark.createDataFrame(rows, HOURLY_COMFORT_SCORE_SCHEMA)
    frame.write.parquet(hour_output_path(root, target_hour))
```

- [ ] **Step 2: 테스트가 실패하는지 확인한다**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk@21 uv run --package batch-jobs pytest services/batch-jobs/tests/test_standard_comfort_score.py -k window_partitions -v`
Expected: FAIL — 파티션 컬럼 필터가 없어 169시간이 전부 읽히거나, `data_period_start` 필터만으로 통과해 결손 테스트에서 갈린다

- [ ] **Step 3: 프루닝을 구현한다**

`comfort_score/loader.py`의 `load_hourly_comfort_score_for_gold`를 바꾼다.

```python
def load_hourly_comfort_score_for_gold(
    spark: SparkSession,
    data_lake_uri: str,
    as_of: datetime,
    window_hours: int = DEFAULT_WINDOW_HOURS,
) -> DataFrame:
    """Load `hourly_comfort_score`, windowed to the last `window_hours` hours.

    - `data_lake_uri`는 로컬 Parquet 루트와 운영 S3 루트를 함수 수정 없이 바꿔 끼울 수
      있는 파라미터다 (join_uri가 두 스킴을 모두 처리한다).
    - 루트를 읽되 파티션 컬럼으로 먼저 잘라, 윈도우 밖 파티션은 파일을 열지 않는다(#469).
    """
    comfort_score_uri = join_uri(data_lake_uri, "silver", "hourly_comfort_score")

    comfort_score_df = _read_validated_parquet(
        spark, comfort_score_uri, HOURLY_COMFORT_SCORE_SCHEMA
    )
    pruned = _filter_window_partitions(comfort_score_df, as_of, window_hours)
    windowed = _filter_window_hours(pruned, as_of, window_hours)
    return _select_latest_scoring_version(windowed)


def _filter_window_partitions(
    df: DataFrame, as_of: datetime, window_hours: int
) -> DataFrame:
    """파티션 컬럼(`data_period_date`, `hour`)만으로 `[start, as_of)`를 정확히 자른다.

    데이터 컬럼(`data_period_start`)이 아니라 파티션 컬럼에 조건을 걸어야 Spark가 파일을
    열기 전에 디렉터리 단위로 걸러낸다. 날짜 범위로만 자르면 양 끝 날의 시(hour)까지는
    못 걸러 최대 24시간을 더 읽으므로, 경계 날짜에서는 hour까지 비교한다.
    """
    start = as_of - timedelta(hours=window_hours)
    date_column = F.col("data_period_date")
    hour_column = F.col("hour")
    at_or_after_start = (date_column > F.lit(start.date())) | (
        (date_column == F.lit(start.date())) & (hour_column >= F.lit(start.hour))
    )
    before_as_of = (date_column < F.lit(as_of.date())) | (
        (date_column == F.lit(as_of.date())) & (hour_column < F.lit(as_of.hour))
    )
    return df.filter(at_or_after_start & before_as_of)
```

기존 `_filter_window_hours`(`:79-89`)는 그대로 둔다 — 파티션 값과 데이터 값이 어긋나는 경우의 안전망이다. 그 docstring에 한 줄 덧붙인다.

```python
    """Keep rows with `data_period_start` in `[as_of - window_hours, as_of)`.

    상한을 배타적으로 둔다: as_of는 "지금"을 뜻하고, 그 시각에 시작하는 시간은
    아직 끝나지 않았으므로 이번 윈도우에 포함하지 않는다.

    `_filter_window_partitions`가 이미 파티션 단위로 잘랐지만, 파티션 경로 값과 행의
    `data_period_start`가 어긋난 경우를 막는 안전망으로 남긴다.
    """
```

- [ ] **Step 4: 테스트가 통과하는지 확인한다**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk@21 uv run --package batch-jobs pytest services/batch-jobs/tests/test_standard_comfort_score.py -v`
Expected: PASS

- [ ] **Step 5: 커밋한다**

```bash
git add services/batch-jobs/src/batch_jobs/comfort_score/loader.py \
        services/batch-jobs/tests/test_standard_comfort_score.py
git commit -F - <<'MSG'
perf: prune hourly_comfort_score partitions to the standard score window

loader가 루트 전체를 읽은 뒤 data_period_start로 168시간을 걸러냈다. 파티션
컬럼이 없어 프루닝이 원천적으로 불가능했고, 168시간 윈도우는 논리적으로만
존재했다.

파티션 컬럼(data_period_date, hour)에 조건을 걸어 파일을 열기 전에 디렉터리
단위로 자른다. 날짜 범위로만 자르면 양 끝 날의 시까지는 못 걸러 최대 24시간을
더 읽으므로, 경계 날짜에서는 hour까지 비교해 정확히 168개만 읽는다.

기존 data_period_start 필터는 남긴다 — 파티션 경로 값과 행의 값이 어긋난
경우를 막는 안전망이다.

Refs #469
MSG
```

---

### Task 5: `report_processing_counts`에 파티션 경로를 넘긴다

**Files:**
- Modify: `services/orchestration/jobs/pipeline_counts.py:71-94`
- Test: `services/orchestration/tests/test_pipeline_counts.py`

**Interfaces:**
- Consumes: Task 1이 만든 파티션 레이아웃
- Produces: `count_standard_score_pipeline_outputs` 시그니처 그대로. 내부에서 `hourly_comfort_output_path`에도 파티션 경로를 조합

- [ ] **Step 1: 실패 테스트를 쓴다**

`services/orchestration/tests/test_pipeline_counts.py` 끝에 추가한다.

```python
def test_hourly_comfort_count_uses_the_target_hour_partition():
    """루트를 재귀 나열하면 다른 시간대와 _staging 잔여물까지 세어버린다 (#469)."""
    store = _FakeObjectStore(
        {
            "file:///lake/hourly_comfort_score/data_period_date=2026-08-18/hour=09/part-0.parquet": 7,
            "file:///lake/hourly_comfort_score/data_period_date=2026-08-18/hour=10/part-0.parquet": 99,
            "file:///lake/hourly_comfort_score/_staging/run-dead/part-0.parquet": 500,
        }
    )

    counts = count_standard_score_pipeline_outputs(
        target_hour=datetime(2026, 8, 18, 9, tzinfo=UTC),
        as_of=datetime(2026, 8, 18, 10, tzinfo=UTC),
        quarantine_output_path="file:///lake/quarantine",
        feature_output_path="file:///lake/features",
        hourly_comfort_output_path="file:///lake/hourly_comfort_score",
        connection=_FakeConnection(result=0),
        store=store,
    )

    assert counts.hourly_comfort_score_count == 7
```

기존 `test_counts_quarantine_feature_and_hourly_comfort_partitions`의 `hourly_comfort_score` 항목을 평면 경로에서 파티션 경로로 바꾼다.

```python
            "file:///lake/hourly_comfort_score/data_period_date=2026-08-18/hour=09/part-0.parquet": 80,
```

- [ ] **Step 2: 테스트가 실패하는지 확인한다**

Run: `uv run --package orchestration pytest services/orchestration/tests/test_pipeline_counts.py -v`
Expected: FAIL — `assert 606 == 7` (루트를 재귀 나열해 세 파일을 모두 셈)

- [ ] **Step 3: 구현한다**

`pipeline_counts.py:71-94`를 바꾼다.

```python
    quarantine_partition = join_uri(
        quarantine_output_path,
        f"target_date={target_hour.date().isoformat()}",
        f"target_hour={target_hour.hour:02d}",
    )
    feature_partition = join_uri(
        feature_output_path,
        f"data_period_date={target_hour.date().isoformat()}",
        f"hour={target_hour.hour:02d}",
    )
    # hourly_comfort_score도 시간 파티션을 갖는다(#469). 루트를 넘기면
    # ObjectStore.list_objects가 재귀라 다른 시간대와 _staging 잔여물까지 센다.
    hourly_comfort_partition = join_uri(
        hourly_comfort_output_path,
        f"data_period_date={target_hour.date().isoformat()}",
        f"hour={target_hour.hour:02d}",
    )
```

그리고 반환부를 바꾼다.

```python
        hourly_comfort_score_count=_count_parquet_rows(
            active_store, hourly_comfort_partition
        ),
```

- [ ] **Step 4: 테스트가 통과하는지 확인한다**

Run: `uv run --package orchestration pytest services/orchestration/tests/test_pipeline_counts.py -v`
Expected: PASS

- [ ] **Step 5: 커밋한다**

```bash
git add services/orchestration/jobs/pipeline_counts.py \
        services/orchestration/tests/test_pipeline_counts.py
git commit -F - <<'MSG'
fix: count only the target hour partition of hourly_comfort_score

hourly_comfort_output_path만 루트를 그대로 넘기고 있었다. ObjectStore.list_objects는
재귀라(storage.py:192), 파티션 도입 후에는 다른 시간대와 _staging 잔여물(#380에서
실제로 겪음)까지 세어 건수가 부풀어 오른다.

quarantine/feature와 같은 방식으로 파티션 경로를 조합해 넘긴다. #470에서 이 이슈로
이관한 항목이다.

Refs #469
MSG
```

---

### Task 6: 문서 갱신

**Files:**
- Modify: `context/data/quality-rules.md:113-134`
- Modify: `context/data/schema-catalog.md` (`hourly_comfort_score` 절, `:371-` 부근)
- Modify: `services/orchestration/README.md`

**Interfaces:**
- Consumes: Task 1~5의 최종 동작
- Produces: 없음 (문서)

- [ ] **Step 1: `quality-rules.md`의 Silver3 절을 다시 쓴다**

`## Hourly comfort score quality (Silver3)` 절 전체를 아래로 교체한다.

```markdown
## Hourly comfort score quality (Silver3)

`hourly_comfort_score` is the `run_hourly_scoring` output, partitioned by
`data_period_date=YYYY-MM-DD/hour=HH` like `hourly_segment_features` (issue
#469). Each run computes and replaces exactly one hour partition, so
validation scopes to that partition rather than the whole table.

- **Directional score ranges**: `vertical_score`, `longitudinal_score`, and
  `lateral_score` must fall between 0 and 100 inclusive. Implemented as a GX
  Expectation Suite (`resources/expectations/hourly_comfort_score_suite.json`).
- **`scoring_version` format**: must be SemVer (`MAJOR.MINOR.PATCH`), matching
  `resources/hourly_comfort.yaml`'s documented constraint. Same suite as above.
- Both run in `batch_jobs.hourly_scoring_validation` (issue #249, ADR-0004), as
  the `validate_hourly_scoring` task right after `run_hourly_scoring`. Schema
  and required-column invariants remain hard invariants enforced by
  `HOURLY_COMFORT_SCORE_SCHEMA` at write time (ADR-0004).

A zero-sample-rate expectation used to live here. It was removed in #469: the
`eligible` filter in `hourly_comfort.py` only admits rows with
`sample_count > 0`, so the rate's numerator was always zero and the check could
never fail. A rejection-rate canary (`rejected / (scored + rejected)`) would be
the meaningful equivalent; it is not implemented yet.

**Mixed `scoring_version` in the standard score window.** Before #469 every run
recomputed the whole table, so bumping `scoring_version` silently reunified all
history on the next run. With hour partitions that side effect is gone: a bump
applies only to hours scored after it, and the 168-hour window
`run_standard_score` reads can hold more than one version. This is accepted —
`N` and `Confidence` stay intact and the change phases in over seven days. See
`docs/superpowers/specs/2026-08-25-hourly-comfort-score-partitioning-design.md`
for the alternatives considered and the path to explicit backfill.
```

- [ ] **Step 2: `schema-catalog.md`에 파티션 키를 명시한다**

`## hourly_comfort_score` 절의 **Primary key** 문단 바로 뒤에 추가한다.

```markdown
**Partitioning:** `data_period_date=YYYY-MM-DD/hour=HH`, derived from
`data_period_start` — the same layout as `hourly_segment_features`. Each
`run_hourly_scoring` execution replaces exactly one hour partition (issue #469).
```

- [ ] **Step 3: `services/orchestration/README.md`에 전환 절차를 기록한다**

`standard_score_pipeline` 설명 근처에 절을 추가한다.

```markdown
### `hourly_comfort_score` 파티션 전환 (#469)

`hourly_comfort_score`는 원래 파티션 없이 루트에 평면으로 쌓였다. 파티션 writer를
배포하기 전에 기존 평면 데이터를 치워야 한다 — 평면 파일과 파티션 디렉터리가 한
루트에 공존하면 `spark.read.parquet()`가 `Conflicting directory structures`로
실패한다.

재파티션하지 않고 reference 버킷으로 옮긴다. 삭제가 아니라 이동이므로 필요하면
되꺼낼 수 있다.

```bash
# 1. standard_score_pipeline DAG 일시정지

# 2~3. 평면 데이터를 아카이브로 이동
aws s3 mv --recursive \
    s3://<lake>/silver/hourly_comfort_score/ \
    s3://<reference>/raw/comfort_score_archive/hourly_comfort_score/
aws s3 mv --recursive \
    s3://<lake>/quarantine/hourly_comfort_score/ \
    s3://<reference>/raw/comfort_score_archive/quarantine_hourly_comfort_score/

# 4. 코드 배포 (파티션 writer/reader)
# 5. DAG 재개, 첫 실행 확인
```

4단계를 2~3단계보다 먼저 하면 구 writer가 평면 파일을 다시 만들어 같은 문제가
재발한다.

**전환 후 168시간은 점수가 눌린다.** 이동 직후 윈도우에는 1시간만 들어 있어
`N`이 1, `Confidence`가 1/11 ≈ 0.091(k=10)이 되고 점수의 91%가 모집단 평균이
된다. 구간 간 구분이 사실상 사라진 상태가 윈도우가 다시 찰 때까지 이어진다.
`current_segment_comfort_score`도 `standard_segment_comfort_score`를 그대로 읽어
날씨 보정만 얹으므로 같은 영향을 받는다. 감수하기로 한 판단이다.
```

- [ ] **Step 4: 링크와 참조가 맞는지 확인한다**

Run: `uv run --only-group dev --frozen ruff check .`
Expected: All checks passed (문서만 바뀌었으므로 통과해야 한다)

문서에 적은 줄 번호나 파일 경로가 실제와 맞는지 눈으로 확인한다.

- [ ] **Step 5: 커밋한다**

```bash
git add context/data/quality-rules.md context/data/schema-catalog.md \
        services/orchestration/README.md
git commit -F - <<'MSG'
docs: record hourly_comfort_score partitioning and its migration

quality-rules.md가 "full recompute of every historical hour"를 데이터 계약으로
기술하고 있었다. 파티션 도입에 맞춰 다시 쓰고, scoring_version 혼합을 허용한
결정과 그 근거를 남긴다.

schema-catalog.md에 파티션 키를 명시하고, orchestration README에 전환 절차와
전환 후 168시간 동안 점수가 눌린다는 점을 기록한다.

제거한 zero-sample-rate 검증에 대해서도 왜 무의미했는지 남긴다 — 다음 사람이
같은 조사를 반복하지 않도록.

Refs #469
MSG
```

---

## 최종 검증

모든 task 완료 후 저장소 루트에서 실행한다.

```bash
uv run --all-packages ruff check .
JAVA_HOME=/opt/homebrew/opt/openjdk@21 uv run --all-packages pytest
```

둘 다 통과하면 push와 PR 생성을 사용자에게 확인받는다. **push와 PR은 매번 실제 내용을 보여주고 명시적 승인을 받은 뒤에 실행한다.**

## 이 계획이 다루지 않는 것

- 배포와 아카이브 이동 실행 자체 — 사람이 수행한다. Task 6이 절차만 문서화한다
- `hourly_comfort_storage.py`와 기존 두 저장 모듈의 Hadoop FS 헬퍼 중복 제거 — 후속 이슈
- `hourly_segment_feature_storage.py:88`의 `.bak` 명명 문제 — 후속 이슈
- rejection-rate 카나리아 — 후속 이슈
- 버전 변경 시 명시적 백필 수단 — spec의 "향후 전환 경로" 참고
