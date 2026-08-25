"""Tests for batch_jobs/hourly_comfort_storage.py (#469)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
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


def test_an_empty_result_removes_the_partition_when_allowed(spark, tmp_path):
    # rejected 출력은 정상 실행에서 비어 있는 것이 기본이라 허용해야 한다.
    root = str(tmp_path / "rejected")
    target_hour = datetime(2026, 8, 25, 9, tzinfo=UTC)
    write_hourly_comfort_partition(
        spark,
        _frame(spark, target_hour, ["a"]),
        root,
        target_hour,
        "run-1",
        SCHEMA,
        allow_empty=True,
    )

    result = write_hourly_comfort_partition(
        spark,
        _frame(spark, target_hour, []),
        root,
        target_hour,
        "run-2",
        SCHEMA,
        allow_empty=True,
    )

    assert result.row_count == 0
    assert not Path(hour_output_path(root, target_hour)).exists()


def test_an_empty_result_is_refused_by_default(spark, tmp_path):
    """점수 출력이 0행인 것은 정상 상황이 아니다 — 기존 파티션을 지우고 끝내면 안 된다.

    `hourly_segment_feature_storage`가 Silver2에 대해 거는 것과 같은 가드다.
    """
    root = str(tmp_path / "hourly_comfort_score")
    target_hour = datetime(2026, 8, 25, 9, tzinfo=UTC)
    write_hourly_comfort_partition(
        spark, _frame(spark, target_hour, ["a"]), root, target_hour, "run-1", SCHEMA
    )

    with pytest.raises(ValueError, match="refusing to write an empty result"):
        write_hourly_comfort_partition(
            spark, _frame(spark, target_hour, []), root, target_hour, "run-2", SCHEMA
        )

    # 기존 데이터가 살아 있어야 한다.
    survivors = spark.read.schema(SCHEMA).parquet(hour_output_path(root, target_hour))
    assert [row.segment_id for row in survivors.collect()] == ["a"]


def test_staging_data_is_cleaned_up(spark, tmp_path):
    root = str(tmp_path / "hourly_comfort_score")
    target_hour = datetime(2026, 8, 25, 9, tzinfo=UTC)

    write_hourly_comfort_partition(
        spark, _frame(spark, target_hour, ["a"]), root, target_hour, "run-1", SCHEMA
    )

    # 빈 부모 디렉터리(`_staging/`)는 로컬 파일시스템에만 남는다 — S3에는 디렉터리
    # 개념이 없어 하위 객체를 지우면 함께 사라진다. 계약은 staged 데이터가 남지
    # 않는다는 것이므로 실행별 경로를 확인한다.
    assert not (Path(root) / "_staging" / "run-1").exists()


def test_a_non_utc_target_hour_is_rejected(spark, tmp_path):
    root = str(tmp_path / "hourly_comfort_score")
    # tzinfo 없는 시각을 거부하는지가 이 테스트의 목적이라 DTZ001은 의도된 것이다.
    naive_hour = datetime(2026, 8, 25, 9)  # noqa: DTZ001

    with pytest.raises(ValueError, match="UTC"):
        write_hourly_comfort_partition(
            spark,
            _frame(spark, datetime(2026, 8, 25, 9, tzinfo=UTC), ["a"]),
            root,
            naive_hour,
            "run-1",
            SCHEMA,
        )


def test_an_unsafe_run_id_is_rejected(spark, tmp_path):
    root = str(tmp_path / "hourly_comfort_score")
    target_hour = datetime(2026, 8, 25, 9, tzinfo=UTC)

    with pytest.raises(ValueError, match="unsafe path characters"):
        write_hourly_comfort_partition(
            spark,
            _frame(spark, target_hour, ["a"]),
            root,
            target_hour,
            "../escape",
            SCHEMA,
        )
