import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from batch_jobs.schemas import HOURLY_COMFORT_SCORE_SCHEMA
from comfort_score.loader import (
    _filter_window_hours,
    _select_latest_scoring_version,
    _validate_schema,
    load_hourly_comfort_score_for_gold,
)
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# collect()가 돌려주는 timestamp는 이 파이썬 프로세스의 로컬 타임존으로 변환되어,
# 고정하지 않으면 실행 머신마다(Asia/Seoul vs UTC) 값이 달라진다.
os.environ["TZ"] = "UTC"
time.tzset()

# collect()가 돌려주는 TimestampType 값은 tzinfo가 없는 naive datetime이라
# (test_hourly_aggregation.py, test_road_segment_persist.py와 동일 관례),
# TZ=UTC 고정 환경에서 naive datetime을 그대로 UTC로 다룬다.
AS_OF = datetime(2026, 8, 16, 0, 0, 0)  # noqa: DTZ001


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
        "trip_count": 5,
        "_run_id": "run-1",
        "_processed_at": AS_OF,
    }
    return base | overrides


def test_load_keeps_only_the_window_and_passes_trip_count_through(spark, tmp_path):
    _write_rows(
        spark,
        tmp_path / "silver" / "hourly_comfort_score",
        HOURLY_COMFORT_SCORE_SCHEMA,
        [
            _comfort_score_row(segment_id="in-window", trip_count=7),
            _comfort_score_row(
                segment_id="out-of-window", data_period_start=AS_OF - timedelta(hours=200)
            ),
        ],
    )

    result = {
        row["segment_id"]: row["trip_count"]
        for row in load_hourly_comfort_score_for_gold(spark, str(tmp_path), AS_OF).collect()
    }

    # out-of-window 행은 아예 빠지고, trip_count는 hourly_comfort_score 자신의
    # 값이 그대로 나온다 (더 이상 hourly_segment_features와 join하지 않는다).
    assert result == {"in-window": 7}


def test_load_raises_clearly_when_hourly_comfort_score_is_missing_a_column(spark, tmp_path):
    incomplete_schema = StructType(
        [field for field in HOURLY_COMFORT_SCORE_SCHEMA.fields if field.name != "sample_count"]
    )
    _write_rows(
        spark,
        tmp_path / "silver" / "hourly_comfort_score",
        incomplete_schema,
        [{k: v for k, v in _comfort_score_row().items() if k != "sample_count"}],
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

    with pytest.raises(ValueError, match="sample_count"):
        load_hourly_comfort_score_for_gold(spark, str(tmp_path), AS_OF)
