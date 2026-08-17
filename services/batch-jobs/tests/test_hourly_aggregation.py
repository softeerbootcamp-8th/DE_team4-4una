import os
import time
from datetime import UTC, datetime

import pytest
from batch_jobs.sensor_features.aggregation import add_hourly_aggregation_keys
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# collect()가 돌려주는 timestamp는 이 파이썬 프로세스의 로컬 타임존으로 변환되어,
# 고정하지 않으면 실행 머신마다(Asia/Seoul vs UTC) 값이 달라진다.
os.environ["TZ"] = "UTC"
time.tzset()

SCHEMA = StructType(
    [
        StructField("event_time", TimestampType(), nullable=False),
        StructField("segment_id", StringType(), nullable=True),
        StructField("vehicle_profile_id", IntegerType(), nullable=False),
        StructField("speed_mps", DoubleType(), nullable=True),
        StructField("accel_x", DoubleType(), nullable=True),
        StructField("accel_y", DoubleType(), nullable=True),
        StructField("accel_z", DoubleType(), nullable=True),
        StructField("jerk_x", DoubleType(), nullable=True),
        StructField("jerk_y", DoubleType(), nullable=True),
        StructField("jerk_z", DoubleType(), nullable=True),
        StructField("steering_rate", DoubleType(), nullable=True),
        StructField("steering_vibration", DoubleType(), nullable=True),
    ]
)


@pytest.fixture(scope="session")
def spark():
    # 세션 전체에서 재사용: SparkSession 기동에 몇 초가 걸린다.
    session = (
        SparkSession.builder.appName("batch-jobs-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def event_time(hour: int, minute: int, second: int) -> datetime:
    return datetime(2026, 8, 11, hour, minute, second, tzinfo=UTC)


def expected(hour: int, minute: int, second: int = 0) -> datetime:
    # collect()가 돌려주는 TimestampType 값은 tzinfo가 없는 naive datetime이다.
    return datetime(2026, 8, 11, hour, minute, second)  # noqa: DTZ001


def sensor_row(
    hour: int,
    minute: int,
    second: int,
    segment_id: str | None = "S1",
    vehicle_profile_id: int = 1,
    speed_mps: float | None = None,
) -> tuple:
    return (
        event_time(hour, minute, second),
        segment_id,
        vehicle_profile_id,
        speed_mps,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )


def sensor_df(spark, rows: list[tuple]):
    return spark.createDataFrame(rows, SCHEMA)


def test_rows_in_the_same_hour_share_data_period_start(spark) -> None:
    rows = [
        sensor_row(10, 0, 0, speed_mps=5.0),
        sensor_row(10, 37, 25, speed_mps=6.0),
        sensor_row(10, 59, 59, speed_mps=7.0),
    ]

    result = add_hourly_aggregation_keys(sensor_df(spark, rows)).collect()

    assert {row["data_period_start"] for row in result} == {expected(10, 0)}


def test_hour_boundary_splits_into_different_periods(spark) -> None:
    rows = [sensor_row(10, 59, 59), sensor_row(11, 0, 0)]

    result = add_hourly_aggregation_keys(sensor_df(spark, rows)).orderBy("event_time").collect()

    assert result[0]["data_period_start"] == expected(10, 0)
    assert result[1]["data_period_start"] == expected(11, 0)


def test_data_period_end_is_exactly_one_hour_after_start(spark) -> None:
    rows = [sensor_row(10, 37, 25)]

    row = add_hourly_aggregation_keys(sensor_df(spark, rows)).first()

    assert row["data_period_end"] == expected(11, 0)


def test_unmatched_events_without_segment_id_are_excluded(spark) -> None:
    rows = [sensor_row(10, 0, 0, segment_id="S1"), sensor_row(10, 0, 0, segment_id=None)]

    result = add_hourly_aggregation_keys(sensor_df(spark, rows)).collect()

    assert len(result) == 1
    assert result[0]["segment_id"] == "S1"


def test_different_segment_ids_are_kept_separate(spark) -> None:
    rows = [sensor_row(10, 0, 0, segment_id="S1"), sensor_row(10, 0, 0, segment_id="S2")]

    result = add_hourly_aggregation_keys(sensor_df(spark, rows)).collect()

    assert {row["segment_id"] for row in result} == {"S1", "S2"}


def test_different_vehicle_profile_ids_are_kept_separate(spark) -> None:
    rows = [
        sensor_row(10, 0, 0, vehicle_profile_id=1),
        sensor_row(10, 0, 0, vehicle_profile_id=2),
    ]

    result = add_hourly_aggregation_keys(sensor_df(spark, rows)).collect()

    assert {row["vehicle_profile_id"] for row in result} == {1, 2}


def test_existing_sensor_columns_are_retained(spark) -> None:
    rows = [sensor_row(10, 0, 0, speed_mps=5.0)]

    row = add_hourly_aggregation_keys(sensor_df(spark, rows)).first()

    assert row["speed_mps"] == pytest.approx(5.0)


def test_missing_required_column_is_rejected(spark) -> None:
    incomplete_schema = StructType([StructField("event_time", TimestampType(), nullable=False)])
    df = spark.createDataFrame([(event_time(10, 0, 0),)], incomplete_schema)

    with pytest.raises(ValueError, match="segment_id"):
        add_hourly_aggregation_keys(df)
