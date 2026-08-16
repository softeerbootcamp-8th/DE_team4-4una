import os
import time
from datetime import UTC, date, datetime

import pytest
from batch_jobs.sensor_features.aggregation import aggregate_hourly_event_counts
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
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

SNAPSHOT = date(2026, 8, 11)

SENSOR_SCHEMA = StructType(
    [
        StructField("event_time", TimestampType(), nullable=False),
        StructField("segment_id", StringType(), nullable=True),
        StructField("vehicle_profile_id", IntegerType(), nullable=False),
        StructField("trip_id", StringType(), nullable=True),
        StructField("road_snapshot_date", DateType(), nullable=True),
        StructField("speed_mps", DoubleType(), nullable=True),
        StructField("accel_x", DoubleType(), nullable=True),
        StructField("accel_y", DoubleType(), nullable=True),
        StructField("accel_z", DoubleType(), nullable=True),
        StructField("jerk_x", DoubleType(), nullable=True),
        StructField("jerk_y", DoubleType(), nullable=True),
        StructField("jerk_z", DoubleType(), nullable=True),
        StructField("steering_rate", DoubleType(), nullable=True),
        StructField("steering_vibration", DoubleType(), nullable=True),
        StructField("hard_brake_event_start", BooleanType(), nullable=True),
        StructField("hard_accel_event_start", BooleanType(), nullable=True),
        StructField("sharp_steer_event_start", BooleanType(), nullable=True),
        StructField("is_steering_reversal", BooleanType(), nullable=True),
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


def event_time(minute: int = 0, hour: int = 10) -> datetime:
    return datetime(2026, 8, 11, hour, minute, 0, tzinfo=UTC)


def sensor_row(
    minute: int = 0,
    hour: int = 10,
    segment_id: str | None = "S1",
    vehicle_profile_id: int = 1,
    trip_id: str = "T1",
    hard_brake_event_start: bool | None = False,
    hard_accel_event_start: bool | None = False,
    sharp_steer_event_start: bool | None = False,
    is_steering_reversal: bool | None = False,
) -> tuple:
    return (
        event_time(minute, hour=hour),
        segment_id,
        vehicle_profile_id,
        trip_id,
        SNAPSHOT,
        10.0,
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        hard_brake_event_start,
        hard_accel_event_start,
        sharp_steer_event_start,
        is_steering_reversal,
    )


def sensor_df(spark, rows: list[tuple]):
    return spark.createDataFrame(rows, SENSOR_SCHEMA)


def one_group(spark, rows: list[tuple]):
    result = aggregate_hourly_event_counts(sensor_df(spark, rows)).collect()
    assert len(result) == 1
    return result[0]


def test_event_and_sample_counts_are_accurate(spark) -> None:
    rows = [
        sensor_row(0, trip_id="T1", hard_brake_event_start=True),  # 급제동 episode 시작
        sensor_row(1, trip_id="T1", hard_brake_event_start=False),  # 같은 episode의 나머지 행
        sensor_row(2, trip_id="T1", hard_accel_event_start=True),
        sensor_row(3, trip_id="T2", sharp_steer_event_start=True),
        sensor_row(4, trip_id="T2", is_steering_reversal=True),
    ]

    row = one_group(spark, rows)

    assert row["hard_brake_count"] == 1  # 여러 행짜리 episode도 1회
    assert row["hard_accel_count"] == 1
    assert row["sharp_steer_count"] == 1
    assert row["steer_reversal_count"] == 1
    assert row["sample_count"] == 5
    assert row["trip_count"] == 2


def test_null_event_flags_are_treated_as_zero(spark) -> None:
    rows = [sensor_row(0, hard_brake_event_start=None)]

    row = one_group(spark, rows)

    assert row["hard_brake_count"] == 0


def test_group_keys_separate_and_unmatched_rows_excluded(spark) -> None:
    rows = [
        sensor_row(0, segment_id="S1", vehicle_profile_id=1),
        sensor_row(0, segment_id="S2", vehicle_profile_id=1),
        sensor_row(0, segment_id="S1", vehicle_profile_id=2),
        sensor_row(0, hour=11, segment_id="S1", vehicle_profile_id=1),
        sensor_row(0, segment_id=None),  # 미매칭 행은 별도 그룹을 만들지 않고 제외되어야 함
    ]

    result = aggregate_hourly_event_counts(sensor_df(spark, rows)).collect()

    assert len(result) == 4
