import os
import time
from datetime import UTC, datetime

import pytest
from batch_jobs.sensor_features.aggregation import aggregate_hourly_sensor_statistics
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


def event_time(hour: int = 10, minute: int = 0, second: int = 0) -> datetime:
    return datetime(2026, 8, 11, hour, minute, second, tzinfo=UTC)


def expected(hour: int, minute: int = 0) -> datetime:
    # collect()가 돌려주는 TimestampType 값은 tzinfo가 없는 naive datetime이다.
    return datetime(2026, 8, 11, hour, minute, 0)  # noqa: DTZ001


def sensor_row(
    minute: int = 0,
    hour: int = 10,
    second: int = 0,
    segment_id: str | None = "S1",
    vehicle_profile_id: int = 1,
    speed_mps: float | None = 10.0,
    accel_x: float | None = None,
) -> tuple:
    return (
        event_time(hour, minute, second),
        segment_id,
        vehicle_profile_id,
        speed_mps,
        accel_x,
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


def one_group(spark, rows: list[tuple]):
    result = aggregate_hourly_sensor_statistics(sensor_df(spark, rows)).collect()
    assert len(result) == 1
    return result[0]


def test_rms_matches_the_formula(spark) -> None:
    rows = [sensor_row(0, accel_x=3.0), sensor_row(1, accel_x=4.0)]

    row = one_group(spark, rows)

    expected_rms = ((3.0**2 + 4.0**2) / 2) ** 0.5
    assert row["rms_accel_x"] == pytest.approx(expected_rms)


def test_p95_uses_absolute_value(spark) -> None:
    values = [1.0, 2.0, 3.0, 4.0, -10.0]
    rows = [sensor_row(minute, accel_x=value) for minute, value in enumerate(values)]

    row = one_group(spark, rows)

    # 부호를 그대로 쓰면 0.95 분위수는 4 근처에 그치지만, 절댓값 기준이면
    # -10의 크기(10)가 반영되어 훨씬 커야 한다.
    assert row["p95_abs_accel_x"] > 5.0


def test_avg_speed_is_computed_correctly(spark) -> None:
    rows = [sensor_row(0, speed_mps=10.0), sensor_row(1, speed_mps=20.0)]

    row = one_group(spark, rows)

    assert row["avg_speed_mps"] == pytest.approx(15.0)


def test_steering_signals_only_have_rms_not_p95(spark) -> None:
    rows = [sensor_row(0)]

    row = one_group(spark, rows)

    assert "rms_steering_rate" in row.asDict()
    assert "rms_steering_vibration" in row.asDict()
    assert "p95_abs_steering_rate" not in row.asDict()
    assert "p95_abs_steering_vibration" not in row.asDict()


def test_different_group_keys_are_aggregated_separately(spark) -> None:
    rows = [
        sensor_row(0, segment_id="S1", vehicle_profile_id=1),
        sensor_row(0, segment_id="S2", vehicle_profile_id=1),
        sensor_row(0, segment_id="S1", vehicle_profile_id=2),
    ]

    result = aggregate_hourly_sensor_statistics(sensor_df(spark, rows)).collect()

    keys = {(row["segment_id"], row["vehicle_profile_id"]) for row in result}
    assert keys == {("S1", 1), ("S2", 1), ("S1", 2)}


def test_hour_boundary_is_aggregated_into_different_groups(spark) -> None:
    rows = [
        sensor_row(minute=59, second=59),
        sensor_row(hour=11, minute=0, second=0),
    ]

    result = aggregate_hourly_sensor_statistics(sensor_df(spark, rows)).collect()

    periods = {row["data_period_start"] for row in result}
    assert periods == {expected(10), expected(11)}


def test_unmatched_events_without_segment_id_are_excluded(spark) -> None:
    rows = [sensor_row(0, segment_id="S1"), sensor_row(0, segment_id=None)]

    result = aggregate_hourly_sensor_statistics(sensor_df(spark, rows)).collect()

    assert len(result) == 1
    assert result[0]["segment_id"] == "S1"


def test_partial_null_values_are_excluded_from_statistics(spark) -> None:
    rows = [
        sensor_row(0, accel_x=3.0),
        sensor_row(1, accel_x=None),
        sensor_row(2, accel_x=4.0),
    ]

    row = one_group(spark, rows)

    expected_rms = ((3.0**2 + 4.0**2) / 2) ** 0.5
    assert row["rms_accel_x"] == pytest.approx(expected_rms)


def test_all_null_values_produce_null_statistics(spark) -> None:
    rows = [sensor_row(0, accel_x=None), sensor_row(1, accel_x=None)]

    row = one_group(spark, rows)

    assert row["rms_accel_x"] is None
    assert row["p95_abs_accel_x"] is None


def test_result_is_independent_of_input_row_order(spark) -> None:
    rows = [sensor_row(0, accel_x=3.0), sensor_row(1, accel_x=4.0)]

    forward = one_group(spark, rows)
    reversed_row = one_group(spark, list(reversed(rows)))

    assert forward["rms_accel_x"] == pytest.approx(reversed_row["rms_accel_x"])
