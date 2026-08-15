import os
import time
from datetime import UTC, datetime, timedelta

import pytest
from pyspark.sql import Row, SparkSession
from sensor_features.events import (
    add_hard_acceleration_event,
    add_hard_braking_event,
    add_sharp_steering_event,
)

# collect()가 돌려주는 timestamp는 이 파이썬 프로세스의 로컬 타임존으로 변환되어,
# 고정하지 않으면 실행 머신마다(Asia/Seoul vs UTC) 값이 달라진다.
os.environ["TZ"] = "UTC"
time.tzset()

COLUMNS = ("event_id", "trip_id", "trip_seq", "event_time", "accel_x")

BASE_EVENT_TIME = datetime(2026, 8, 14, 10, 0, 0, tzinfo=UTC)

ACCEL_THRESHOLD = 3.0
BRAKE_THRESHOLD = -3.0


@pytest.fixture(scope="session")
def spark():
    # 세션 전체에서 재사용: SparkSession 기동에 몇 초가 걸린다.
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


def event_time(offset_seconds: float = 0.0) -> datetime:
    return BASE_EVENT_TIME + timedelta(seconds=offset_seconds)


def accel_row(event_id: str, trip_id: str, trip_seq: int, second: float, accel_x) -> Row:
    return Row(
        event_id=event_id,
        trip_id=trip_id,
        trip_seq=trip_seq,
        event_time=event_time(second),
        accel_x=accel_x,
    )


def flags_by_event_id(df, column: str) -> dict:
    return {row["event_id"]: row[column] for row in df.collect()}


def add_hard_accel(df, min_event_duration_seconds: float = 0.0, max_gap_seconds: float = 0.5):
    return add_hard_acceleration_event(
        df, ACCEL_THRESHOLD, min_event_duration_seconds, max_gap_seconds
    )


def add_hard_brake(df, min_event_duration_seconds: float = 0.0, max_gap_seconds: float = 0.5):
    return add_hard_braking_event(
        df, BRAKE_THRESHOLD, min_event_duration_seconds, max_gap_seconds
    )


STEER_THRESHOLD = 100.0


def steer_rate_row(event_id: str, trip_seq: int, second: float, steering_rate) -> Row:
    return Row(
        event_id=event_id,
        trip_id="A",
        trip_seq=trip_seq,
        event_time=event_time(second),
        steering_rate=steering_rate,
    )


def add_sharp_steer(df, min_event_duration_seconds: float = 0.0, max_gap_seconds: float = 0.5):
    return add_sharp_steering_event(
        df, STEER_THRESHOLD, min_event_duration_seconds, max_gap_seconds
    )


def test_sustained_hard_acceleration_flags_only_the_start_row(spark) -> None:
    rows = [
        accel_row("e1", "A", 1, 0.0, 3.5),
        accel_row("e2", "A", 2, 0.1, 3.8),
        accel_row("e3", "A", 3, 0.2, 4.1),
        accel_row("e4", "A", 4, 0.3, 3.7),
    ]
    df = spark.createDataFrame(rows)

    result = flags_by_event_id(
        add_hard_accel(df, min_event_duration_seconds=0.3), "hard_accel_event_start"
    )

    assert result == {"e1": True, "e2": False, "e3": False, "e4": False}


def test_short_hard_acceleration_is_not_flagged(spark) -> None:
    rows = [
        accel_row("e1", "A", 1, 0.0, 3.5),
        accel_row("e2", "A", 2, 0.1, 3.8),
        accel_row("e3", "A", 3, 0.2, 1.0),
    ]
    df = spark.createDataFrame(rows)

    result = flags_by_event_id(
        add_hard_accel(df, min_event_duration_seconds=0.3), "hard_accel_event_start"
    )

    assert result == {"e1": False, "e2": False, "e3": False}


def test_multi_row_hard_braking_counts_once(spark) -> None:
    rows = [
        accel_row("e1", "A", 1, 0.0, -3.5),
        accel_row("e2", "A", 2, 0.1, -3.8),
        accel_row("e3", "A", 3, 0.2, -4.1),
        accel_row("e4", "A", 4, 0.3, -3.7),
    ]
    df = spark.createDataFrame(rows)

    result = flags_by_event_id(
        add_hard_brake(df, min_event_duration_seconds=0.3), "hard_brake_event_start"
    )

    assert sum(1 for flagged in result.values() if flagged) == 1
    assert result["e1"] is True


def test_episode_splits_when_condition_is_released(spark) -> None:
    rows = [
        accel_row("e1", "A", 1, 0.0, 3.5),
        accel_row("e2", "A", 2, 0.1, 3.8),
        accel_row("e3", "A", 3, 0.2, 1.0),  # 조건 해제 -> episode 종료
        accel_row("e4", "A", 4, 0.3, 3.6),
        accel_row("e5", "A", 5, 0.4, 3.9),
    ]
    df = spark.createDataFrame(rows)

    result = flags_by_event_id(
        add_hard_accel(df, min_event_duration_seconds=0.05), "hard_accel_event_start"
    )

    assert result == {"e1": True, "e2": False, "e3": False, "e4": True, "e5": False}


def test_different_trips_are_not_connected(spark) -> None:
    rows = [
        accel_row("e1", "A", 1, 0.0, 3.5),
        accel_row("e2", "A", 2, 0.1, 3.8),
        accel_row("e3", "B", 1, 0.2, 3.6),
        accel_row("e4", "B", 2, 0.3, 3.9),
    ]
    df = spark.createDataFrame(rows)

    result = flags_by_event_id(
        add_hard_accel(df, min_event_duration_seconds=0.05), "hard_accel_event_start"
    )

    assert result == {"e1": True, "e2": False, "e3": True, "e4": False}


def test_sampling_gap_splits_the_episode(spark) -> None:
    rows = [
        accel_row("e1", "A", 1, 0.0, 3.5),
        accel_row("e2", "A", 2, 0.1, 3.8),
        accel_row("e3", "A", 3, 60.0, 3.6),
        accel_row("e4", "A", 4, 60.1, 3.9),
    ]
    df = spark.createDataFrame(rows)

    result = flags_by_event_id(
        add_hard_accel(df, min_event_duration_seconds=0.05, max_gap_seconds=0.5),
        "hard_accel_event_start",
    )

    assert result == {"e1": True, "e2": False, "e3": True, "e4": False}


def test_null_accel_x_splits_the_episode(spark) -> None:
    rows = [
        accel_row("e1", "A", 1, 0.0, 3.5),
        accel_row("e1b", "A", 2, 0.1, 3.6),
        accel_row("e2", "A", 3, 0.2, None),
        accel_row("e3", "A", 4, 0.3, 3.6),
        accel_row("e4", "A", 5, 0.4, 3.9),
    ]
    df = spark.createDataFrame(rows)

    result = flags_by_event_id(
        add_hard_accel(df, min_event_duration_seconds=0.05), "hard_accel_event_start"
    )

    assert result == {"e1": True, "e1b": False, "e2": False, "e3": True, "e4": False}


def test_result_is_independent_of_input_row_order(spark) -> None:
    rows = [
        accel_row("e1", "A", 1, 0.0, 3.5),
        accel_row("e2", "A", 2, 0.1, 3.8),
        accel_row("e3", "A", 3, 0.2, 4.1),
    ]
    in_order = spark.createDataFrame(rows)
    shuffled = spark.createDataFrame(list(reversed(rows)))

    expected = flags_by_event_id(
        add_hard_accel(in_order, min_event_duration_seconds=0.15), "hard_accel_event_start"
    )
    actual = flags_by_event_id(
        add_hard_accel(shuffled, min_event_duration_seconds=0.15), "hard_accel_event_start"
    )

    assert actual == expected


def test_existing_columns_are_preserved(spark) -> None:
    rows = [accel_row("e1", "A", 1, 0.0, 3.5)]
    df = spark.createDataFrame(rows)

    result = add_hard_accel(df)

    assert set(COLUMNS).issubset(set(result.columns))
    assert "hard_accel_event_start" in result.columns


def test_existing_run_id_lineage_column_is_not_overwritten(spark) -> None:
    # processed_sensor_event의 lineage 컬럼 _run_id가 episode 계산용 내부 컬럼과
    # 이름이 겹쳐 덮어써지거나 삭제되지 않는지 확인한다.
    rows = [
        Row(
            event_id="e1",
            trip_id="A",
            trip_seq=1,
            event_time=event_time(0.0),
            accel_x=3.5,
            _run_id="nyc-actual-20260814-v1",
        ),
    ]
    df = spark.createDataFrame(rows)

    result = add_hard_accel(df)

    assert result.select("_run_id").collect()[0]["_run_id"] == "nyc-actual-20260814-v1"


@pytest.mark.parametrize("threshold", [0.0, -1.0])
def test_non_positive_hard_accel_threshold_is_rejected(spark, threshold: float) -> None:
    df = spark.createDataFrame([accel_row("e1", "A", 1, 0.0, 3.5)])

    with pytest.raises(ValueError, match="hard_accel_threshold_mps2"):
        add_hard_acceleration_event(df, threshold, 0.0, 0.5)


@pytest.mark.parametrize("threshold", [0.0, 1.0])
def test_non_negative_hard_brake_threshold_is_rejected(spark, threshold: float) -> None:
    df = spark.createDataFrame([accel_row("e1", "A", 1, 0.0, -3.5)])

    with pytest.raises(ValueError, match="hard_brake_threshold_mps2"):
        add_hard_braking_event(df, threshold, 0.0, 0.5)


@pytest.mark.parametrize("max_gap_seconds", [0.0, -1.0])
def test_non_positive_max_gap_seconds_is_rejected(spark, max_gap_seconds: float) -> None:
    df = spark.createDataFrame([accel_row("e1", "A", 1, 0.0, 3.5)])

    with pytest.raises(ValueError, match="max_gap_seconds"):
        add_hard_acceleration_event(df, ACCEL_THRESHOLD, 0.0, max_gap_seconds)


def test_negative_min_event_duration_seconds_is_rejected(spark) -> None:
    df = spark.createDataFrame([accel_row("e1", "A", 1, 0.0, 3.5)])

    with pytest.raises(ValueError, match="min_duration_seconds"):
        add_hard_acceleration_event(df, ACCEL_THRESHOLD, -1.0, 0.5)


STEER_COLUMNS = ("event_id", "trip_id", "trip_seq", "event_time", "steering_rate")


def test_positive_steering_rate_triggers_sharp_steer(spark) -> None:
    rows = [
        ("e1", "A", 1, event_time(0.0), 120.0),
        ("e2", "A", 2, event_time(0.1), 130.0),
        ("e3", "A", 3, event_time(0.2), 125.0),
    ]
    df = spark.createDataFrame(rows, STEER_COLUMNS)

    result = flags_by_event_id(
        add_sharp_steer(df, min_event_duration_seconds=0.15), "sharp_steer_event_start"
    )

    assert result == {"e1": True, "e2": False, "e3": False}


def test_negative_steering_rate_triggers_sharp_steer(spark) -> None:
    rows = [
        ("e1", "A", 1, event_time(0.0), -120.0),
        ("e2", "A", 2, event_time(0.1), -130.0),
    ]
    df = spark.createDataFrame(rows, STEER_COLUMNS)

    result = flags_by_event_id(
        add_sharp_steer(df, min_event_duration_seconds=0.05), "sharp_steer_event_start"
    )

    assert result == {"e1": True, "e2": False}


def test_steering_rate_below_threshold_is_excluded(spark) -> None:
    rows = [
        ("e1", "A", 1, event_time(0.0), 20.0),
        ("e2", "A", 2, event_time(0.1), 30.0),
    ]
    df = spark.createDataFrame(rows, STEER_COLUMNS)

    result = flags_by_event_id(
        add_sharp_steer(df, min_event_duration_seconds=0.0), "sharp_steer_event_start"
    )

    assert result == {"e1": False, "e2": False}


def test_sign_reversal_within_threshold_stays_one_episode(spark) -> None:
    # 부호가 바뀌어도 절대값이 계속 threshold 이상이면 하나의 episode로 본다
    # (방향 반전 자체는 is_steering_reversal이 별도로 기록한다).
    rows = [
        ("e1", "A", 1, event_time(0.0), 120.0),
        ("e2", "A", 2, event_time(0.1), 130.0),
        ("e3", "A", 3, event_time(0.2), -125.0),
        ("e4", "A", 4, event_time(0.3), -110.0),
    ]
    df = spark.createDataFrame(rows, STEER_COLUMNS)

    result = flags_by_event_id(
        add_sharp_steer(df, min_event_duration_seconds=0.25), "sharp_steer_event_start"
    )

    assert result == {"e1": True, "e2": False, "e3": False, "e4": False}


def test_null_steering_rate_is_not_a_candidate(spark) -> None:
    rows = [
        steer_rate_row("e1", 1, 0.0, 120.0),
        steer_rate_row("e2", 2, 0.1, None),
    ]
    df = spark.createDataFrame(rows)

    result = flags_by_event_id(
        add_sharp_steer(df, min_event_duration_seconds=0.0), "sharp_steer_event_start"
    )

    assert result["e2"] is False


def test_sharp_steer_existing_columns_are_preserved(spark) -> None:
    rows = [("e1", "A", 1, event_time(0.0), 120.0)]
    df = spark.createDataFrame(rows, STEER_COLUMNS)

    result = add_sharp_steer(df)

    assert set(STEER_COLUMNS).issubset(set(result.columns))
    assert "sharp_steer_event_start" in result.columns


@pytest.mark.parametrize("threshold", [0.0, -1.0])
def test_non_positive_sharp_steer_threshold_is_rejected(spark, threshold: float) -> None:
    df = spark.createDataFrame([("e1", "A", 1, event_time(0.0), 120.0)], STEER_COLUMNS)

    with pytest.raises(ValueError, match="sharp_steer_threshold_deg_per_sec"):
        add_sharp_steering_event(df, threshold, 0.0, 0.5)
