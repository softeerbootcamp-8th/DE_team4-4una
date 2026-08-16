import os
import time
from datetime import UTC, datetime, timedelta

import pytest
from batch_jobs.sensor_features.steering import add_steering_rate, add_steering_reversal
from pyspark.sql import Row, SparkSession

# collect()가 돌려주는 timestamp는 이 파이썬 프로세스의 로컬 타임존으로 변환되어,
# 고정하지 않으면 실행 머신마다(Asia/Seoul vs UTC) 값이 달라진다.
os.environ["TZ"] = "UTC"
time.tzset()

COLUMNS = ("event_id", "trip_id", "trip_seq", "event_time", "steering_angle")

BASE_EVENT_TIME = datetime(2026, 8, 14, 10, 0, 0, tzinfo=UTC)

# timestamp -> double 캐스팅에서 생기는 미세한 부동소수점 오차를 흡수한다.
RATE_TOLERANCE = 1e-3


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


def _column_by_event_id(df, column: str) -> dict:
    return {row["event_id"]: row[column] for row in df.collect()}


def rows_by_event_id(df) -> dict:
    return _column_by_event_id(df, "steering_rate")


def reversal_by_event_id(df) -> dict:
    return _column_by_event_id(df, "is_steering_reversal")


def test_normal_steering_rate_uses_actual_time_delta(spark) -> None:
    rows = [
        ("e1", "A", 1, event_time(0.0), 0.0),
        ("e2", "A", 2, event_time(0.1), 5.0),
        ("e3", "A", 3, event_time(0.2), 8.0),
    ]
    df = spark.createDataFrame(rows, COLUMNS)

    result = rows_by_event_id(add_steering_rate(df, max_gap_seconds=5.0))

    assert result["e1"] is None
    assert result["e2"] == pytest.approx(50.0, abs=RATE_TOLERANCE)
    assert result["e3"] == pytest.approx(30.0, abs=RATE_TOLERANCE)


def test_trip_boundary_does_not_reference_previous_trip(spark) -> None:
    rows = [
        ("e1", "A", 1, event_time(0.0), 0.0),
        ("e2", "A", 2, event_time(0.1), 10.0),
        ("e3", "B", 1, event_time(0.2), -2.0),
    ]
    df = spark.createDataFrame(rows, COLUMNS)

    result = rows_by_event_id(add_steering_rate(df, max_gap_seconds=5.0))

    assert result["e3"] is None


@pytest.mark.parametrize(
    "offset_seconds, max_gap_seconds, expected_rate",
    [
        (0.5, 5.0, 20.0),  # 실제 event_time 간격(0.5초) 사용, 10Hz를 가정해 0.1초로 나누지 않음
        (60.0, 5.0, None),  # 간격이 max_gap_seconds 초과 -> sampling gap -> NULL
        (4.0, 5.0, 2.5),  # 간격이 max_gap_seconds 이내 -> 정상 계산
    ],
)
def test_steering_rate_depends_on_actual_time_gap(
    spark, offset_seconds: float, max_gap_seconds: float, expected_rate: float | None
) -> None:
    rows = [
        ("e1", "A", 1, event_time(0.0), 0.0),
        ("e2", "A", 2, event_time(offset_seconds), 10.0),
    ]
    df = spark.createDataFrame(rows, COLUMNS)

    result = rows_by_event_id(add_steering_rate(df, max_gap_seconds=max_gap_seconds))

    if expected_rate is None:
        assert result["e2"] is None
    else:
        assert result["e2"] == pytest.approx(expected_rate, abs=RATE_TOLERANCE)


def test_non_positive_time_delta_is_null(spark) -> None:
    rows = [
        ("e1", "A", 1, event_time(0.0), 0.0),
        ("e2", "A", 2, event_time(0.0), 5.0),
        ("e3", "A", 3, event_time(-0.1), 8.0),
    ]
    df = spark.createDataFrame(rows, COLUMNS)

    result = rows_by_event_id(add_steering_rate(df, max_gap_seconds=5.0))

    assert result["e2"] is None
    assert result["e3"] is None


def sensor_row(event_id: str, trip_seq: int, second: float, steering_angle: float | None) -> Row:
    return Row(
        event_id=event_id,
        trip_id="A",
        trip_seq=trip_seq,
        event_time=event_time(second),
        steering_angle=steering_angle,
    )


def test_null_steering_angle_is_null(spark) -> None:
    rows = [
        sensor_row("e1", 1, 0.0, None),
        sensor_row("e2", 2, 0.1, 5.0),
        sensor_row("e3", 3, 0.2, None),
    ]
    df = spark.createDataFrame(rows)

    result = rows_by_event_id(add_steering_rate(df, max_gap_seconds=5.0))

    assert result["e2"] is None
    assert result["e3"] is None


def test_result_is_independent_of_input_row_order(spark) -> None:
    rows = [
        ("e1", "A", 1, event_time(0.0), 0.0),
        ("e2", "A", 2, event_time(0.1), 5.0),
        ("e3", "A", 3, event_time(0.2), 8.0),
    ]
    in_order = spark.createDataFrame(rows, COLUMNS)
    shuffled = spark.createDataFrame(list(reversed(rows)), COLUMNS)

    expected = rows_by_event_id(add_steering_rate(in_order, max_gap_seconds=5.0))
    actual = rows_by_event_id(add_steering_rate(shuffled, max_gap_seconds=5.0))

    assert actual.keys() == expected.keys()
    for event_id, expected_rate in expected.items():
        if expected_rate is None:
            assert actual[event_id] is None
        else:
            assert actual[event_id] == pytest.approx(expected_rate, abs=RATE_TOLERANCE)


def test_existing_columns_are_preserved(spark) -> None:
    rows = [
        ("e1", "A", 1, event_time(0.0), 0.0),
        ("e2", "A", 2, event_time(0.1), 5.0),
    ]
    df = spark.createDataFrame(rows, COLUMNS)

    result = add_steering_rate(df, max_gap_seconds=5.0)

    assert set(COLUMNS).issubset(set(result.columns))
    assert "steering_rate" in result.columns


@pytest.mark.parametrize("max_gap_seconds", [0.0, -1.0])
def test_non_positive_max_gap_seconds_is_rejected(spark, max_gap_seconds: float) -> None:
    rows = [("e1", "A", 1, event_time(0.0), 0.0)]
    df = spark.createDataFrame(rows, COLUMNS)

    with pytest.raises(ValueError, match="max_gap_seconds"):
        add_steering_rate(df, max_gap_seconds=max_gap_seconds)


RATE_COLUMNS = ("event_id", "trip_id", "trip_seq", "event_time", "steering_rate")


def rate_row(event_id: str, trip_seq: int, second: float, steering_rate: float | None) -> Row:
    return Row(
        event_id=event_id,
        trip_id="A",
        trip_seq=trip_seq,
        event_time=event_time(second),
        steering_rate=steering_rate,
    )


def test_deadband_excludes_small_changes_from_direction(spark) -> None:
    rows = [
        ("e1", "A", 1, event_time(0.0), 15.0),
        ("e2", "A", 2, event_time(0.1), 3.0),
        ("e3", "A", 3, event_time(0.2), -15.0),
    ]
    df = spark.createDataFrame(rows, RATE_COLUMNS)

    result = reversal_by_event_id(
        add_steering_reversal(df, steering_rate_deadband_deg_per_sec=5.0)
    )

    assert result["e1"] is None
    assert result["e2"] is None
    assert result["e3"] is True


def test_same_direction_is_not_a_reversal(spark) -> None:
    rows = [
        ("e1", "A", 1, event_time(0.0), 15.0),
        ("e2", "A", 2, event_time(0.1), 20.0),
    ]
    df = spark.createDataFrame(rows, RATE_COLUMNS)

    result = reversal_by_event_id(
        add_steering_reversal(df, steering_rate_deadband_deg_per_sec=5.0)
    )

    assert result["e1"] is None
    assert result["e2"] is False


def test_reversal_does_not_cross_trip_boundary(spark) -> None:
    rows = [
        ("e1", "A", 1, event_time(0.0), 15.0),
        ("e2", "B", 1, event_time(0.1), -15.0),
    ]
    df = spark.createDataFrame(rows, RATE_COLUMNS)

    result = reversal_by_event_id(
        add_steering_reversal(df, steering_rate_deadband_deg_per_sec=5.0)
    )

    assert result["e2"] is None


def test_null_steering_rate_breaks_reversal_continuity(spark) -> None:
    # gap(e2) 이후 첫 유효 방향(e3)은 gap 이전 방향(e1)과 비교되면 안 된다.
    rows = [
        rate_row("e1", 1, 0.0, 15.0),
        rate_row("e2", 2, 0.1, None),
        rate_row("e3", 3, 0.2, -15.0),
    ]
    df = spark.createDataFrame(rows)

    result = reversal_by_event_id(
        add_steering_reversal(df, steering_rate_deadband_deg_per_sec=5.0)
    )

    assert result["e2"] is None
    assert result["e3"] is None


def test_reversal_existing_columns_are_preserved(spark) -> None:
    rows = [
        ("e1", "A", 1, event_time(0.0), 15.0),
        ("e2", "A", 2, event_time(0.1), -15.0),
    ]
    df = spark.createDataFrame(rows, RATE_COLUMNS)

    result = add_steering_reversal(df, steering_rate_deadband_deg_per_sec=5.0)

    assert set(RATE_COLUMNS).issubset(set(result.columns))
    assert "is_steering_reversal" in result.columns


def test_negative_steering_rate_deadband_is_rejected(spark) -> None:
    rows = [("e1", "A", 1, event_time(0.0), 0.0)]
    df = spark.createDataFrame(rows, RATE_COLUMNS)

    with pytest.raises(ValueError, match="steering_rate_deadband_deg_per_sec"):
        add_steering_reversal(df, steering_rate_deadband_deg_per_sec=-1.0)
