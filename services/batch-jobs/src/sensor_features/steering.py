"""Trip-level steering features derived from processed_sensor_event."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def _trip_window() -> Window:
    """같은 trip 내부에서만 이전 행을 찾기 위한 결정적 정렬 Window."""
    return Window.partitionBy("trip_id").orderBy("trip_seq", "event_time", "event_id")


def add_steering_rate(df: DataFrame, max_gap_seconds: float) -> DataFrame:
    """Add a `steering_rate` column (degree/second) derived within each trip.

    trip 내 직전 이벤트 대비 steering_angle 변화량을 실제 event_time 차이(초)로 나눈다.
    `max_gap_seconds`(초)는 확정값이 없어 기본값을 두지 않는다. event_time을 DOUBLE로
    캐스팅해 계산하므로 마이크로초까지는 정확하지 않아 근사 비교가 필요하다.
    """
    if max_gap_seconds <= 0:
        raise ValueError("max_gap_seconds must be greater than 0")

    trip_window = _trip_window()
    prev_steering_angle = "_prev_steering_angle"
    prev_event_time = "_prev_event_time"
    delta_time_seconds = "_delta_time_seconds"

    with_lags = df.withColumn(
        prev_steering_angle, F.lag("steering_angle").over(trip_window)
    ).withColumn(prev_event_time, F.lag("event_time").over(trip_window))

    with_delta = with_lags.withColumn(
        delta_time_seconds,
        F.col("event_time").cast("double") - F.col(prev_event_time).cast("double"),
    )

    is_missing_input = (
        F.col("steering_angle").isNull()
        | F.col(prev_steering_angle).isNull()
        | F.col("event_time").isNull()
        | F.col(prev_event_time).isNull()
    )
    is_invalid_gap = (F.col(delta_time_seconds) <= 0) | (
        F.col(delta_time_seconds) > max_gap_seconds
    )

    delta_angle = F.col("steering_angle") - F.col(prev_steering_angle)
    steering_rate = F.when(
        is_missing_input | is_invalid_gap,
        F.lit(None).cast("double"),
    ).otherwise(delta_angle / F.col(delta_time_seconds))

    return with_delta.withColumn("steering_rate", steering_rate).drop(
        prev_steering_angle, prev_event_time, delta_time_seconds
    )
