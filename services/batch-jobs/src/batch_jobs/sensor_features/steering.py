"""Trip-level steering features derived from processed_sensor_event."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from batch_jobs.sensor_features.trip_window import trip_window as _trip_window


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


def add_steering_reversal(df: DataFrame, steering_rate_deadband_deg_per_sec: float) -> DataFrame:
    """Add an `is_steering_reversal` column derived within each trip.

    steering_rate 부호를 조향 방향으로 보고, 직전 유효 방향과 부호가 뒤집히면 True다.
    steering_rate가 NULL이면(sampling gap 등) 연속성이 끊긴 것으로 보아 새
    continuity group을 시작하고, deadband로 인한 무방향은 연속성을 유지한 채 건너뛴다.
    """
    if steering_rate_deadband_deg_per_sec < 0:
        raise ValueError("steering_rate_deadband_deg_per_sec must be non-negative")

    trip_window = _trip_window()
    continuity_group = "_continuity_group"
    steering_direction = "_steering_direction"
    prev_valid_direction = "_prev_valid_steering_direction"

    is_gap = F.col("steering_rate").isNull()
    with_group = df.withColumn(
        continuity_group,
        F.sum(F.when(is_gap, 1).otherwise(0)).over(
            trip_window.rowsBetween(Window.unboundedPreceding, 0)
        ),
    )

    continuity_preceding_window = _trip_window(continuity_group).rowsBetween(
        Window.unboundedPreceding, -1
    )

    direction = F.when(
        is_gap | (F.abs(F.col("steering_rate")) <= steering_rate_deadband_deg_per_sec),
        F.lit(None).cast("double"),
    ).otherwise(F.signum(F.col("steering_rate")))

    with_direction = with_group.withColumn(steering_direction, direction)
    with_prev_direction = with_direction.withColumn(
        prev_valid_direction,
        F.last(F.col(steering_direction), ignorenulls=True).over(continuity_preceding_window),
    )

    is_reversal = F.when(
        F.col(steering_direction).isNull() | F.col(prev_valid_direction).isNull(),
        F.lit(None).cast("boolean"),
    ).otherwise(F.col(steering_direction) != F.col(prev_valid_direction))

    return with_prev_direction.withColumn("is_steering_reversal", is_reversal).drop(
        continuity_group, steering_direction, prev_valid_direction
    )
