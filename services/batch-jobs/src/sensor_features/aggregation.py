"""Build hourly aggregation keys and statistics for map-matched sensor events."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

HOURLY_GROUP_KEYS = (
    "data_period_start",
    "data_period_end",
    "segment_id",
    "vehicle_profile_id",
)

# RMS를 계산할 신호 전체.
RMS_COLUMNS = (
    "accel_x",
    "accel_y",
    "accel_z",
    "jerk_x",
    "jerk_y",
    "jerk_z",
    "steering_rate",
    "steering_vibration",
)

# P95(절댓값 기준)를 계산할 신호. steering_rate/steering_vibration은 최종
# 스키마상 P95 지표가 정의되어 있지 않다.
P95_ABS_COLUMNS = (
    "accel_x",
    "accel_y",
    "accel_z",
    "jerk_x",
    "jerk_y",
    "jerk_z",
)

_REQUIRED_COLUMNS = {
    "event_time",
    "segment_id",
    "vehicle_profile_id",
    "speed_mps",
    *RMS_COLUMNS,
}

# percentile_approx의 정확도 파라미터. 클수록 정확하지만 메모리를 더 쓴다.
PERCENTILE_ACCURACY = 10_000


def add_hourly_aggregation_keys(df: DataFrame) -> DataFrame:
    """시간·Segment·차량 프로필 집계 키를 추가하고, Map Matching에 실패한 행은 제외한다."""
    missing_columns = _REQUIRED_COLUMNS - set(df.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"df is missing required columns: {missing}")

    data_period_start = F.date_trunc("hour", F.col("event_time"))

    return (
        df.filter(F.col("segment_id").isNotNull())
        .withColumn("data_period_start", data_period_start)
        .withColumn("data_period_end", F.col("data_period_start") + F.expr("INTERVAL 1 HOUR"))
    )


def _rms(column_name: str) -> Column:
    value = F.col(column_name).cast("double")
    return F.sqrt(F.avg(value * value))


def _p95_abs(column_name: str) -> Column:
    # -8이 +5보다 실제 충격은 크지만 부호 그대로면 작게 취급되므로 절댓값 기준으로 본다.
    value = F.abs(F.col(column_name).cast("double"))
    return F.percentile_approx(value, 0.95, PERCENTILE_ACCURACY)


def aggregate_hourly_sensor_statistics(df: DataFrame) -> DataFrame:
    """시간·Segment·차량 프로필별 평균 속도와 가속도/jerk/조향 신호의 RMS·P95를 계산한다."""
    keyed = add_hourly_aggregation_keys(df)

    expressions = [F.avg(F.col("speed_mps").cast("double")).alias("avg_speed_mps")]
    for column_name in RMS_COLUMNS:
        expressions.append(_rms(column_name).alias(f"rms_{column_name}"))
    for column_name in P95_ABS_COLUMNS:
        expressions.append(_p95_abs(column_name).alias(f"p95_abs_{column_name}"))

    return keyed.groupBy(*HOURLY_GROUP_KEYS).agg(*expressions)
