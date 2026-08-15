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

EVENT_GROUP_KEYS = (*HOURLY_GROUP_KEYS, "road_snapshot_date")

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

# 집계 결과 컬럼명 -> 원본 이벤트 시작 플래그 컬럼명
EVENT_FLAG_COLUMNS = {
    "hard_brake_count": "hard_brake_event_start",
    "hard_accel_count": "hard_accel_event_start",
    "sharp_steer_count": "sharp_steer_event_start",
    "steer_reversal_count": "is_steering_reversal",
}

_KEY_REQUIRED_COLUMNS = {
    "event_time",
    "segment_id",
    "vehicle_profile_id",
}

_STATISTICS_REQUIRED_COLUMNS = {
    "speed_mps",
    *RMS_COLUMNS,
}

_EVENT_REQUIRED_COLUMNS = {
    "road_snapshot_date",
    "trip_id",
    *EVENT_FLAG_COLUMNS.values(),
}

# percentile_approx의 정확도 파라미터. 클수록 정확하지만 메모리를 더 쓴다.
PERCENTILE_ACCURACY = 10_000


def _require_columns(df: DataFrame, required_columns: set[str]) -> None:
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"df is missing required columns: {missing}")


def add_hourly_aggregation_keys(df: DataFrame) -> DataFrame:
    """시간·Segment·차량 프로필 집계 키를 추가하고, Map Matching에 실패한 행은 제외한다."""
    _require_columns(df, _KEY_REQUIRED_COLUMNS)

    return (
        df.filter(F.col("segment_id").isNotNull())
        .withColumn("data_period_start", F.date_trunc("hour", F.col("event_time")))
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
    _require_columns(df, _STATISTICS_REQUIRED_COLUMNS)
    keyed = add_hourly_aggregation_keys(df)

    expressions = [F.avg(F.col("speed_mps").cast("double")).alias("avg_speed_mps")]
    for column_name in RMS_COLUMNS:
        expressions.append(_rms(column_name).alias(f"rms_{column_name}"))
    for column_name in P95_ABS_COLUMNS:
        expressions.append(_p95_abs(column_name).alias(f"p95_abs_{column_name}"))

    return keyed.groupBy(*HOURLY_GROUP_KEYS).agg(*expressions)


def _count_true(column_name: str, output_name: str) -> Column:
    # 이벤트는 시작 행 하나만 True이므로, True 개수를 세면 곧 이벤트 발생 횟수가 된다.
    flag = F.coalesce(F.col(column_name).cast("boolean"), F.lit(False))
    return F.sum(F.when(flag, 1).otherwise(0).cast("int")).cast("int").alias(output_name)


def aggregate_hourly_event_counts(df: DataFrame) -> DataFrame:
    """시간·Segment·차량 프로필·road_snapshot_date별로 표본·Trip·이벤트 횟수를 집계한다."""
    _require_columns(df, _EVENT_REQUIRED_COLUMNS)
    keyed = add_hourly_aggregation_keys(df)

    count_expressions = [
        _count_true(flag_column, output_column)
        for output_column, flag_column in EVENT_FLAG_COLUMNS.items()
    ]
    count_expressions.extend(
        [
            F.count(F.lit(1)).cast("long").alias("sample_count"),
            F.countDistinct("trip_id").cast("long").alias("trip_count"),
        ]
    )

    return keyed.groupBy(*EVENT_GROUP_KEYS).agg(*count_expressions)
