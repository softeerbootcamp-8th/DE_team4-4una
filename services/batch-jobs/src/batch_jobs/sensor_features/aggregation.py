"""Build and validate hourly features for map-matched sensor events."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from batch_jobs.schemas import HOURLY_SEGMENT_FEATURE_SCHEMA

HOURLY_GROUP_KEYS = (
    "data_period_start",
    "data_period_end",
    "segment_id",
    "vehicle_profile_id",
)

HOURLY_PRIMARY_KEY = (
    "data_period_start",
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

# NOT NULL이어야 하는 최종 출력 컬럼. 스키마의 nullable 플래그를 그대로 따른다.
_NON_NULL_OUTPUT_COLUMNS = tuple(
    field.name for field in HOURLY_SEGMENT_FEATURE_SCHEMA.fields if not field.nullable
)

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


_COMBINED_REQUIRED_COLUMNS = _KEY_REQUIRED_COLUMNS | _STATISTICS_REQUIRED_COLUMNS | _EVENT_REQUIRED_COLUMNS


def _aggregate_hourly_segment_features(df: DataFrame) -> DataFrame:
    """통계와 이벤트 횟수를 한 번의 groupBy로 계산한다 — 기존 두 aggregation+join의 중복 스캔을 없앤다(#474)."""
    _require_columns(df, _COMBINED_REQUIRED_COLUMNS)
    keyed = add_hourly_aggregation_keys(df)

    expressions = [F.avg(F.col("speed_mps").cast("double")).alias("avg_speed_mps")]
    for column_name in RMS_COLUMNS:
        expressions.append(_rms(column_name).alias(f"rms_{column_name}"))
    for column_name in P95_ABS_COLUMNS:
        expressions.append(_p95_abs(column_name).alias(f"p95_abs_{column_name}"))
    expressions.extend(
        _count_true(flag_column, output_column)
        for output_column, flag_column in EVENT_FLAG_COLUMNS.items()
    )
    expressions.extend(
        [
            F.count(F.lit(1)).cast("long").alias("sample_count"),
            F.countDistinct("trip_id").cast("long").alias("trip_count"),
        ]
    )

    return keyed.groupBy(*EVENT_GROUP_KEYS).agg(*expressions)


def build_hourly_segment_features(
    df: DataFrame,
    *,
    feature_version: str,
    run_id: str,
    processed_at: datetime | None = None,
) -> DataFrame:
    """센서 통계와 이벤트 집계를 합쳐 HOURLY_SEGMENT_FEATURE_SCHEMA 형태의 결과를 만든다.

    validate_hourly_segment_features()는 호출하지 않는다 — 호출부가 persist 후 별도로 검증해야 중복 계산을 피한다(#474).
    """
    if not feature_version.strip():
        raise ValueError("feature_version must not be blank")
    if not run_id.strip():
        raise ValueError("run_id must not be blank")

    processed_at_value = processed_at or datetime.now(UTC)
    if processed_at_value.utcoffset() != timedelta(0):
        raise ValueError("processed_at must be UTC timezone-aware")

    combined = (
        _aggregate_hourly_segment_features(df)
        .withColumn("feature_version", F.lit(feature_version))
        .withColumn("_processed_at", F.lit(processed_at_value).cast("timestamp"))
        .withColumn("_run_id", F.lit(run_id))
    )

    return combined.select(
        *(
            F.col(field.name).cast(field.dataType).alias(field.name)
            for field in HOURLY_SEGMENT_FEATURE_SCHEMA.fields
        )
    )


def validate_hourly_segment_features(df: DataFrame) -> int:
    """스키마·필수값·PK 중복·시간 구간·카운트 값이 계약을 위반하면 예외를 던지고, 통과하면 행 수를 돌려준다."""
    expected_fields = HOURLY_SEGMENT_FEATURE_SCHEMA.fields
    expected_names = [field.name for field in expected_fields]
    if df.columns != expected_names:
        raise ValueError(
            "hourly feature columns do not match the canonical schema: "
            f"expected={expected_names}, actual={df.columns}"
        )

    actual_types = {field.name: field.dataType for field in df.schema.fields}
    type_mismatches = [
        f"{field.name}: expected {field.dataType.simpleString()}, "
        f"got {actual_types[field.name].simpleString()}"
        for field in expected_fields
        if actual_types[field.name] != field.dataType
    ]
    if type_mismatches:
        raise ValueError(
            "hourly feature types do not match the canonical schema: " + "; ".join(type_mismatches)
        )

    null_condition = F.lit(False)
    for column_name in _NON_NULL_OUTPUT_COLUMNS:
        null_condition = null_condition | F.col(column_name).isNull()

    invalid_period = F.col("data_period_end") != F.col("data_period_start") + F.expr(
        "INTERVAL 1 HOUR"
    )

    count_columns = (*EVENT_FLAG_COLUMNS.keys(), "sample_count", "trip_count")
    invalid_count = F.lit(False)
    for column_name in count_columns:
        invalid_count = invalid_count | (F.col(column_name) < 0)
    invalid_count = (
        invalid_count
        | (F.col("sample_count") <= 0)
        | (F.col("trip_count") <= 0)
        | (F.col("sample_count") < F.col("trip_count"))
    )
    for column_name in EVENT_FLAG_COLUMNS:
        invalid_count = invalid_count | (F.col(column_name) > F.col("sample_count"))

    # 아래 두 액션이 상류 lineage를 다시 계산하지 않도록 캐시하되, 호출부가 이미 persist해 뒀으면 그대로 두고 건드리지 않는다(#474).
    already_persisted = df.storageLevel.useMemory or df.storageLevel.useDisk
    if not already_persisted:
        df = df.cache()
    try:
        # 행 단위 위반 3종 + 전체 행 수를 개별 count()/스캔 대신 하나의 스캔으로 같이
        # 계산한다(#539) — 이 count를 호출부가 재사용하면 result.count()를 또 안 해도 된다.
        violations = df.select(
            F.max(null_condition.cast("int")).alias("has_null"),
            F.max(invalid_period.cast("int")).alias("has_invalid_period"),
            F.max(invalid_count.cast("int")).alias("has_invalid_count"),
            F.count(F.lit(1)).alias("row_count"),
        ).first()

        if violations["has_null"]:
            raise ValueError("hourly feature output contains NULL in a required column")
        if violations["has_invalid_period"]:
            raise ValueError("data_period_end must be exactly one hour after start")
        if violations["has_invalid_count"]:
            raise ValueError("hourly feature output contains invalid count values")

        duplicate = (
            df.groupBy(*HOURLY_PRIMARY_KEY).count().filter(F.col("count") > 1).limit(1).collect()
        )
        if duplicate:
            key = {column: duplicate[0][column] for column in HOURLY_PRIMARY_KEY}
            raise ValueError(f"duplicate hourly feature primary key: {key}")

        return violations["row_count"]
    finally:
        if not already_persisted:
            df.unpersist()
