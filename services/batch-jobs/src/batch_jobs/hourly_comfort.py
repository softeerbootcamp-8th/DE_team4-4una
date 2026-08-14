"""Calculate hourly directional comfort scores from aggregated sensor features."""

from dataclasses import dataclass
from datetime import datetime
from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from batch_jobs.comfort_scoring_config import (
    DEFAULT_HOURLY_SCORING_CONFIG,
    ComponentRule,
    HourlyScoringConfig,
)
from batch_jobs.schemas import HOURLY_SEGMENT_FEATURE_SCHEMA

PRIMARY_KEY = ("segment_id", "vehicle_profile_id", "data_period_start")
SCORE_COLUMNS = ("vertical_score", "longitudinal_score", "lateral_score")
RATE_SOURCE_COLUMNS = {
    "hard_brake_rate": "hard_brake_count",
    "hard_accel_rate": "hard_accel_count",
    "sharp_steer_rate": "sharp_steer_count",
    "steer_reversal_rate": "steer_reversal_count",
}


@dataclass(frozen=True, slots=True)
class HourlyScoringResult:
    scored: DataFrame
    rejected: DataFrame


def calculate_hourly_comfort_scores(
    features: DataFrame,
    run_id: str,
    processed_at: datetime,
    config: HourlyScoringConfig = DEFAULT_HOURLY_SCORING_CONFIG,
) -> HourlyScoringResult:
    """Score valid rows and retain rows without enough reliable features."""
    _validate_arguments(features, run_id, processed_at, config)

    # 운행량이 많은 구간일수록 불편하게 보이지 않도록 횟수를 Trip당 비율로 바꾼다
    feature_columns = {
        field.name: F.col(field.name) for field in HOURLY_SEGMENT_FEATURE_SCHEMA
    }
    for rate_name, count_name in RATE_SOURCE_COLUMNS.items():
        feature_columns[rate_name] = F.when(
            F.col("trip_count") > 0,
            F.col(count_name).cast("double") / F.col("trip_count"),
        )

    speed_scale = _speed_anchor_scale(config)
    scored = features
    for component in config.components:
        scored = scored.withColumn(
            component.output_column,
            _component_score(component, feature_columns, speed_scale, config),
        )

    source_features = {
        RATE_SOURCE_COLUMNS.get(name, name)
        for rule in config.components
        for name, _ in rule.weights
    }
    nonnegative = reduce(
        lambda left, right: left & right,
        [F.col(name).isNull() | (F.col(name) >= 0) for name in source_features],
    )
    eligible = (
        F.col("avg_speed_mps").isNotNull()
        & (F.col("avg_speed_mps") >= 0)
        & (F.col("sample_count") > 0)
        & (F.col("trip_count") > 0)
        & nonnegative
    )
    has_every_score = reduce(
        lambda left, right: left & right,
        [F.col(column).isNotNull() for column in SCORE_COLUMNS],
    )

    output = scored.filter(eligible & has_every_score).select(
        "segment_id",
        "vehicle_profile_id",
        "data_period_start",
        "data_period_end",
        "road_snapshot_date",
        *SCORE_COLUMNS,
        F.lit(config.scoring_version).alias("scoring_version"),
        "sample_count",
        F.lit(run_id).alias("_run_id"),
        F.lit(processed_at).alias("_processed_at"),
    )
    # 점수를 만들 수 없는 행을 0점으로 숨기지 않고 후속 저장 단계가 분리해 다루게 한다
    rejected = scored.filter(~eligible | ~has_every_score).select(
        *PRIMARY_KEY,
        "feature_version",
        F.when(~eligible, F.lit("INVALID_SCORING_INPUT"))
        .otherwise(F.lit("INSUFFICIENT_SCORING_FEATURES"))
        .alias("reject_reason"),
    )
    return HourlyScoringResult(scored=output, rejected=rejected)


def _component_score(
    component: ComponentRule,
    feature_columns: dict[str, Column],
    speed_scale: Column,
    config: HourlyScoringConfig,
) -> Column:
    normalizers = dict(config.normalizers)
    valid_weight = F.lit(0.0)
    weighted_penalty = F.lit(0.0)
    for feature_name, weight in component.weights:
        value = feature_columns[feature_name]
        anchors = normalizers[feature_name]
        penalty = _normalized_penalty(
            value, anchors.comfortable, anchors.uncomfortable, speed_scale
        )
        valid_weight += F.when(value.isNotNull(), F.lit(weight)).otherwise(F.lit(0.0))
        weighted_penalty += F.when(value.isNotNull(), penalty * weight).otherwise(
            F.lit(0.0)
        )

    # 일부 선택 Feature가 비어도 남은 가중치를 다시 맞춰 결측을 0점이나 100점으로 오해하지 않는다
    return F.when(
        valid_weight >= config.minimum_valid_weight,
        F.round(100.0 * (1.0 - weighted_penalty / valid_weight), 6),
    )


def _normalized_penalty(
    value: Column, low: float, high: float, scale: Column
) -> Column:
    scaled_low = F.lit(low) * scale
    scaled_high = F.lit(high) * scale
    ratio = (value - scaled_low) / (scaled_high - scaled_low)
    return F.greatest(F.lit(0.0), F.least(F.lit(1.0), ratio))


def _speed_anchor_scale(config: HourlyScoringConfig) -> Column:
    expression: Column | None = None
    for band in config.speed_bands:
        if band.upper_mps is None:
            assert expression is not None
            return expression.otherwise(F.lit(band.anchor_scale))
        condition = F.col("avg_speed_mps") < band.upper_mps
        expression = (
            F.when(condition, band.anchor_scale)
            if expression is None
            else expression.when(condition, band.anchor_scale)
        )
    raise ValueError("scoring config has no open-ended speed band")


def _validate_arguments(
    features: DataFrame,
    run_id: str,
    processed_at: datetime,
    config: HourlyScoringConfig,
) -> None:
    missing = {field.name for field in HOURLY_SEGMENT_FEATURE_SCHEMA} - set(
        features.columns
    )
    if missing:
        raise ValueError(f"hourly feature columns are missing: {sorted(missing)}")
    if not run_id:
        raise ValueError("run_id must be non-empty")
    if processed_at.utcoffset() is None:
        raise ValueError("processed_at must be timezone-aware")

    versions = {
        row.feature_version
        for row in features.select("feature_version").distinct().collect()
    }
    if unsupported := versions - config.compatible_feature_versions:
        raise ValueError(f"unsupported feature versions: {sorted(unsupported)}")
    if (
        features.groupBy(*PRIMARY_KEY)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    ):
        raise ValueError("hourly feature input contains duplicate primary keys")
