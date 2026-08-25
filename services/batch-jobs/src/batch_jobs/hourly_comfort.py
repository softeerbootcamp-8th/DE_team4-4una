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
from batch_jobs.schemas import (
    HOURLY_COMFORT_SCORE_SCHEMA,
    HOURLY_SEGMENT_FEATURE_SCHEMA,
)

PRIMARY_KEY = ("segment_id", "vehicle_profile_id", "data_period_start")
SCORE_COLUMNS = ("vertical_score", "longitudinal_score", "lateral_score")
RATE_SOURCE_COLUMNS = {
    "hard_brake_rate": "hard_brake_count",
    "hard_accel_rate": "hard_accel_count",
    "sharp_steer_rate": "sharp_steer_count",
    "steer_reversal_rate": "steer_reversal_count",
}

REJECT_REASON_COLUMN = "reject_reason"
INVALID_INPUT_REASON = "INVALID_SCORING_INPUT"
INSUFFICIENT_FEATURES_REASON = "INSUFFICIENT_SCORING_FEATURES"

# 채점이 끝난 뒤 downstream(점수 출력 / 격리 출력 / 감사 집계)이 실제로 쓰는 컬럼만
# 남긴다. 이 목록으로 좁힌 뒤에 캐시해야 원본 feature 컬럼 20여 개가 캐시에 남지 않는다.
# `scoring_version`, `_run_id`, `_processed_at`은 점수 출력에서 F.lit()으로 붙으므로
# 여기 포함하지 않는다.
CLASSIFIED_COLUMNS = (
    *PRIMARY_KEY,
    "data_period_end",
    "road_snapshot_date",
    *SCORE_COLUMNS,
    "sample_count",
    "trip_count",
    "feature_version",
    REJECT_REASON_COLUMN,
)

REJECTED_COLUMNS = (*PRIMARY_KEY, "feature_version", REJECT_REASON_COLUMN)


@dataclass(frozen=True, slots=True)
class AuditCounts:
    """감사 집계 한 번으로 얻는 행 수. writer의 `expected_count`로 그대로 넘어간다."""

    scored_count: int
    rejected_count: int


@dataclass(frozen=True, slots=True)
class HourlyScoringPlan:
    """채점 결과와, 그 결과를 검증·분리하는 방법을 함께 들고 있는 계획.

    `classified`는 아직 materialize되지 않은 공통 DataFrame이다. 호출자가 이것을 캐시한
    뒤 `audit()` / `scored()` / `rejected()`에 그 핸들을 넘기면, 점수 계산 lineage가
    한 번만 계산된다. 캐시 여부는 Spark 세션을 아는 job의 책임이므로 여기서 persist하지
    않는다 — 이 모듈은 표현식만 만든다.

    검증 로직과 에러 메시지를 이 모듈이 계속 소유하고, Spark Action만 호출자가 실행한다.
    """

    classified: DataFrame
    audit_columns: tuple[Column, ...]
    config: HourlyScoringConfig
    run_id: str
    processed_at: datetime

    def audit(self, classified: DataFrame | None = None) -> AuditCounts:
        """단 한 번의 aggregation으로 입력 검증과 두 출력의 행 수를 함께 구한다.

        여기서 실행되는 Action 하나가 `classified`를 실체화하므로, 이후 `scored()` /
        `rejected()`는 캐시된 결과에서 필터만 한다.
        """
        frame = self.classified if classified is None else classified
        row = frame.agg(*self.audit_columns).first()
        if row is None:
            # groupBy 없는 global aggregation은 빈 입력에도 한 행을 돌려주므로 정상
            # 경로에서는 도달하지 않는다. 여기 걸리면 집계식 구성이 잘못된 것이다.
            raise RuntimeError("hourly scoring audit returned no aggregation row")
        return self._interpret_audit(row)

    def scored(self, classified: DataFrame | None = None) -> DataFrame:
        """점수를 만든 행을 선언된 Silver3 스키마 그대로 돌려준다."""
        frame = self.classified if classified is None else classified
        accepted = frame.filter(F.col(REJECT_REASON_COLUMN).isNull()).withColumns(
            {
                "scoring_version": F.lit(self.config.scoring_version),
                "_run_id": F.lit(self.run_id),
                "_processed_at": F.lit(self.processed_at),
            }
        )
        return accepted.select(
            *[
                accepted[field.name].cast(field.dataType).alias(field.name)
                for field in HOURLY_COMFORT_SCORE_SCHEMA
            ]
        )

    def rejected(self, classified: DataFrame | None = None) -> DataFrame:
        """점수를 만들 수 없던 행을 이유와 함께 돌려준다.

        컬럼을 명시적으로 좁힌다 — `classified`를 그대로 흘려보내면 격리 데이터셋의
        스키마가 조용히 넓어진다.
        """
        frame = self.classified if classified is None else classified
        return frame.filter(F.col(REJECT_REASON_COLUMN).isNotNull()).select(
            *REJECTED_COLUMNS
        )

    def _interpret_audit(self, row) -> AuditCounts:
        if row["null_version_count"]:
            raise ValueError("feature_version must not be null")
        if row["unsupported_version_count"]:
            raise ValueError(
                "unsupported feature versions detected: "
                f"{row['unsupported_version_sample']!r}"
            )
        if row["total_count"] != row["distinct_key_count"]:
            raise ValueError("hourly feature input contains duplicate primary keys")
        return AuditCounts(
            scored_count=row["scored_count"], rejected_count=row["rejected_count"]
        )


def build_hourly_scoring_plan(
    features: DataFrame,
    run_id: str,
    processed_at: datetime,
    config: HourlyScoringConfig = DEFAULT_HOURLY_SCORING_CONFIG,
) -> HourlyScoringPlan:
    """Score every row and mark the ones that cannot produce a score."""
    _validate_arguments(features, run_id, processed_at)

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

    # 점수를 만들 수 없는 행을 0점으로 숨기지 않고 후속 저장 단계가 분리해 다루게 한다.
    # 상태를 reject_reason 하나로 표현하므로 중간 boolean 컬럼은 남기지 않는다.
    classified = scored.withColumn(
        REJECT_REASON_COLUMN,
        F.when(~_eligible(config), F.lit(INVALID_INPUT_REASON)).when(
            ~_has_every_score(), F.lit(INSUFFICIENT_FEATURES_REASON)
        ),
    ).select(*CLASSIFIED_COLUMNS)

    return HourlyScoringPlan(
        classified=classified,
        audit_columns=_audit_columns(config),
        config=config,
        run_id=run_id,
        processed_at=processed_at,
    )


def _audit_columns(config: HourlyScoringConfig) -> tuple[Column, ...]:
    """입력 검증과 두 출력의 행 수를 한 번에 구하는 집계식.

    버전 호환은 distinct 목록을 driver로 가져와 집합 연산하지 않고 Spark 안에서
    카운터로 판정한다. driver로 넘어오는 것은 숫자와 예시 한 건뿐이라, feature_version
    카디널리티가 커져도 driver 메모리가 늘지 않는다.
    """
    rejected = F.col(REJECT_REASON_COLUMN)
    version = F.col("feature_version")
    unsupported_version = version.isNotNull() & ~version.isin(
        *sorted(config.compatible_feature_versions)
    )
    return (
        F.count(F.lit(1)).alias("total_count"),
        F.count_distinct(F.struct(*[F.col(name) for name in PRIMARY_KEY])).alias(
            "distinct_key_count"
        ),
        # 빈 입력에서 F.sum은 NULL이라 0으로 확정한다.
        _count_where(version.isNull()).alias("null_version_count"),
        _count_where(unsupported_version).alias("unsupported_version_count"),
        # 실패 메시지에 쓸 예시 한 건. 해당 행이 없으면 NULL이다.
        F.min(F.when(unsupported_version, version)).alias("unsupported_version_sample"),
        _count_where(rejected.isNull()).alias("scored_count"),
        _count_where(rejected.isNotNull()).alias("rejected_count"),
    )


def _count_where(condition: Column) -> Column:
    return F.coalesce(F.sum(F.when(condition, 1).otherwise(0)), F.lit(0))


def _eligible(config: HourlyScoringConfig) -> Column:
    """채점해도 되는 입력인지. NULL이 통과하지 않도록 boolean으로 확정한다.

    `sample_count`처럼 nullable=False로 선언된 컬럼도 Parquet 읽기에서는 강제되지
    않으므로, coalesce 없이 두면 조건식이 NULL이 되어 어느 분기에도 안 걸린다.
    """
    source_features = {
        RATE_SOURCE_COLUMNS.get(name, name)
        for rule in config.components
        for name, _ in rule.weights
    }
    # initializer가 없으면 source_features가 빈 경우 reduce가 TypeError를 낸다.
    # config 검증이 빈 components를 막지만, 여기서도 안전하게 접힌다.
    nonnegative = reduce(
        lambda left, right: left & right,
        [F.col(name).isNull() | (F.col(name) >= 0) for name in sorted(source_features)],
        F.lit(True),
    )
    return F.coalesce(
        F.col("avg_speed_mps").isNotNull()
        & (F.col("avg_speed_mps") >= 0)
        & (F.col("sample_count") > 0)
        & (F.col("trip_count") > 0)
        & nonnegative,
        F.lit(False),
    )


def _has_every_score() -> Column:
    return F.coalesce(
        reduce(
            lambda left, right: left & right,
            [F.col(column).isNotNull() for column in SCORE_COLUMNS],
        ),
        F.lit(False),
    )


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


def _normalized_penalty(value: Column, low: float, high: float, scale: Column) -> Column:
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
) -> None:
    """Spark Action이 필요 없는 검사만 한다.

    데이터를 읽어야 아는 검사(feature_version 호환, PK 중복)는 `HourlyScoringPlan.audit`의
    단일 aggregation에서 처리한다 — 채점 전에 별도 Action을 두 번 더 실행하지 않기 위함이다.
    """
    missing = {field.name for field in HOURLY_SEGMENT_FEATURE_SCHEMA} - set(
        features.columns
    )
    if missing:
        raise ValueError(f"hourly feature columns are missing: {sorted(missing)}")
    if not run_id:
        raise ValueError("run_id must be non-empty")
    if processed_at.utcoffset() is None:
        raise ValueError("processed_at must be timezone-aware")
