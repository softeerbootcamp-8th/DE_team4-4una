"""Compute Segment x vehicle-profile comfort score and confidence (#127).

context/comfort-score.md에 정의된 Step 1~5(및 vehicle-agnostic 버전)를 그대로 구현한다.
입력은 comfort_score/loader.py가 이미 168시간 윈도우로 필터링하고 최신 scoring_version만
남겨 둔 hourly_comfort_score DataFrame이다. Gold PostgreSQL 테이블 적재 자체는 후속
이슈(#101 "데이터 적재")의 범위다.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from batch_jobs.comfort_score.config import ComfortScoreConfig
from batch_jobs.comfort_score.loader import _validate_schema
from batch_jobs.schemas import HOURLY_COMFORT_SCORE_SCHEMA

# 이 계산 로직의 형태(shape) 버전. 공식 구조가 바뀌면 올린다 (comfort-score.md 참고).
SCORE_VERSION = "1.0.0"

# vehicle_profile_id=0은 Gold 적재 시 전체 차량 대표값을, 1부터는 실제 차량 구분을 의미한다 (OQ-038).
VEHICLE_AGNOSTIC_VEHICLE_PROFILE_ID = 0

REQUIRED_COLUMNS = (
    "segment_id",
    "vehicle_profile_id",
    "data_period_start",
    "vertical_score",
    "longitudinal_score",
    "lateral_score",
    "trip_count",
    "sample_count",
)

# HOURLY_COMFORT_SCORE_SCHEMA에서 이 함수가 실제로 쓰는 컬럼만 추린 스키마. loader.py의
# _validate_schema를 그대로 재사용해 "필수 컬럼 누락" 검사 로직이 두 군데서 따로 관리되지
# 않게 하고, 덤으로 loader.py와 동일한 타입 검사도 얻는다.
REQUIRED_SCHEMA = StructType(
    [field for field in HOURLY_COMFORT_SCORE_SCHEMA.fields if field.name in REQUIRED_COLUMNS]
)


def compute_segment_comfort_scores(
    hourly_df: DataFrame,
    config: ComfortScoreConfig,
) -> DataFrame:
    """Segment x vehicle_profile 단위와, 차량 구분 없는 Segment 단위(vehicle_profile_id=0)
    comfort_score/confidence_score를 한 DataFrame에 담아 반환한다."""
    _validate_schema(hourly_df.schema, REQUIRED_SCHEMA, source="hourly_comfort_score")
    _validate_no_reserved_vehicle_profile_id(hourly_df)

    # Step 1(c_h)은 두 grain(per-vehicle / vehicle-agnostic)이 공통으로 쓰는 시작점이라
    # 한 번만 계산해서 넘긴다 — 각자 다시 계산하면 나중에 한쪽만 고치고 잊기 쉽다.
    combined = hourly_df.withColumn("c_h", _combined_hourly_score(config))
    per_vehicle = _per_vehicle_scores(combined, config)
    vehicle_agnostic = _vehicle_agnostic_scores(combined, config)
    return per_vehicle.unionByName(vehicle_agnostic)


def _validate_no_reserved_vehicle_profile_id(hourly_df: DataFrame) -> None:
    """vehicle_profile_id=0은 vehicle-agnostic sentinel 전용이다. 실제 관측 데이터에 이
    값이 들어오면 vehicle-agnostic 행과 구분이 안 돼 조용히 섞여버리므로 여기서 막는다."""
    reserved_id_present = (
        hourly_df.filter(F.col("vehicle_profile_id") == VEHICLE_AGNOSTIC_VEHICLE_PROFILE_ID)
        .limit(1)
        .count()
        > 0
    )
    if reserved_id_present:
        raise ValueError(
            f"vehicle_profile_id={VEHICLE_AGNOSTIC_VEHICLE_PROFILE_ID} is reserved for the "
            "vehicle-agnostic sentinel row and must not appear in real hourly_comfort_score data"
        )


def _combined_hourly_score(config: ComfortScoreConfig) -> Column:
    """Step 1 - c_h = w_v*vertical + w_l*longitudinal + w_a*lateral."""
    return (
        F.lit(config.vertical_weight.value) * F.col("vertical_score")
        + F.lit(config.longitudinal_weight.value) * F.col("longitudinal_score")
        + F.lit(config.lateral_weight.value) * F.col("lateral_score")
    )


def _per_vehicle_scores(combined: DataFrame, config: ComfortScoreConfig) -> DataFrame:
    """`combined`는 `c_h` 컬럼이 이미 붙은 hourly_comfort_score DataFrame이다."""
    group_keys = ("segment_id", "vehicle_profile_id")

    qualifying = _qualifying_hours(combined, config)
    observed_full = _observed_with_universe(combined, qualifying, group_keys)
    population = qualifying.groupBy("vehicle_profile_id").agg(
        F.avg("c_h").alias("population_mean")
    )

    joined = observed_full.join(population, on="vehicle_profile_id", how="left")
    return _apply_shrinkage(joined, group_keys=group_keys, config=config)


def _vehicle_agnostic_scores(combined: DataFrame, config: ComfortScoreConfig) -> DataFrame:
    """`combined`는 `c_h` 컬럼이 이미 붙은 hourly_comfort_score DataFrame이다."""
    # Vehicle-agnostic Step 1 - c_h,s = sum_p(T_h,p * c_h,p) / sum_p(T_h,p): 같은 시간(segment,
    # data_period_start)에 여러 vehicle_profile이 있으면 트래픽으로 가중 평균해 하나로 합친다.
    pooled_hourly = combined.groupBy("segment_id", "data_period_start").agg(
        (F.sum(F.col("trip_count") * F.col("c_h")) / F.sum("trip_count")).alias("c_h"),
        F.sum("trip_count").alias("trip_count"),
        F.sum("sample_count").alias("sample_count"),
    )

    qualifying = _qualifying_hours(pooled_hourly, config)
    observed_full = _observed_with_universe(pooled_hourly, qualifying, ("segment_id",))
    # 전역 mu: 프로필 구분 없이 이 윈도우의 모든 qualifying 시간을 pool한 평균 하나뿐이므로
    # group 없이 crossJoin으로 모든 segment 행에 동일하게 붙인다.
    population_mean = qualifying.agg(F.avg("c_h").alias("population_mean"))

    joined = observed_full.crossJoin(population_mean)
    scored = _apply_shrinkage(joined, group_keys=("segment_id",), config=config)
    return scored.withColumn(
        "vehicle_profile_id", F.lit(VEHICLE_AGNOSTIC_VEHICLE_PROFILE_ID)
    )


def _qualifying_hours(candidate_hours: DataFrame, config: ComfortScoreConfig) -> DataFrame:
    """Step 2 - T_h(trip_count) >= T_min을 통과한 시간만 남긴다."""
    return candidate_hours.filter(F.col("trip_count") >= config.min_traffic_threshold.value)


def _observed_with_universe(
    candidate_hours: DataFrame, qualifying: DataFrame, group_keys: tuple[str, ...]
) -> DataFrame:
    """Step 3 - qualifying 시간을 평균 낸다.

    원본 윈도우에 한 번이라도 등장한 키 전체(`candidate_hours`)를 기준 집합으로 삼아,
    T_min을 넘긴 시간이 하나도 없어 groupBy에 안 잡히는 키도 N=0 행으로 남긴다. 그래야
    Step4 공식이 mu(_p)로 자연스럽게 대체한다("Handling a vehicle profile that never
    traversed a segment", comfort-score.md).
    """
    observed = qualifying.groupBy(*group_keys).agg(
        F.count(F.lit(1)).alias("qualifying_hours"),
        F.avg("c_h").alias("observed_score"),
        F.sum("sample_count").alias("sample_count"),
    )
    universe = candidate_hours.select(*group_keys).distinct()
    return universe.join(observed, on=list(group_keys), how="left").fillna(
        {"qualifying_hours": 0, "sample_count": 0}
    )


def _apply_shrinkage(
    joined: DataFrame, group_keys: tuple[str, ...], config: ComfortScoreConfig
) -> DataFrame:
    """Step 4~5 - ComfortScore = (N*c_obs + k*mu)/(N+k), Confidence = N/(N+k).

    `population_mean`(mu 또는 mu_p) 자체가 없는 행(이 윈도우 전체에서 그 그룹이 qualifying
    hour를 하나도 못 채운 경우)은 대체할 값이 없으므로 NULL 점수를 내보내는 대신 행을
    통째로 제외한다 — "지나간 적 없는 조합은 만들지 않는다"는 원칙과 동일하다.
    """
    scorable = joined.filter(F.col("population_mean").isNotNull())

    n = F.col("qualifying_hours")
    k = F.lit(config.shrinkage_k.value)
    observed = F.coalesce(F.col("observed_score"), F.lit(0.0))
    comfort_score = (n * observed + k * F.col("population_mean")) / (n + k)
    confidence_score = n / (n + k)

    return scorable.select(
        *group_keys,
        F.round(comfort_score, 6).alias("comfort_score"),
        F.round(confidence_score, 6).alias("confidence_score"),
        F.col("sample_count"),
        n.alias("qualifying_hours"),
        F.col("observed_score"),
        F.col("population_mean"),
        F.lit(SCORE_VERSION).alias("score_version"),
    )
