"""Compute Segment x vehicle-profile comfort score and confidence (#127, #198).

context/comfort-score.md에 정의된 Step 1~5(및 vehicle-agnostic 버전)를 그대로 구현한다.
입력은 comfort_score/loader.py가 이미 168시간 윈도우로 필터링하고 최신 scoring_version만
남겨 둔 hourly_comfort_score DataFrame이다.

Step 2~5는 방향(수직/종방향/횡방향)별로 따로 적용하고, 최종 comfort_score는 그 결과를
Step 1의 가중치로 합쳐서 만든다. Step 1~5가 전부 선형이라 "먼저 합치고 축소"와
"축소하고 합치기"의 결과가 같기 때문이다.

    c_h    = sum_d w_d * d_h                          (Step 1)
    c_obs  = avg_h(c_h) = sum_d w_d * avg_h(d_h)      (Step 3; H는 방향과 무관)
    mu     = avg(c_h)   = sum_d w_d * avg(d_h)        (Step 4)
    Score  = (N*c_obs + k*mu)/(N+k) = sum_d w_d * Score_d

즉 방향별 점수를 저장하려고 계산 구조를 바꿔도 기존 comfort_score 값은 달라지지
않는다. T_min 필터가 trip_count(방향과 무관한 값)에만 걸리므로 qualifying hour 집합 H도
세 방향이 공유한다.

진입점은 compute_standard_comfort_scores 하나다 — 호출자가 넘긴 universe의 모든
조합에 대해 행을 만든다. 관측된 조합만 산출하던 구 segment_comfort_score 경로는
#227에서 제거했다.
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

# Step 1의 가중치가 붙는 세 방향 점수. 순서는 출력 컬럼 순서와 무관하지만,
# 가중치 조회(_direction_weights)와 짝을 이루므로 함께 관리한다.
DIRECTION_COLUMNS = ("vertical_score", "longitudinal_score", "lateral_score")

REQUIRED_COLUMNS = (
    "segment_id",
    "vehicle_profile_id",
    "data_period_start",
    "data_period_end",
    *DIRECTION_COLUMNS,
    "trip_count",
    "sample_count",
)

# HOURLY_COMFORT_SCORE_SCHEMA에서 이 함수가 실제로 쓰는 컬럼만 추린 스키마. loader.py의
# _validate_schema를 그대로 재사용해 "필수 컬럼 누락" 검사 로직이 두 군데서 따로 관리되지
# 않게 하고, 덤으로 loader.py와 동일한 타입 검사도 얻는다.
REQUIRED_SCHEMA = StructType(
    [field for field in HOURLY_COMFORT_SCORE_SCHEMA.fields if field.name in REQUIRED_COLUMNS]
)

UNIVERSE_COLUMNS = ("segment_id", "vehicle_profile_id")


def compute_standard_comfort_scores(
    hourly_df: DataFrame,
    config: ComfortScoreConfig,
    universe_df: DataFrame,
) -> DataFrame:
    """standard_segment_comfort_score용 산출 (#198).

    `universe_df`는 (segment_id, vehicle_profile_id) 두 컬럼으로 이번 실행이 행을
    만들어야 할 실제 차량 프로필 조합 전체를 담는다(sentinel 0 제외 — vehicle-agnostic
    행은 그 안의 segment 목록으로 여기서 직접 만든다). 관측이 전혀 없는 조합도
    N=0, confidence=0으로 행이 나온다.

    mu_p가 정의되지 않는 프로필(윈도우 전체에서 qualifying hour가 없는 경우)은
    vehicle-agnostic 경로의 전역 mu로 대체한다.
    """
    _validate_universe(universe_df)
    return _compute(hourly_df, config, universe_df)


def _compute(
    hourly_df: DataFrame,
    config: ComfortScoreConfig,
    universe_df: DataFrame,
) -> DataFrame:
    _validate_schema(hourly_df.schema, REQUIRED_SCHEMA, source="hourly_comfort_score")
    _validate_no_reserved_vehicle_profile_id(hourly_df)

    # 시간별 원본은 per-vehicle 경로가 그대로 쓰고, vehicle-agnostic 경로는 프로필을
    # 트래픽 가중으로 접은 뒤 같은 Step 2~5를 탄다.
    pooled_hourly = _pool_vehicle_profiles(hourly_df)

    # 전역 mu는 vehicle-agnostic 경로의 모집단 평균이다. per-vehicle 경로의 mu_p
    # 대체값으로도 쓰이므로 한 번만 계산해 양쪽이 같은 값을 보게 한다.
    global_mu = _population_means(_qualifying_hours(pooled_hourly, config), group_keys=())

    per_vehicle = _per_vehicle_scores(hourly_df, config, universe_df, global_mu)
    vehicle_agnostic = _vehicle_agnostic_scores(pooled_hourly, config, universe_df, global_mu)
    return per_vehicle.unionByName(vehicle_agnostic)


def _validate_universe(universe_df: DataFrame) -> None:
    missing = [column for column in UNIVERSE_COLUMNS if column not in universe_df.columns]
    if missing:
        raise ValueError(f"universe: missing required column(s): {', '.join(missing)}")
    reserved_present = (
        universe_df.filter(
            F.col("vehicle_profile_id") == VEHICLE_AGNOSTIC_VEHICLE_PROFILE_ID
        )
        .limit(1)
        .count()
        > 0
    )
    if reserved_present:
        raise ValueError(
            f"universe must not contain vehicle_profile_id="
            f"{VEHICLE_AGNOSTIC_VEHICLE_PROFILE_ID}; the vehicle-agnostic row is "
            "generated from the universe's segment list instead"
        )


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


def _direction_weights(config: ComfortScoreConfig) -> dict[str, float]:
    return {
        "vertical_score": config.vertical_weight.value,
        "longitudinal_score": config.longitudinal_weight.value,
        "lateral_score": config.lateral_weight.value,
    }


def _combine_directions(config: ComfortScoreConfig, column_of: dict[str, Column]) -> Column:
    """Step 1 - sum_d w_d * d. `column_of`는 방향 컬럼명 -> 실제 Column 매핑이다."""
    weights = _direction_weights(config)
    combined = F.lit(0.0)
    for direction in DIRECTION_COLUMNS:
        combined = combined + F.lit(weights[direction]) * column_of[direction]
    return combined


def _pool_vehicle_profiles(hourly_df: DataFrame) -> DataFrame:
    """Vehicle-agnostic Step 1 - d_h,s = sum_p(T_h,p * d_h,p) / sum_p(T_h,p).

    같은 시간(segment, data_period_start)에 여러 vehicle_profile이 있으면 트래픽으로
    가중 평균해 하나로 합친다. 방향별로 각각 접는다 — 합친 뒤에 가중치를 적용하든
    가중치를 적용한 뒤에 합치든 결과는 같다(선형).
    """
    pooled_directions = [
        (F.sum(F.col("trip_count") * F.col(direction)) / F.sum("trip_count")).alias(direction)
        for direction in DIRECTION_COLUMNS
    ]
    return hourly_df.groupBy("segment_id", "data_period_start").agg(
        *pooled_directions,
        F.sum("trip_count").alias("trip_count"),
        F.sum("sample_count").alias("sample_count"),
        # 같은 (segment_id, data_period_start) 시간대의 프로필들은 같은 시간 윈도우를
        # 가리키므로 동일한 값이어야 하지만, groupBy 키로 삼는 대신 MAX로 골라 둔다.
        F.max("data_period_end").alias("data_period_end"),
    )


def _per_vehicle_scores(
    hourly_df: DataFrame,
    config: ComfortScoreConfig,
    universe_df: DataFrame | None,
    global_mu: DataFrame | None,
) -> DataFrame:
    group_keys = ("segment_id", "vehicle_profile_id")

    qualifying = _qualifying_hours(hourly_df, config)
    universe = (
        universe_df.select(*group_keys).distinct()
        if universe_df is not None
        else hourly_df.select(*group_keys).distinct()
    )
    observed_full = _observed_with_universe(qualifying, universe, group_keys)

    population = _population_means(qualifying, group_keys=("vehicle_profile_id",))
    if global_mu is not None:
        # mu_p가 없는 프로필은 전역 mu로 대체한다. universe의 프로필 전체를 기준으로
        # 왼쪽 조인해야 관측이 하나도 없는 프로필까지 값을 갖는다.
        population = _fill_population_means(
            universe.select("vehicle_profile_id").distinct(), population, global_mu
        )

    joined = observed_full.join(population, on="vehicle_profile_id", how="left")
    return _apply_shrinkage(joined, group_keys=group_keys, config=config)


def _vehicle_agnostic_scores(
    pooled_hourly: DataFrame,
    config: ComfortScoreConfig,
    universe_df: DataFrame | None,
    global_mu: DataFrame,
) -> DataFrame:
    group_keys = ("segment_id",)

    qualifying = _qualifying_hours(pooled_hourly, config)
    universe = (
        universe_df.select("segment_id").distinct()
        if universe_df is not None
        else pooled_hourly.select("segment_id").distinct()
    )
    observed_full = _observed_with_universe(qualifying, universe, group_keys)

    # 전역 mu는 그룹 없는 한 행짜리라 crossJoin으로 모든 segment 행에 동일하게 붙인다.
    joined = observed_full.crossJoin(global_mu)
    scored = _apply_shrinkage(joined, group_keys=group_keys, config=config)
    return scored.withColumn(
        "vehicle_profile_id", F.lit(VEHICLE_AGNOSTIC_VEHICLE_PROFILE_ID)
    )


def _qualifying_hours(candidate_hours: DataFrame, config: ComfortScoreConfig) -> DataFrame:
    """Step 2 - T_h(trip_count) >= T_min을 통과한 시간만 남긴다."""
    return candidate_hours.filter(F.col("trip_count") >= config.min_traffic_threshold.value)


def _population_means(qualifying: DataFrame, group_keys: tuple[str, ...]) -> DataFrame:
    """Step 4의 mu(_p) - 방향별로 이 윈도우의 모든 qualifying 시간을 pool한 평균.

    `group_keys`가 비어 있으면 전역 mu(한 행)를 만든다.
    """
    aggregations = [
        F.avg(direction).alias(_population_column(direction))
        for direction in DIRECTION_COLUMNS
    ]
    if group_keys:
        return qualifying.groupBy(*group_keys).agg(*aggregations)
    return qualifying.agg(*aggregations)


def _fill_population_means(
    profiles: DataFrame, population: DataFrame, global_mu: DataFrame
) -> DataFrame:
    """mu_p가 없는 프로필의 모집단 평균을 전역 mu로 채운다 (#198).

    전역 mu 자체가 NULL이면(윈도우 전체에 qualifying hour가 하나도 없는 경우) 여기서
    막지 않고 NULL을 그대로 흘려보낸다 — 그 판단은 실행 단위로 실패시켜야 하므로
    호출자(standard_job)의 책임이다.
    """
    global_columns = {
        _population_column(direction): F.col(f"_global_{_population_column(direction)}")
        for direction in DIRECTION_COLUMNS
    }
    renamed_global = global_mu.select(
        *[
            F.col(_population_column(direction)).alias(f"_global_{_population_column(direction)}")
            for direction in DIRECTION_COLUMNS
        ]
    )
    joined = profiles.join(population, on="vehicle_profile_id", how="left").crossJoin(
        renamed_global
    )
    return joined.select(
        "vehicle_profile_id",
        *[
            F.coalesce(F.col(_population_column(direction)), global_columns[
                _population_column(direction)
            ]).alias(_population_column(direction))
            for direction in DIRECTION_COLUMNS
        ],
    )


def _observed_with_universe(
    qualifying: DataFrame, universe: DataFrame, group_keys: tuple[str, ...]
) -> DataFrame:
    """Step 3 - qualifying 시간을 방향별로 평균 낸다.

    `universe`에 있는 키는 qualifying hour가 하나도 없어도 N=0 행으로 남긴다. 그래야
    Step 4 공식이 mu(_p)로 자연스럽게 대체한다("Handling a vehicle profile that never
    traversed a segment", comfort-score.md).
    """
    observed = qualifying.groupBy(*group_keys).agg(
        F.count(F.lit(1)).alias("qualifying_hours"),
        *[
            F.avg(direction).alias(_observed_column(direction))
            for direction in DIRECTION_COLUMNS
        ],
        F.sum("sample_count").alias("sample_count"),
        # 새로 계산하는 값이 아니라 입력이 이미 갖고 있는 시간 경계를 그대로 롤업한다.
        # qualifying hour가 하나도 없는 키는 여기서 NULL로 남고, 그 채움은 배치
        # 윈도우 경계를 아는 job의 책임이다(#163, #198).
        F.min("data_period_start").alias("data_period_start"),
        F.max("data_period_end").alias("data_period_end"),
    )
    return universe.join(observed, on=list(group_keys), how="left").fillna(
        {"qualifying_hours": 0, "sample_count": 0}
    )


def _apply_shrinkage(
    joined: DataFrame, group_keys: tuple[str, ...], config: ComfortScoreConfig
) -> DataFrame:
    """Step 4~5 - Score_d = (N*d_obs + k*mu_d)/(N+k), Confidence = N/(N+k).

    방향별로 축소한 뒤 Step 1의 가중치로 합쳐 comfort_score를 만든다. 반올림은 합친
    뒤 마지막에 한 번만 적용해서, 방향별 반올림 오차가 comfort_score에 누적되지 않게 한다.

    모집단 평균이 없는 행(이 윈도우 전체에서 그 그룹이 qualifying hour를 하나도 못 채운
    경우)은 대체할 값이 없으므로 NULL 점수를 내보내는 대신 행을 통째로 제외한다 —
    "지나간 적 없는 조합은 만들지 않는다"는 원칙과 동일하다. standard 경로는 전역 mu로
    미리 채워 두므로 여기서 걸리지 않는다.
    """
    first_population = _population_column(DIRECTION_COLUMNS[0])
    scorable = joined.filter(F.col(first_population).isNotNull())

    n = F.col("qualifying_hours")
    k = F.lit(config.shrinkage_k.value)

    shrunk = {
        direction: (
            n * F.coalesce(F.col(_observed_column(direction)), F.lit(0.0))
            + k * F.col(_population_column(direction))
        )
        / (n + k)
        for direction in DIRECTION_COLUMNS
    }
    comfort_score = _combine_directions(config, shrunk)
    confidence_score = n / (n + k)

    return scorable.select(
        *group_keys,
        F.col("data_period_start"),
        F.col("data_period_end"),
        *[
            F.round(shrunk[direction], 6).alias(direction)
            for direction in DIRECTION_COLUMNS
        ],
        F.round(comfort_score, 6).alias("comfort_score"),
        F.round(confidence_score, 6).alias("confidence_score"),
        F.col("sample_count"),
        n.alias("qualifying_hours"),
        # 진단용으로 남기는 결합 관측치/모집단 평균. 방향별 값의 가중합이라
        # 기존 경로가 직접 계산하던 값과 같다.
        F.round(
            _combine_directions(
                config,
                {
                    direction: F.coalesce(
                        F.col(_observed_column(direction)), F.lit(0.0)
                    )
                    for direction in DIRECTION_COLUMNS
                },
            ),
            6,
        ).alias("observed_score"),
        F.round(
            _combine_directions(
                config,
                {
                    direction: F.col(_population_column(direction))
                    for direction in DIRECTION_COLUMNS
                },
            ),
            6,
        ).alias("population_mean"),
        F.lit(SCORE_VERSION).alias("score_version"),
    )


def _observed_column(direction: str) -> str:
    return f"_observed_{direction}"


def _population_column(direction: str) -> str:
    return f"_population_{direction}"
