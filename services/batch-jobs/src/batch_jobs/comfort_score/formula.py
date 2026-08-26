"""Compute Segment x vehicle-profile comfort score and confidence (#127, #198, #566).

context/comfort-score.md에 정의된 Step 1~5(및 vehicle-agnostic 버전)를 그대로 구현한다.
입력은 comfort_score/loader.py가 이미 168시간 윈도우로 필터링하고 최신 scoring_version만
남겨 둔 hourly_comfort_score DataFrame이다.

Step 2~5는 방향(수직/종방향/횡방향)별로 따로 적용하고, 최종 comfort_score는 그 결과를
Step 1의 가중치로 합쳐서 만든다. Step 1~5가 전부 선형이라 "먼저 합치고 축소"와
"축소하고 합치기"의 결과가 같기 때문이다.

    c_h    = sum_d w_d * d_h                                    (Step 1)
    e_h    = min(1, trip_count_h / evidence_saturation_trip_count)  (Step 2, #566)
    c_obs  = sum_h(e_h*c_h)/sum_h(e_h) = sum_d w_d * (weighted avg_h d_h)  (Step 3)
    mu     = sum_h(e_h*c_h)/sum_h(e_h) (pooled)                  = sum_d w_d * mu_d  (Step 4)
    Score  = (N_eff*c_obs + k*mu)/(N_eff+k) = sum_d w_d * Score_d

즉 방향별 점수를 저장하려고 계산 구조를 바꿔도 기존 comfort_score 값은 달라지지
않는다. e_h가 trip_count(방향과 무관한 값)에만 걸리므로 세 방향이 같은 e_h를 공유한다.

hard cutoff(trip_count >= T_min)는 #566에서 제거했다 — 1~4대만 지나간 시간도
evidence_saturation_trip_count에 비례한 부분 evidence로 인정하고, 그 이상은 1시간
분량으로 포화시킨다. trip_count=0인 시간만 evidence=0으로 제외된다.

진입점은 compute_standard_comfort_scores 하나다 — 호출자가 넘긴 universe의 모든
조합에 대해 행을 만든다. 관측된 조합만 산출하던 구 segment_comfort_score 경로는
#227에서 제거했다.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from batch_jobs.comfort_score.config import ComfortScoreConfig
from batch_jobs.comfort_score.loader import _validate_schema
from batch_jobs.schemas import HOURLY_COMFORT_SCORE_SCHEMA

# 이 계산 로직의 형태(shape) 버전. 공식 구조가 바뀌면 올린다 (comfort-score.md 참고).
# 2.0.0: hard traffic cutoff를 evidence weight로 대체 — N/confidence의 정의가 바뀐다(#566).
SCORE_VERSION = "2.0.0"

# Step 2 evidence weight 컬럼 이름. groupBy 집계 여러 곳에서 이 값을 재사용한다.
_EVIDENCE_WEIGHT_COLUMN = "_evidence_weight"

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


@dataclass(frozen=True, slots=True)
class Universe:
    """이번 실행이 행을 만들어야 할 (segment, vehicle profile) 조합.

    두 축을 합친 cross join 결과 하나로 들고 다니면, 소비처마다 필요한 축만 뽑으려고
    그 큰 프레임에 distinct를 걸게 된다. 운영 기준 831,110행(166,222 세그먼트 x 5
    프로필)에 셔플이 세 번 붙었다. 축을 따로 들고 있으면 그 셔플이 전부 없어진다.

    - `segments`: `segment_id` 한 컬럼. 도로망 artifact에서 이미 중복이 제거돼 온다.
    - `profile_ids`: PostgreSQL `vehicle_profile`의 실제 프로필 ID(sentinel 0 제외).
      수가 적어 파이썬 값으로 들고 있는다 — 덕분에 sentinel 검사도 Spark Action 없이
      끝난다.
    """

    segments: DataFrame
    profile_ids: tuple[int, ...]


def compute_standard_comfort_scores(
    hourly_df: DataFrame,
    config: ComfortScoreConfig,
    universe: Universe,
) -> DataFrame:
    """standard_segment_comfort_score용 산출 (#198).

    `universe`는 이번 실행이 행을 만들어야 할 세그먼트와 실제 차량 프로필을 담는다
    (sentinel 0 제외 — vehicle-agnostic 행은 그 segment 목록으로 여기서 직접 만든다).
    관측이 전혀 없는 조합도 N=0, confidence=0으로 행이 나온다.

    mu_p가 정의되지 않는 프로필(윈도우 전체에서 evidence가 없는 경우, 즉 모든 시간이
    trip_count=0인 경우)은 vehicle-agnostic 경로의 전역 mu로 대체한다.
    """
    _validate_universe(universe)
    return _compute(hourly_df, config, universe)


def _compute(
    hourly_df: DataFrame,
    config: ComfortScoreConfig,
    universe: Universe,
) -> DataFrame:
    _validate_schema(hourly_df.schema, REQUIRED_SCHEMA, source="hourly_comfort_score")
    _validate_no_reserved_vehicle_profile_id(hourly_df)

    # 시간별 원본은 per-vehicle 경로가 그대로 쓰고, vehicle-agnostic 경로는 프로필을
    # 트래픽 가중으로 접은 뒤 같은 Step 2~5를 탄다. 전역 mu와 vehicle-agnostic 경로가
    # 똑같이 evidence weight를 매기므로 그 결과를 한 번만 만들어 둘이 나눠 쓴다.
    #
    # 이 프레임을 persist하는 것도 시도했지만 실측 결과 손해였다 — 캐시가 AQE의
    # post-shuffle coalescing을 끊어서, 입력 756,000행 기준 실행 task가 188 -> 807로
    # 늘었다. Spark가 셔플을 재사용해 주지도 않는다(계획에 ReusedExchange 없음).
    pooled_weighted = _attach_evidence_weight(_pool_vehicle_profiles(hourly_df), config)

    # 전역 mu는 vehicle-agnostic 경로의 모집단 평균이다. per-vehicle 경로의 mu_p
    # 대체값으로도 쓰이므로 한 번만 계산해 양쪽이 같은 값을 보게 한다.
    global_mu = _collect_global_population_means(pooled_weighted)

    per_vehicle = _per_vehicle_scores(hourly_df, config, universe, global_mu)
    vehicle_agnostic = _vehicle_agnostic_scores(
        pooled_weighted, config, universe, global_mu
    )
    return per_vehicle.unionByName(vehicle_agnostic)


def _collect_global_population_means(
    pooled_weighted: DataFrame,
) -> dict[str, Column]:
    """전역 mu를 드라이버로 한 번 걷어 방향별 리터럴로 만든다.

    한 행짜리 DataFrame으로 들고 다니면 그 한 행을 만들기 위해 168시간 lineage(윈도우
    필터 -> scoring_version 윈도우 -> pooling)가 소비처마다 계획에 다시 붙는다. 전역 mu는
    per-vehicle 경로와 vehicle-agnostic 경로 양쪽이 쓰므로, 실측한 physical plan에서
    그 서브트리가 두 벌 더 생기고 crossJoin(BroadcastNestedLoopJoin)도 두 개 붙었다.

    값 자체는 한 행 세 컬럼뿐이라 드라이버로 걷어도 안전하다. 리터럴로 바꾸면 두 소비처가
    같은 상수를 보고, crossJoin과 중복 서브트리가 함께 사라진다.

    윈도우 전체에 evidence가 하나도 없으면(모든 시간이 trip_count=0) 평균이 NULL이다.
    여기서 막지 않고 NULL 리터럴을 그대로 흘려보낸다 — 실행을 실패시킬지는
    호출자(standard_job)의 책임이다.
    """
    row = _population_means(pooled_weighted, group_keys=()).first()
    return {
        _population_column(direction): F.lit(
            None if row is None else row[_population_column(direction)]
        ).cast("double")
        for direction in DIRECTION_COLUMNS
    }


def _validate_universe(universe: Universe) -> None:
    """프로필 목록이 파이썬 값이라 sentinel 검사에 Spark Action이 필요 없다."""
    if "segment_id" not in universe.segments.columns:
        raise ValueError("universe: missing required column(s): segment_id")
    if not universe.profile_ids:
        raise ValueError("universe: no vehicle_profile_id was resolved")
    if VEHICLE_AGNOSTIC_VEHICLE_PROFILE_ID in universe.profile_ids:
        raise ValueError(
            f"universe must not contain vehicle_profile_id="
            f"{VEHICLE_AGNOSTIC_VEHICLE_PROFILE_ID}; the vehicle-agnostic row is "
            "generated from the universe's segment list instead"
        )


def _universe_pairs(universe: Universe) -> DataFrame:
    """(segment x profile) 조합을 만든다.

    crossJoin 대신 explode를 쓴다 — 프로필 목록이 파이썬 값이라 리터럴 배열로 펼치면
    되고, explode는 좁은(narrow) 연산이라 셔플이 없다.
    """
    return universe.segments.withColumn(
        "vehicle_profile_id",
        F.explode(F.array(*[F.lit(profile_id) for profile_id in universe.profile_ids])),
    )


def _profiles_frame(universe: Universe) -> DataFrame:
    """프로필 목록만 담은 작은 프레임. 831,110행에 distinct를 걸던 자리를 대신한다."""
    return universe.segments.sparkSession.createDataFrame(
        [(profile_id,) for profile_id in universe.profile_ids],
        "vehicle_profile_id int",
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
    universe: Universe,
    global_mu: dict[str, Column] | None,
) -> DataFrame:
    group_keys = ("segment_id", "vehicle_profile_id")

    weighted = _attach_evidence_weight(hourly_df, config)
    # cross join 결과는 이미 중복이 없으므로 distinct를 걸지 않는다.
    observed_full = _observed_with_universe(
        weighted, _universe_pairs(universe), group_keys
    )

    population = _population_means(weighted, group_keys=("vehicle_profile_id",))
    if global_mu is not None:
        # mu_p가 없는 프로필은 전역 mu로 대체한다. universe의 프로필 전체를 기준으로
        # 왼쪽 조인해야 관측이 하나도 없는 프로필까지 값을 갖는다.
        population = _fill_population_means(
            _profiles_frame(universe), population, global_mu
        )

    joined = observed_full.join(population, on="vehicle_profile_id", how="left")
    return _apply_shrinkage(joined, group_keys=group_keys, config=config)


def _vehicle_agnostic_scores(
    pooled_weighted: DataFrame,
    config: ComfortScoreConfig,
    universe: Universe,
    global_mu: dict[str, Column],
) -> DataFrame:
    group_keys = ("segment_id",)

    # evidence weight는 _compute가 이미 붙였다. segment 목록도 이미 중복이 없다.
    observed_full = _observed_with_universe(
        pooled_weighted, universe.segments, group_keys
    )

    # 전역 mu는 이미 드라이버에서 걷어 온 리터럴이라 조인 없이 컬럼으로 붙인다.
    joined = observed_full.withColumns(global_mu)
    scored = _apply_shrinkage(joined, group_keys=group_keys, config=config)
    return scored.withColumn(
        "vehicle_profile_id", F.lit(VEHICLE_AGNOSTIC_VEHICLE_PROFILE_ID)
    )


def _attach_evidence_weight(candidate_hours: DataFrame, config: ComfortScoreConfig) -> DataFrame:
    """Step 2 - hard cutoff 대신 e_h = min(1, trip_count_h / evidence_saturation_trip_count)를 매긴다(#566).

    trip_count=0인 시간은 여기서 걸러낸다 — e_h가 정확히 0이라 이후 가중합에도 어차피
    기여하지 않지만, 미리 빼야 data_period_start/end 롤업과 evidence_hours 합계가
    "evidence 없는 시간"까지 세는 걸 막는다.
    """
    saturation = F.lit(config.evidence_saturation_trip_count.value)
    return candidate_hours.filter(F.col("trip_count") > 0).withColumn(
        _EVIDENCE_WEIGHT_COLUMN, F.least(F.lit(1.0), F.col("trip_count") / saturation)
    )


def _population_means(weighted_hours: DataFrame, group_keys: tuple[str, ...]) -> DataFrame:
    """Step 4의 mu(_p) - evidence-weighted 평균 sum(e_h*d_h)/sum(e_h)(#566).

    `group_keys`가 비어 있으면 전역 mu(한 행)를 만든다.
    """
    weight = F.col(_EVIDENCE_WEIGHT_COLUMN)
    aggregations = [
        (F.sum(weight * F.col(direction)) / F.sum(weight)).alias(_population_column(direction))
        for direction in DIRECTION_COLUMNS
    ]
    if group_keys:
        return weighted_hours.groupBy(*group_keys).agg(*aggregations)
    return weighted_hours.agg(*aggregations)


def _fill_population_means(
    profiles: DataFrame, population: DataFrame, global_mu: dict[str, Column]
) -> DataFrame:
    """mu_p가 없는 프로필의 모집단 평균을 전역 mu로 채운다 (#198).

    전역 mu는 이미 드라이버에서 걷어 온 리터럴이라 crossJoin과 컬럼 rename 없이 그대로
    coalesce에 넣는다.

    전역 mu 자체가 NULL이면(윈도우 전체에 evidence가 하나도 없는 경우) 여기서
    막지 않고 NULL을 그대로 흘려보낸다 — 그 판단은 실행 단위로 실패시켜야 하므로
    호출자(standard_job)의 책임이다.
    """
    joined = profiles.join(population, on="vehicle_profile_id", how="left")
    return joined.select(
        "vehicle_profile_id",
        *[
            F.coalesce(
                F.col(_population_column(direction)),
                global_mu[_population_column(direction)],
            ).alias(_population_column(direction))
            for direction in DIRECTION_COLUMNS
        ],
    )


def _observed_with_universe(
    weighted_hours: DataFrame, universe: DataFrame, group_keys: tuple[str, ...]
) -> DataFrame:
    """Step 3 - evidence-weighted 평균 sum(e_h*d_h)/sum(e_h)를 낸다(#566).

    `universe`에 있는 키는 evidence가 하나도 없어도 N_eff=0 행으로 남긴다. 그래야
    Step 4 공식이 mu(_p)로 자연스럽게 대체한다("Handling a vehicle profile that never
    traversed a segment", comfort-score.md).
    """
    weight = F.col(_EVIDENCE_WEIGHT_COLUMN)
    observed = weighted_hours.groupBy(*group_keys).agg(
        F.sum(weight).alias("evidence_hours"),
        *[
            (F.sum(weight * F.col(direction)) / F.sum(weight)).alias(_observed_column(direction))
            for direction in DIRECTION_COLUMNS
        ],
        F.sum("sample_count").alias("sample_count"),
        # 새로 계산하는 값이 아니라 입력이 이미 갖고 있는 시간 경계를 그대로 롤업한다.
        # evidence가 하나도 없는 키는 여기서 NULL로 남고, 그 채움은 배치
        # 윈도우 경계를 아는 job의 책임이다(#163, #198).
        F.min("data_period_start").alias("data_period_start"),
        F.max("data_period_end").alias("data_period_end"),
    )
    return universe.join(observed, on=list(group_keys), how="left").fillna(
        {"evidence_hours": 0.0, "sample_count": 0}
    )


def _apply_shrinkage(
    joined: DataFrame, group_keys: tuple[str, ...], config: ComfortScoreConfig
) -> DataFrame:
    """Step 4~5 - Score_d = (N_eff*d_obs + k*mu_d)/(N_eff+k), Confidence = N_eff/(N_eff+k)(#566).

    방향별로 축소한 뒤 Step 1의 가중치로 합쳐 comfort_score를 만든다. 반올림은 합친
    뒤 마지막에 한 번만 적용해서, 방향별 반올림 오차가 comfort_score에 누적되지 않게 한다.

    모집단 평균이 없는 행(이 윈도우 전체에서 그 그룹이 evidence를 하나도 못 채운
    경우)은 대체할 값이 없으므로 NULL 점수를 내보내는 대신 행을 통째로 제외한다 —
    "지나간 적 없는 조합은 만들지 않는다"는 원칙과 동일하다. standard 경로는 전역 mu로
    미리 채워 두므로 여기서 걸리지 않는다.
    """
    first_population = _population_column(DIRECTION_COLUMNS[0])
    scorable = joined.filter(F.col(first_population).isNotNull())

    n = F.col("evidence_hours")
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
        n.alias("evidence_hours"),
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
