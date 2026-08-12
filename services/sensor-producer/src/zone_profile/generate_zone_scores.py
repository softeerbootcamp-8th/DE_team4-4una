"""
Generate TLC Zone Scores & Tags

Input
-----
data/processed/zone_profile_features.parquet
data/reference/tlc_zone/zone_master.parquet

Output
------
data/processed/zone_scores.parquet

생성 항목
---------
- 8개 category score
- zone_tag
- zone_tag_ko
- comfort_relevance_score

※ category score는 NYC Zone 사이의 상대적인 특성을 나타낸다.
※ 실제 도로 승차감 comfort_score와는 다른 개념이다.
"""

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

# ============================================================
# Path
# ============================================================

DATA_DIR = Path("data")

FEATURE_PATH = DATA_DIR / "processed/zone_profile_features.parquet"
ZONE_MASTER_PATH = DATA_DIR / "reference/tlc_zone/zone_master.parquet"
RAW_DIR = DATA_DIR / "raw/zone_score"
OUTPUT_PATH = DATA_DIR / "processed/zone_scores.parquet"


# ============================================================
# NYS DOH
# ============================================================

NYC_COUNTIES = {"Bronx", "Kings", "New York", "Queens", "Richmond"}

WGS84 = "EPSG:4326"


# ============================================================
# Score 설정
# ============================================================

"""
아래 weight는 통계적으로 산출한 값이 아니라 팀의 판단이다. business_score
등은 우리가 새로 정의하는 지표라 비교할 정답(label)이 없어서, 회귀분석이나
PCA로 "최적 weight"를 학습할 방법이 없다. 각 weight는 "이 feature가 해당
category 성격을 얼마나 잘 대표하는지"에 대한 도메인 판단이며, 필요하면
팀 논의로 조정한다. (category별 weight 합은 1.0이어야 한다 — 아래 assert로
검증)
"""

# 전체 weight 중 실제 사용 가능한 feature의 비중이 이보다 낮으면 score를
# 신뢰할 수 없다고 보고 NaN 처리한다. Feature 2~3개만으로 계산된 점수가
# 우연히 극단값이 나오는 걸 막기 위함.
MIN_FEATURE_COVERAGE = 0.7

CATEGORY_WEIGHTS = {
    "business_score": {
        "office_area_ratio": 0.20,
        "job_density": 0.25,
        "finance_job_ratio": 0.20,
        "professional_job_ratio": 0.15,
        "information_job_ratio": 0.10,
        "real_estate_job_ratio": 0.10,
    },
    "residential_score": {
        "residential_area_ratio": 0.55,
        "residential_unit_density": 0.45,
    },
    "shopping_score": {
        "retail_area_ratio": 0.35,
        "retail_job_ratio": 0.25,
        "poi_shop_density": 0.40,
    },
    "nightlife_score": {
        "poi_restaurant_density": 0.35,
        "poi_nightlife_density": 0.40,
        "accommodation_food_job_ratio": 0.25,
    },
    "tourism_score": {
        "poi_hotel_density": 0.25,
        "poi_museum_density": 0.20,
        "poi_attraction_density": 0.30,
        "arts_recreation_job_ratio": 0.25,
    },
    "transit_score": {
        "subway_station_count": 0.55,
        "subway_complex_count": 0.45,
    },
    "public_service_score": {
        "healthcare_job_ratio": 0.20,
        "education_job_ratio": 0.10,
        "public_admin_job_ratio": 0.10,
        "facility_medical_count": 0.20,
        "facility_education_count": 0.10,
        "facility_government_count": 0.10,
        "doh_hospital_bed_count": 0.20,
    },
    "park_score": {
        "park_area_ratio": 0.70,
        "park_area_km2": 0.20,
        "arts_recreation_job_ratio": 0.10,
    },
}

COMFORT_WEIGHTS = {
    "median_household_income": 0.25,
    "median_home_value": 0.20,
    "family_household_ratio": 0.10,
    "children_household_ratio": 0.10,
    "senior_ratio": 0.15,
    "doh_hospital_bed_count": 0.15,
    "facility_medical_count": 0.05,
}

for _score_name, _weights in CATEGORY_WEIGHTS.items():
    assert abs(sum(_weights.values()) - 1.0) < 1e-9, (
        f"{_score_name}의 weight 합이 1.0이 아닙니다: {sum(_weights.values())}"
    )

assert abs(sum(COMFORT_WEIGHTS.values()) - 1.0) < 1e-9, (
    f"COMFORT_WEIGHTS의 weight 합이 1.0이 아닙니다: {sum(COMFORT_WEIGHTS.values())}"
)


# ============================================================
# Common
# ============================================================

def load_data():
    features = pd.read_parquet(FEATURE_PATH)
    zones = gpd.read_parquet(ZONE_MASTER_PATH)

    features["location_id"] = features["location_id"].astype("int64")
    zones["location_id"] = zones["location_id"].astype("int64")

    return features, zones


def percentile_normalize(series: pd.Series) -> pd.Series:
    """
    NYC TLC Zone 내 상대적 percentile을 0~1로 변환한다.
    0 값은 0으로 유지하고, 양수 값만 percentile ranking한다.
    """

    values = pd.to_numeric(series, errors="coerce")
    result = pd.Series(np.nan, index=values.index, dtype="float64")

    zero_mask = values == 0
    positive_mask = values > 0

    result.loc[zero_mask] = 0.0

    if positive_mask.any():
        result.loc[positive_mask] = values.loc[positive_mask].rank(
            method="average", pct=True,
        )

    return result


def weighted_score(df: pd.DataFrame, weights: dict[str, float]) -> pd.Series:
    """
    정규화 Feature들의 가중 평균을 계산한다.

    결측 Feature가 있으면 해당 Feature의 weight를 제외하고 나머지 weight로
    다시 정규화한다. 단, 실제 사용된 weight 비중(coverage)이
    MIN_FEATURE_COVERAGE보다 낮으면, 소수의 feature만으로 만든 점수가
    과대/과소평가될 수 있으므로 NaN으로 처리한다.
    """

    total_weight = sum(weights.values())

    numerator = pd.Series(0.0, index=df.index)
    denominator = pd.Series(0.0, index=df.index)

    for feature, weight in weights.items():
        norm_col = f"{feature}_norm"

        if norm_col not in df.columns:
            print(f"[WARNING] {feature} 컬럼이 없어 score에서 제외")
            continue

        valid = df[norm_col].notna()
        numerator.loc[valid] += df.loc[valid, norm_col] * weight
        denominator.loc[valid] += weight

    score = numerator / denominator.replace(0, np.nan)

    coverage = denominator / total_weight
    score.loc[coverage < MIN_FEATURE_COVERAGE] = np.nan

    return score


# ============================================================
# NYS DOH
# ============================================================

def extract_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    """NYS DOH dataset 버전에 따라 좌표 필드명이 달라도 최대한 자동으로 탐색한다."""

    df = df.copy()

    # 일반적인 latitude / longitude
    if {"latitude", "longitude"}.issubset(df.columns):
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        return df

    # Socrata location object
    possible_location_cols = ["location", "location_1", "geocoded_column"]

    for col in possible_location_cols:
        if col not in df.columns:
            continue

        def get_value(value, key):
            return value.get(key) if isinstance(value, dict) else None

        df["latitude"] = df[col].apply(lambda x: get_value(x, "latitude"))
        df["longitude"] = df[col].apply(lambda x: get_value(x, "longitude"))

        if df["latitude"].notna().any() and df["longitude"].notna().any():
            df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
            df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
            return df

    raise ValueError(
        "NYS DOH 데이터에서 좌표 컬럼을 찾지 못했습니다.\n"
        f"columns={df.columns.tolist()}"
    )


def build_doh_features(zones: gpd.GeoDataFrame) -> pd.DataFrame:
    print("\n=== NYS DOH ===")

    # download_doh_raw.py로 미리 받아둔 raw 파일을 읽는다.
    facilities = pd.read_json(RAW_DIR / "doh_facilities.json")
    certification = pd.read_json(RAW_DIR / "doh_certification.json")

    # NYC만
    facilities = facilities[facilities["county"].isin(NYC_COUNTIES)].copy()
    certification = certification[
        certification["county"].isin(NYC_COUNTIES)
    ].copy()

    # Hospital
    facility_text = (
        facilities["description"].fillna("").astype(str).str.strip().str.lower()
    )

    # description은 "Hospital", "Hospital Extension Clinic",
    # "School Based Hospital Extension Clinic", "Mobile Hospital Extension
    # Clinic" 4종류뿐이다. Extension Clinic은 병원이 아니라 위성 소규모
    # 클리닉이므로 정확히 "Hospital"인 것만 남긴다.
    facilities = facilities[facility_text.eq("hospital")].copy()

    # Bed 정보만
    certification["measure_value"] = pd.to_numeric(
        certification["measure_value"], errors="coerce",
    )

    bed_rows = certification[
        certification["attribute_type"].fillna("").str.lower().eq("bed")
    ].copy()

    # permanent가 존재하면 permanent만 사용
    if "sub_type" in bed_rows.columns:
        permanent = bed_rows[
            bed_rows["sub_type"].fillna("").str.lower().eq("permanent")
        ]
        if not permanent.empty:
            bed_rows = permanent

    beds = (
        bed_rows.groupby("fac_id")["measure_value"]
        .sum()
        .rename("hospital_bed_count")
        .reset_index()
    )

    facilities["fac_id"] = facilities["fac_id"].astype(str)
    beds["fac_id"] = beds["fac_id"].astype(str)

    facilities = facilities.merge(beds, on="fac_id", how="left")
    facilities["hospital_bed_count"] = facilities["hospital_bed_count"].fillna(0)

    # 좌표
    facilities = extract_coordinates(facilities)
    facilities = facilities.dropna(subset=["latitude", "longitude"])

    hospital_points = gpd.GeoDataFrame(
        facilities,
        geometry=gpd.points_from_xy(facilities["longitude"], facilities["latitude"]),
        crs=WGS84,
    )

    spatial_zones = zones[zones.geometry.notna()].to_crs(WGS84)

    joined = gpd.sjoin(
        hospital_points,
        spatial_zones[["location_id", "geometry"]],
        how="inner",
        predicate="within",
    )

    return (
        joined.groupby("location_id")
        .agg(
            doh_hospital_count=("fac_id", "nunique"),
            doh_hospital_bed_count=("hospital_bed_count", "sum"),
        )
        .reset_index()
    )


# ============================================================
# Normalize Features
# ============================================================

def add_normalized_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    used_features = set()

    for weights in CATEGORY_WEIGHTS.values():
        used_features.update(weights.keys())

    used_features.update(COMFORT_WEIGHTS.keys())

    for feature in sorted(used_features):
        if feature not in df.columns:
            print(f"[WARNING] {feature} 없음")
            continue

        df[f"{feature}_norm"] = percentile_normalize(df[feature])

    return df


# ============================================================
# Category Scores
# ============================================================

def generate_category_scores(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for score_name, weights in CATEGORY_WEIGHTS.items():
        df[score_name] = weighted_score(df, weights)

    return df


# ============================================================
# Comfort Relevance
# ============================================================

def generate_comfort_relevance(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["comfort_relevance_score"] = weighted_score(df, COMFORT_WEIGHTS)
    return df


# ============================================================
# Zone Tag
# ============================================================

CATEGORY_LABELS = {
    "business_score": ("business", "업무·비즈니스"),
    "residential_score": ("residential", "주거"),
    "shopping_score": ("shopping", "쇼핑"),
    "nightlife_score": ("dining_nightlife", "외식·야간"),
    "tourism_score": ("tourism_culture", "관광·문화"),
    "transit_score": ("transit", "교통·환승"),
    "public_service_score": ("public_service", "행정·의료·교육"),
    "park_score": ("park_leisure", "공원·레저"),
}


def get_top_categories(row: pd.Series, top_n: int = 2) -> set[str]:
    """
    8개 category score 중 상위 top_n개의 컬럼명을 반환한다.

    조합 규칙이 임계값만 넘으면 바로 확정되는 걸 막기 위해, 해당 category가
    그 Zone에서 실제로 두드러진 특성인지 같이 확인하는 용도로 쓴다.
    """

    values = {col: row.get(col, np.nan) for col in CATEGORY_LABELS}

    ranked = sorted(
        ((value, col) for col, value in values.items() if pd.notna(value)),
        reverse=True,
    )

    return {col for _, col in ranked[:top_n]}


# ============================================================
# Tag 규칙
# ============================================================

"""
category score(business_score 등)는 이미 percentile 평균이라 zone마다
분포 모양이 달라서, 고정된 절대값(예: 0.65)이 실제로는 zone별로 전혀 다른
상위 %를 의미한다. 그래서 score 관련 조건은 "상위 quantile" 기준으로
설정하고, 실제 임계값은 compute_score_quantiles에서 zone 전체 분포로부터
계산한다.

median_income_norm 같은 `_norm` 컬럼은 percentile_normalize를 거쳐 이미
0~1 percentile이라, norm_min에 쓰는 값은 그 자체로 "상위 %"다.
"""

TAG_RULES = {
    "luxury_residential": {
        "label": ("luxury_residential", "고급주거"),
        "score_quantile": {"residential_score": 0.60},  # 주거 상위 40%
        "norm_min": {
            "median_household_income_norm": 0.75,  # 소득 상위 25%
            "median_home_value_norm": 0.75,  # 주택가치 상위 25%
        },
    },
    "finance_business": {
        "label": ("finance_business", "금융·업무"),
        "score_quantile": {"business_score": 0.75},  # 업무 상위 25%
        "norm_min": {"finance_job_ratio_norm": 0.75},  # 금융고용비중 상위 25%
        "top_k_categories": ["business_score"],
    },
    "residential_medical": {
        "label": ("residential_medical", "주거·의료"),
        "score_quantile": {
            "residential_score": 0.60,  # 주거 상위 40%
            "public_service_score": 0.75,  # 의료 상위 25%
        },
        "top_k_categories": ["residential_score", "public_service_score"],
    },
    "education_residential": {
        "label": ("education_residential", "교육·주거"),
        "score_quantile": {"residential_score": 0.60},  # 주거 상위 40%
        "norm_any_min": {
            "columns": [
                "education_job_ratio_norm",
                "facility_education_count_norm",
            ],
            "min": 0.70,  # 둘 중 하나라도 상위 30%
        },
        "top_k_categories": ["residential_score"],
    },
    "shopping_tourism": {
        "label": ("shopping_tourism", "쇼핑·관광"),
        "score_quantile": {
            "shopping_score": 0.70,  # 쇼핑 상위 30%
            "tourism_score": 0.75,  # 관광 상위 25%
        },
        "top_k_categories": ["shopping_score", "tourism_score"],
    },
    "transit_business": {
        "label": ("transit_business", "교통·업무"),
        "score_quantile": {
            "transit_score": 0.75,  # 교통 상위 25%
            "business_score": 0.60,  # 업무 상위 40%
        },
        "top_k_categories": ["transit_score", "business_score"],
    },
    "dining_nightlife": {
        "label": ("dining_nightlife", "외식·야간"),
        "score_quantile": {"nightlife_score": 0.75},  # 외식·야간 상위 25%
        "top_k_categories": ["nightlife_score"],
    },
}


def compute_score_quantiles(
    df: pd.DataFrame, rules: dict,
) -> dict[tuple[str, float], float]:
    """TAG_RULES가 참조하는 (score 컬럼, quantile)의 실제 임계값을 전체 Zone 분포에서 계산한다."""

    thresholds: dict[tuple[str, float], float] = {}

    for rule in rules.values():
        for col, quantile in rule.get("score_quantile", {}).items():
            key = (col, quantile)

            if key in thresholds:
                continue

            thresholds[key] = df[col].quantile(quantile)

    return thresholds


def _get_numeric(row: pd.Series, col: str, default: float = 0.0) -> float:
    value = row.get(col, default)
    return default if pd.isna(value) else value


def _passes_tag_rule(
    row: pd.Series,
    rule: dict,
    quantiles: dict[tuple[str, float], float],
    top_categories: set[str],
) -> bool:

    for col, quantile in rule.get("score_quantile", {}).items():
        threshold = quantiles[(col, quantile)]
        if _get_numeric(row, col, -np.inf) < threshold:
            return False

    for col, minimum in rule.get("norm_min", {}).items():
        if _get_numeric(row, col) < minimum:
            return False

    any_of = rule.get("norm_any_min")

    if any_of:
        best = max(_get_numeric(row, col) for col in any_of["columns"])
        if best < any_of["min"]:
            return False

    top_k_categories = rule.get("top_k_categories")

    if not top_k_categories:
        return True

    return any(col in top_categories for col in top_k_categories)


def _rule_strength(row: pd.Series, rule: dict) -> float:
    """
    규칙이 근거로 삼는 score/norm 컬럼 중 최댓값.

    여러 규칙이 동시에 조건을 통과했을 때, 등장 순서가 아니라 실제로 더
    두드러진 특성을 가진 규칙을 고르는 기준으로 쓴다. luxury_residential처럼
    category score(residential_score)보다 norm 신호(소득/주택가치)가 더
    극단적인 규칙도 정당하게 이길 수 있도록 score_quantile과 norm_min
    컬럼을 모두 포함한다.
    """

    columns = [
        *rule.get("score_quantile", {}).keys(),
        *rule.get("norm_min", {}).keys(),
    ]

    return max(
        (_get_numeric(row, col, -np.inf) for col in columns), default=-np.inf,
    )


def generate_tag(
    row: pd.Series, quantiles: dict[tuple[str, float], float],
) -> tuple[str, str]:

    top_categories = get_top_categories(row)

    passing_rules = [
        rule
        for rule in TAG_RULES.values()
        if _passes_tag_rule(row, rule, quantiles, top_categories)
    ]

    if passing_rules:
        best_rule = max(passing_rules, key=lambda rule: _rule_strength(row, rule))
        return best_rule["label"]

    # 어떤 규칙에도 안 걸리면 가장 높은 category로 fallback
    values = {col: row.get(col, np.nan) for col in CATEGORY_LABELS}
    valid = {key: value for key, value in values.items() if pd.notna(value)}

    if not valid:
        return ("unknown", "미분류")

    top_score = max(valid, key=valid.get)
    return CATEGORY_LABELS[top_score]


def generate_zone_tags(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    quantiles = compute_score_quantiles(df, TAG_RULES)
    tags = df.apply(lambda row: generate_tag(row, quantiles), axis=1)

    df["zone_tag"] = [value[0] for value in tags]
    df["zone_tag_ko"] = [value[1] for value in tags]

    return df


# ============================================================
# Validation
# ============================================================

SCORE_COLUMNS = [
    "business_score",
    "residential_score",
    "shopping_score",
    "nightlife_score",
    "tourism_score",
    "transit_score",
    "public_service_score",
    "park_score",
    "comfort_relevance_score",
]


def validate(df: pd.DataFrame) -> None:
    if df["location_id"].duplicated().any():
        raise ValueError("location_id 중복 발생")

    for col in SCORE_COLUMNS:
        values = df[col].dropna()

        if not values.between(0, 1).all():
            raise ValueError(f"{col}이 0~1 범위를 벗어남")

    print("\n=== Validation ===")
    print(f"rows: {len(df)}")
    print("duplicate location_id:", df["location_id"].duplicated().sum())

    print("\nScore range:")
    print(df[SCORE_COLUMNS].agg(["min", "max", "mean"]).T)


# ============================================================
# Main
# ============================================================

def main():
    features, zones = load_data()
    print(f"zone features: {features.shape}")

    # NYS DOH
    doh = build_doh_features(zones)
    df = features.merge(doh, on="location_id", how="left", validate="one_to_one")

    # 병원이 없는 Zone은 0
    hospital_cols = ["doh_hospital_count", "doh_hospital_bed_count"]
    df[hospital_cols] = df[hospital_cols].fillna(0)

    # 분석 대상 Zone 선택
    #
    # geometry 없는 TLC placeholder(264/265)와 EWR(Newark Airport, NJ
    # 소속이라 NY주 기준 PLUTO/ACS/LODES 커버리지가 없음)은 분석 대상에서
    # 제외한다. normalize/score/tag보다 먼저 걸러야, 이 Zone들이 percentile
    # 순위 계산에 섞여 들어가서 다른 Zone들의 결과를 왜곡하는 걸 막을 수 있다.
    valid_ids = set(
        zones.loc[
            zones.geometry.notna() & (zones["borough"] != "EWR"), "location_id",
        ]
    )

    valid_mask = df["location_id"].isin(valid_ids)
    analysis_df = df.loc[valid_mask].copy()

    analysis_df = add_normalized_features(analysis_df)
    analysis_df = generate_category_scores(analysis_df)
    analysis_df = generate_comfort_relevance(analysis_df)
    analysis_df = generate_zone_tags(analysis_df)

    # 분석 대상 Zone의 결과를 전체 location_id에 다시 붙인다. 제외된 Zone은
    # 매칭이 안 돼서 SCORE_COLUMNS/tag가 자동으로 NaN이 되고, tag만
    # 명시적으로 "excluded"로 표시한다.
    result_cols = ["location_id", *SCORE_COLUMNS, "zone_tag", "zone_tag_ko"]
    df = df.merge(analysis_df[result_cols], on="location_id", how="left")

    excluded_mask = ~df["location_id"].isin(valid_ids)
    df.loc[excluded_mask, "zone_tag"] = "excluded"
    df.loc[excluded_mask, "zone_tag_ko"] = "분석 제외"

    # 최종 출력
    output_cols = [
        "location_id",
        "business_score",
        "residential_score",
        "shopping_score",
        "nightlife_score",
        "tourism_score",
        "transit_score",
        "public_service_score",
        "park_score",
        "zone_tag",
        "zone_tag_ko",
        "comfort_relevance_score",
        "doh_hospital_count",  # 확인용
        "doh_hospital_bed_count",  # 확인용
    ]

    result = df[output_cols].copy()
    validate(result)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(OUTPUT_PATH, index=False)
    print(f"\nSaved: {OUTPUT_PATH}")

    print("\n=== Sample ===")
    print(result.head(20).to_string(index=False))


if __name__ == "__main__":
    main()