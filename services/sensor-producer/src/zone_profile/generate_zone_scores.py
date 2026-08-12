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
- comfort_preference_score

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

FEATURE_PATH = (
    DATA_DIR / "processed/zone_profile_features.parquet"
)

ZONE_MASTER_PATH = (
    DATA_DIR / "reference/tlc_zone/zone_master.parquet"
)

RAW_DIR = DATA_DIR / "raw/zone_score"

OUTPUT_PATH = (
    DATA_DIR / "processed/zone_scores.parquet"
)


# ============================================================
# Score 설정
# ============================================================

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

    "public_medical_score": {
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


# ============================================================
# Common
# ============================================================

def load_data():
    features = pd.read_parquet(FEATURE_PATH)
    zones = gpd.read_parquet(ZONE_MASTER_PATH)

    features["location_id"] = (
        features["location_id"].astype("int64")
    )

    zones["location_id"] = (
        zones["location_id"].astype("int64")
    )

    return features, zones


def percentile_normalize(
    series: pd.Series,
) -> pd.Series:
    """
    NYC TLC Zone 내 상대적 percentile을 0~1로 변환한다.

    0 값은 0으로 유지하고,
    양수 값만 percentile ranking한다.
    """

    values = pd.to_numeric(
        series,
        errors="coerce",
    )

    result = pd.Series(
        np.nan,
        index=values.index,
        dtype="float64",
    )

    zero_mask = values == 0
    positive_mask = values > 0

    result.loc[zero_mask] = 0.0

    if positive_mask.any():
        result.loc[positive_mask] = (
            values.loc[positive_mask]
            .rank(
                method="average",
                pct=True,
            )
        )

    return result


def weighted_score(
    df: pd.DataFrame,
    weights: dict[str, float],
) -> pd.Series:
    """
    정규화 Feature들의 가중 평균을 계산한다.

    결측 Feature가 있으면 해당 Feature의 weight를 제외하고
    나머지 weight를 다시 정규화한다.
    """

    numerator = pd.Series(
        0.0,
        index=df.index,
    )

    denominator = pd.Series(
        0.0,
        index=df.index,
    )

    for feature, weight in weights.items():

        norm_col = f"{feature}_norm"

        if norm_col not in df.columns:
            print(
                f"[WARNING] "
                f"{feature} 컬럼이 없어 score에서 제외"
            )
            continue

        valid = df[norm_col].notna()

        numerator.loc[valid] += (
            df.loc[valid, norm_col]
            * weight
        )

        denominator.loc[valid] += weight

    return (
        numerator
        / denominator.replace(0, np.nan)
    )