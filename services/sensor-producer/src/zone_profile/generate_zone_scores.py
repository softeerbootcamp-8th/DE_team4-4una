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
import requests

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
# NYS DOH
# ============================================================

DOH_FACILITY_URL = (
    "https://health.data.ny.gov/resource/"
    "vn5v-hh5r.json"
)

DOH_CERT_URL = (
    "https://health.data.ny.gov/resource/"
    "2g9y-7kqm.json"
)

NYC_COUNTIES = {
    "Bronx",
    "Kings",
    "New York",
    "Queens",
    "Richmond",
}

WGS84 = "EPSG:4326"


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


# ============================================================
# NYS DOH download
# ============================================================

def download_socrata(
    url: str,
    limit: int = 50000,
) -> pd.DataFrame:

    rows = []
    offset = 0

    while True:

        params = {
            "$limit": limit,
            "$offset": offset,
        }

        response = requests.get(
            url,
            params=params,
            timeout=180,
        )

        response.raise_for_status()

        batch = response.json()

        if not batch:
            break

        rows.extend(batch)

        offset += len(batch)

        print(
            f"DOH downloaded: {offset:,}"
        )

        if len(batch) < limit:
            break

    return pd.DataFrame(rows)


def extract_coordinates(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    NYS DOH dataset 버전에 따라 좌표 필드명이 달라도
    최대한 자동으로 탐색한다.
    """

    df = df.copy()

    # 일반적인 latitude / longitude
    if {
        "latitude",
        "longitude",
    }.issubset(df.columns):

        df["latitude"] = pd.to_numeric(
            df["latitude"],
            errors="coerce",
        )

        df["longitude"] = pd.to_numeric(
            df["longitude"],
            errors="coerce",
        )

        return df

    # Socrata location object
    possible_location_cols = [
        "location",
        "location_1",
        "geocoded_column",
    ]

    for col in possible_location_cols:

        if col not in df.columns:
            continue

        def get_value(value, key):

            if isinstance(value, dict):
                return value.get(key)

            return None

        df["latitude"] = df[col].apply(
            lambda x: get_value(
                x,
                "latitude",
            )
        )

        df["longitude"] = df[col].apply(
            lambda x: get_value(
                x,
                "longitude",
            )
        )

        if (
            df["latitude"].notna().any()
            and df["longitude"].notna().any()
        ):
            df["latitude"] = pd.to_numeric(
                df["latitude"],
                errors="coerce",
            )

            df["longitude"] = pd.to_numeric(
                df["longitude"],
                errors="coerce",
            )

            return df

    raise ValueError(
        "NYS DOH 데이터에서 좌표 컬럼을 찾지 못했습니다.\n"
        f"columns={df.columns.tolist()}"
    )


# ============================================================
# DOH → TLC Zone
# ============================================================

def build_doh_features(
    zones: gpd.GeoDataFrame,
) -> pd.DataFrame:

    print("\n=== NYS DOH ===")

    facilities = download_socrata(
        DOH_FACILITY_URL
    )

    certification = download_socrata(
        DOH_CERT_URL
    )

    RAW_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    facilities.to_json(
        RAW_DIR / "doh_facilities.json",
        orient="records",
    )

    certification.to_json(
        RAW_DIR / "doh_certification.json",
        orient="records",
    )

    # ------------------------------
    # NYC만
    # ------------------------------

    facilities = facilities[
        facilities["county"].isin(
            NYC_COUNTIES
        )
    ].copy()

    certification = certification[
        certification["county"].isin(
            NYC_COUNTIES
        )
    ].copy()

    # ------------------------------
    # Hospital
    # ------------------------------

    facility_text = (
        facilities["description"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
    )

    # description은 "Hospital", "Hospital Extension Clinic",
    # "School Based Hospital Extension Clinic",
    # "Mobile Hospital Extension Clinic" 4종류뿐이다.
    # Extension Clinic은 병원이 아니라 위성 소규모 클리닉이므로
    # 정확히 "Hospital"인 것만 남긴다.
    facilities = facilities[
        facility_text.eq("hospital")
    ].copy()

    # ------------------------------
    # Bed 정보만
    # ------------------------------

    certification["measure_value"] = (
        pd.to_numeric(
            certification["measure_value"],
            errors="coerce",
        )
    )

    bed_rows = certification[
        certification["attribute_type"]
        .fillna("")
        .str.lower()
        .eq("bed")
    ].copy()

    # permanent가 존재하면 permanent만 사용
    if "sub_type" in bed_rows.columns:

        permanent = bed_rows[
            bed_rows["sub_type"]
            .fillna("")
            .str.lower()
            .eq("permanent")
        ]

        if not permanent.empty:
            bed_rows = permanent

    beds = (
        bed_rows
        .groupby("fac_id")["measure_value"]
        .sum()
        .rename("hospital_bed_count")
        .reset_index()
    )

    facilities["fac_id"] = (
        facilities["fac_id"].astype(str)
    )

    beds["fac_id"] = (
        beds["fac_id"].astype(str)
    )

    facilities = facilities.merge(
        beds,
        on="fac_id",
        how="left",
    )

    facilities[
        "hospital_bed_count"
    ] = (
        facilities[
            "hospital_bed_count"
        ]
        .fillna(0)
    )

    # ------------------------------
    # 좌표
    # ------------------------------

    facilities = extract_coordinates(
        facilities
    )

    facilities = facilities.dropna(
        subset=[
            "latitude",
            "longitude",
        ]
    )

    hospital_points = gpd.GeoDataFrame(
        facilities,
        geometry=gpd.points_from_xy(
            facilities["longitude"],
            facilities["latitude"],
        ),
        crs=WGS84,
    )

    spatial_zones = (
        zones[
            zones.geometry.notna()
        ]
        .to_crs(WGS84)
    )

    joined = gpd.sjoin(
        hospital_points,
        spatial_zones[
            [
                "location_id",
                "geometry",
            ]
        ],
        how="inner",
        predicate="within",
    )

    result = (
        joined
        .groupby("location_id")
        .agg(
            doh_hospital_count=(
                "fac_id",
                "nunique",
            ),
            doh_hospital_bed_count=(
                "hospital_bed_count",
                "sum",
            ),
        )
        .reset_index()
    )

    return result


# ============================================================
# Normalize Features
# ============================================================

def add_normalized_features(
    df: pd.DataFrame,
) -> pd.DataFrame:

    df = df.copy()

    used_features = set()

    for weights in CATEGORY_WEIGHTS.values():
        used_features.update(
            weights.keys()
        )

    used_features.update(
        COMFORT_WEIGHTS.keys()
    )

    for feature in sorted(
        used_features
    ):

        if feature not in df.columns:
            print(
                f"[WARNING] "
                f"{feature} 없음"
            )
            continue

        df[
            f"{feature}_norm"
        ] = percentile_normalize(
            df[feature]
        )

    return df


# ============================================================
# Category Scores
# ============================================================

def generate_category_scores(
    df: pd.DataFrame,
) -> pd.DataFrame:

    df = df.copy()

    for score_name, weights in (
        CATEGORY_WEIGHTS.items()
    ):

        df[score_name] = (
            weighted_score(
                df,
                weights,
            )
        )

    return df


# ============================================================
# Comfort Preference
# ============================================================

def generate_comfort_preference(
    df: pd.DataFrame,
) -> pd.DataFrame:

    df = df.copy()

    df[
        "comfort_preference_score"
    ] = weighted_score(
        df,
        COMFORT_WEIGHTS,
    )

    return df