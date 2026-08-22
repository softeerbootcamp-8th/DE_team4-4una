"""
Build TLC Zone Profile Features

raw data
    ↓
TLC zone 단위 spatial join / aggregation
    ↓
zone_profile_features.parquet

※ 아직 category score / comfort_need_score는 계산하지 않는다.
"""

import json
import re
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

# ============================================================
# Path
# ============================================================

DATA_DIR = Path("data")

ZONE_MASTER_PATH = DATA_DIR / "reference/tlc_zone/zone_master.parquet"

RAW_DIR = DATA_DIR / "raw/zone_profile"

MAPPLUTO_PATH = RAW_DIR / "mappluto.csv"
ACS_PATH = RAW_DIR / "acs_block_group.csv"
ACS_BG_PATH = RAW_DIR / "tl_2024_36_bg.zip"

LODES_WAC_PATH = RAW_DIR / "ny_wac_S000_JT00_2023.csv.gz"
LODES_XWALK_PATH = RAW_DIR / "ny_xwalk.csv.gz"

OSM_POI_PATH = RAW_DIR / "osm_poi.json"
MTA_PATH = RAW_DIR / "mta_stations.geojson"
FACILITIES_PATH = RAW_DIR / "facilities.json"
PARKS_PATH = RAW_DIR / "parks.geojson"
DOH_FACILITY_PATH = RAW_DIR / "doh_facilities.json"
DOH_CERT_PATH = RAW_DIR / "doh_certification.json"

OUTPUT_PATH = DATA_DIR / "processed/zone_profile_features.parquet"

# 저장은 4326, 면적 계산할 때만 projected CRS 사용
PROJECTED_CRS = "EPSG:2263"
WGS84 = "EPSG:4326"

NYC_COUNTIES = {"Bronx", "Kings", "New York", "Queens", "Richmond"}

SQ_FT_TO_SQ_KM = 0.09290304 / 1_000_000


# ============================================================
# Common
# ============================================================

def snake_case(name: str) -> str:
    """컬럼명을 snake_case로 변환한다."""
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", str(name))
    name = re.sub(r"[^a-zA-Z0-9]+", "_", name)

    return name.lower().strip("_")


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [snake_case(col) for col in df.columns]
    return df


def safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denominator = denominator.replace(0, np.nan)
    return numerator / denominator


def load_zone_master() -> gpd.GeoDataFrame:
    zones = gpd.read_parquet(ZONE_MASTER_PATH)
    zones["location_id"] = zones["location_id"].astype("int64")
    return zones


def get_spatial_zones(zones: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """geometry가 존재하는 Zone만 반환한다."""
    return zones[zones.geometry.notna()].copy()


def get_zone_area(zones: gpd.GeoDataFrame) -> pd.DataFrame:
    zones_proj = get_spatial_zones(zones).to_crs(PROJECTED_CRS)

    result = zones_proj[["location_id", "geometry"]].copy()
    result["zone_area_km2"] = result.geometry.area * SQ_FT_TO_SQ_KM

    return result[["location_id", "zone_area_km2"]]


# ============================================================
# 1. PLUTO
# ============================================================

def build_pluto_features(zones: gpd.GeoDataFrame) -> pd.DataFrame:
    print("[1/8] PLUTO")

    df = pd.read_csv(MAPPLUTO_PATH)
    df = normalize_columns(df)

    numeric_cols = [
        "bldg_area",
        "res_area",
        "office_area",
        "commercial_area",
        "retail_area",
        "residential_units",
        "latitude",
        "longitude",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["latitude", "longitude"])

    pluto = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["longitude"], df["latitude"]),
        crs=WGS84,
    )

    spatial_zones = get_spatial_zones(zones).to_crs(WGS84)

    joined = gpd.sjoin(
        pluto,
        spatial_zones[["location_id", "geometry"]],
        how="inner",
        predicate="within",
    )

    sum_cols = [
        "bldg_area",
        "res_area",
        "office_area",
        "commercial_area",
        "retail_area",
        "residential_units",
    ]

    agg = joined.groupby("location_id")[sum_cols].sum().reset_index()

    agg["residential_area_ratio"] = safe_ratio(agg["res_area"], agg["bldg_area"])
    agg["office_area_ratio"] = safe_ratio(agg["office_area"], agg["bldg_area"])
    agg["commercial_area_ratio"] = safe_ratio(agg["commercial_area"], agg["bldg_area"])
    agg["retail_area_ratio"] = safe_ratio(agg["retail_area"], agg["bldg_area"])

    zone_area = get_zone_area(zones)
    agg = agg.merge(zone_area, on="location_id", how="left")

    agg["residential_unit_density"] = safe_ratio(
        agg["residential_units"], agg["zone_area_km2"],
    )

    return agg[
        [
            "location_id",
            "residential_area_ratio",
            "office_area_ratio",
            "commercial_area_ratio",
            "retail_area_ratio",
            "residential_unit_density",
        ]
    ]


# ============================================================
# 2. ACS
# ============================================================

def build_acs_features(zones: gpd.GeoDataFrame) -> pd.DataFrame:
    print("[2/8] ACS")

    acs = pd.read_csv(ACS_PATH, dtype=str)

    rename_map = {
        "B01003_001E": "population",
        "B11001_001E": "household_count",
        "B11001_002E": "family_household_count",
        "B11005_002E": "children_household_count",
        "B19013_001E": "median_household_income",
        "B25077_001E": "median_home_value",
        "B25064_001E": "median_gross_rent",
    }

    acs = acs.rename(columns=rename_map)

    # 65세 이상 인구
    senior_columns = [
        "B01001_020E",
        "B01001_021E",
        "B01001_022E",
        "B01001_023E",
        "B01001_024E",
        "B01001_025E",
        "B01001_044E",
        "B01001_045E",
        "B01001_046E",
        "B01001_047E",
        "B01001_048E",
        "B01001_049E",
    ]

    numeric_cols = list(rename_map.values()) + senior_columns

    for col in numeric_cols:
        acs[col] = pd.to_numeric(acs[col], errors="coerce")

        # ACS의 음수 special value 제거
        acs.loc[acs[col] < 0, col] = np.nan

    acs["senior_count"] = acs[senior_columns].sum(axis=1, min_count=1)

    # Census Block Group GEOID
    acs["geoid"] = (
        acs["state"].str.zfill(2)
        + acs["county"].str.zfill(3)
        + acs["tract"].str.zfill(6)
        + acs["block group"].str.zfill(1)
    )

    # TIGER Block Group Geometry
    bg = gpd.read_file(f"zip://{ACS_BG_PATH}")
    bg = normalize_columns(bg)
    bg["geoid"] = bg["geoid"].astype(str)

    bg = bg[["geoid", "geometry"]].merge(
        acs, on="geoid", how="inner", validate="one_to_one",
    )

    bg = bg.to_crs(PROJECTED_CRS)

    spatial_zones = get_spatial_zones(zones).to_crs(PROJECTED_CRS)

    bg["source_area"] = bg.geometry.area

    intersection = gpd.overlay(
        bg,
        spatial_zones[["location_id", "geometry"]],
        how="intersection",
        keep_geom_type=False,
    )

    intersection["area_weight"] = safe_ratio(
        intersection.geometry.area, intersection["source_area"],
    )

    # ------------------------------
    # Count 계열
    # ------------------------------

    count_cols = [
        "population",
        "household_count",
        "family_household_count",
        "children_household_count",
        "senior_count",
    ]

    for col in count_cols:
        intersection[f"{col}_weighted"] = (
            intersection[col] * intersection["area_weight"]
        )

    weighted_cols = [f"{col}_weighted" for col in count_cols]

    counts = (
        intersection.groupby("location_id")[weighted_cols].sum().reset_index()
    )

    counts = counts.rename(
        columns={f"{col}_weighted": col for col in count_cols}
    )

    counts["family_household_ratio"] = safe_ratio(
        counts["family_household_count"], counts["household_count"],
    )

    counts["children_household_ratio"] = safe_ratio(
        counts["children_household_count"], counts["household_count"],
    )

    counts["senior_ratio"] = safe_ratio(counts["senior_count"], counts["population"])

    # ------------------------------
    # Median 계열
    #
    # TLC Zone의 진짜 median을 다시 계산할 수 없으므로
    # household × area weight 기반 근사치 사용
    # ------------------------------

    intersection["effective_households"] = (
        intersection["household_count"] * intersection["area_weight"]
    )

    median_cols = ["median_household_income", "median_home_value", "median_gross_rent"]

    median_result = pd.DataFrame(
        {"location_id": intersection["location_id"].unique()}
    )

    for col in median_cols:
        valid = intersection[
            intersection[col].notna() & (intersection["effective_households"] > 0)
        ].copy()

        valid["weighted_value"] = valid[col] * valid["effective_households"]

        numerator = valid.groupby("location_id")["weighted_value"].sum()
        denominator = valid.groupby("location_id")["effective_households"].sum()

        value = (numerator / denominator).rename(col)

        median_result = median_result.merge(value, on="location_id", how="left")

    result = counts.merge(median_result, on="location_id", how="left")

    return result[
        [
            "location_id",
            "population",
            "household_count",
            "family_household_ratio",
            "children_household_ratio",
            "senior_ratio",
            "median_household_income",
            "median_home_value",
            "median_gross_rent",
        ]
    ]


# ============================================================
# 3. LODES WAC
# ============================================================

def build_lodes_features(zones: gpd.GeoDataFrame) -> pd.DataFrame:
    print("[3/8] LODES")

    wac = pd.read_csv(LODES_WAC_PATH, dtype={"w_geocode": str})
    wac = normalize_columns(wac)

    # WAC industry columns
    industry_cols = [
        "c000",
        "cns07",  # retail
        "cns09",  # information
        "cns10",  # finance
        "cns11",  # real estate
        "cns12",  # professional
        "cns13",  # management
        "cns15",  # education
        "cns16",  # healthcare
        "cns17",  # arts/recreation
        "cns18",  # accommodation/food
        "cns20",  # public administration
    ]

    for col in industry_cols:
        wac[col] = pd.to_numeric(wac[col], errors="coerce").fillna(0)

    # Crosswalk 컬럼을 먼저 확인
    header = pd.read_csv(LODES_XWALK_PATH, nrows=0).columns.tolist()

    block_col = next(col for col in header if col.lower().startswith("tabblk"))
    lat_col = next(col for col in header if col.lower() == "blklatdd")
    lon_col = next(col for col in header if col.lower() == "blklondd")

    xwalk = pd.read_csv(
        LODES_XWALK_PATH,
        usecols=[block_col, lat_col, lon_col],
        dtype={block_col: str},
    )

    xwalk = xwalk.rename(
        columns={block_col: "w_geocode", lat_col: "latitude", lon_col: "longitude"}
    )

    wac["w_geocode"] = wac["w_geocode"].astype(str).str.zfill(15)
    xwalk["w_geocode"] = xwalk["w_geocode"].astype(str).str.zfill(15)

    df = wac.merge(xwalk, on="w_geocode", how="inner", validate="one_to_one")

    points = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["longitude"], df["latitude"]),
        crs=WGS84,
    )

    spatial_zones = get_spatial_zones(zones).to_crs(WGS84)

    joined = gpd.sjoin(
        points,
        spatial_zones[["location_id", "geometry"]],
        how="inner",
        predicate="within",
    )

    # 전문서비스 + 기업관리
    joined["professional_jobs"] = joined["cns12"] + joined["cns13"]

    rename_jobs = {
        "c000": "total_jobs",
        "cns07": "retail_jobs",
        "cns09": "information_jobs",
        "cns10": "finance_jobs",
        "cns11": "real_estate_jobs",
        "cns15": "education_jobs",
        "cns16": "healthcare_jobs",
        "cns17": "arts_recreation_jobs",
        "cns18": "accommodation_food_jobs",
        "cns20": "public_admin_jobs",
    }

    joined = joined.rename(columns=rename_jobs)

    job_cols = [
        "total_jobs",
        "retail_jobs",
        "information_jobs",
        "finance_jobs",
        "real_estate_jobs",
        "professional_jobs",
        "education_jobs",
        "healthcare_jobs",
        "arts_recreation_jobs",
        "accommodation_food_jobs",
        "public_admin_jobs",
    ]

    # 매칭된 LODES block이 없는 Zone(대부분 공원)은 일자리가 0인 게
    # 맞으므로 결측 대신 0을 채운다. safe_ratio가 분모 0을 NaN으로
    # 처리하므로 *_job_ratio는 여전히 정의되지 않은 값(NaN)으로 남는다.
    agg = (
        joined.groupby("location_id")[job_cols]
        .sum()
        .reindex(spatial_zones["location_id"], fill_value=0)
        .reset_index()
    )

    ratio_cols = job_cols[1:]

    for col in ratio_cols:
        name = col.replace("_jobs", "_job_ratio")
        agg[name] = safe_ratio(agg[col], agg["total_jobs"])

    zone_area = get_zone_area(zones)
    agg = agg.merge(zone_area, on="location_id", how="left")

    agg["job_density"] = safe_ratio(agg["total_jobs"], agg["zone_area_km2"])

    output_cols = ["location_id", "total_jobs", "job_density"]
    output_cols += [col.replace("_jobs", "_job_ratio") for col in ratio_cols]

    return agg[output_cols]


# ============================================================
# 4. OSM POI
# ============================================================

def get_osm_categories(tags: dict) -> list[str]:
    result = []

    if tags.get("shop"):
        result.append("shop")

    amenity = tags.get("amenity")

    if amenity in {"restaurant", "cafe"}:
        result.append("restaurant")

    if amenity in {"bar", "pub", "nightclub"}:
        result.append("nightlife")

    tourism = tags.get("tourism")

    if tourism == "hotel":
        result.append("hotel")

    if tourism == "museum":
        result.append("museum")

    if tourism == "attraction":
        result.append("attraction")

    return result


def build_osm_features(zones: gpd.GeoDataFrame) -> pd.DataFrame:
    print("[4/8] OSM POI")

    with OSM_POI_PATH.open(encoding="utf-8") as file:
        data = json.load(file)

    rows = []

    for element in data.get("elements", []):
        tags = element.get("tags", {})

        lon = element.get("lon")
        lat = element.get("lat")

        if lon is None or lat is None:
            center = element.get("center", {})
            lon = center.get("lon")
            lat = center.get("lat")

        if lon is None or lat is None:
            continue

        categories = get_osm_categories(tags)

        for category in categories:
            rows.append({"longitude": lon, "latitude": lat, "category": category})

    expected = ["shop", "restaurant", "nightlife", "hotel", "museum", "attraction"]

    spatial_zones = get_spatial_zones(zones).to_crs(WGS84)

    if rows:
        df = pd.DataFrame(rows)

        poi = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df["longitude"], df["latitude"]),
            crs=WGS84,
        )

        joined = gpd.sjoin(
            poi,
            spatial_zones[["location_id", "geometry"]],
            how="inner",
            predicate="within",
        )

        counts = (
            joined.groupby(["location_id", "category"]).size().unstack(fill_value=0)
        )
    else:
        counts = pd.DataFrame(
            columns=expected, index=pd.Index([], name="location_id"),
        )

    for category in expected:
        if category not in counts.columns:
            counts[category] = 0

    # 매칭된 POI가 하나도 없는 Zone은 결측이 아니라 진짜 0건이다.
    counts = (
        counts[expected]
        .reindex(spatial_zones["location_id"], fill_value=0)
        .reset_index()
    )

    counts = counts.rename(columns={col: f"poi_{col}_count" for col in expected})

    zone_area = get_zone_area(zones)
    counts = counts.merge(zone_area, on="location_id", how="left")

    for category in expected:
        count_col = f"poi_{category}_count"
        density_col = f"poi_{category}_density"

        counts[density_col] = safe_ratio(counts[count_col], counts["zone_area_km2"])

    return counts.drop(columns="zone_area_km2")


# ============================================================
# 5. MTA
# ============================================================

def build_mta_features(zones: gpd.GeoDataFrame) -> pd.DataFrame:
    print("[5/8] MTA")

    mta = gpd.read_file(MTA_PATH)
    mta = normalize_columns(mta)

    # geometry가 없다면 위경도로 생성
    if (
        mta.geometry.isna().all()
        and "latitude" in mta.columns
        and "longitude" in mta.columns
    ):
        mta = gpd.GeoDataFrame(
            mta.drop(columns="geometry"),
            geometry=gpd.points_from_xy(
                pd.to_numeric(mta["longitude"]), pd.to_numeric(mta["latitude"]),
            ),
            crs=WGS84,
        )

    if mta.crs is None:
        mta = mta.set_crs(WGS84)

    mta = mta.to_crs(WGS84)

    mta["number_of_stations_in_complex"] = pd.to_numeric(
        mta["number_of_stations_in_complex"], errors="coerce",
    ).fillna(1)

    spatial_zones = get_spatial_zones(zones).to_crs(WGS84)

    joined = gpd.sjoin(
        mta,
        spatial_zones[["location_id", "geometry"]],
        how="inner",
        predicate="within",
    )

    result = (
        joined.groupby("location_id")
        .agg(
            subway_complex_count=("location_id", "size"),
            subway_station_count=("number_of_stations_in_complex", "sum"),
        )
        # 지하철역이 없는 Zone은 결측이 아니라 진짜 0건이다.
        .reindex(spatial_zones["location_id"], fill_value=0)
        .reset_index()
    )

    return result


# ============================================================
# 6. NYC Facilities
# ============================================================

def build_facility_features(zones: gpd.GeoDataFrame) -> pd.DataFrame:
    print("[6/8] NYC Facilities")

    # facilities.json은 GeoJSON이 아니라 위경도 컬럼을 가진 평범한 레코드 배열이다.
    facilities = pd.read_json(FACILITIES_PATH)
    facilities = normalize_columns(facilities)

    facilities["latitude"] = pd.to_numeric(facilities["latitude"], errors="coerce")
    facilities["longitude"] = pd.to_numeric(facilities["longitude"], errors="coerce")
    facilities = facilities.dropna(subset=["latitude", "longitude"])

    facilities = gpd.GeoDataFrame(
        facilities,
        geometry=gpd.points_from_xy(facilities["longitude"], facilities["latitude"]),
        crs=WGS84,
    )

    # 데이터 버전마다 세부 컬럼 이름이 다를 수 있으므로
    # 존재하는 설명 컬럼을 합쳐 분류한다.
    candidate_cols = [
        "facdomain",
        "facgroup",
        "facsubgrp",
        "factype",
        "facname",
        "name",
        "agencyoper",
        "opname",
    ]

    text_cols = [col for col in candidate_cols if col in facilities.columns]

    if not text_cols:
        raise ValueError(
            "Facilities 분류용 컬럼을 찾지 못했습니다.\n"
            f"columns={facilities.columns.tolist()}"
        )

    facilities["facility_text"] = (
        facilities[text_cols].fillna("").astype(str).agg(" ".join, axis=1).str.lower()
    )

    rules = {
        "education": r"school|education|college|university|academy",
        "medical": r"hospital|health|clinic|medical|nursing",
        "government": (
            r"government|administration|administrative|"
            r"courthouse|police|fire station|city hall"
        ),
        "cultural": (
            r"museum|library|cultural|theater|theatre|performing arts"
        ),
    }

    rows = []

    for category, pattern in rules.items():
        mask = facilities["facility_text"].str.contains(
            pattern, regex=True, na=False,
        )

        part = facilities[mask].copy()
        part["facility_category"] = category

        rows.append(part)

    classified = pd.concat(rows, ignore_index=True)
    classified = gpd.GeoDataFrame(classified, geometry="geometry", crs=WGS84)

    spatial_zones = get_spatial_zones(zones).to_crs(WGS84)

    joined = gpd.sjoin(
        classified,
        spatial_zones[["location_id", "geometry"]],
        how="inner",
        predicate="within",
    )

    result = (
        joined.groupby(["location_id", "facility_category"]).size().unstack(fill_value=0)
    )

    expected = ["education", "medical", "government", "cultural"]

    for col in expected:
        if col not in result.columns:
            result[col] = 0

    # 분류된 시설이 하나도 없는 Zone은 결측이 아니라 진짜 0건이다.
    result = (
        result[expected]
        .reindex(spatial_zones["location_id"], fill_value=0)
        .reset_index()
    )

    result = result.rename(columns={col: f"facility_{col}_count" for col in expected})

    return result


# ============================================================
# 7. Parks
# ============================================================

def build_park_features(zones: gpd.GeoDataFrame) -> pd.DataFrame:
    print("[7/8] NYC Parks")

    parks = gpd.read_file(PARKS_PATH)

    if parks.crs is None:
        parks = parks.set_crs(WGS84)

    parks = parks[parks.geometry.notna()].to_crs(PROJECTED_CRS)

    spatial_zones = get_spatial_zones(zones).to_crs(PROJECTED_CRS)

    # 중복 polygon으로 면적이 이중 계산되는 것을 막기 위해 union
    try:
        park_union = parks.geometry.union_all()
    except AttributeError:
        park_union = parks.geometry.unary_union

    rows = []

    for _, zone in spatial_zones.iterrows():
        intersection_area = zone.geometry.intersection(park_union).area
        zone_area = zone.geometry.area

        rows.append(
            {
                "location_id": zone["location_id"],
                "park_area_km2": intersection_area * SQ_FT_TO_SQ_KM,
                "park_area_ratio": (
                    intersection_area / zone_area if zone_area > 0 else np.nan
                ),
            }
        )

    return pd.DataFrame(rows)


# ============================================================
# 8. NYS DOH
# ============================================================

def extract_doh_coordinates(df: pd.DataFrame) -> pd.DataFrame:
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
    print("[8/8] NYS DOH")

    facilities = pd.read_json(DOH_FACILITY_PATH)
    certification = pd.read_json(DOH_CERT_PATH)

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
    facilities = extract_doh_coordinates(facilities)
    facilities = facilities.dropna(subset=["latitude", "longitude"])

    hospital_points = gpd.GeoDataFrame(
        facilities,
        geometry=gpd.points_from_xy(facilities["longitude"], facilities["latitude"]),
        crs=WGS84,
    )

    spatial_zones = get_spatial_zones(zones).to_crs(WGS84)

    result = (
        gpd.sjoin(
            hospital_points,
            spatial_zones[["location_id", "geometry"]],
            how="inner",
            predicate="within",
        )
        .groupby("location_id")
        .agg(doh_hospital_bed_count=("hospital_bed_count", "sum"))
        # 매칭된 병원이 없는 Zone은 결측이 아니라 진짜 0건이다.
        .reindex(spatial_zones["location_id"], fill_value=0)
        .reset_index()
    )

    return result


# ============================================================
# Integration
# ============================================================

def build_zone_profile_features() -> pd.DataFrame:
    zones = load_zone_master()

    # 최종 테이블의 기준은 zone_master
    result = zones[["location_id"]].copy()

    feature_tables = [
        build_pluto_features(zones),
        build_acs_features(zones),
        build_lodes_features(zones),
        build_osm_features(zones),
        build_mta_features(zones),
        build_facility_features(zones),
        build_park_features(zones),
        build_doh_features(zones),
    ]

    for features in feature_tables:
        if features.empty:
            continue

        if features["location_id"].duplicated().any():
            raise ValueError("Feature table에 location_id 중복이 있습니다.")

        result = result.merge(
            features, on="location_id", how="left", validate="one_to_one",
        )

    return result


# ============================================================
# Validation
# ============================================================

def validate(df: pd.DataFrame, zones: gpd.GeoDataFrame) -> None:
    if df["location_id"].isna().any():
        raise ValueError("location_id에 null이 있습니다.")

    if df["location_id"].duplicated().any():
        raise ValueError("location_id가 중복되었습니다.")

    expected_ids = set(zones["location_id"])
    actual_ids = set(df["location_id"])

    if expected_ids != actual_ids:
        raise ValueError("zone_master와 location_id가 일치하지 않습니다.")

    # ratio 범위 검증
    ratio_cols = [col for col in df.columns if col.endswith("_ratio")]

    for col in ratio_cols:
        values = df[col].dropna()
        invalid = ~values.between(0, 1.000001)

        if invalid.any():
            print(f"[WARNING] {col}: [0, 1] 범위 밖 값 {invalid.sum()}건")

    # 음수가 있으면 안 되는 컬럼
    nonnegative_keywords = ("_count", "_density", "_jobs", "_area_km2")

    for col in df.columns:
        if any(keyword in col for keyword in nonnegative_keywords):
            values = pd.to_numeric(df[col], errors="coerce").dropna()

            if (values < 0).any():
                raise ValueError(f"{col}에 음수 값이 있습니다.")

    print("\n=== Validation ===")
    print(f"rows: {len(df)}")
    print(f"columns: {len(df.columns)}")
    print(f"duplicate location_id: {df['location_id'].duplicated().sum()}")


COVERAGE_GROUPS = {
    "PLUTO": [
        "residential_area_ratio",
        "office_area_ratio",
        "commercial_area_ratio",
        "retail_area_ratio",
        "residential_unit_density",
    ],
    "ACS": [
        "population",
        "household_count",
        "family_household_ratio",
        "children_household_ratio",
        "senior_ratio",
        "median_household_income",
        "median_home_value",
        "median_gross_rent",
    ],
    "LODES": [
        "total_jobs",
        "job_density",
        "retail_job_ratio",
        "information_job_ratio",
        "finance_job_ratio",
        "real_estate_job_ratio",
        "professional_job_ratio",
        "education_job_ratio",
        "healthcare_job_ratio",
        "arts_recreation_job_ratio",
        "accommodation_food_job_ratio",
        "public_admin_job_ratio",
    ],
}


def report_missing_coverage(df: pd.DataFrame, zones: gpd.GeoDataFrame) -> None:
    """
    소스 join이 하나도 안 붙은(전체 컬럼이 NaN인) Zone을 보고한다.
    PLUTO/ACS/LODES는 count/density를 0으로 채우면 안 되는 값(median 등)이
    섞여 있어, 결측을 지우지 않고 커버리지 범위만 확인한다.
    """

    zone_names = zones.set_index("location_id")["zone"]

    print("\n=== Missing Source Coverage ===")

    for source, cols in COVERAGE_GROUPS.items():
        missing_ids = df.loc[df[cols].isna().all(axis=1), "location_id"]

        if missing_ids.empty:
            print(f"{source}: 없음")
            continue

        labels = [
            f"{location_id}({zone_names.get(location_id, '?')})"
            for location_id in missing_ids
        ]
        print(f"{source}: {len(missing_ids)}개 zone — {', '.join(labels)}")


def print_summary(df: pd.DataFrame) -> None:
    print("\n=== Output Columns ===")

    for col in df.columns:
        print(col)

    print("\n=== Null Rate ===")

    null_rate = df.isna().mean().sort_values(ascending=False)
    print(null_rate.head(30).to_string())

    print("\n=== Sample ===")
    print(df.head(10).to_string(index=False))


# ============================================================
# Main
# ============================================================

def main() -> None:
    zones = load_zone_master()

    result = build_zone_profile_features()

    validate(result, zones)

    report_missing_coverage(result, zones)
    print_summary(result)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(OUTPUT_PATH, index=False)

    print(f"\nSaved: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()