from pathlib import Path

import geopandas as gpd
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[5]

LOOKUP_PATH = REPO_ROOT / "data/reference/tlc_zone/taxi_zone_lookup.csv"
GEOMETRY_PATH = REPO_ROOT / "data/reference/tlc_zone/taxi_zones/taxi_zones.shp"
OUTPUT_PATH = REPO_ROOT / "data/reference/tlc_zone/zone_master.parquet"

# PLUTO/ACS/LODES/POI 등 이후 공간 조인 대상이 대부분 EPSG:4326을 쓰므로
# zone_master도 여기에 맞춰 통일한다 (schema-catalog.md의 lon/lat 규칙과 동일).
TARGET_CRS = "EPSG:4326"

OUTPUT_COLUMNS = [
    "location_id",
    "borough",
    "zone",
    "service_zone",
    "geometry",
    "representative_latitude",
    "representative_longitude",
]

# TLC 룩업에만 존재하는 비공간 placeholder 행 (N/A, Outside of NYC).
# 폴리곤이 없는 게 정상이므로 geometry 결측 검증에서 예외로 둔다.
NON_SPATIAL_LOCATION_IDS = {264, 265}


def snake_case_columns(columns: pd.Index) -> pd.Index:
    return columns.str.strip().str.lower().str.replace(" ", "_")


def read_lookup(path: Path) -> pd.DataFrame:
    lookup = pd.read_csv(path)
    lookup.columns = snake_case_columns(lookup.columns)
    lookup = lookup.rename(columns={"locationid": "location_id"})
    lookup["location_id"] = lookup["location_id"].astype("int64")
    return lookup


def read_geometry(path: Path) -> gpd.GeoDataFrame:
    zones = gpd.read_file(path)
    zones.columns = snake_case_columns(zones.columns)
    zones = zones.rename(columns={"locationid": "location_id"})
    zones["location_id"] = zones["location_id"].astype("int64")
    return zones[["location_id", "geometry"]]


def build_zone_master(lookup: pd.DataFrame, zones: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    merged = lookup.merge(zones, on="location_id", how="left")
    zone_master = gpd.GeoDataFrame(merged, geometry="geometry", crs=zones.crs)
    zone_master = zone_master.to_crs(TARGET_CRS)

    # Open-Meteo 조회에 쓸 zone 대표 좌표 (#196, schema-catalog.md
    # "zone_master"). 폴리곤 무게중심(centroid)은 오목한 폴리곤에서 폴리곤
    # 바깥으로 나갈 수 있어, 항상 폴리곤 내부에 있도록 보장하는
    # representative_point()를 쓴다. geometry가 없는 행(264, 265)은
    # representative_point()도 그대로 결측으로 전파된다.
    representative_point = zone_master["geometry"].representative_point()
    zone_master["representative_latitude"] = representative_point.y
    zone_master["representative_longitude"] = representative_point.x

    return zone_master[OUTPUT_COLUMNS]


def validate(zone_master: gpd.GeoDataFrame) -> None:
    assert zone_master["location_id"].notna().all()
    assert not zone_master["location_id"].duplicated().any()

    missing_geometry = set(zone_master.loc[zone_master["geometry"].isna(), "location_id"])
    unexpected_missing = missing_geometry - NON_SPATIAL_LOCATION_IDS
    assert not unexpected_missing, f"unexpected missing geometry: {unexpected_missing}"

    has_geometry = zone_master["geometry"].notna()
    assert zone_master.loc[has_geometry, "geometry"].is_valid.all()

    missing_representative_point = set(
        zone_master.loc[zone_master["representative_latitude"].isna(), "location_id"]
    )
    assert missing_representative_point == missing_geometry, (
        "representative point missing/present should exactly match geometry: "
        f"{missing_representative_point.symmetric_difference(missing_geometry)}"
    )

    has_representative_point = zone_master["representative_latitude"].notna()
    representative_points = gpd.GeoSeries.from_xy(
        zone_master.loc[has_representative_point, "representative_longitude"],
        zone_master.loc[has_representative_point, "representative_latitude"],
        crs=TARGET_CRS,
    )
    assert representative_points.within(
        zone_master.loc[has_representative_point, "geometry"]
    ).all()


def main() -> None:
    lookup = read_lookup(LOOKUP_PATH)
    zones = read_geometry(GEOMETRY_PATH)

    zone_master = build_zone_master(lookup, zones)
    validate(zone_master)

    print("rows:", len(zone_master))
    print("crs:", zone_master.crs)
    print(zone_master.head())

    zone_master.to_parquet(OUTPUT_PATH, index=False)
    print(f"saved: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
