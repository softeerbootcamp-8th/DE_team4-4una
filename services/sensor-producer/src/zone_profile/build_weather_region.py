# TLC zone 263개를 날씨 조회용 권역(weather region) 20개로 묶어 reference 데이터로
# 저장한다. Open-Meteo 호출 좌표를 263 -> 20으로 줄이는 게 목적이다 — Open-Meteo는
# 요청 1건을 좌표 수로 가중해 API call을 세므로, 15분 주기(96 tick/일)에서 일일 가중
# call이 약 25,000(무료 한도 10,000 초과)에서 약 1,900으로 내려간다.
#
# zone_master.parquet과 같은 오프라인 1회 생성 스크립트다. 15분마다 공간 연산을
# 다시 하지 않도록 결과를 reference 데이터로 고정하고, TLC zone geometry가 바뀌거나
# TARGET_REGION_COUNT를 조정할 때만 다시 실행한다.

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely import union_all
from shapely.geometry import Point
from shapely.ops import nearest_points

REPO_ROOT = Path(__file__).resolve().parents[4]

ZONE_MASTER_PATH = REPO_ROOT / "data/reference/tlc_zone/zone_master.parquet"
OUTPUT_DIR = REPO_ROOT / "data/reference/weather_region"
REGION_MASTER_PATH = OUTPUT_DIR / "weather_region_master.parquet"
ZONE_REGION_MAP_PATH = OUTPUT_DIR / "zone_weather_region_map.parquet"

# zone_master와 같은 출력 CRS (schema-catalog.md의 lon/lat 규칙). zone_master가
# 이미 이 CRS로 저장돼 있어 출력 geometry는 재투영 없이 그대로 합집합만 취한다 —
# 합쳐진 폴리곤을 재투영하면 경계에 hairline sliver가 생겨 무효 폴리곤이 된다.
TARGET_CRS = "EPSG:4326"
# 면적/거리 비교를 도 단위(EPSG:4326)로 하면 경도 1도가 위도 1도보다 짧아 왜곡된다.
# 병합 판단에만 쓰는 별도 사본을 NYC를 덮는 UTM 18N(미터)으로 만들어 쓴다.
METRIC_CRS = "EPSG:32618"

TARGET_REGION_COUNT = 20

# zone_master와 같은 이유로 제외 — 폴리곤이 없는 비공간 placeholder 행.
NON_SPATIAL_LOCATION_IDS = {264, 265}

REGION_MASTER_COLUMNS = [
    "weather_region_id",
    "geometry",
    "representative_latitude",
    "representative_longitude",
]
ZONE_REGION_MAP_COLUMNS = ["location_id", "weather_region_id"]


def read_zone_master(path: Path) -> gpd.GeoDataFrame:
    zone_master = gpd.read_parquet(
        path,
        columns=[
            "location_id",
            "geometry",
            "representative_latitude",
            "representative_longitude",
        ],
    )
    spatial = zone_master[~zone_master["location_id"].isin(NON_SPATIAL_LOCATION_IDS)]
    spatial = spatial[spatial.geometry.notna()]
    spatial = spatial[spatial["representative_latitude"].notna()]
    return spatial.reset_index(drop=True)


# 폴리곤이 맞닿는 zone 쌍을 인접 그래프로 만든다. TLC zone은 육지 기준 폴리곤이라
# 강/해협을 건너는 쌍(맨해튼-브루클린 등)은 여기서 잡히지 않는다 — 그 처리는
# _merge_target()의 최근접 fallback이 담당한다.
def build_adjacency(zones: gpd.GeoDataFrame) -> dict[int, set[int]]:
    location_ids = [int(value) for value in zones["location_id"]]
    geometries = list(zones.geometry)
    spatial_index = zones.sindex
    adjacency: dict[int, set[int]] = {location_id: set() for location_id in location_ids}
    for position, geometry in enumerate(geometries):
        for candidate in spatial_index.query(geometry, predicate="intersects"):
            if int(candidate) != position:
                adjacency[location_ids[position]].add(location_ids[int(candidate)])
    return adjacency


def _merge_target(
    source: int,
    members: dict[int, set[int]],
    geometry: dict[int, object],
    neighbours: dict[int, set[int]],
) -> int:
    candidates = neighbours[source] & set(members)
    if candidates:
        # 면적이 가장 작은 인접 권역에 붙인다. 날씨는 공간에 퍼진 장(場)이고 권역
        # 하나를 대표좌표 1점으로 조회하므로, 표본 오차는 권역이 물리적으로 얼마나
        # 넓은지로 결정된다 — TLC zone은 크기가 제각각이라 zone 수로 묶으면 면적이
        # 7~107km²로 벌어져 권역별 오차가 14배 차이 난다. 동률은 id로 깬다.
        return min(candidates, key=lambda key: (geometry[key].area, key))
    # 물길로 끊긴 섬(루스벨트/거버너스/시티 아일랜드 등)은 맞닿는 zone이 없다.
    # 날씨는 가장 가까운 권역과 사실상 같으므로 최근접 권역에 붙인다. 이 fallback이
    # 없으면 섬 하나가 권역 하나를 차지해 20개 예산을 잠식한다.
    others = [key for key in members if key != source]
    return min(others, key=lambda key: (geometry[source].distance(geometry[key]), key))


# 면적이 가장 작은 권역을 인접(없으면 최근접) 권역에 흡수시키는 걸 목표 개수에
# 도달할 때까지 반복한다(기준은 _merge_target() 참고). 병합만 하므로 권역은 항상
# 원래 zone들의 분할이고, 입력이 같으면 결과도 같다(동률은 location_id로 깬다).
# geometry는 면적/거리 판단에만 쓰는 METRIC_CRS 사본이며 출력에는 쓰지 않는다.
def merge_regions(
    metric_zones: gpd.GeoDataFrame, adjacency: dict[int, set[int]], target_count: int
) -> dict[int, set[int]]:
    members = {
        int(location_id): {int(location_id)} for location_id in metric_zones["location_id"]
    }
    geometry = {
        int(location_id): geom
        for location_id, geom in zip(
            metric_zones["location_id"], metric_zones.geometry, strict=True
        )
    }
    neighbours = {key: set(value) for key, value in adjacency.items()}

    while len(members) > target_count:
        source = min(members, key=lambda key: (geometry[key].area, key))
        target = _merge_target(source, members, geometry, neighbours)

        members[target] |= members.pop(source)
        geometry[target] = geometry[target].union(geometry.pop(source))

        # source가 사라지므로 그 인접 관계를 target으로 옮긴다.
        neighbours[target] |= neighbours.pop(source)
        neighbours[target].discard(target)
        for key, group in neighbours.items():
            if source in group:
                group.discard(source)
                if key != target:
                    group.add(target)

    return members


# weather_region_id를 멤버 중 가장 작은 location_id 순으로 1..N을 부여한다 —
# 재생성해도 같은 권역이 같은 ID를 갖게 하려고 dict 순서에 의존하지 않는다.
def assign_region_ids(members: dict[int, set[int]]) -> dict[int, int]:
    ordered = sorted(members, key=lambda key: min(members[key]))
    return {key: region_id for region_id, key in enumerate(ordered, start=1)}


# 권역을 대표해 Open-Meteo에 보낼 좌표 1점을 고른다. 폴리곤의 representative_point()는
# 라벨 배치용이라 중심성을 보장하지 않아, 실제로 날씨를 부여받는 대상인 zone 대표좌표들의
# 평균을 쓴다(권역 소속 zone까지의 평균 거리 3.40 -> 3.02km, 최악 권역 14.5 -> 9.9km).
# 평균점이 물길로 끊긴 권역에서 폴리곤 밖(물 위)으로 나가면 육지 기온과 어긋날 수 있어
# 가장 가까운 폴리곤 경계로 스냅한다 — 스냅 손실은 0.1km 수준이다.
def _region_query_point(polygon, zone_points: list[Point]) -> Point:
    mean_point = Point(
        sum(point.x for point in zone_points) / len(zone_points),
        sum(point.y for point in zone_points) / len(zone_points),
    )
    if polygon.contains(mean_point):
        return mean_point
    return nearest_points(polygon, mean_point)[0]


def build_region_master(
    zones: gpd.GeoDataFrame,
    metric_zones: gpd.GeoDataFrame,
    members: dict[int, set[int]],
    region_ids: dict[int, int],
) -> gpd.GeoDataFrame:
    location_ids = [int(value) for value in zones["location_id"]]
    geometry_by_zone = dict(zip(location_ids, zones.geometry, strict=True))
    metric_geometry_by_zone = dict(zip(location_ids, metric_zones.geometry, strict=True))
    metric_points = gpd.GeoSeries.from_xy(
        zones["representative_longitude"], zones["representative_latitude"], crs=TARGET_CRS
    ).to_crs(METRIC_CRS)
    metric_point_by_zone = dict(zip(location_ids, metric_points, strict=True))

    keys = sorted(region_ids, key=lambda key: region_ids[key])
    query_points = [
        _region_query_point(
            union_all([metric_geometry_by_zone[member] for member in members[key]]),
            [metric_point_by_zone[member] for member in members[key]],
        )
        for key in keys
    ]
    query_points_wgs84 = gpd.GeoSeries(query_points, crs=METRIC_CRS).to_crs(TARGET_CRS)

    regions = gpd.GeoDataFrame(
        {"weather_region_id": [region_ids[key] for key in keys]},
        geometry=[union_all([geometry_by_zone[member] for member in members[key]]) for key in keys],
        crs=TARGET_CRS,
    )
    regions["representative_latitude"] = query_points_wgs84.y.to_numpy()
    regions["representative_longitude"] = query_points_wgs84.x.to_numpy()
    return regions[REGION_MASTER_COLUMNS]


def build_zone_region_map(
    members: dict[int, set[int]], region_ids: dict[int, int]
) -> pd.DataFrame:
    rows = [
        {"location_id": location_id, "weather_region_id": region_ids[key]}
        for key, group in members.items()
        for location_id in group
    ]
    zone_region_map = pd.DataFrame(rows, columns=ZONE_REGION_MAP_COLUMNS)
    zone_region_map = zone_region_map.astype(
        {"location_id": "int64", "weather_region_id": "int64"}
    )
    return zone_region_map.sort_values("location_id").reset_index(drop=True)


def validate(
    zones: gpd.GeoDataFrame,
    regions: gpd.GeoDataFrame,
    zone_region_map: pd.DataFrame,
    target_count: int,
) -> None:
    expected_location_ids = {int(value) for value in zones["location_id"]}
    mapped_location_ids = set(zone_region_map["location_id"])
    assert not zone_region_map["location_id"].duplicated().any()
    assert mapped_location_ids == expected_location_ids, (
        "zone_weather_region_map must cover every spatial zone exactly once: "
        f"{mapped_location_ids.symmetric_difference(expected_location_ids)}"
    )

    assert len(regions) == target_count, f"expected {target_count} regions, got {len(regions)}"
    assert list(regions["weather_region_id"]) == list(range(1, target_count + 1))
    # 멤버가 없는 권역은 Open-Meteo 호출만 낭비하므로 있으면 안 된다.
    assert set(zone_region_map["weather_region_id"]) == set(regions["weather_region_id"])

    assert regions.geometry.notna().all()
    assert regions.geometry.is_valid.all()

    # 조회 좌표는 폴리곤 내부이거나(대부분) 경계로 스냅된 점이다. 경계 위 점은
    # within()이 False이므로 covers()로 판정하고, 재투영 부동소수 오차만큼 여유를 준다.
    query_points = gpd.GeoSeries.from_xy(
        regions["representative_longitude"],
        regions["representative_latitude"],
        crs=TARGET_CRS,
    ).to_crs(METRIC_CRS)
    metric_regions = regions.to_crs(METRIC_CRS).geometry.buffer(0.01)  # 1cm 허용
    assert metric_regions.covers(query_points).all(), (
        "every region query point must fall on or inside its own region polygon"
    )


def summarize(regions: gpd.GeoDataFrame, zone_region_map: pd.DataFrame) -> pd.DataFrame:
    zone_counts = (
        zone_region_map.groupby("weather_region_id").size().rename("zone_count").reset_index()
    )
    summary = regions[["weather_region_id"]].copy()
    summary["area_km2"] = (regions.to_crs(METRIC_CRS).geometry.area / 1_000_000).round(1).to_numpy()
    summary["latitude"] = regions["representative_latitude"].round(4).to_numpy()
    summary["longitude"] = regions["representative_longitude"].round(4).to_numpy()
    return summary.merge(zone_counts, on="weather_region_id")


def main() -> None:
    zones = read_zone_master(ZONE_MASTER_PATH)
    metric_zones = zones.to_crs(METRIC_CRS)
    adjacency = build_adjacency(zones)
    members = merge_regions(metric_zones, adjacency, TARGET_REGION_COUNT)
    region_ids = assign_region_ids(members)

    regions = build_region_master(zones, metric_zones, members, region_ids)
    zone_region_map = build_zone_region_map(members, region_ids)
    validate(zones, regions, zone_region_map, TARGET_REGION_COUNT)

    print("spatial zones:", len(zones))
    print("regions:", len(regions))
    print(summarize(regions, zone_region_map).to_string(index=False))

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    regions.to_parquet(REGION_MASTER_PATH, index=False)
    zone_region_map.to_parquet(ZONE_REGION_MAP_PATH, index=False)
    print(f"saved: {REGION_MASTER_PATH}")
    print(f"saved: {ZONE_REGION_MAP_PATH}")


if __name__ == "__main__":
    main()
