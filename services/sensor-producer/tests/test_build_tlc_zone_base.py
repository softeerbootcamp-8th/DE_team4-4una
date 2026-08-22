"""zone_master representative point generation (#196)."""

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Polygon
from zone_profile.build_tlc_zone_base import (
    NON_SPATIAL_LOCATION_IDS,
    TARGET_CRS,
    build_zone_master,
    validate,
)

# U자 모양 오목 폴리곤 — 무게중심(centroid)은 두 다리 사이의 빈 홈(notch)으로
# 빠져 폴리곤 바깥에 찍히지만, representative_point()는 항상 폴리곤 내부에 있다.
CONCAVE_ZONE = Polygon([(0, 0), (3, 0), (3, 3), (2, 3), (2, 1), (1, 1), (1, 3), (0, 3)])


def lookup(*location_ids: int) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "location_id": list(location_ids),
            "borough": ["Manhattan"] * len(location_ids),
            "zone": [f"Zone {i}" for i in location_ids],
            "service_zone": ["Boro Zone"] * len(location_ids),
        }
    )


def zones(geometries: dict[int, object]) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {"location_id": list(geometries), "geometry": list(geometries.values())},
        crs=TARGET_CRS,
    )


def test_representative_point_falls_inside_a_concave_polygon():
    zone_master = build_zone_master(lookup(1), zones({1: CONCAVE_ZONE}))

    row = zone_master.set_index("location_id").loc[1]
    assert row["representative_latitude"] is not None
    assert row["representative_longitude"] is not None

    point = gpd.GeoSeries.from_xy(
        [row["representative_longitude"]], [row["representative_latitude"]], crs=TARGET_CRS
    )
    assert point.within(gpd.GeoSeries([CONCAVE_ZONE], crs=TARGET_CRS)).all()
    # 오목한 꼭짓점 근처(무게중심이 몰릴 법한 자리)가 아니라 폴리곤 내부 어딘가임을
    # 재확인 — centroid는 이 폴리곤 밖으로 나간다.
    assert not CONCAVE_ZONE.centroid.within(CONCAVE_ZONE)


def test_representative_point_is_null_when_geometry_is_null():
    input_lookup = lookup(264, 265)
    zone_master = build_zone_master(input_lookup, zones({}))

    assert zone_master["geometry"].isna().all()
    assert zone_master["representative_latitude"].isna().all()
    assert zone_master["representative_longitude"].isna().all()
    assert set(zone_master["location_id"]) == NON_SPATIAL_LOCATION_IDS


def test_validate_passes_for_a_normal_zone_and_the_non_spatial_placeholders():
    zone_master = build_zone_master(
        lookup(1, 264, 265), zones({1: CONCAVE_ZONE})
    )

    validate(zone_master)  # raises on failure


def test_validate_raises_when_representative_point_is_missing_for_a_spatial_zone():
    zone_master = build_zone_master(lookup(1), zones({1: CONCAVE_ZONE}))
    zone_master.loc[0, "representative_latitude"] = None

    with pytest.raises(AssertionError):
        validate(zone_master)
