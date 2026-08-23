from math import isclose

import shapely
from dashboard.road_geometry import parse_s3_uri, wkb_to_map_geometry
from pyproj import Transformer
from shapely.geometry import LineString


def test_wkb_to_map_geometry_reprojects_epsg_32118_to_epsg_4326() -> None:
    source_coordinates = [(-74.0060, 40.7128), (-73.9857, 40.7484)]
    to_road_crs = Transformer.from_crs("EPSG:4326", "EPSG:32118", always_xy=True)
    projected = LineString(
        [
            to_road_crs.transform(longitude, latitude)
            for longitude, latitude in source_coordinates
        ]
    )

    geometry = wkb_to_map_geometry(shapely.to_wkb(projected))

    assert geometry["type"] == "LineString"
    for actual, expected in zip(
        geometry["coordinates"], source_coordinates, strict=True
    ):
        assert isclose(actual[0], expected[0], abs_tol=1e-6)
        assert isclose(actual[1], expected[1], abs_tol=1e-6)


def test_parse_s3_uri_returns_bucket_and_key() -> None:
    assert parse_s3_uri(
        "s3://example-bucket/road_segment/snapshot_date=2026-08-24/data.parquet"
    ) == (
        "example-bucket",
        "road_segment/snapshot_date=2026-08-24/data.parquet",
    )
