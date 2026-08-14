import zipfile
from dataclasses import replace
from datetime import UTC, date, datetime

import pytest
import shapefile
import shapely
from pyproj import CRS, Transformer
from road_segment.geometry import TARGET_CRS
from road_segment.taxi_zone import assign_taxi_zones, find_location_id, load_taxi_zones
from road_segment.validate import RoadSegmentRecord
from shapely import STRtree
from shapely.geometry import LineString, Polygon

INGESTED_AT = datetime(2026, 8, 13, 0, 0, 0, tzinfo=UTC)

ZONE_A = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
ZONE_B = Polygon([(20, 0), (30, 0), (30, 10), (20, 10)])
OVERLAPPING_A = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
OVERLAPPING_B = Polygon([(5, 5), (15, 5), (15, 15), (5, 15)])


def sample_record(segment_id: str, **overrides: object) -> RoadSegmentRecord:
    fields = {
        "segment_id": segment_id,
        "snapshot_date": date(2026, 8, 13),
        "street_name": "TEST STREET",
        "from_node_id": "0047201",
        "to_node_id": "0047258",
        "traffic_direction": "T",
        "segment_type": "U",
        "feature_type": "0",
        "roadway_type": "1",
        "roadbed_layer": "B",
        "from_node_level": "M",
        "to_node_level": "M",
        "posted_speed_mph": 25,
        "curve_flag": None,
        "curve_radius_m": None,
        "length_m": 100.0,
        "geometry_wkb": shapely.to_wkb(LineString([(2, 2), (8, 8)])),
        "source_version": "26B",
        "ingested_at": INGESTED_AT,
    }
    fields.update(overrides)
    return RoadSegmentRecord(**fields)


def write_taxi_zone_zip(output_path, zones: dict[int, Polygon], source_crs: CRS) -> None:
    shapefile_dir = output_path.parent / "taxi-shape"
    shapefile_dir.mkdir(exist_ok=True)
    stem = shapefile_dir / "taxi_zones"
    with shapefile.Writer(str(stem), shapeType=shapefile.POLYGON) as writer:
        writer.field("LocationID", "N", decimal=0)
        for location_id, polygon in zones.items():
            writer.poly([list(polygon.exterior.coords)])
            writer.record(location_id)
    stem.with_suffix(".prj").write_text(source_crs.to_wkt())
    with zipfile.ZipFile(output_path, "w") as archive:
        for suffix in (".shp", ".shx", ".dbf", ".prj"):
            path = stem.with_suffix(suffix)
            archive.write(path, arcname=path.name)


def test_find_location_id_returns_zone_containing_midpoint() -> None:
    zone_ids = [181, 182]
    zone_geometries = [ZONE_A, ZONE_B]
    tree = STRtree(zone_geometries)
    line = LineString([(2, 2), (8, 8)])

    assert find_location_id(line, zone_ids, zone_geometries, tree) == 181


def test_find_location_id_returns_none_outside_all_zones() -> None:
    zone_ids = [181, 182]
    zone_geometries = [ZONE_A, ZONE_B]
    tree = STRtree(zone_geometries)
    line = LineString([(100, 100), (101, 101)])

    assert find_location_id(line, zone_ids, zone_geometries, tree) is None


def test_find_location_id_picks_a_single_zone_when_overlapping() -> None:
    zone_ids = [181, 182]
    zone_geometries = [OVERLAPPING_A, OVERLAPPING_B]
    tree = STRtree(zone_geometries)
    line = LineString([(6, 6), (7, 7)])  # midpoint (6.5, 6.5) is inside both zones

    assert find_location_id(line, zone_ids, zone_geometries, tree) in zone_ids


def test_assign_taxi_zones_sets_location_id_and_preserves_other_fields() -> None:
    record = sample_record("0000001")
    taxi_zones = {181: ZONE_A, 182: ZONE_B}

    report = assign_taxi_zones([record], taxi_zones)

    assert len(report.records) == 1
    assigned = report.records[0]
    assert assigned.location_id == 181
    assert assigned == replace(record, location_id=181)
    assert report.unmatched_segment_ids == ()


def test_assign_taxi_zones_tracks_unmatched_when_no_zone_covers_segment() -> None:
    record = sample_record(
        "0000002", geometry_wkb=shapely.to_wkb(LineString([(100, 100), (101, 101)]))
    )
    taxi_zones = {181: ZONE_A}

    report = assign_taxi_zones([record], taxi_zones)

    assert report.records[0].location_id is None
    assert report.unmatched_segment_ids == ("0000002",)


def test_assign_taxi_zones_tracks_unmatched_when_geometry_is_missing() -> None:
    record = sample_record("0000003", geometry_wkb=None)
    taxi_zones = {181: ZONE_A}

    report = assign_taxi_zones([record], taxi_zones)

    assert report.records[0].location_id is None
    assert report.unmatched_segment_ids == ("0000003",)


def test_assign_taxi_zones_does_not_duplicate_or_drop_records() -> None:
    records = [sample_record("0000001"), sample_record("0000002")]
    taxi_zones = {181: ZONE_A}

    report = assign_taxi_zones(records, taxi_zones)

    assert len(report.records) == len(records)


def test_load_taxi_zones_reprojects_to_target_crs(tmp_path) -> None:
    source_crs = CRS.from_epsg(2263)
    square = Polygon([(980000, 200000), (981000, 200000), (981000, 201000), (980000, 201000)])
    zip_path = tmp_path / "taxi_zones.zip"
    write_taxi_zone_zip(zip_path, {181: square}, source_crs)

    zones = load_taxi_zones(zip_path)

    assert set(zones) == {181}
    transformer = Transformer.from_crs(source_crs, TARGET_CRS, always_xy=True)
    expected_x, expected_y = transformer.transform(980000, 200000)
    actual_x, actual_y = zones[181].exterior.coords[0]
    assert actual_x == pytest.approx(expected_x)
    assert actual_y == pytest.approx(expected_y)
