import json

import shapely
from road_segment.geometry import (
    SOURCE_CRS,
    TARGET_CRS,
    build_segment_geometries,
    build_segment_geometry,
    geometry_from_wkb,
    load_lion_rows_with_geometry,
    normalize_line_geometry,
    reproject_to_target_crs,
)
from shapely.geometry import LineString, MultiLineString, Point, mapping

LINE_A = [(-73.99, 40.67), (-73.989, 40.671)]
LINE_B = [(-73.989, 40.671), (-73.988, 40.672)]


def lion_row(
    segment_id: str,
    object_id: int,
    geometry: dict | None,
    feature_type: str = "0",
    **overrides: object,
) -> dict[str, object]:
    row = {
        "SegmentID": segment_id,
        "OBJECTID": object_id,
        "NodeIDFrom": "0047201",
        "NodeIDTo": "0047258",
        "TrafDir": "T",
        "SegmentTyp": "U",
        "FeatureTyp": feature_type,
        "RW_TYPE": " 1",
        "Status": "2",
        "RB_Layer": "B",
        "NodeLevelF": "M",
        "NodeLevelT": "M",
        "POSTED_SPEED": "25",
        "CurveFlag": " ",
        "Radius": 0,
        "Shape__Length": 100.0,
        "Street": "TEST STREET",
        "_geometry": geometry,
    }
    row.update(overrides)
    return row


def lion_feature(row: dict[str, object]) -> dict[str, object]:
    properties = {key: value for key, value in row.items() if key != "_geometry"}
    return {"type": "Feature", "properties": properties, "geometry": row["_geometry"]}


def test_reproject_to_target_crs_converts_wgs84_to_state_plane_meters() -> None:
    point = shapely.Point(-73.99, 40.67)

    projected = reproject_to_target_crs(point)

    assert projected.x > 10_000
    assert projected.y > 10_000


def test_reproject_to_target_crs_distance_is_reasonable_in_meters() -> None:
    # 위도 0.001도 차이는 어디서나 약 111m. feet 단위였다면 약 364ft로
    # 이 범위를 크게 벗어나므로, meter 단위로 나온다는 걸 확인하는 회귀 테스트.
    point_a = reproject_to_target_crs(shapely.Point(-73.99, 40.700))
    point_b = reproject_to_target_crs(shapely.Point(-73.99, 40.701))

    assert 100 < point_a.distance(point_b) < 120


def test_normalize_line_geometry_returns_none_for_missing_geometry() -> None:
    assert normalize_line_geometry(None) is None


def test_normalize_line_geometry_returns_none_for_empty_linestring() -> None:
    empty = mapping(LineString())

    assert normalize_line_geometry(empty) is None


def test_normalize_line_geometry_returns_none_for_unsupported_type() -> None:
    point = mapping(Point(-73.99, 40.67))

    assert normalize_line_geometry(point) is None


def test_normalize_line_geometry_keeps_existing_linestring() -> None:
    raw = mapping(LineString(LINE_A))

    line = normalize_line_geometry(raw)

    assert line is not None
    assert line.geom_type == "LineString"


def test_normalize_line_geometry_converts_single_part_multilinestring() -> None:
    raw = mapping(MultiLineString([LINE_A]))

    line = normalize_line_geometry(raw)

    assert line is not None
    assert line.geom_type == "LineString"


def test_normalize_line_geometry_excludes_multipart_multilinestring_even_when_connected() -> None:
    # part가 이어져 있어도 방향을 보장할 수 없으므로 제외한다.
    raw = mapping(MultiLineString([LINE_A, LINE_B]))

    assert normalize_line_geometry(raw) is None


def test_normalize_line_geometry_rejects_disconnected_multilinestring() -> None:
    raw = mapping(MultiLineString([LINE_A, [(-73.95, 40.70), (-73.94, 40.71)]]))

    assert normalize_line_geometry(raw) is None


def test_normalize_line_geometry_preserves_coordinate_order_for_linestring() -> None:
    raw = mapping(LineString(LINE_A))

    line = normalize_line_geometry(raw)

    expected = reproject_to_target_crs(LineString(LINE_A))
    assert line.coords[0] == expected.coords[0]
    assert line.coords[-1] == expected.coords[-1]


def test_load_lion_rows_with_geometry_keeps_geometry_alongside_properties(
    tmp_path,
) -> None:
    path = tmp_path / "lion.geojson"
    row = lion_row("0000001", 1, mapping(LineString(LINE_A)))
    path.write_text(
        json.dumps({"type": "FeatureCollection", "features": [lion_feature(row)]})
    )

    rows = load_lion_rows_with_geometry(path)

    assert rows[0]["SegmentID"] == "0000001"
    assert rows[0]["_geometry"]["type"] == "LineString"


def test_build_segment_geometry_round_trips_through_wkb() -> None:
    row = lion_row("0000001", 1, mapping(LineString(LINE_A)))

    result = build_segment_geometry(row)

    assert result is not None
    assert result.segment_id == "0000001"
    restored = geometry_from_wkb(result.geometry_wkb)
    assert restored.geom_type == "LineString"
    expected = reproject_to_target_crs(LineString(LINE_A))
    assert restored.equals_exact(expected, tolerance=1e-6)


def test_build_segment_geometry_returns_none_for_null_geometry() -> None:
    row = lion_row("0000001", 1, None)

    assert build_segment_geometry(row) is None


def test_build_segment_geometries_excludes_invalid_and_multipart_geometry(tmp_path) -> None:
    path = tmp_path / "lion.geojson"
    features = [
        lion_feature(lion_row("0000001", 1, mapping(LineString(LINE_A)))),
        lion_feature(lion_row("0000002", 2, None)),  # null geometry
        lion_feature(lion_row("0000003", 3, mapping(LineString(LINE_B)), feature_type="F")),
        lion_feature(lion_row("0000004", 4, mapping(MultiLineString([LINE_A, LINE_B])))),
    ]
    path.write_text(json.dumps({"type": "FeatureCollection", "features": features}))

    report = build_segment_geometries(path)

    assert [geometry.segment_id for geometry in report.geometries] == ["0000001"]
    assert report.input_segment_count == 3  # 0000003은 FeatureTyp="F"라 vehicle 아님
    assert report.excluded_geometry_count == 2  # null geometry + multipart geometry


def test_crs_constants_are_wgs84_source_and_state_plane_meters_target() -> None:
    assert SOURCE_CRS == "EPSG:4326"
    assert TARGET_CRS == "EPSG:32118"
