from datetime import UTC, date, datetime

import shapely
from batch_jobs.road_segment.geometry import SegmentGeometry
from batch_jobs.road_segment.transform import RoadSegmentRow
from batch_jobs.road_segment.validate import (
    combine_segment_records,
    deserialize_geometry,
    find_duplicate_keys,
    has_positive_length,
    is_valid_line_geometry,
    missing_required_fields,
    validate_road_segments,
)
from shapely.geometry import LineString, Point

INGESTED_AT = datetime(2026, 8, 13, 0, 0, 0, tzinfo=UTC)
LINE_WKB = shapely.to_wkb(LineString([(0, 0), (1, 1)]))


def road_segment_row(segment_id: str, **overrides: object) -> RoadSegmentRow:
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
        "source_version": "26B",
        "ingested_at": INGESTED_AT,
    }
    fields.update(overrides)
    return RoadSegmentRow(**fields)


def segment_geometry(segment_id: str, geometry_wkb: bytes = LINE_WKB) -> SegmentGeometry:
    return SegmentGeometry(segment_id=segment_id, geometry_wkb=geometry_wkb)


def test_combine_segment_records_matches_by_segment_id() -> None:
    result = combine_segment_records([road_segment_row("0000001")], [segment_geometry("0000001")])

    assert len(result.records) == 1
    assert result.records[0].geometry_wkb == LINE_WKB
    assert result.transform_only_segment_ids == ()
    assert result.geometry_only_segment_ids == ()


def test_combine_segment_records_detects_transform_only_segment() -> None:
    result = combine_segment_records([road_segment_row("0000001")], [])

    assert result.transform_only_segment_ids == ("0000001",)
    assert result.records[0].geometry_wkb is None


def test_combine_segment_records_detects_geometry_only_segment() -> None:
    result = combine_segment_records([], [segment_geometry("0000002")])

    assert result.geometry_only_segment_ids == ("0000002",)
    assert result.records == ()


def test_missing_required_fields_flags_null_from_node_id() -> None:
    record = combine_segment_records(
        [road_segment_row("0000001", from_node_id=None)], [segment_geometry("0000001")]
    ).records[0]

    assert missing_required_fields(record) is True


def test_has_positive_length_rejects_zero_and_negative() -> None:
    zero = combine_segment_records(
        [road_segment_row("0000001", length_m=0.0)], [segment_geometry("0000001")]
    ).records[0]
    negative = combine_segment_records(
        [road_segment_row("0000002", length_m=-1.0)], [segment_geometry("0000002")]
    ).records[0]
    positive = combine_segment_records(
        [road_segment_row("0000003", length_m=1.0)], [segment_geometry("0000003")]
    ).records[0]

    assert has_positive_length(zero) is False
    assert has_positive_length(negative) is False
    assert has_positive_length(positive) is True


def test_deserialize_geometry_returns_none_for_invalid_wkb() -> None:
    assert deserialize_geometry(b"not valid wkb") is None


def test_is_valid_line_geometry_rejects_non_linestring() -> None:
    geometry = deserialize_geometry(shapely.to_wkb(Point(0, 0)))

    assert is_valid_line_geometry(geometry) is False


def test_is_valid_line_geometry_accepts_linestring() -> None:
    assert is_valid_line_geometry(deserialize_geometry(LINE_WKB)) is True


def test_find_duplicate_keys_detects_repeated_segment_id_and_snapshot_date() -> None:
    records = combine_segment_records(
        [road_segment_row("0000001"), road_segment_row("0000001")],
        [segment_geometry("0000001")],
    ).records

    assert find_duplicate_keys(list(records)) == {("0000001", date(2026, 8, 13)): 2}


def test_validate_road_segments_passes_clean_data() -> None:
    report = validate_road_segments([road_segment_row("0000001")], [segment_geometry("0000001")])

    assert len(report.valid_records) == 1
    assert report.rule_failures == {}
    assert report.transform_only_segment_ids == ()
    assert report.geometry_only_segment_ids == ()


def test_validate_road_segments_fails_duplicate_primary_key() -> None:
    rows = [road_segment_row("0000001"), road_segment_row("0000001")]

    report = validate_road_segments(rows, [segment_geometry("0000001")])

    assert report.valid_records == ()
    assert report.rule_failures["duplicate_primary_key"] == ("0000001", "0000001")


def test_validate_road_segments_flags_duplicate_primary_key_even_when_one_row_is_invalid() -> (
    None
):
    # 한쪽이 다른 규칙(필수 컬럼 NULL)으로 먼저 빠지더라도 PK 중복은 놓치면 안 된다.
    rows = [
        road_segment_row("0000001"),
        road_segment_row("0000001", from_node_id=None),
    ]

    report = validate_road_segments(rows, [segment_geometry("0000001")])

    assert report.valid_records == ()
    assert report.rule_failures["duplicate_primary_key"] == ("0000001", "0000001")
    assert report.rule_failures["required_field_null"] == ("0000001",)


def test_validate_road_segments_fails_required_field_null() -> None:
    rows = [road_segment_row("0000001", from_node_id=None)]

    report = validate_road_segments(rows, [segment_geometry("0000001")])

    assert report.valid_records == ()
    assert report.rule_failures["required_field_null"] == ("0000001",)


def test_validate_road_segments_fails_non_positive_length() -> None:
    rows = [
        road_segment_row("0000001", length_m=0.0),
        road_segment_row("0000002", length_m=-1.0),
    ]

    report = validate_road_segments(
        rows, [segment_geometry("0000001"), segment_geometry("0000002")]
    )

    assert report.valid_records == ()
    assert report.rule_failures["length_not_positive"] == ("0000001", "0000002")


def test_validate_road_segments_fails_invalid_wkb() -> None:
    rows = [road_segment_row("0000001")]
    geometries = [segment_geometry("0000001", geometry_wkb=b"not valid wkb")]

    report = validate_road_segments(rows, geometries)

    assert report.valid_records == ()
    assert report.rule_failures["geometry_invalid"] == ("0000001",)


def test_validate_road_segments_fails_non_linestring_wkb() -> None:
    rows = [road_segment_row("0000001")]
    geometries = [segment_geometry("0000001", geometry_wkb=shapely.to_wkb(Point(0, 0)))]

    report = validate_road_segments(rows, geometries)

    assert report.valid_records == ()
    assert report.rule_failures["geometry_invalid"] == ("0000001",)


def test_validate_road_segments_detects_transform_only_and_geometry_only() -> None:
    rows = [road_segment_row("0000001"), road_segment_row("0000002")]
    geometries = [segment_geometry("0000001"), segment_geometry("0000003")]

    report = validate_road_segments(rows, geometries)

    assert report.transform_only_segment_ids == ("0000002",)
    assert report.geometry_only_segment_ids == ("0000003",)
    assert [record.segment_id for record in report.valid_records] == ["0000001"]
