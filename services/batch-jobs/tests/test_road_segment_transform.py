import json
from datetime import UTC, date, datetime

from road_segment.transform import (
    build_road_segment_row,
    feature_type_is_vehicle,
    find_duplicate_segment_ids,
    is_constructed,
    is_vehicle_segment,
    load_lion_rows,
    profile_distinct_values,
    roadway_type_is_vehicle,
    select_representative_rows,
    traffic_direction_is_vehicle,
    transform_road_segments,
)


def lion_row(
    segment_id: str,
    object_id: int,
    street: str = "TEST STREET",
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
        "Street": street,
    }
    row.update(overrides)
    return row


def lion_feature(properties: dict[str, object]) -> dict[str, object]:
    return {"type": "Feature", "properties": properties, "geometry": None}


def test_load_lion_rows_reads_feature_properties(tmp_path) -> None:
    path = tmp_path / "lion.geojson"
    path.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [lion_feature(lion_row("0000001", 1))],
            }
        )
    )

    rows = load_lion_rows(path)

    assert len(rows) == 1
    assert rows[0]["SegmentID"] == "0000001"


def test_profile_distinct_values_counts_each_column() -> None:
    rows = [
        lion_row("0000001", 1, feature_type="0"),
        lion_row("0000002", 2, feature_type="0"),
        lion_row("0000003", 3, feature_type="6"),
    ]

    profile = profile_distinct_values(rows, columns=("FeatureTyp",))

    assert profile["FeatureTyp"] == {"0": 2, "6": 1}


def test_find_duplicate_segment_ids_only_reports_counts_above_one() -> None:
    rows = [
        lion_row("0000001", 1),
        lion_row("0000001", 2),
        lion_row("0000002", 3),
    ]

    duplicates = find_duplicate_segment_ids(rows)

    assert duplicates == {"0000001": 2}


def test_feature_type_is_vehicle_accepts_street_alley_and_public_use_types() -> None:
    assert feature_type_is_vehicle(lion_row("0000001", 1, feature_type="0")) is True
    assert feature_type_is_vehicle(lion_row("0000001", 1, feature_type="6")) is True
    assert feature_type_is_vehicle(lion_row("0000001", 1, feature_type="A")) is True
    assert feature_type_is_vehicle(lion_row("0000001", 1, feature_type="C")) is True
    assert feature_type_is_vehicle(lion_row("0000001", 1, feature_type="F")) is False


def test_is_constructed_requires_status_2() -> None:
    assert is_constructed(lion_row("0000001", 1, Status="2")) is True
    assert is_constructed(lion_row("0000001", 1, Status="3")) is False
    assert is_constructed(lion_row("0000001", 1, Status="")) is False


def test_roadway_type_is_vehicle_excludes_path_and_ferry() -> None:
    assert roadway_type_is_vehicle(lion_row("0000001", 1, RW_TYPE=" 1")) is True
    assert roadway_type_is_vehicle(lion_row("0000001", 1, RW_TYPE=" 6")) is False
    assert roadway_type_is_vehicle(lion_row("0000001", 1, RW_TYPE="14")) is False


def test_traffic_direction_is_vehicle_excludes_pedestrian_and_blank() -> None:
    assert traffic_direction_is_vehicle(lion_row("0000001", 1, TrafDir="T")) is True
    assert traffic_direction_is_vehicle(lion_row("0000001", 1, TrafDir="P")) is False
    assert traffic_direction_is_vehicle(lion_row("0000001", 1, TrafDir="")) is False


def test_is_vehicle_segment_requires_all_four_conditions() -> None:
    vehicle_street = lion_row("0000001", 1)
    ferry = lion_row("0000002", 2, feature_type="F", RW_TYPE="14")
    paper_street = lion_row("0000003", 3, feature_type="6", Status="3")
    path_trail = lion_row("0000004", 4, RW_TYPE=" 6")
    pedestrian_only = lion_row("0000005", 5, TrafDir="P")

    assert is_vehicle_segment(vehicle_street) is True
    assert is_vehicle_segment(ferry) is False
    assert is_vehicle_segment(paper_street) is False
    assert is_vehicle_segment(path_trail) is False
    assert is_vehicle_segment(pedestrian_only) is False


def test_select_representative_rows_keeps_lowest_object_id() -> None:
    rows = [
        lion_row("0000001", 20, street="ALIAS A"),
        lion_row("0000001", 10, street="ALIAS B"),
        lion_row("0000001", 30, street="ALIAS C"),
    ]

    representative = select_representative_rows(rows)

    assert len(representative) == 1
    assert representative[0]["OBJECTID"] == 10
    assert representative[0]["Street"] == "ALIAS B"


def test_select_representative_rows_drops_rows_without_segment_id() -> None:
    rows = [lion_row("", 1), lion_row("0000001", 2)]

    representative = select_representative_rows(rows)

    assert [row["SegmentID"] for row in representative] == ["0000001"]


def test_build_road_segment_row_converts_units_and_normalizes_blanks() -> None:
    row = lion_row(
        "0000001",
        1,
        NodeIDFrom="0047201",
        Radius=100,
        Shape__Length=200.0,
        POSTED_SPEED="  ",
        CurveFlag=" ",
    )
    ingested_at = datetime(2026, 8, 13, 0, 0, 0, tzinfo=UTC)

    result = build_road_segment_row(row, date(2026, 8, 13), "26B", ingested_at)

    assert result.segment_id == "0000001"
    assert result.from_node_id == "0047201"
    assert result.curve_radius_m == 100 * 0.3048
    assert result.length_m == 200.0 * 0.3048
    assert result.posted_speed_mph is None
    assert result.curve_flag is None
    assert result.source_version == "26B"
    assert result.ingested_at == ingested_at


def test_build_road_segment_row_keeps_zero_curve_radius_distinct_from_null() -> None:
    row = lion_row("0000001", 1, Radius=0)

    result = build_road_segment_row(row, date(2026, 8, 13), "26B", datetime.now(UTC))

    assert result.curve_radius_m == 0.0


def test_transform_road_segments_end_to_end(tmp_path) -> None:
    path = tmp_path / "lion.geojson"
    path.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    lion_feature(lion_row("0000001", 1, feature_type="0")),
                    lion_feature(lion_row("0000001", 2, feature_type="0")),
                    lion_feature(lion_row("0000002", 3, feature_type="F")),
                ],
            }
        )
    )
    ingested_at = datetime(2026, 8, 13, 0, 0, 0, tzinfo=UTC)

    result = transform_road_segments(path, date(2026, 8, 13), "26B", ingested_at)

    assert [row.segment_id for row in result] == ["0000001"]
