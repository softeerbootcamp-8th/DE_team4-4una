import json
import os
import time
import urllib.parse
import zipfile
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from typing import ClassVar

import duckdb
import pytest
import shapefile
import shapely
from batch_jobs.hourly_segment_feature_job import (
    HourlySegmentFeatureJobConfig,
    run_hourly_segment_feature_job,
)
from batch_jobs.pipeline import build_and_publish_environment
from batch_jobs.road_segment.build import build_road_segments
from batch_jobs.road_segment.geometry import (
    SOURCE_CRS,
    TARGET_CRS,
    SegmentGeometry,
    build_segment_geometries,
    build_segment_geometry,
    geometry_from_wkb,
    load_lion_rows_with_geometry,
    normalize_line_geometry,
    reproject_to_target_crs,
)
from batch_jobs.road_segment.persist import (
    read_road_segment_parquet,
    write_road_segment_snapshot,
)
from batch_jobs.road_segment.taxi_zone import (
    assign_taxi_zones,
    find_location_id,
    load_taxi_zones,
)
from batch_jobs.road_segment.transform import (
    RoadSegmentRow,
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
from batch_jobs.road_segment.validate import (
    RoadSegmentRecord,
    combine_segment_records,
    deserialize_geometry,
    find_duplicate_keys,
    has_positive_length,
    is_valid_line_geometry,
    missing_required_fields,
    validate_road_segments,
)
from batch_jobs.schemas import (
    PROCESSED_SENSOR_EVENT_SCHEMA,
    RAW_RECORD_COLUMN,
    SENSOR_EVENT_QUARANTINE_SCHEMA,
)
from pyproj import CRS, Transformer
from pyspark.sql import SparkSession
from shapely import STRtree
from shapely import wkt as shapely_wkt
from shapely.geometry import LineString, MultiLineString, Point, Polygon, mapping

# collect()가 돌려주는 timestamp는 이 파이썬 프로세스의 로컬 타임존으로 변환되어,
# 고정하지 않으면 실행 머신마다(Asia/Seoul vs UTC) 값이 달라진다.
os.environ["TZ"] = "UTC"
time.tzset()

INGESTED_AT = datetime(2026, 8, 13, 0, 0, 0, tzinfo=UTC)
LINE_WKB = shapely.to_wkb(LineString([(0, 0), (1, 1)]))


class TestTransform:
    @staticmethod
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

    @staticmethod
    def lion_feature(properties: dict[str, object]) -> dict[str, object]:
        return {"type": "Feature", "properties": properties, "geometry": None}

    def test_load_lion_rows_reads_feature_properties(self, tmp_path) -> None:
        path = tmp_path / "lion.geojson"
        path.write_text(
            json.dumps(
                {
                    "type": "FeatureCollection",
                    "features": [self.lion_feature(self.lion_row("0000001", 1))],
                }
            )
        )

        rows = load_lion_rows(path)

        assert len(rows) == 1
        assert rows[0]["SegmentID"] == "0000001"

    def test_profile_distinct_values_counts_each_column(self) -> None:
        rows = [
            self.lion_row("0000001", 1, feature_type="0"),
            self.lion_row("0000002", 2, feature_type="0"),
            self.lion_row("0000003", 3, feature_type="6"),
        ]

        profile = profile_distinct_values(rows, columns=("FeatureTyp",))

        assert profile["FeatureTyp"] == {"0": 2, "6": 1}

    def test_find_duplicate_segment_ids_only_reports_counts_above_one(self) -> None:
        rows = [
            self.lion_row("0000001", 1),
            self.lion_row("0000001", 2),
            self.lion_row("0000002", 3),
        ]

        duplicates = find_duplicate_segment_ids(rows)

        assert duplicates == {"0000001": 2}

    def test_feature_type_is_vehicle_accepts_street_alley_and_public_use_types(self) -> None:
        assert feature_type_is_vehicle(self.lion_row("0000001", 1, feature_type="0")) is True
        assert feature_type_is_vehicle(self.lion_row("0000001", 1, feature_type="6")) is True
        assert feature_type_is_vehicle(self.lion_row("0000001", 1, feature_type="A")) is True
        assert feature_type_is_vehicle(self.lion_row("0000001", 1, feature_type="C")) is True
        assert feature_type_is_vehicle(self.lion_row("0000001", 1, feature_type="F")) is False

    def test_is_constructed_requires_status_2(self) -> None:
        assert is_constructed(self.lion_row("0000001", 1, Status="2")) is True
        assert is_constructed(self.lion_row("0000001", 1, Status="3")) is False
        assert is_constructed(self.lion_row("0000001", 1, Status="")) is False

    def test_roadway_type_is_vehicle_excludes_path_and_ferry(self) -> None:
        assert roadway_type_is_vehicle(self.lion_row("0000001", 1, RW_TYPE=" 1")) is True
        assert roadway_type_is_vehicle(self.lion_row("0000001", 1, RW_TYPE=" 6")) is False
        assert roadway_type_is_vehicle(self.lion_row("0000001", 1, RW_TYPE="14")) is False

    def test_traffic_direction_is_vehicle_excludes_pedestrian_and_blank(self) -> None:
        assert traffic_direction_is_vehicle(self.lion_row("0000001", 1, TrafDir="T")) is True
        assert traffic_direction_is_vehicle(self.lion_row("0000001", 1, TrafDir="P")) is False
        assert traffic_direction_is_vehicle(self.lion_row("0000001", 1, TrafDir="")) is False

    def test_is_vehicle_segment_requires_all_four_conditions(self) -> None:
        vehicle_street = self.lion_row("0000001", 1)
        ferry = self.lion_row("0000002", 2, feature_type="F", RW_TYPE="14")
        paper_street = self.lion_row("0000003", 3, feature_type="6", Status="3")
        path_trail = self.lion_row("0000004", 4, RW_TYPE=" 6")
        pedestrian_only = self.lion_row("0000005", 5, TrafDir="P")

        assert is_vehicle_segment(vehicle_street) is True
        assert is_vehicle_segment(ferry) is False
        assert is_vehicle_segment(paper_street) is False
        assert is_vehicle_segment(path_trail) is False
        assert is_vehicle_segment(pedestrian_only) is False

    def test_select_representative_rows_keeps_lowest_object_id(self) -> None:
        rows = [
            self.lion_row("0000001", 20, street="ALIAS A"),
            self.lion_row("0000001", 10, street="ALIAS B"),
            self.lion_row("0000001", 30, street="ALIAS C"),
        ]

        representative = select_representative_rows(rows)

        assert len(representative) == 1
        assert representative[0]["OBJECTID"] == 10
        assert representative[0]["Street"] == "ALIAS B"

    def test_select_representative_rows_drops_rows_without_segment_id(self) -> None:
        rows = [self.lion_row("", 1), self.lion_row("0000001", 2)]

        representative = select_representative_rows(rows)

        assert [row["SegmentID"] for row in representative] == ["0000001"]

    def test_build_road_segment_row_converts_units_and_normalizes_blanks(self) -> None:
        row = self.lion_row(
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

    def test_build_road_segment_row_keeps_zero_curve_radius_distinct_from_null(self) -> None:
        row = self.lion_row("0000001", 1, Radius=0)

        result = build_road_segment_row(row, date(2026, 8, 13), "26B", datetime.now(UTC))

        assert result.curve_radius_m == 0.0

    def test_transform_road_segments_end_to_end(self, tmp_path) -> None:
        path = tmp_path / "lion.geojson"
        path.write_text(
            json.dumps(
                {
                    "type": "FeatureCollection",
                    "features": [
                        self.lion_feature(self.lion_row("0000001", 1, feature_type="0")),
                        self.lion_feature(self.lion_row("0000001", 2, feature_type="0")),
                        self.lion_feature(self.lion_row("0000002", 3, feature_type="F")),
                    ],
                }
            )
        )
        ingested_at = datetime(2026, 8, 13, 0, 0, 0, tzinfo=UTC)

        result = transform_road_segments(path, date(2026, 8, 13), "26B", ingested_at)

        assert [row.segment_id for row in result] == ["0000001"]


class TestGeometry:
    LINE_A: ClassVar = [(-73.99, 40.67), (-73.989, 40.671)]
    LINE_B: ClassVar = [(-73.989, 40.671), (-73.988, 40.672)]

    @staticmethod
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

    @staticmethod
    def lion_feature(row: dict[str, object]) -> dict[str, object]:
        properties = {key: value for key, value in row.items() if key != "_geometry"}
        return {"type": "Feature", "properties": properties, "geometry": row["_geometry"]}

    def test_reproject_to_target_crs_converts_wgs84_to_state_plane_meters(self) -> None:
        point = shapely.Point(-73.99, 40.67)

        projected = reproject_to_target_crs(point)

        assert projected.x > 10_000
        assert projected.y > 10_000

    def test_reproject_to_target_crs_distance_is_reasonable_in_meters(self) -> None:
        # 위도 0.001도 차이는 어디서나 약 111m. feet 단위였다면 약 364ft로
        # 이 범위를 크게 벗어나므로, meter 단위로 나온다는 걸 확인하는 회귀 테스트.
        point_a = reproject_to_target_crs(shapely.Point(-73.99, 40.700))
        point_b = reproject_to_target_crs(shapely.Point(-73.99, 40.701))

        assert 100 < point_a.distance(point_b) < 120

    def test_normalize_line_geometry_returns_none_for_missing_geometry(self) -> None:
        assert normalize_line_geometry(None) is None

    def test_normalize_line_geometry_returns_none_for_empty_linestring(self) -> None:
        empty = mapping(LineString())

        assert normalize_line_geometry(empty) is None

    def test_normalize_line_geometry_returns_none_for_unsupported_type(self) -> None:
        point = mapping(Point(-73.99, 40.67))

        assert normalize_line_geometry(point) is None

    def test_normalize_line_geometry_keeps_existing_linestring(self) -> None:
        raw = mapping(LineString(self.LINE_A))

        line = normalize_line_geometry(raw)

        assert line is not None
        assert line.geom_type == "LineString"

    def test_normalize_line_geometry_converts_single_part_multilinestring(self) -> None:
        raw = mapping(MultiLineString([self.LINE_A]))

        line = normalize_line_geometry(raw)

        assert line is not None
        assert line.geom_type == "LineString"

    def test_normalize_line_geometry_excludes_multipart_multilinestring_even_when_connected(
        self,
    ) -> None:
        # part가 이어져 있어도 방향을 보장할 수 없으므로 제외한다.
        raw = mapping(MultiLineString([self.LINE_A, self.LINE_B]))

        assert normalize_line_geometry(raw) is None

    def test_normalize_line_geometry_rejects_disconnected_multilinestring(self) -> None:
        raw = mapping(MultiLineString([self.LINE_A, [(-73.95, 40.70), (-73.94, 40.71)]]))

        assert normalize_line_geometry(raw) is None

    def test_normalize_line_geometry_preserves_coordinate_order_for_linestring(self) -> None:
        raw = mapping(LineString(self.LINE_A))

        line = normalize_line_geometry(raw)

        expected = reproject_to_target_crs(LineString(self.LINE_A))
        assert line.coords[0] == expected.coords[0]
        assert line.coords[-1] == expected.coords[-1]

    def test_load_lion_rows_with_geometry_keeps_geometry_alongside_properties(
        self, tmp_path
    ) -> None:
        path = tmp_path / "lion.geojson"
        row = self.lion_row("0000001", 1, mapping(LineString(self.LINE_A)))
        path.write_text(
            json.dumps({"type": "FeatureCollection", "features": [self.lion_feature(row)]})
        )

        rows = load_lion_rows_with_geometry(path)

        assert rows[0]["SegmentID"] == "0000001"
        assert rows[0]["_geometry"]["type"] == "LineString"

    def test_build_segment_geometry_round_trips_through_wkb(self) -> None:
        row = self.lion_row("0000001", 1, mapping(LineString(self.LINE_A)))

        result = build_segment_geometry(row)

        assert result is not None
        assert result.segment_id == "0000001"
        restored = geometry_from_wkb(result.geometry_wkb)
        assert restored.geom_type == "LineString"
        expected = reproject_to_target_crs(LineString(self.LINE_A))
        assert restored.equals_exact(expected, tolerance=1e-6)

    def test_build_segment_geometry_returns_none_for_null_geometry(self) -> None:
        row = self.lion_row("0000001", 1, None)

        assert build_segment_geometry(row) is None

    def test_build_segment_geometries_excludes_invalid_and_multipart_geometry(
        self, tmp_path
    ) -> None:
        path = tmp_path / "lion.geojson"
        features = [
            self.lion_feature(self.lion_row("0000001", 1, mapping(LineString(self.LINE_A)))),
            self.lion_feature(self.lion_row("0000002", 2, None)),  # null geometry
            self.lion_feature(
                self.lion_row("0000003", 3, mapping(LineString(self.LINE_B)), feature_type="F")
            ),
            self.lion_feature(
                self.lion_row("0000004", 4, mapping(MultiLineString([self.LINE_A, self.LINE_B])))
            ),
        ]
        path.write_text(json.dumps({"type": "FeatureCollection", "features": features}))

        report = build_segment_geometries(path)

        assert [geometry.segment_id for geometry in report.geometries] == ["0000001"]
        assert report.input_segment_count == 3  # 0000003은 FeatureTyp="F"라 vehicle 아님
        assert report.excluded_geometry_count == 2  # null geometry + multipart geometry

    def test_crs_constants_are_wgs84_source_and_state_plane_meters_target(self) -> None:
        assert SOURCE_CRS == "EPSG:4326"
        assert TARGET_CRS == "EPSG:32118"


class TestValidate:
    @staticmethod
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

    @staticmethod
    def segment_geometry(segment_id: str, geometry_wkb: bytes = LINE_WKB) -> SegmentGeometry:
        return SegmentGeometry(segment_id=segment_id, geometry_wkb=geometry_wkb)

    def test_combine_segment_records_matches_by_segment_id(self) -> None:
        result = combine_segment_records(
            [self.road_segment_row("0000001")], [self.segment_geometry("0000001")]
        )

        assert len(result.records) == 1
        assert result.records[0].geometry_wkb == LINE_WKB
        assert result.transform_only_segment_ids == ()
        assert result.geometry_only_segment_ids == ()

    def test_combine_segment_records_detects_transform_only_segment(self) -> None:
        result = combine_segment_records([self.road_segment_row("0000001")], [])

        assert result.transform_only_segment_ids == ("0000001",)
        assert result.records[0].geometry_wkb is None

    def test_combine_segment_records_detects_geometry_only_segment(self) -> None:
        result = combine_segment_records([], [self.segment_geometry("0000002")])

        assert result.geometry_only_segment_ids == ("0000002",)
        assert result.records == ()

    def test_missing_required_fields_flags_null_from_node_id(self) -> None:
        record = combine_segment_records(
            [self.road_segment_row("0000001", from_node_id=None)],
            [self.segment_geometry("0000001")],
        ).records[0]

        assert missing_required_fields(record) is True

    def test_has_positive_length_rejects_zero_and_negative(self) -> None:
        zero = combine_segment_records(
            [self.road_segment_row("0000001", length_m=0.0)], [self.segment_geometry("0000001")]
        ).records[0]
        negative = combine_segment_records(
            [self.road_segment_row("0000002", length_m=-1.0)], [self.segment_geometry("0000002")]
        ).records[0]
        positive = combine_segment_records(
            [self.road_segment_row("0000003", length_m=1.0)], [self.segment_geometry("0000003")]
        ).records[0]

        assert has_positive_length(zero) is False
        assert has_positive_length(negative) is False
        assert has_positive_length(positive) is True

    def test_deserialize_geometry_returns_none_for_invalid_wkb(self) -> None:
        assert deserialize_geometry(b"not valid wkb") is None

    def test_is_valid_line_geometry_rejects_non_linestring(self) -> None:
        geometry = deserialize_geometry(shapely.to_wkb(Point(0, 0)))

        assert is_valid_line_geometry(geometry) is False

    def test_is_valid_line_geometry_accepts_linestring(self) -> None:
        assert is_valid_line_geometry(deserialize_geometry(LINE_WKB)) is True

    def test_find_duplicate_keys_detects_repeated_segment_id_and_snapshot_date(self) -> None:
        records = combine_segment_records(
            [self.road_segment_row("0000001"), self.road_segment_row("0000001")],
            [self.segment_geometry("0000001")],
        ).records

        assert find_duplicate_keys(list(records)) == {("0000001", date(2026, 8, 13)): 2}

    def test_validate_road_segments_passes_clean_data(self) -> None:
        report = validate_road_segments(
            [self.road_segment_row("0000001")], [self.segment_geometry("0000001")]
        )

        assert len(report.valid_records) == 1
        assert report.rule_failures == {}
        assert report.transform_only_segment_ids == ()
        assert report.geometry_only_segment_ids == ()

    def test_validate_road_segments_fails_duplicate_primary_key(self) -> None:
        rows = [self.road_segment_row("0000001"), self.road_segment_row("0000001")]

        report = validate_road_segments(rows, [self.segment_geometry("0000001")])

        assert report.valid_records == ()
        assert report.rule_failures["duplicate_primary_key"] == ("0000001", "0000001")

    def test_validate_road_segments_flags_duplicate_primary_key_even_when_one_row_is_invalid(
        self,
    ) -> None:
        # 한쪽이 다른 규칙(필수 컬럼 NULL)으로 먼저 빠지더라도 PK 중복은 놓치면 안 된다.
        rows = [
            self.road_segment_row("0000001"),
            self.road_segment_row("0000001", from_node_id=None),
        ]

        report = validate_road_segments(rows, [self.segment_geometry("0000001")])

        assert report.valid_records == ()
        assert report.rule_failures["duplicate_primary_key"] == ("0000001", "0000001")
        assert report.rule_failures["required_field_null"] == ("0000001",)

    def test_validate_road_segments_fails_required_field_null(self) -> None:
        rows = [self.road_segment_row("0000001", from_node_id=None)]

        report = validate_road_segments(rows, [self.segment_geometry("0000001")])

        assert report.valid_records == ()
        assert report.rule_failures["required_field_null"] == ("0000001",)

    def test_validate_road_segments_fails_non_positive_length(self) -> None:
        rows = [
            self.road_segment_row("0000001", length_m=0.0),
            self.road_segment_row("0000002", length_m=-1.0),
        ]

        report = validate_road_segments(
            rows, [self.segment_geometry("0000001"), self.segment_geometry("0000002")]
        )

        assert report.valid_records == ()
        assert report.rule_failures["length_not_positive"] == ("0000001", "0000002")

    def test_validate_road_segments_fails_invalid_wkb(self) -> None:
        rows = [self.road_segment_row("0000001")]
        geometries = [self.segment_geometry("0000001", geometry_wkb=b"not valid wkb")]

        report = validate_road_segments(rows, geometries)

        assert report.valid_records == ()
        assert report.rule_failures["geometry_invalid"] == ("0000001",)

    def test_validate_road_segments_fails_non_linestring_wkb(self) -> None:
        rows = [self.road_segment_row("0000001")]
        geometries = [self.segment_geometry("0000001", geometry_wkb=shapely.to_wkb(Point(0, 0)))]

        report = validate_road_segments(rows, geometries)

        assert report.valid_records == ()
        assert report.rule_failures["geometry_invalid"] == ("0000001",)

    def test_validate_road_segments_detects_transform_only_and_geometry_only(self) -> None:
        rows = [self.road_segment_row("0000001"), self.road_segment_row("0000002")]
        geometries = [self.segment_geometry("0000001"), self.segment_geometry("0000003")]

        report = validate_road_segments(rows, geometries)

        assert report.transform_only_segment_ids == ("0000002",)
        assert report.geometry_only_segment_ids == ("0000003",)
        assert [record.segment_id for record in report.valid_records] == ["0000001"]


class TestTaxiZone:
    ZONE_A = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
    ZONE_B = Polygon([(20, 0), (30, 0), (30, 10), (20, 10)])
    OVERLAPPING_A = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
    OVERLAPPING_B = Polygon([(5, 5), (15, 5), (15, 15), (5, 15)])

    @staticmethod
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

    @staticmethod
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

    def test_find_location_id_returns_zone_containing_midpoint(self) -> None:
        zone_ids = [181, 182]
        zone_geometries = [self.ZONE_A, self.ZONE_B]
        tree = STRtree(zone_geometries)
        line = LineString([(2, 2), (8, 8)])

        assert find_location_id(line, zone_ids, zone_geometries, tree) == 181

    def test_find_location_id_returns_none_outside_all_zones(self) -> None:
        zone_ids = [181, 182]
        zone_geometries = [self.ZONE_A, self.ZONE_B]
        tree = STRtree(zone_geometries)
        line = LineString([(100, 100), (101, 101)])

        assert find_location_id(line, zone_ids, zone_geometries, tree) is None

    def test_find_location_id_picks_a_single_zone_when_overlapping(self) -> None:
        zone_ids = [181, 182]
        zone_geometries = [self.OVERLAPPING_A, self.OVERLAPPING_B]
        tree = STRtree(zone_geometries)
        line = LineString([(6, 6), (7, 7)])  # midpoint (6.5, 6.5) is inside both zones

        assert find_location_id(line, zone_ids, zone_geometries, tree) in zone_ids

    def test_assign_taxi_zones_sets_location_id_and_preserves_other_fields(self) -> None:
        record = self.sample_record("0000001")
        taxi_zones = {181: self.ZONE_A, 182: self.ZONE_B}

        report = assign_taxi_zones([record], taxi_zones)

        assert len(report.records) == 1
        assigned = report.records[0]
        assert assigned.location_id == 181
        assert assigned == replace(record, location_id=181)
        assert report.unmatched_segment_ids == ()

    def test_assign_taxi_zones_tracks_unmatched_when_no_zone_covers_segment(self) -> None:
        record = self.sample_record(
            "0000002", geometry_wkb=shapely.to_wkb(LineString([(100, 100), (101, 101)]))
        )
        taxi_zones = {181: self.ZONE_A}

        report = assign_taxi_zones([record], taxi_zones)

        assert report.records[0].location_id is None
        assert report.unmatched_segment_ids == ("0000002",)

    def test_assign_taxi_zones_tracks_unmatched_when_geometry_is_missing(self) -> None:
        record = self.sample_record("0000003", geometry_wkb=None)
        taxi_zones = {181: self.ZONE_A}

        report = assign_taxi_zones([record], taxi_zones)

        assert report.records[0].location_id is None
        assert report.unmatched_segment_ids == ("0000003",)

    def test_assign_taxi_zones_does_not_duplicate_or_drop_records(self) -> None:
        records = [self.sample_record("0000001"), self.sample_record("0000002")]
        taxi_zones = {181: self.ZONE_A}

        report = assign_taxi_zones(records, taxi_zones)

        assert len(report.records) == len(records)

    def test_load_taxi_zones_reprojects_to_target_crs(self, tmp_path) -> None:
        source_crs = CRS.from_epsg(2263)
        square = Polygon([(980000, 200000), (981000, 200000), (981000, 201000), (980000, 201000)])
        zip_path = tmp_path / "taxi_zones.zip"
        self.write_taxi_zone_zip(zip_path, {181: square}, source_crs)

        zones = load_taxi_zones(zip_path)

        assert set(zones) == {181}
        transformer = Transformer.from_crs(source_crs, TARGET_CRS, always_xy=True)
        expected_x, expected_y = transformer.transform(980000, 200000)
        actual_x, actual_y = zones[181].exterior.coords[0]
        assert actual_x == pytest.approx(expected_x)
        assert actual_y == pytest.approx(expected_y)


class TestPersist:
    EXPECTED_COLUMNS: ClassVar = [
        "segment_id",
        "snapshot_date",
        "street_name",
        "from_node_id",
        "to_node_id",
        "traffic_direction",
        "segment_type",
        "feature_type",
        "roadway_type",
        "roadbed_layer",
        "from_node_level",
        "to_node_level",
        "posted_speed_mph",
        "curve_flag",
        "curve_radius_m",
        "length_m",
        "geometry_wkb",
        "source_version",
        "ingested_at",
        "location_id",
    ]

    @staticmethod
    def sample_record(
        segment_id: str, snapshot_date: date = date(2026, 8, 13), **overrides: object
    ) -> RoadSegmentRecord:
        fields = {
            "segment_id": segment_id,
            "snapshot_date": snapshot_date,
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
            "geometry_wkb": LINE_WKB,
            "source_version": "26B",
            "ingested_at": datetime(2026, 8, 13, 0, 0, 0, tzinfo=UTC),
        }
        fields.update(overrides)
        return RoadSegmentRecord(**fields)

    def test_write_road_segment_snapshot_creates_partition_directory(self, tmp_path) -> None:
        records = [self.sample_record("0000001"), self.sample_record("0000002")]

        path = write_road_segment_snapshot(records, tmp_path)

        assert path == tmp_path / "snapshot_date=2026-08-13" / "data.parquet"
        assert path.is_file()

    def test_write_and_read_road_segment_snapshot_round_trips(self, tmp_path) -> None:
        records = [self.sample_record("0000001"), self.sample_record("0000002")]

        path = write_road_segment_snapshot(records, tmp_path)
        summary = read_road_segment_parquet(path)

        assert summary.row_count == 2
        assert [name for name, _ in summary.columns] == self.EXPECTED_COLUMNS

    def test_write_road_segment_snapshot_preserves_geometry_wkb(self, tmp_path) -> None:
        path = write_road_segment_snapshot([self.sample_record("0000001")], tmp_path)

        row = duckdb.sql(
            "SELECT segment_id, geometry_wkb FROM read_parquet(?)", params=[str(path)]
        ).fetchone()

        assert row[0] == "0000001"
        assert shapely.from_wkb(row[1]).geom_type == "LineString"

    def test_write_road_segment_snapshot_preserves_utc_instant(self, tmp_path) -> None:
        # plain TIMESTAMP는 DuckDB 세션 타임존으로 값을 재해석해버리므로,
        # TIMESTAMPTZ가 실제 UTC 시각(epoch)을 그대로 보존하는지 확인한다.
        ingested_at = datetime(2026, 8, 13, 12, 0, 0, tzinfo=UTC)
        record = replace(self.sample_record("0000001"), ingested_at=ingested_at)
        path = write_road_segment_snapshot([record], tmp_path)

        epoch_seconds = duckdb.sql(
            "SELECT epoch(ingested_at) FROM read_parquet(?)", params=[str(path)]
        ).fetchone()[0]

        assert epoch_seconds == ingested_at.timestamp()

    def test_write_road_segment_snapshot_rejects_naive_ingested_at(self, tmp_path) -> None:
        naive = datetime(2026, 8, 13, 12, 0, 0)  # noqa: DTZ001 (deliberately naive for this test)
        record = replace(self.sample_record("0000001"), ingested_at=naive)

        with pytest.raises(ValueError, match="ingested_at must be UTC"):
            write_road_segment_snapshot([record], tmp_path)

    def test_write_road_segment_snapshot_rejects_non_utc_ingested_at(self, tmp_path) -> None:
        non_utc = datetime(2026, 8, 13, 12, 0, 0, tzinfo=timezone(timedelta(hours=9)))
        record = replace(self.sample_record("0000001"), ingested_at=non_utc)

        with pytest.raises(ValueError, match="ingested_at must be UTC"):
            write_road_segment_snapshot([record], tmp_path)

    def test_write_road_segment_snapshot_rejects_duplicate_segment_id(self, tmp_path) -> None:
        records = [self.sample_record("0000001"), self.sample_record("0000001")]

        with pytest.raises(ValueError, match="duplicate segment_id"):
            write_road_segment_snapshot(records, tmp_path)

    def test_write_road_segment_snapshot_rejects_mixed_snapshot_dates(self, tmp_path) -> None:
        records = [
            self.sample_record("0000001", snapshot_date=date(2026, 8, 13)),
            self.sample_record("0000002", snapshot_date=date(2026, 5, 1)),
        ]

        with pytest.raises(ValueError, match="multiple snapshot_date"):
            write_road_segment_snapshot(records, tmp_path)

    def test_new_snapshot_does_not_touch_existing_partitions(self, tmp_path) -> None:
        write_road_segment_snapshot(
            [self.sample_record("0000001", snapshot_date=date(2026, 5, 1))], tmp_path
        )

        write_road_segment_snapshot(
            [self.sample_record("0000002", snapshot_date=date(2026, 8, 1))], tmp_path
        )

        old_partition = tmp_path / "snapshot_date=2026-05-01" / "data.parquet"
        new_partition = tmp_path / "snapshot_date=2026-08-01" / "data.parquet"
        assert read_road_segment_parquet(old_partition).row_count == 1
        assert read_road_segment_parquet(new_partition).row_count == 1

    def test_rerunning_same_snapshot_date_replaces_instead_of_appending(self, tmp_path) -> None:
        write_road_segment_snapshot(
            [self.sample_record("0000001", snapshot_date=date(2026, 8, 1))], tmp_path
        )

        write_road_segment_snapshot(
            [
                self.sample_record("0000002", snapshot_date=date(2026, 8, 1)),
                self.sample_record("0000003", snapshot_date=date(2026, 8, 1)),
            ],
            tmp_path,
        )

        partition = tmp_path / "snapshot_date=2026-08-01" / "data.parquet"
        summary = read_road_segment_parquet(partition)
        assert summary.row_count == 2
        segment_ids = duckdb.sql(
            "SELECT segment_id FROM read_parquet(?)", params=[str(partition)]
        ).fetchall()
        assert {row[0] for row in segment_ids} == {"0000002", "0000003"}

    def test_read_road_segment_parquet_queries_full_history_across_partitions(
        self, tmp_path
    ) -> None:
        write_road_segment_snapshot(
            [self.sample_record("0000001", snapshot_date=date(2026, 5, 1))], tmp_path
        )
        write_road_segment_snapshot(
            [self.sample_record("0000002", snapshot_date=date(2026, 8, 1))], tmp_path
        )

        history_glob = str(tmp_path / "snapshot_date=*" / "data.parquet")
        summary = read_road_segment_parquet(history_glob)

        assert summary.row_count == 2
        assert [name for name, _ in summary.columns] == self.EXPECTED_COLUMNS


class TestBuild:
    SNAPSHOT = date(2026, 8, 13)
    SOURCE_VERSION = "26B"
    TARGET_HOUR = datetime(2026, 8, 16, 10, 0, 0, tzinfo=UTC)
    PROCESSED_AT = datetime(2026, 8, 16, 12, 0, 0, tzinfo=UTC)
    FEATURE_VERSION = "v1"
    RUN_ID = "run-1"

    # Manhattan 근처 실제 위경도. 이 영역을 덮는 Taxi Zone 하나만 두고, 완전히 벗어난 지점도 함께 쓴다.
    IN_ZONE_COORDINATES: ClassVar = [(-73.99, 40.67), (-73.989, 40.671)]
    OUT_OF_ZONE_COORDINATES: ClassVar = [(-73.5, 40.0), (-73.499, 40.001)]
    ZONE_BBOX: ClassVar = [
        (-74.0, 40.60),
        (-73.95, 40.60),
        (-73.95, 40.80),
        (-74.0, 40.80),
        (-74.0, 40.60),
    ]

    # 짧은 남북 도로(약 111m)의 중앙점 — 기본 Map Matching 반경(30m) 안에서 매칭돼야 한다.
    BASE_LAT, BASE_LON = 40.7484, -73.9857
    LAT_OFFSET = 0.0005
    _FORWARD = Transformer.from_crs("EPSG:4326", "EPSG:32118", always_xy=True)

    @classmethod
    @pytest.fixture(scope="class")
    def spark(cls):
        session = (
            SparkSession.builder.appName("batch-jobs-tests")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        yield session
        session.stop()

    @classmethod
    def lion_row(
        cls,
        segment_id: str,
        coordinates: list[tuple[float, float]],
        object_id: int = 1,
        **overrides: object,
    ) -> dict[str, object]:
        properties = {
            "SegmentID": segment_id,
            "OBJECTID": object_id,
            "NodeIDFrom": "0047201",
            "NodeIDTo": "0047258",
            "TrafDir": "T",
            "SegmentTyp": "U",
            "FeatureTyp": "0",
            "Status": "2",
            "RW_TYPE": "1",
            "RB_Layer": "B",
            "NodeLevelF": "M",
            "NodeLevelT": "M",
            "POSTED_SPEED": "25",
            "CurveFlag": None,
            "Radius": 0,
            "Shape__Length": 300.0,
            "Street": "TEST STREET",
        }
        properties.update(overrides)
        return {
            "type": "Feature",
            "properties": properties,
            "geometry": {"type": "LineString", "coordinates": coordinates},
        }

    @staticmethod
    def write_lion(path: Path, features: list[dict[str, object]]) -> None:
        path.write_text(json.dumps({"type": "FeatureCollection", "features": features}))

    @classmethod
    def write_source_files(cls, source_dir: Path) -> None:
        source_dir.mkdir()
        cls.write_lion(
            source_dir / "lion.geojson",
            [
                cls.lion_row(
                    "REAL-SEG-1",
                    [
                        [cls.BASE_LON, cls.BASE_LAT - cls.LAT_OFFSET],
                        [cls.BASE_LON, cls.BASE_LAT + cls.LAT_OFFSET],
                    ],
                    Shape__Length=364.0,
                )
            ],
        )
        empty_collection = {"type": "FeatureCollection", "features": []}
        (source_dir / "pavement.geojson").write_text(json.dumps(empty_collection))
        (source_dir / "speed_humps.geojson").write_text(json.dumps(empty_collection))
        cls.write_taxi_zone_zip(source_dir / "taxi_zones.zip")

    @classmethod
    def write_taxi_zone_zip(cls, output_path: Path) -> None:
        shapefile_dir = output_path.parent / "taxi-shape"
        shapefile_dir.mkdir(exist_ok=True)
        stem = shapefile_dir / "taxi_zones"
        with shapefile.Writer(str(stem), shapeType=shapefile.POLYGON) as writer:
            writer.field("LocationID", "N", decimal=0)
            writer.poly([cls.ZONE_BBOX])
            writer.record(181)
        stem.with_suffix(".prj").write_text(CRS.from_epsg(4326).to_wkt())
        with zipfile.ZipFile(output_path, "w") as archive:
            for suffix in (".shp", ".shx", ".dbf", ".prj"):
                path = stem.with_suffix(suffix)
                archive.write(path, arcname=path.name)

    @staticmethod
    def local_path_from_uri(uri: str) -> Path:
        return Path(urllib.parse.unquote(urllib.parse.urlparse(uri).path))

    @classmethod
    def sensor_row(cls, event_id: str) -> tuple:
        return (
            event_id,
            1,
            "T1",
            0,
            cls.TARGET_HOUR,
            cls.BASE_LAT,
            cls.BASE_LON,
            10.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            cls.PROCESSED_AT,
            cls.RUN_ID,
        )

    # --- build_road_segments(): transform -> geometry -> validate -> taxi zone (Spark 불필요) ---

    def test_build_road_segments_produces_records_in_epsg_32118_with_taxi_zone_assigned(
        self, tmp_path
    ) -> None:
        lion_path = tmp_path / "lion.geojson"
        self.write_lion(lion_path, [self.lion_row("0000001", self.IN_ZONE_COORDINATES)])
        taxi_zone_zip = tmp_path / "taxi_zones.zip"
        self.write_taxi_zone_zip(taxi_zone_zip)

        report = build_road_segments(
            lion_path, taxi_zone_zip, self.SNAPSHOT, self.SOURCE_VERSION, INGESTED_AT
        )

        assert len(report.records) == 1
        record = report.records[0]
        assert record.segment_id == "0000001"
        assert record.location_id == 181
        assert record.source_version == self.SOURCE_VERSION
        assert record.ingested_at == INGESTED_AT

        geometry = geometry_from_wkb(record.geometry_wkb)
        # EPSG:32118(NY State Plane, 미터)은 WGS84 도 단위와 달리 수십만 단위 offset을 갖는다.
        assert abs(geometry.coords[0][0]) > 10_000
        assert abs(geometry.coords[0][1]) > 10_000
        assert set(report.taxi_zones) == {181}

    def test_build_road_segments_excludes_non_vehicle_segments(self, tmp_path) -> None:
        lion_path = tmp_path / "lion.geojson"
        self.write_lion(
            lion_path,
            [
                self.lion_row("0000001", self.IN_ZONE_COORDINATES),
                self.lion_row(
                    "0000002", self.IN_ZONE_COORDINATES, object_id=2, Status="3"
                ),  # 미시공
            ],
        )
        taxi_zone_zip = tmp_path / "taxi_zones.zip"
        self.write_taxi_zone_zip(taxi_zone_zip)

        report = build_road_segments(
            lion_path, taxi_zone_zip, self.SNAPSHOT, self.SOURCE_VERSION, INGESTED_AT
        )

        assert [record.segment_id for record in report.records] == ["0000001"]

    def test_build_road_segments_reports_segments_unmatched_to_any_taxi_zone(
        self, tmp_path
    ) -> None:
        lion_path = tmp_path / "lion.geojson"
        self.write_lion(
            lion_path,
            [
                self.lion_row("0000001", self.IN_ZONE_COORDINATES),
                self.lion_row("0000002", self.OUT_OF_ZONE_COORDINATES, object_id=2),
            ],
        )
        taxi_zone_zip = tmp_path / "taxi_zones.zip"
        self.write_taxi_zone_zip(taxi_zone_zip)

        report = build_road_segments(
            lion_path, taxi_zone_zip, self.SNAPSHOT, self.SOURCE_VERSION, INGESTED_AT
        )

        by_id = {record.segment_id: record for record in report.records}
        assert by_id["0000001"].location_id == 181
        assert by_id["0000002"].location_id is None
        assert report.unmatched_taxi_zone_segment_ids == ("0000002",)

    def test_build_road_segments_excludes_segments_with_non_positive_length(
        self, tmp_path
    ) -> None:
        lion_path = tmp_path / "lion.geojson"
        self.write_lion(
            lion_path,
            [
                self.lion_row("0000001", self.IN_ZONE_COORDINATES),
                self.lion_row(
                    "0000002", self.IN_ZONE_COORDINATES, object_id=2, Shape__Length=0.0
                ),
            ],
        )
        taxi_zone_zip = tmp_path / "taxi_zones.zip"
        self.write_taxi_zone_zip(taxi_zone_zip)

        report = build_road_segments(
            lion_path, taxi_zone_zip, self.SNAPSHOT, self.SOURCE_VERSION, INGESTED_AT
        )

        assert [record.segment_id for record in report.records] == ["0000001"]
        assert report.rule_failures["length_not_positive"] == ("0000002",)

    # --- build-road-environment -> Manifest -> Transform 2, 경로 우회 없이 이어지는지 ---

    def test_build_road_environment_output_feeds_transform2_without_bypass(
        self, spark, tmp_path
    ) -> None:
        source_dir = tmp_path / "source"
        data_lake = tmp_path / "lake"
        self.write_source_files(source_dir)

        result = build_and_publish_environment(
            source_dir,
            data_lake.as_uri(),
            reference_date=self.SNAPSHOT,
            road_snapshot_date=self.SNAPSHOT,
            build_id="build-1",
        )

        # 1. Manifest 품질 지표가 실제 LION Segment 1건 + Taxi Zone 1건을 반영하는지
        assert result.manifest.quality["lion_segment_count"] == 1
        assert result.manifest.quality["taxi_zone_count"] == 1

        road_segment_uri = result.manifest.artifact("road_segment").uri
        road_segment_path = self.local_path_from_uri(road_segment_uri)

        # 2. geometry_wkb가 Binary이고 EPSG:32118(미터) 좌표인지
        row = duckdb.sql(
            "SELECT geometry_wkb FROM read_parquet(?) WHERE segment_id = 'REAL-SEG-1'",
            params=[str(road_segment_path)],
        ).fetchone()
        assert isinstance(row[0], (bytes, bytearray))
        geometry = shapely.from_wkb(bytes(row[0]))
        expected_x, expected_y = self._FORWARD.transform(
            self.BASE_LON, self.BASE_LAT - self.LAT_OFFSET
        )
        actual_x, actual_y = geometry.coords[0]
        assert actual_x == pytest.approx(expected_x, abs=1.0)
        assert actual_y == pytest.approx(expected_y, abs=1.0)

        # 3. 시뮬레이터용 simulation_road_environment.geometry_wkt는 여전히 EPSG:4326(도)인지
        simulation_path = self.local_path_from_uri(
            result.manifest.artifact("simulation_road_environment").uri
        )
        sim_row = duckdb.sql(
            "SELECT geometry_wkt FROM read_parquet(?) WHERE segment_id = 'REAL-SEG-1'",
            params=[str(simulation_path)],
        ).fetchone()
        sim_geometry = shapely_wkt.loads(sim_row[0])
        assert sim_geometry.coords[0][0] == pytest.approx(self.BASE_LON, abs=1e-6)
        assert sim_geometry.coords[0][1] == pytest.approx(
            self.BASE_LAT - self.LAT_OFFSET, abs=1e-6
        )

        # 4. Transform 2가 경로를 추가로 조립하지 않고 Manifest URI를 그대로 읽어 실제 GPS를 매칭하는지
        sensor_df = spark.createDataFrame(
            [self.sensor_row("e1")], PROCESSED_SENSOR_EVENT_SCHEMA
        )

        config = HourlySegmentFeatureJobConfig.from_env(
            {
                "HOURLY_SEGMENT_FEATURE_ROAD_SEGMENT_PATH": road_segment_uri,
                "HOURLY_SEGMENT_FEATURE_OUTPUT_PATH": str(tmp_path / "hourly_segment_features"),
            }
        )

        summary = run_hourly_segment_feature_job(
            spark, sensor_df, config, self.TARGET_HOUR, self.SNAPSHOT, self.FEATURE_VERSION,
            self.RUN_ID, self.PROCESSED_AT,
            cleansing_quarantine=spark.createDataFrame([], SENSOR_EVENT_QUARANTINE_SCHEMA),
            cleansing_quarantined_count=0,
            raw_record_source=spark.createDataFrame(
                [("e1", "{}")], ["event_id", RAW_RECORD_COLUMN]
            ),
            quarantine_output_path=str(tmp_path / "sensor_event_quarantine"),
        )

        assert summary.result_count == 1
        output_rows = spark.read.parquet(summary.output_path).collect()
        assert len(output_rows) == 1
        assert output_rows[0]["segment_id"] == "REAL-SEG-1"
        assert output_rows[0]["road_snapshot_date"] == self.SNAPSHOT

    def test_hourly_segment_feature_job_rejects_road_segment_snapshot_date_mismatch(
        self, spark, tmp_path
    ) -> None:
        source_dir = tmp_path / "source"
        data_lake = tmp_path / "lake"
        self.write_source_files(source_dir)

        result = build_and_publish_environment(
            source_dir,
            data_lake.as_uri(),
            reference_date=self.SNAPSHOT,
            road_snapshot_date=self.SNAPSHOT,
            build_id="build-1",
        )
        road_segment_uri = result.manifest.artifact("road_segment").uri

        sensor_df = spark.createDataFrame(
            [self.sensor_row("e1")], PROCESSED_SENSOR_EVENT_SCHEMA
        )

        config = HourlySegmentFeatureJobConfig.from_env(
            {
                "HOURLY_SEGMENT_FEATURE_ROAD_SEGMENT_PATH": road_segment_uri,
                "HOURLY_SEGMENT_FEATURE_OUTPUT_PATH": str(tmp_path / "hourly_segment_features"),
            }
        )

        wrong_snapshot_date = date(2020, 1, 1)
        with pytest.raises(ValueError, match="snapshot_date"):
            run_hourly_segment_feature_job(
                spark, sensor_df, config, self.TARGET_HOUR, wrong_snapshot_date,
                self.FEATURE_VERSION,
                self.RUN_ID, self.PROCESSED_AT,
                cleansing_quarantine=spark.createDataFrame(
                    [], SENSOR_EVENT_QUARANTINE_SCHEMA
                ),
                cleansing_quarantined_count=0,
                raw_record_source=spark.createDataFrame(
                    [("e1", "{}")], ["event_id", RAW_RECORD_COLUMN]
                ),
                quarantine_output_path=str(tmp_path / "sensor_event_quarantine"),
            )
