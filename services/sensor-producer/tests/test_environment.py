from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import shapely
from pyproj import Transformer
from sensor_producer.domain import RoadSegment
from sensor_producer.environment import RoadEnvironment, load_road_segments
from shapely.geometry import LineString
from shapely.ops import transform as reproject_geometry

_TO_ROAD_SEGMENT_CRS = Transformer.from_crs("EPSG:4326", "EPSG:32118", always_xy=True)


def _geometry_wkb(coordinates: list[tuple[float, float]]) -> bytes:
    line = LineString(coordinates)
    projected = reproject_geometry(_TO_ROAD_SEGMENT_CRS.transform, line)
    return shapely.to_wkb(projected)


def write_road_segment(path, rows: list[dict]) -> None:
    table = pa.table(
        {
            "segment_id": pa.array([row["segment_id"] for row in rows], type=pa.string()),
            "snapshot_date": pa.array(
                [row.get("snapshot_date", date(2026, 8, 19)) for row in rows], type=pa.date32()
            ),
            "street_name": pa.array(
                [row.get("street_name", "TEST ST") for row in rows], type=pa.string()
            ),
            "from_node_id": pa.array([row["from_node_id"] for row in rows], type=pa.string()),
            "to_node_id": pa.array([row["to_node_id"] for row in rows], type=pa.string()),
            "traffic_direction": pa.array(
                [row.get("traffic_direction", "T") for row in rows], type=pa.string()
            ),
            "posted_speed_mph": pa.array(
                [row.get("posted_speed_mph", 25) for row in rows], type=pa.int32()
            ),
            "curve_radius_m": pa.array(
                [row.get("curve_radius_m") for row in rows], type=pa.float64()
            ),
            "length_m": pa.array([row.get("length_m", 100.0) for row in rows], type=pa.float64()),
            "geometry_wkb": pa.array(
                [row.get("geometry_wkb", _geometry_wkb([(-74.0, 40.7), (-73.999, 40.701)])) for row in rows],
                type=pa.binary(),
            ),
        }
    )
    pq.write_table(table, path)


class TestLoadRoadSegments:
    def test_reprojects_geometry_back_to_wgs84_and_builds_the_segment(self, tmp_path):
        path = tmp_path / "data.parquet"
        coordinates = [(-74.0, 40.7), (-73.999, 40.701)]
        write_road_segment(
            path,
            [
                {
                    "segment_id": "s1",
                    "from_node_id": "n1",
                    "to_node_id": "n2",
                    "traffic_direction": "W",
                    "posted_speed_mph": 30,
                    "curve_radius_m": 12.5,
                    "length_m": 142.3,
                    "geometry_wkb": _geometry_wkb(coordinates),
                }
            ],
        )

        segments, snapshot_date = load_road_segments(path)

        assert snapshot_date == date(2026, 8, 19)
        assert len(segments) == 1
        segment = segments[0]
        assert segment.segment_id == "s1"
        assert segment.from_node_id == "n1"
        assert segment.to_node_id == "n2"
        assert segment.traffic_direction == "W"
        assert segment.length_m == 142.3
        assert segment.posted_speed_mph == 30.0
        assert segment.curve_radius_m == 12.5
        for (lon, lat), (out_lon, out_lat) in zip(coordinates, segment.geometry.coords, strict=True):
            assert out_lon == pytest.approx(lon, abs=1e-7)
            assert out_lat == pytest.approx(lat, abs=1e-7)

    def test_excludes_a_segment_whose_traffic_direction_is_not_w_a_or_t(self, tmp_path):
        path = tmp_path / "data.parquet"
        write_road_segment(
            path,
            [
                {"segment_id": "s1", "from_node_id": "n1", "to_node_id": "n2", "traffic_direction": "T"},
                {"segment_id": "s2", "from_node_id": "n2", "to_node_id": "n3", "traffic_direction": "X"},
            ],
        )

        segments, _ = load_road_segments(path)

        assert [segment.segment_id for segment in segments] == ["s1"]

    def test_excludes_a_segment_with_non_positive_length(self, tmp_path):
        path = tmp_path / "data.parquet"
        write_road_segment(
            path,
            [
                {"segment_id": "s1", "from_node_id": "n1", "to_node_id": "n2", "length_m": 100.0},
                {"segment_id": "s2", "from_node_id": "n2", "to_node_id": "n3", "length_m": 0.0},
            ],
        )

        segments, _ = load_road_segments(path)

        assert [segment.segment_id for segment in segments] == ["s1"]

    def test_raises_when_the_parquet_spans_more_than_one_snapshot_date(self, tmp_path):
        path = tmp_path / "data.parquet"
        write_road_segment(
            path,
            [
                {
                    "segment_id": "s1",
                    "from_node_id": "n1",
                    "to_node_id": "n2",
                    "snapshot_date": date(2026, 8, 18),
                },
                {
                    "segment_id": "s2",
                    "from_node_id": "n2",
                    "to_node_id": "n3",
                    "snapshot_date": date(2026, 8, 19),
                },
            ],
        )

        with pytest.raises(ValueError, match="snapshot_date"):
            load_road_segments(path)

    def test_raises_when_segment_id_is_null(self, tmp_path):
        path = tmp_path / "data.parquet"
        write_road_segment(path, [{"segment_id": "s1", "from_node_id": "n1", "to_node_id": "n2"}])
        # segment_id를 강제로 NULL로 덮어써서 결측 검증을 확인한다.
        table = pq.read_table(path)
        table = table.set_column(
            table.schema.get_field_index("segment_id"),
            "segment_id",
            pa.array([None], type=pa.string()),
        )
        pq.write_table(table, path)

        with pytest.raises(ValueError, match="segment_id"):
            load_road_segments(path)


class TestRoadEnvironment:
    def test_records_the_road_segment_snapshot_date_it_was_built_from(self):
        segment = RoadSegment(
            segment_id="s1",
            from_node_id="n1",
            to_node_id="n2",
            traffic_direction="T",
            street_name="TEST ST",
            geometry=LineString([(-74.0, 40.7), (-73.999, 40.701)]),
            length_m=100.0,
            posted_speed_mph=25.0,
            curve_radius_m=None,
        )

        environment = RoadEnvironment([segment], {}, date(2026, 8, 19))

        assert environment.road_segment_snapshot_date == date(2026, 8, 19)
