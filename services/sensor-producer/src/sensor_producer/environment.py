"""Load and enrich the routable NYC road environment."""

from __future__ import annotations

import io
import json
import re
import zipfile
from collections import defaultdict
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq
import shapefile
import shapely
from pyproj import CRS, Transformer
from shapely.geometry import LineString, shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform

from sensor_producer.domain import RoadSegment
from sensor_producer.geo import as_line

MAX_REFERENCE_DISTANCE_DEGREES = 0.00035

# canonical road_segment.geometry_wkb는 EPSG:32118(meter)라 Producer가 쓸 위경도로 되돌린다.
_ROAD_SEGMENT_TRANSFORMER = Transformer.from_crs("EPSG:32118", "EPSG:4326", always_xy=True)

VALID_TRAFFIC_DIRECTIONS = frozenset({"W", "A", "T"})

_ROAD_SEGMENT_COLUMNS = (
    "segment_id",
    "snapshot_date",
    "street_name",
    "from_node_id",
    "to_node_id",
    "traffic_direction",
    "posted_speed_mph",
    "curve_radius_m",
    "length_m",
    "geometry_wkb",
)


def normalize_street(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"[^A-Z0-9]", "", value.upper())


class RoadEnvironment:
    def __init__(
        self,
        segments: list[RoadSegment],
        taxi_zones: dict[int, BaseGeometry],
        road_segment_snapshot_date: date,
    ):
        if not segments:
            raise ValueError("road environment requires at least one road segment")
        self.segments = segments
        self.taxi_zones = taxi_zones
        self.road_segment_snapshot_date = road_segment_snapshot_date

    @classmethod
    def from_files(
        cls,
        road_segment_path: Path,
        pavement_path: Path,
        hump_path: Path,
        taxi_zone_zip: Path,
    ) -> RoadEnvironment:
        segments, snapshot_date = load_road_segments(road_segment_path)
        attach_pavement(segments, pavement_path)
        attach_speed_humps(segments, hump_path)
        return cls(segments, load_taxi_zones(taxi_zone_zip), snapshot_date)


# batch-jobs build-road-environment가 만든 canonical road_segment(단일 snapshot_date 파일)를 읽는다.
def load_road_segments(road_segment_path: Path) -> tuple[list[RoadSegment], date]:
    table = pq.ParquetFile(road_segment_path).read(columns=list(_ROAD_SEGMENT_COLUMNS))
    snapshot_date = _validate_road_segment_snapshot(table)

    segments: list[RoadSegment] = []
    for row in table.to_pylist():
        # W/A/T가 아니거나 길이가 0 이하인 segment는 주행 불가라 이전과 동일하게 제외한다.
        if row["traffic_direction"] not in VALID_TRAFFIC_DIRECTIONS:
            continue
        if row["length_m"] is None or row["length_m"] <= 0:
            continue
        geometry = as_line(
            transform(_ROAD_SEGMENT_TRANSFORMER.transform, shapely.from_wkb(row["geometry_wkb"]))
        )
        segments.append(
            RoadSegment(
                segment_id=row["segment_id"],
                from_node_id=row["from_node_id"],
                to_node_id=row["to_node_id"],
                traffic_direction=row["traffic_direction"],
                street_name=row["street_name"] or "",
                geometry=geometry,
                length_m=row["length_m"],
                posted_speed_mph=(
                    float(row["posted_speed_mph"])
                    if row["posted_speed_mph"] is not None
                    else None
                ),
                curve_radius_m=row["curve_radius_m"],
            )
        )
    return segments, snapshot_date


# Producer와 Transform 2가 같은 snapshot을 쓰는지, 필수 컬럼에 결측이 없는지 확인한다.
def _validate_road_segment_snapshot(table) -> date:
    snapshot_dates = set(table.column("snapshot_date").to_pylist())
    if len(snapshot_dates) != 1:
        raise ValueError(
            f"road_segment must be a single snapshot_date, got {sorted(snapshot_dates)}"
        )
    for column in ("segment_id", "from_node_id", "to_node_id", "geometry_wkb"):
        if table.column(column).null_count > 0:
            raise ValueError(f"road_segment.{column} must not contain nulls")
    return next(iter(snapshot_dates))


def attach_pavement(segments: list[RoadSegment], path: Path) -> None:
    document = json.loads(path.read_text())
    by_street: dict[str, list[tuple[LineString, float | None]]] = defaultdict(list)
    for feature in document.get("features", []):
        properties = feature.get("properties") or {}
        if not feature.get("geometry"):
            continue
        rating = parse_float(properties.get("systemrating"))
        by_street[normalize_street(properties.get("onstreetna"))].append(
            (as_line(shape(feature["geometry"])), rating if rating and rating > 0 else None)
        )

    for segment in segments:
        candidates = by_street.get(normalize_street(segment.street_name), [])
        if not candidates:
            continue
        geometry, rating = min(candidates, key=lambda item: segment.geometry.distance(item[0]))
        if segment.geometry.distance(geometry) <= MAX_REFERENCE_DISTANCE_DEGREES:
            segment.pavement_rating = rating


def attach_speed_humps(segments: list[RoadSegment], path: Path) -> None:
    document = json.loads(path.read_text())
    by_street: dict[str, list[RoadSegment]] = defaultdict(list)
    for segment in segments:
        by_street[normalize_street(segment.street_name)].append(segment)

    for feature in document.get("features", []):
        properties = feature.get("properties") or {}
        if not feature.get("geometry"):
            continue
        candidates = by_street.get(normalize_street(properties.get("on_street")), [])
        if not candidates:
            continue
        hump_geometry = as_line(shape(feature["geometry"]))
        nearest = min(candidates, key=lambda item: item.geometry.distance(hump_geometry))
        if nearest.geometry.distance(hump_geometry) > MAX_REFERENCE_DISTANCE_DEGREES:
            continue
        count = max(0, int(parse_float(properties.get("humps")) or 0))
        nearest.hump_fractions.extend((index + 1) / (count + 1) for index in range(count))


def load_taxi_zones(path: Path) -> dict[int, BaseGeometry]:
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        shp_name = next(name for name in names if name.lower().endswith(".shp"))
        stem = shp_name[:-4]
        reader = shapefile.Reader(
            shp=io.BytesIO(archive.read(f"{stem}.shp")),
            shx=io.BytesIO(archive.read(f"{stem}.shx")),
            dbf=io.BytesIO(archive.read(f"{stem}.dbf")),
        )
        prj_name = next(name for name in names if name.lower().endswith(".prj"))
        source_crs = CRS.from_wkt(archive.read(prj_name).decode())
        transformer = Transformer.from_crs(source_crs, "EPSG:4326", always_xy=True)
        field_names = [field[0] for field in reader.fields[1:]]
        location_index = next(
            index for index, name in enumerate(field_names) if name.lower() == "locationid"
        )
        return {
            int(record.record[location_index]): transform(
                transformer.transform, shape(record.shape.__geo_interface__)
            )
            for record in reader.iterShapeRecords()
        }


def parse_float(value: object) -> float | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None
