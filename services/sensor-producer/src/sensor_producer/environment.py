"""Load and enrich the routable NYC road environment."""

from __future__ import annotations

import io
import json
import re
import zipfile
from collections import defaultdict
from pathlib import Path

import duckdb
import shapefile
from pyproj import CRS, Transformer
from shapely import from_wkt
from shapely.geometry import LineString, shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform

from sensor_producer.domain import RoadSegment
from sensor_producer.geo import as_line, line_length_m

MAX_REFERENCE_DISTANCE_DEGREES = 0.00035


def normalize_street(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"[^A-Z0-9]", "", value.upper())


class RoadEnvironment:
    def __init__(self, segments: list[RoadSegment], taxi_zones: dict[int, BaseGeometry]):
        if not segments:
            raise ValueError("road environment requires at least one LION segment")
        if not taxi_zones:
            raise ValueError("road environment requires at least one taxi zone")
        self.segments = segments
        self.taxi_zones = taxi_zones

    @classmethod
    def from_files(
        cls,
        lion_path: Path,
        pavement_path: Path,
        hump_path: Path,
        taxi_zone_zip: Path,
    ) -> RoadEnvironment:
        segments = load_lion_segments(lion_path)
        attach_pavement(segments, pavement_path)
        attach_speed_humps(segments, hump_path)
        return cls(segments, load_taxi_zones(taxi_zone_zip))

    @classmethod
    def from_parquet(cls, road_path: Path, taxi_zone_path: Path) -> RoadEnvironment:
        connection = duckdb.connect()
        try:
            road_rows = connection.execute(
                """
                SELECT segment_id, from_node_id, to_node_id, traffic_direction,
                       street_name, geometry_wkt, length_m, posted_speed_mph,
                       curve_radius_m, pavement_rating, hump_fractions_json
                FROM read_parquet(?)
                ORDER BY segment_id
                """,
                [str(road_path)],
            ).fetchall()
            zone_rows = connection.execute(
                """
                SELECT location_id, geometry_wkt
                FROM read_parquet(?)
                ORDER BY location_id
                """,
                [str(taxi_zone_path)],
            ).fetchall()
        finally:
            connection.close()

        segments = [road_segment_from_row(row) for row in road_rows]
        if len({segment.segment_id for segment in segments}) != len(segments):
            raise ValueError("road-environment parquet contains duplicate segment_id values")
        taxi_zones = {int(row[0]): from_wkt(row[1]) for row in zone_rows}
        if len(taxi_zones) != len(zone_rows):
            raise ValueError("taxi-zone parquet contains duplicate location_id values")
        return cls(segments, taxi_zones)


def road_segment_from_row(row: tuple[object, ...]) -> RoadSegment:
    fractions = json.loads(str(row[10]))
    if not isinstance(fractions, list) or any(
        not isinstance(value, (int, float)) or not 0 <= value <= 1
        for value in fractions
    ):
        raise ValueError("hump_fractions_json must be a list of values in [0, 1]")
    return RoadSegment(
        segment_id=str(row[0]),
        from_node_id=str(row[1]),
        to_node_id=str(row[2]),
        traffic_direction=str(row[3]),
        street_name=str(row[4]),
        geometry=as_line(from_wkt(str(row[5]))),
        length_m=float(row[6]),
        posted_speed_mph=float(row[7]) if row[7] is not None else None,
        curve_radius_m=float(row[8]) if row[8] is not None else None,
        pavement_rating=float(row[9]) if row[9] is not None else None,
        hump_fractions=[float(value) for value in fractions],
    )


def load_lion_segments(path: Path) -> list[RoadSegment]:
    document = json.loads(path.read_text())
    segments: list[RoadSegment] = []
    for feature in document.get("features", []):
        properties = feature.get("properties") or {}
        traffic_direction = str(properties.get("TrafDir") or "").strip()
        from_node = str(properties.get("NodeIDFrom") or "").strip()
        to_node = str(properties.get("NodeIDTo") or "").strip()
        segment_id = str(properties.get("SegmentID") or "").strip()
        if traffic_direction not in {"W", "A", "T"} or not all(
            (from_node, to_node, segment_id, feature.get("geometry"))
        ):
            continue
        geometry = as_line(shape(feature["geometry"]))
        length_m = line_length_m(geometry)
        if length_m <= 0:
            continue
        posted_speed = parse_float(properties.get("POSTED_SPEED"))
        radius_feet = parse_float(properties.get("Radius"))
        segments.append(
            RoadSegment(
                segment_id=segment_id,
                from_node_id=from_node,
                to_node_id=to_node,
                traffic_direction=traffic_direction,
                street_name=str(properties.get("Street") or "").strip(),
                geometry=geometry,
                length_m=length_m,
                posted_speed_mph=posted_speed,
                curve_radius_m=radius_feet * 0.3048 if radius_feet else None,
            )
        )
    return segments


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
