"""Normalize and enrich raw NYC road-reference snapshots."""

from __future__ import annotations

import io
import json
import re
import zipfile
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

import shapefile
from pyproj import CRS, Transformer
from shapely import STRtree
from shapely.geometry import LineString, shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform

from batch_jobs.geo import as_line, line_length_m

MAX_REFERENCE_DISTANCE_DEGREES = 0.00035


@dataclass(slots=True)
class PreparedRoadSegment:
    segment_id: str
    from_node_id: int
    to_node_id: int
    traffic_direction: str
    segment_type: str
    feature_type: str
    roadbed_layer: str
    from_node_level: str
    to_node_level: str
    street_name: str
    geometry: LineString
    length_m: float
    posted_speed_mph: int | None
    curve_flag: str | None
    curve_radius_m: float | None
    location_id: int | None = None
    pavement_rating: float | None = None
    pavement_rating_date: date | None = None
    pavement_quality_flag: str = "NOT_FOUND"
    hump_count: int = 0
    hump_fractions: list[float] = field(default_factory=list)
    hump_quality_flag: str = "NO_HUMP_RECORD"


@dataclass(frozen=True, slots=True)
class PreparationQuality:
    lion_segment_count: int
    taxi_zone_count: int
    pavement_source_count: int
    pavement_matched_segment_count: int
    pavement_rated_segment_count: int
    hump_source_count: int
    hump_mapped_source_count: int
    hump_mapped_segment_count: int
    hump_count: int

    def to_dict(self) -> dict[str, int | float | str]:
        return {
            "lion_segment_count": self.lion_segment_count,
            "taxi_zone_count": self.taxi_zone_count,
            "pavement_source_count": self.pavement_source_count,
            "pavement_matched_segment_count": self.pavement_matched_segment_count,
            "pavement_rated_segment_count": self.pavement_rated_segment_count,
            "pavement_segment_match_rate": round(
                self.pavement_matched_segment_count / self.lion_segment_count, 6
            ),
            "hump_source_count": self.hump_source_count,
            "hump_mapped_source_count": self.hump_mapped_source_count,
            "hump_source_match_rate": round(
                self.hump_mapped_source_count / max(1, self.hump_source_count), 6
            ),
            "hump_mapped_segment_count": self.hump_mapped_segment_count,
            "hump_count": self.hump_count,
            "status": "PASSED",
        }


@dataclass(frozen=True, slots=True)
class PreparedEnvironment:
    segments: tuple[PreparedRoadSegment, ...]
    taxi_zones: dict[int, BaseGeometry]
    quality: PreparationQuality


def prepare_environment(
    lion_path: Path,
    pavement_path: Path,
    hump_path: Path,
    taxi_zone_zip: Path,
    reference_date: date,
) -> PreparedEnvironment:
    segments = load_lion_segments(lion_path)
    taxi_zones = load_taxi_zones(taxi_zone_zip)
    assign_taxi_zones(segments, taxi_zones)
    pavement_source_count = attach_pavement(segments, pavement_path, reference_date)
    hump_source_count, hump_mapped_source_count = attach_speed_humps(
        segments, hump_path, reference_date
    )
    quality = PreparationQuality(
        lion_segment_count=len(segments),
        taxi_zone_count=len(taxi_zones),
        pavement_source_count=pavement_source_count,
        pavement_matched_segment_count=sum(
            segment.pavement_quality_flag != "NOT_FOUND" for segment in segments
        ),
        pavement_rated_segment_count=sum(
            segment.pavement_rating is not None for segment in segments
        ),
        hump_source_count=hump_source_count,
        hump_mapped_source_count=hump_mapped_source_count,
        hump_mapped_segment_count=sum(segment.hump_count > 0 for segment in segments),
        hump_count=sum(segment.hump_count for segment in segments),
    )
    validate_environment(segments, taxi_zones)
    return PreparedEnvironment(tuple(segments), taxi_zones, quality)


def load_lion_segments(path: Path) -> list[PreparedRoadSegment]:
    document = load_feature_collection(path)
    segments: list[PreparedRoadSegment] = []
    seen_ids: set[str] = set()
    for feature in document["features"]:
        properties = feature.get("properties") or {}
        segment_id = string_property(properties, "SegmentID")
        traffic_direction = string_property(properties, "TrafDir")
        from_node = integer_property(properties, "NodeIDFrom")
        to_node = integer_property(properties, "NodeIDTo")
        if (
            not segment_id
            or segment_id in seen_ids
            or traffic_direction not in {"W", "A", "T"}
            or from_node is None
            or to_node is None
            or not feature.get("geometry")
        ):
            continue
        geometry = as_line(shape(feature["geometry"]))
        length_m = line_length_m(geometry)
        if length_m <= 0:
            continue
        radius_feet = float_property(properties, "Radius")
        seen_ids.add(segment_id)
        segments.append(
            PreparedRoadSegment(
                segment_id=segment_id,
                from_node_id=from_node,
                to_node_id=to_node,
                traffic_direction=traffic_direction,
                segment_type=string_property(properties, "SegmentTyp"),
                feature_type=string_property(properties, "FeatureTyp"),
                roadbed_layer=string_property(properties, "RB_Layer"),
                from_node_level=string_property(properties, "NodeLevelF"),
                to_node_level=string_property(properties, "NodeLevelT"),
                street_name=string_property(properties, "Street"),
                geometry=geometry,
                length_m=length_m,
                posted_speed_mph=integer_property(properties, "POSTED_SPEED"),
                curve_flag=nullable_string_property(properties, "CurveFlag"),
                curve_radius_m=radius_feet * 0.3048 if radius_feet else None,
            )
        )
    if not segments:
        raise ValueError("LION snapshot contains no valid routable segments")
    return segments


def attach_pavement(
    segments: list[PreparedRoadSegment], path: Path, reference_date: date
) -> int:
    document = load_feature_collection(path)
    by_street: dict[
        str, list[tuple[LineString, float | None, date | None]]
    ] = defaultdict(list)
    source_count = 0
    for feature in document["features"]:
        properties = feature.get("properties") or {}
        if not feature.get("geometry"):
            continue
        source_count += 1
        rating = float_property(properties, "systemrating", "SystemRating")
        if rating is not None and rating <= 0:
            rating = None
        by_street[normalize_street(property_value(properties, "onstreetna", "OnStreetName"))].append(
            (
                as_line(shape(feature["geometry"])),
                rating,
                parse_source_date(
                    property_value(properties, "inspectiontime", "InspectionTime")
                ),
            )
        )

    for segment in segments:
        candidates = by_street.get(normalize_street(segment.street_name), [])
        applicable = [
            item for item in candidates if item[2] is None or item[2] <= reference_date
        ]
        if not applicable:
            continue
        geometry, rating, inspection_date = min(
            applicable,
            key=lambda item: (
                segment.geometry.distance(item[0]),
                -(item[2].toordinal() if item[2] else 0),
                item[0].wkt,
            ),
        )
        if segment.geometry.distance(geometry) > MAX_REFERENCE_DISTANCE_DEGREES:
            continue
        segment.pavement_rating = rating
        segment.pavement_rating_date = inspection_date
        segment.pavement_quality_flag = "MATCHED" if rating is not None else "MATCHED_UNRATED"
    return source_count


def attach_speed_humps(
    segments: list[PreparedRoadSegment], path: Path, reference_date: date
) -> tuple[int, int]:
    document = load_feature_collection(path)
    by_street: dict[str, list[PreparedRoadSegment]] = defaultdict(list)
    for segment in segments:
        by_street[normalize_street(segment.street_name)].append(segment)

    source_count = 0
    mapped_count = 0
    for feature in document["features"]:
        properties = feature.get("properties") or {}
        if not feature.get("geometry"):
            continue
        source_count += 1
        installation_date = parse_source_date(
            property_value(properties, "date_insta", "InstallationDate")
        )
        if installation_date is not None and installation_date > reference_date:
            continue
        candidates = by_street.get(
            normalize_street(property_value(properties, "on_street", "OnStreet")), []
        )
        if not candidates:
            continue
        hump_geometry = as_line(shape(feature["geometry"]))
        nearest = min(candidates, key=lambda item: item.geometry.distance(hump_geometry))
        if nearest.geometry.distance(hump_geometry) > MAX_REFERENCE_DISTANCE_DEGREES:
            continue
        count = max(0, integer_property(properties, "humps", "HumpCount") or 0)
        nearest.hump_count += count
        nearest.hump_quality_flag = "MATCHED"
        nearest.hump_fractions.extend(
            (index + 1) / (count + 1) for index in range(count)
        )
        mapped_count += 1
    for segment in segments:
        segment.hump_fractions.sort()
    return source_count, mapped_count


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
        zones = {
            int(record.record[location_index]): transform(
                transformer.transform, shape(record.shape.__geo_interface__)
            )
            for record in reader.iterShapeRecords()
        }
    if not zones:
        raise ValueError("taxi-zone snapshot contains no zones")
    return zones


def assign_taxi_zones(
    segments: list[PreparedRoadSegment], taxi_zones: dict[int, BaseGeometry]
) -> None:
    zone_ids = list(taxi_zones)
    geometries = [taxi_zones[zone_id] for zone_id in zone_ids]
    tree = STRtree(geometries)
    for segment in segments:
        midpoint = segment.geometry.interpolate(0.5, normalized=True)
        for candidate_index in tree.query(midpoint):
            index = int(candidate_index)
            if geometries[index].covers(midpoint):
                segment.location_id = zone_ids[index]
                break


def validate_environment(
    segments: list[PreparedRoadSegment], taxi_zones: dict[int, BaseGeometry]
) -> None:
    if not segments or not taxi_zones:
        raise ValueError("prepared environment must contain roads and taxi zones")
    segment_ids = [segment.segment_id for segment in segments]
    if len(segment_ids) != len(set(segment_ids)):
        raise ValueError("prepared environment contains duplicate segment IDs")
    invalid_ratings = [
        segment.pavement_rating
        for segment in segments
        if segment.pavement_rating is not None
        and not 0 < segment.pavement_rating <= 10
    ]
    if invalid_ratings:
        raise ValueError("prepared environment contains pavement ratings outside (0, 10]")


def pavement_condition(rating: float | None) -> str | None:
    if rating is None:
        return None
    if rating >= 8:
        return "Good"
    if rating >= 6:
        return "Fair"
    return "Poor"


def normalize_street(value: object) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


def load_feature_collection(path: Path) -> dict[str, list[dict[str, object]]]:
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict) or not isinstance(document.get("features"), list):
        raise TypeError(f"{path.name} must be a GeoJSON FeatureCollection")
    return document


def property_value(properties: dict[str, object], *names: str) -> object | None:
    lowered = {str(key).lower(): value for key, value in properties.items()}
    for name in names:
        if name.lower() in lowered:
            return lowered[name.lower()]
    return None


def string_property(properties: dict[str, object], *names: str) -> str:
    return str(property_value(properties, *names) or "").strip()


def nullable_string_property(
    properties: dict[str, object], *names: str
) -> str | None:
    value = string_property(properties, *names)
    return value or None


def float_property(properties: dict[str, object], *names: str) -> float | None:
    value = property_value(properties, *names)
    if value is None or str(value).strip() == "":
        return None
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


def integer_property(properties: dict[str, object], *names: str) -> int | None:
    value = float_property(properties, *names)
    return int(value) if value is not None else None


def parse_source_date(value: object | None) -> date | None:
    if value is None or not str(value).strip():
        return None
    text = str(value).strip()
    for format_string in (
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%Y",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d",
    ):
        try:
            return (
                datetime.strptime(text.removesuffix(".000"), format_string)
                .replace(tzinfo=UTC)
                .date()
            )
        except ValueError:
            continue
    return None
