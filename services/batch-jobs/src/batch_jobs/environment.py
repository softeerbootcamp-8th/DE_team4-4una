# road_segment 정본(RoadSegmentRecord)에 포장 상태·과속방지턱 참조 데이터를 결합한다.

from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry

from batch_jobs.geo import as_line
from batch_jobs.road_segment.geometry import (
    geometry_from_wkb,
    reproject_to_target_crs,
)
from batch_jobs.road_segment.validate import RoadSegmentRecord

# OQ-011: 포장·방지턱 매칭은 도로명 정규화 + 약 39m 이내 최근접 Geometry를 쓴다.
# (구 EPSG:4326 기준 0.00035도 threshold는 NYC 위도에서 대략 39m에 해당했다.)
MAX_REFERENCE_DISTANCE_M = 39.0


# RoadSegmentRecord는 frozen이라 복제하지 않고 감싸기만 한다 — road_segment/가 단일 기준이다.
@dataclass(slots=True)
class EnrichedRoadSegment:
    road: RoadSegmentRecord
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
    segments: tuple[EnrichedRoadSegment, ...]
    taxi_zones: dict[int, BaseGeometry]
    quality: PreparationQuality


def prepare_environment(
    road_records: list[RoadSegmentRecord],
    pavement_path: Path,
    hump_path: Path,
    taxi_zones: dict[int, BaseGeometry],
    reference_date: date,
) -> PreparedEnvironment:
    segments = [EnrichedRoadSegment(road=record) for record in road_records]
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


def attach_pavement(
    segments: list[EnrichedRoadSegment], path: Path, reference_date: date
) -> int:
    document = load_feature_collection(path)
    by_street: dict[
        str, list[tuple[BaseGeometry, float | None, date | None]]
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
        geometry = reproject_to_target_crs(as_line(shape(feature["geometry"])))
        by_street[normalize_street(property_value(properties, "onstreetna", "OnStreetName"))].append(
            (
                geometry,
                rating,
                parse_source_date(
                    property_value(properties, "inspectiontime", "InspectionTime")
                ),
            )
        )

    for segment in segments:
        candidates = by_street.get(normalize_street(segment.road.street_name), [])
        applicable = [
            item for item in candidates if item[2] is None or item[2] <= reference_date
        ]
        if not applicable:
            continue
        segment_geometry = geometry_from_wkb(segment.road.geometry_wkb)
        geometry, rating, inspection_date = min(
            applicable,
            key=lambda item: (
                segment_geometry.distance(item[0]),
                -(item[2].toordinal() if item[2] else 0),
                item[0].wkt,
            ),
        )
        if segment_geometry.distance(geometry) > MAX_REFERENCE_DISTANCE_M:
            continue
        segment.pavement_rating = rating
        segment.pavement_rating_date = inspection_date
        segment.pavement_quality_flag = "MATCHED" if rating is not None else "MATCHED_UNRATED"
    return source_count


def attach_speed_humps(
    segments: list[EnrichedRoadSegment], path: Path, reference_date: date
) -> tuple[int, int]:
    document = load_feature_collection(path)
    by_street: dict[str, list[EnrichedRoadSegment]] = defaultdict(list)
    for segment in segments:
        by_street[normalize_street(segment.road.street_name)].append(segment)

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
        hump_geometry = reproject_to_target_crs(as_line(shape(feature["geometry"])))
        nearest = min(
            candidates,
            key=lambda item: geometry_from_wkb(item.road.geometry_wkb).distance(hump_geometry),
        )
        if geometry_from_wkb(nearest.road.geometry_wkb).distance(hump_geometry) > MAX_REFERENCE_DISTANCE_M:
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


def validate_environment(
    segments: list[EnrichedRoadSegment], taxi_zones: dict[int, BaseGeometry]
) -> None:
    if not segments or not taxi_zones:
        raise ValueError("prepared environment must contain roads and taxi zones")
    segment_ids = [segment.road.segment_id for segment in segments]
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
