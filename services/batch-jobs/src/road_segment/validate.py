"""Combine road_segment transform and geometry outputs, then validate them."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

import shapely
from shapely.geometry.base import BaseGeometry

from road_segment.geometry import SegmentGeometry
from road_segment.transform import RoadSegmentRow

REQUIRED_FIELDS = (
    "segment_id",
    "snapshot_date",
    "from_node_id",
    "to_node_id",
    "length_m",
    "geometry_wkb",
    "source_version",
    "ingested_at",
)


@dataclass(frozen=True, slots=True)
class RoadSegmentRecord:
    segment_id: str
    snapshot_date: date
    street_name: str | None
    from_node_id: str
    to_node_id: str
    traffic_direction: str | None
    segment_type: str | None
    feature_type: str | None
    roadway_type: str | None
    roadbed_layer: str | None
    from_node_level: str | None
    to_node_level: str | None
    posted_speed_mph: int | None
    curve_flag: str | None
    curve_radius_m: float | None
    length_m: float | None
    geometry_wkb: bytes | None
    source_version: str
    ingested_at: datetime
    location_id: int | None = None


@dataclass(frozen=True, slots=True)
class CombineResult:
    records: tuple[RoadSegmentRecord, ...]
    transform_only_segment_ids: tuple[str, ...]
    geometry_only_segment_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ValidationReport:
    valid_records: tuple[RoadSegmentRecord, ...]
    rule_failures: dict[str, tuple[str, ...]]
    transform_only_segment_ids: tuple[str, ...]
    geometry_only_segment_ids: tuple[str, ...]


def combine_segment_records(
    rows: list[RoadSegmentRow], geometries: list[SegmentGeometry]
) -> CombineResult:
    geometry_by_id = {geometry.segment_id: geometry.geometry_wkb for geometry in geometries}
    unmatched_geometry_ids = set(geometry_by_id)

    records: list[RoadSegmentRecord] = []
    transform_only_ids: list[str] = []
    for row in rows:
        geometry_wkb = geometry_by_id.get(row.segment_id)
        if geometry_wkb is None:
            transform_only_ids.append(row.segment_id)
        else:
            unmatched_geometry_ids.discard(row.segment_id)
        records.append(_to_record(row, geometry_wkb))

    return CombineResult(
        records=tuple(records),
        transform_only_segment_ids=tuple(transform_only_ids),
        geometry_only_segment_ids=tuple(sorted(unmatched_geometry_ids)),
    )


def _to_record(row: RoadSegmentRow, geometry_wkb: bytes | None) -> RoadSegmentRecord:
    return RoadSegmentRecord(
        segment_id=row.segment_id,
        snapshot_date=row.snapshot_date,
        street_name=row.street_name,
        from_node_id=row.from_node_id,
        to_node_id=row.to_node_id,
        traffic_direction=row.traffic_direction,
        segment_type=row.segment_type,
        feature_type=row.feature_type,
        roadway_type=row.roadway_type,
        roadbed_layer=row.roadbed_layer,
        from_node_level=row.from_node_level,
        to_node_level=row.to_node_level,
        posted_speed_mph=row.posted_speed_mph,
        curve_flag=row.curve_flag,
        curve_radius_m=row.curve_radius_m,
        length_m=row.length_m,
        geometry_wkb=geometry_wkb,
        source_version=row.source_version,
        ingested_at=row.ingested_at,
    )


def missing_required_fields(record: RoadSegmentRecord) -> bool:
    return any(getattr(record, field) is None for field in REQUIRED_FIELDS)


def has_positive_length(record: RoadSegmentRecord) -> bool:
    return record.length_m is not None and record.length_m > 0


def deserialize_geometry(geometry_wkb: bytes | None) -> BaseGeometry | None:
    if geometry_wkb is None:
        return None
    try:
        return shapely.from_wkb(geometry_wkb)
    except shapely.errors.ShapelyError:
        return None


def is_valid_line_geometry(geometry: BaseGeometry | None) -> bool:
    return (
        geometry is not None
        and geometry.geom_type == "LineString"
        and not geometry.is_empty
        and geometry.is_valid
    )


def failed_rules(record: RoadSegmentRecord) -> list[str]:
    failures = []
    if missing_required_fields(record):
        failures.append("required_field_null")
    if not has_positive_length(record):
        failures.append("length_not_positive")
    if not is_valid_line_geometry(deserialize_geometry(record.geometry_wkb)):
        failures.append("geometry_invalid")
    return failures


def find_duplicate_keys(records: list[RoadSegmentRecord]) -> dict[tuple[str, date], int]:
    counts: dict[tuple[str, date], int] = {}
    for record in records:
        key = (record.segment_id, record.snapshot_date)
        counts[key] = counts.get(key, 0) + 1
    return {key: count for key, count in counts.items() if count > 1}


def validate_road_segments(
    rows: list[RoadSegmentRow], geometries: list[SegmentGeometry]
) -> ValidationReport:
    combined = combine_segment_records(rows, geometries)
    # PK 유일성은 그 자체로 품질 규칙이므로, 다른 규칙으로 이미 제외된 row도
    # 놓치지 않도록 combined.records 전체를 대상으로 먼저 검사한다.
    duplicate_keys = find_duplicate_keys(list(combined.records))

    rule_failures: dict[str, list[str]] = {}
    valid_records: list[RoadSegmentRecord] = []
    for record in combined.records:
        key = (record.segment_id, record.snapshot_date)
        failures = failed_rules(record)
        if key in duplicate_keys:
            failures.append("duplicate_primary_key")
        if failures:
            for rule in failures:
                rule_failures.setdefault(rule, []).append(record.segment_id)
        else:
            valid_records.append(record)

    return ValidationReport(
        valid_records=tuple(valid_records),
        rule_failures={rule: tuple(ids) for rule, ids in rule_failures.items()},
        transform_only_segment_ids=combined.transform_only_segment_ids,
        geometry_only_segment_ids=combined.geometry_only_segment_ids,
    )
