"""Load, profile, filter, and normalize NYC LION features into road_segment rows."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

FEET_TO_METERS = 0.3048

PROFILE_COLUMNS = ("FeatureTyp", "SegmentTyp", "RW_TYPE", "Status")

VEHICLE_FEATURE_TYPES = frozenset({"0", "6", "A", "C"})
"""
차량 주행 후보 FeatureTyp. "A"/"C"는 현재 LION snapshot에서
차량용 RW_TYPE과의 조합을 확인하여 포함한다.
"""

CONSTRUCTED_STATUS = "2"

# 차량용 RW_TYPE(도로/고속도로/다리/터널/진입로/램프/골목/U턴)만 포함한다.
VEHICLE_ROADWAY_TYPES = frozenset({"1", "2", "3", "4", "8", "9", "10", "13"})

# 차량 통행 방향(양방향/일방향)만 포함하고 보행자 전용·공백은 제외한다.
VEHICLE_TRAFFIC_DIRECTIONS = frozenset({"T", "W", "A"})


@dataclass(frozen=True, slots=True)
class RoadSegmentRow:
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
    source_version: str
    ingested_at: datetime


def load_lion_rows(path: Path) -> list[dict[str, object]]:
    document = json.loads(path.read_text())
    features = document.get("features")
    if not isinstance(features, list):
        raise TypeError(f"{path.name} must be a GeoJSON FeatureCollection")
    return [feature.get("properties") or {} for feature in features]


def profile_distinct_values(
    rows: list[dict[str, object]],
    columns: Iterable[str] = PROFILE_COLUMNS,
) -> dict[str, Counter[str]]:
    counters = {column: Counter() for column in columns}
    for row in rows:
        for column in columns:
            counters[column][normalize_string(row.get(column)) or ""] += 1
    return counters


def find_duplicate_segment_ids(rows: list[dict[str, object]]) -> dict[str, int]:
    counts = Counter(row.get("SegmentID") for row in rows)
    return {
        segment_id: count
        for segment_id, count in counts.items()
        if segment_id and count > 1
    }


def feature_type_is_vehicle(row: dict[str, object]) -> bool:
    return normalize_string(row.get("FeatureTyp")) in VEHICLE_FEATURE_TYPES


def is_constructed(row: dict[str, object]) -> bool:
    return normalize_string(row.get("Status")) == CONSTRUCTED_STATUS


def roadway_type_is_vehicle(row: dict[str, object]) -> bool:
    return normalize_string(row.get("RW_TYPE")) in VEHICLE_ROADWAY_TYPES


def traffic_direction_is_vehicle(row: dict[str, object]) -> bool:
    return normalize_string(row.get("TrafDir")) in VEHICLE_TRAFFIC_DIRECTIONS


def is_vehicle_segment(row: dict[str, object]) -> bool:
    return (
        feature_type_is_vehicle(row)
        and is_constructed(row)
        and roadway_type_is_vehicle(row)
        and traffic_direction_is_vehicle(row)
    )


def select_representative_rows(
    rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Keep exactly one row per SegmentID.

    중복 SegmentID는 Street 별칭만 다르고 나머지 값은 동일하므로
    OBJECTID가 가장 작은 행을 대표로 남긴다.
    """
    best: dict[str, dict[str, object]] = {}
    for row in rows:
        segment_id = row.get("SegmentID")
        if not segment_id:
            continue
        current = best.get(segment_id)
        if current is None or object_id(row) < object_id(current):
            best[segment_id] = row
    return list(best.values())


def object_id(row: dict[str, object]) -> int:
    return int(row.get("OBJECTID") or 0)


def normalize_string(value: object) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def to_int(value: object) -> int | None:
    text = normalize_string(value)
    return int(text) if text is not None else None


def to_float(value: object) -> float | None:
    text = normalize_string(value)
    return float(text) if text is not None else None


def build_road_segment_row(
    row: dict[str, object],
    snapshot_date: date,
    source_version: str,
    ingested_at: datetime,
) -> RoadSegmentRow:
    radius_feet = to_float(row.get("Radius"))
    length_feet = to_float(row.get("Shape__Length"))
    return RoadSegmentRow(
        segment_id=normalize_string(row.get("SegmentID")),
        snapshot_date=snapshot_date,
        street_name=normalize_string(row.get("Street")),
        from_node_id=normalize_string(row.get("NodeIDFrom")),
        to_node_id=normalize_string(row.get("NodeIDTo")),
        traffic_direction=normalize_string(row.get("TrafDir")),
        segment_type=normalize_string(row.get("SegmentTyp")),
        feature_type=normalize_string(row.get("FeatureTyp")),
        roadway_type=normalize_string(row.get("RW_TYPE")),
        roadbed_layer=normalize_string(row.get("RB_Layer")),
        from_node_level=normalize_string(row.get("NodeLevelF")),
        to_node_level=normalize_string(row.get("NodeLevelT")),
        posted_speed_mph=to_int(row.get("POSTED_SPEED")),
        curve_flag=normalize_string(row.get("CurveFlag")),
        curve_radius_m=(
            radius_feet * FEET_TO_METERS if radius_feet is not None else None
        ),
        length_m=length_feet * FEET_TO_METERS if length_feet is not None else None,
        source_version=source_version,
        ingested_at=ingested_at,
    )


def transform_road_segments(
    path: Path,
    snapshot_date: date,
    source_version: str,
    ingested_at: datetime,
) -> list[RoadSegmentRow]:
    rows = load_lion_rows(path)
    vehicle_rows = [row for row in rows if is_vehicle_segment(row)]
    representative_rows = select_representative_rows(vehicle_rows)
    return [
        build_road_segment_row(row, snapshot_date, source_version, ingested_at)
        for row in representative_rows
    ]
