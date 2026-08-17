# 도로 데이터 생성 과정(정제 -> 좌표 변환 -> 검증 -> 택시존 배정)을 한 번에 실행하는 진입점.

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from shapely.geometry.base import BaseGeometry

from batch_jobs.road_segment.geometry import build_segment_geometries
from batch_jobs.road_segment.taxi_zone import assign_taxi_zones, load_taxi_zones
from batch_jobs.road_segment.transform import transform_road_segments
from batch_jobs.road_segment.validate import RoadSegmentRecord, validate_road_segments


@dataclass(frozen=True, slots=True)
class RoadSegmentBuildReport:
    records: tuple[RoadSegmentRecord, ...]
    taxi_zones: dict[int, BaseGeometry]
    input_segment_count: int
    rule_failures: dict[str, tuple[str, ...]]
    unmatched_taxi_zone_segment_ids: tuple[str, ...]


def build_road_segments(
    lion_path: Path,
    taxi_zone_zip: Path,
    snapshot_date: date,
    source_version: str,
    ingested_at: datetime,
) -> RoadSegmentBuildReport:
    rows = transform_road_segments(lion_path, snapshot_date, source_version, ingested_at)
    geometry_report = build_segment_geometries(lion_path)
    validation = validate_road_segments(rows, list(geometry_report.geometries))

    taxi_zones = load_taxi_zones(taxi_zone_zip)
    zone_report = assign_taxi_zones(list(validation.valid_records), taxi_zones)

    return RoadSegmentBuildReport(
        records=zone_report.records,
        taxi_zones=taxi_zones,
        input_segment_count=geometry_report.input_segment_count,
        rule_failures=validation.rule_failures,
        unmatched_taxi_zone_segment_ids=zone_report.unmatched_segment_ids,
    )
