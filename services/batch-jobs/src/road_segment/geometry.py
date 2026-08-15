"""Standardize NYC LION geometry into EPSG:32118 LineString WKB."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import shapely
from pyproj import Transformer
from shapely.geometry import LineString, MultiLineString, shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform as reproject_geometry

from road_segment.transform import (
    is_vehicle_segment,
    normalize_string,
    select_representative_rows,
)

SOURCE_CRS = "EPSG:4326"
# EPSG:2263(NY Long Island, feet)와 동일한 투영이되 단위만 meter인 CRS.
# downstream 거리 계산(Map Matching, threshold)이 전부 meter 기준이라
# geometry도 feet 변환 없이 바로 meter로 쓸 수 있게 이걸 쓴다.
TARGET_CRS = "EPSG:32118"

# Transformer 생성 비용이 있어 모듈 로드 시 한 번만 만들어 재사용한다.
_TRANSFORMER = Transformer.from_crs(SOURCE_CRS, TARGET_CRS, always_xy=True)


@dataclass(frozen=True, slots=True)
class SegmentGeometry:
    """segment_id paired with a WKB-encoded EPSG:32118 LineString.

    WKB 자체엔 CRS 정보가 없으므로, geometry_wkb는 항상 EPSG:32118(meter)
    기준이라는 계약을 여기 명시한다.
    """

    segment_id: str
    geometry_wkb: bytes


@dataclass(frozen=True, slots=True)
class GeometryBuildReport:
    geometries: tuple[SegmentGeometry, ...]
    input_segment_count: int
    excluded_geometry_count: int


def load_lion_rows_with_geometry(path: Path) -> list[dict[str, object]]:
    """Load LION rows with geometry attached under the "_geometry" key.

    transform.load_lion_rows()는 geometry를 버리므로, is_vehicle_segment와
    select_representative_rows를 그대로 재사용할 수 있도록 여기서는 geometry를
    같은 dict에 "_geometry" 키로 같이 담아 둔다.
    """
    document = json.loads(path.read_text())
    features = document.get("features")
    if not isinstance(features, list):
        raise TypeError(f"{path.name} must be a GeoJSON FeatureCollection")
    rows = []
    for feature in features:
        row = dict(feature.get("properties") or {})
        row["_geometry"] = feature.get("geometry")
        rows.append(row)
    return rows


def reproject_to_target_crs(geometry: BaseGeometry) -> BaseGeometry:
    return reproject_geometry(_TRANSFORMER.transform, geometry)


def normalize_line_geometry(raw_geometry: object) -> BaseGeometry | None:
    """Reproject to EPSG:32118 and collapse to a single LineString.

    LION 소스 기하는 fetch_lion()의 outSR=4326으로 이미 WGS84이므로,
    LineString 정규화 전에 EPSG:32118로 먼저 투영한다.
    """
    if not raw_geometry:
        return None
    try:
        geometry = shape(raw_geometry)
    except (AttributeError, TypeError, ValueError):
        return None
    if geometry.is_empty or not geometry.is_valid:
        return None
    projected = reproject_to_target_crs(geometry)
    line = merge_to_single_line(projected)
    if line is None or line.is_empty or not line.is_valid:
        return None
    return line


def merge_to_single_line(geometry: BaseGeometry) -> LineString | None:
    """part가 여러 개인 MultiLineString은 방향(NodeIDFrom->To)을 보장할 수 없어 제외한다."""
    if isinstance(geometry, LineString):
        return geometry
    if isinstance(geometry, MultiLineString):
        if len(geometry.geoms) != 1:
            return None
        return geometry.geoms[0]
    return None


def build_segment_geometry(row: dict[str, object]) -> SegmentGeometry | None:
    segment_id = normalize_string(row.get("SegmentID"))
    if not segment_id:
        return None
    line = normalize_line_geometry(row.get("_geometry"))
    if line is None:
        return None
    return SegmentGeometry(segment_id=segment_id, geometry_wkb=shapely.to_wkb(line))


def geometry_from_wkb(geometry_wkb: bytes) -> BaseGeometry:
    return shapely.from_wkb(geometry_wkb)


def build_segment_geometries(path: Path) -> GeometryBuildReport:
    rows = load_lion_rows_with_geometry(path)
    vehicle_rows = [row for row in rows if is_vehicle_segment(row)]
    representative_rows = select_representative_rows(vehicle_rows)

    geometries: list[SegmentGeometry] = []
    excluded_count = 0
    for row in representative_rows:
        geometry = build_segment_geometry(row)
        if geometry is None:
            excluded_count += 1
            continue
        geometries.append(geometry)

    return GeometryBuildReport(
        geometries=tuple(geometries),
        input_segment_count=len(representative_rows),
        excluded_geometry_count=excluded_count,
    )
