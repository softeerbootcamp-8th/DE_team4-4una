"""Assign TLC taxi zone location_id to road_segment records via spatial join."""

from __future__ import annotations

import io
import zipfile
from dataclasses import dataclass, replace
from pathlib import Path

import shapefile
from pyproj import CRS, Transformer
from shapely import STRtree
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform as reproject_geometry

from road_segment.geometry import TARGET_CRS, geometry_from_wkb
from road_segment.validate import RoadSegmentRecord


@dataclass(frozen=True, slots=True)
class TaxiZoneJoinReport:
    records: tuple[RoadSegmentRecord, ...]
    unmatched_segment_ids: tuple[str, ...]


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
        transformer = Transformer.from_crs(source_crs, TARGET_CRS, always_xy=True)
        field_names = [field[0] for field in reader.fields[1:]]
        location_index = next(
            index for index, name in enumerate(field_names) if name.lower() == "locationid"
        )
        zones = {
            int(record.record[location_index]): reproject_geometry(
                transformer.transform, shape(record.shape.__geo_interface__)
            )
            for record in reader.iterShapeRecords()
        }
    if not zones:
        raise ValueError("taxi-zone snapshot contains no zones")
    return zones


def find_location_id(
    geometry: BaseGeometry,
    zone_ids: list[int],
    zone_geometries: list[BaseGeometry],
    tree: STRtree,
) -> int | None:
    # 여러 Zone과 겹치는 경우 Segment 중간점이 속한 Zone 하나로 결정한다
    # (environment.py의 assign_taxi_zones()와 동일한 기준).
    midpoint = geometry.interpolate(0.5, normalized=True)
    for candidate_index in tree.query(midpoint):
        index = int(candidate_index)
        if zone_geometries[index].covers(midpoint):
            return zone_ids[index]
    return None


def assign_taxi_zones(
    records: list[RoadSegmentRecord], taxi_zones: dict[int, BaseGeometry]
) -> TaxiZoneJoinReport:
    zone_ids = list(taxi_zones)
    zone_geometries = [taxi_zones[zone_id] for zone_id in zone_ids]
    tree = STRtree(zone_geometries)

    assigned: list[RoadSegmentRecord] = []
    unmatched: list[str] = []
    for record in records:
        location_id = None
        if record.geometry_wkb is not None:
            geometry = geometry_from_wkb(record.geometry_wkb)
            location_id = find_location_id(geometry, zone_ids, zone_geometries, tree)
        if location_id is None:
            unmatched.append(record.segment_id)
        assigned.append(replace(record, location_id=location_id))

    return TaxiZoneJoinReport(
        records=tuple(assigned),
        unmatched_segment_ids=tuple(unmatched),
    )
