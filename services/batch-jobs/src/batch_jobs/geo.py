"""Geometry helpers for monthly road-reference preparation."""

from __future__ import annotations

from shapely.geometry import LineString, MultiLineString
from shapely.geometry.base import BaseGeometry
from shapely.ops import linemerge


def as_line(geometry: BaseGeometry) -> LineString:
    if isinstance(geometry, LineString):
        return geometry
    if isinstance(geometry, MultiLineString):
        merged = linemerge(geometry)
        if isinstance(merged, LineString):
            return merged
        return LineString(max(merged.geoms, key=lambda value: value.length).coords)
    raise ValueError(f"expected line geometry, received {geometry.geom_type}")
