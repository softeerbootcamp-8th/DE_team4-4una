"""Small WGS84 geometry helpers without a heavyweight GIS dataframe layer."""

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
        longest = max(merged.geoms, key=lambda item: item.length)
        return LineString(longest.coords)
    raise ValueError(f"expected line geometry, received {geometry.geom_type}")


def reversed_line(line: LineString) -> LineString:
    return LineString(list(line.coords)[::-1])
