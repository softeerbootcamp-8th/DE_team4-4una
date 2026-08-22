"""Small WGS84 geometry helpers without a heavyweight GIS dataframe layer."""

from __future__ import annotations

import math
from collections.abc import Iterable

from shapely.geometry import LineString, MultiLineString, Point
from shapely.geometry.base import BaseGeometry
from shapely.ops import linemerge

EARTH_RADIUS_M = 6_371_008.8


def haversine_m(first: tuple[float, float], second: tuple[float, float]) -> float:
    lon1, lat1 = map(math.radians, first)
    lon2, lat2 = map(math.radians, second)
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    value = math.sin(dlat / 2) ** 2 + (
        math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(value))


def line_length_m(line: LineString) -> float:
    return sum(haversine_m(first, second) for first, second in pairwise(line.coords))


def pairwise(
    values: Iterable[tuple[float, float]],
) -> Iterable[tuple[tuple[float, float], tuple[float, float]]]:
    iterator = iter(values)
    try:
        previous = next(iterator)
    except StopIteration:
        return
    for current in iterator:
        yield previous, current
        previous = current


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


def point_and_heading(line: LineString, fraction: float) -> tuple[Point, float]:
    fraction = min(1.0, max(0.0, fraction))
    point = line.interpolate(fraction, normalized=True)
    delta = min(0.001, 1 / max(1000, len(line.coords) * 100))
    before = line.interpolate(max(0.0, fraction - delta), normalized=True)
    after = line.interpolate(min(1.0, fraction + delta), normalized=True)
    heading = bearing_degrees((before.x, before.y), (after.x, after.y))
    return point, heading


def bearing_degrees(first: tuple[float, float], second: tuple[float, float]) -> float:
    lon1, lat1 = map(math.radians, first)
    lon2, lat2 = map(math.radians, second)
    dlon = lon2 - lon1
    x = math.sin(dlon) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - (
        math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
    )
    return math.degrees(math.atan2(x, y)) % 360
