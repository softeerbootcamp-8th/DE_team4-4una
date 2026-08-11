"""Geometry helpers for monthly road-reference preparation."""

from __future__ import annotations

import math
from collections.abc import Iterable

from shapely.geometry import LineString, MultiLineString
from shapely.geometry.base import BaseGeometry
from shapely.ops import linemerge

EARTH_RADIUS_M = 6_371_008.8


def as_line(geometry: BaseGeometry) -> LineString:
    if isinstance(geometry, LineString):
        return geometry
    if isinstance(geometry, MultiLineString):
        merged = linemerge(geometry)
        if isinstance(merged, LineString):
            return merged
        return LineString(max(merged.geoms, key=lambda value: value.length).coords)
    raise ValueError(f"expected line geometry, received {geometry.geom_type}")


def line_length_m(line: LineString) -> float:
    return sum(haversine_m(first, second) for first, second in pairwise(line.coords))


def haversine_m(first: tuple[float, float], second: tuple[float, float]) -> float:
    lon1, lat1 = map(math.radians, first)
    lon2, lat2 = map(math.radians, second)
    longitude_delta = lon2 - lon1
    latitude_delta = lat2 - lat1
    value = math.sin(latitude_delta / 2) ** 2 + (
        math.cos(lat1)
        * math.cos(lat2)
        * math.sin(longitude_delta / 2) ** 2
    )
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(value))


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
