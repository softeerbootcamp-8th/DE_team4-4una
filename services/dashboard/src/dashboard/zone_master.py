"""Read the TLC zone reference from S3 for borough lookups and outlines.

`road_segment` carries only `location_id`, and ADR-0005 makes `zone_master` the
canonical zone reference, so borough information is read here rather than
denormalised into every road segment.
"""

from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from typing import Any

import boto3
import pyarrow.parquet as pq
import shapely
from shapely.geometry import mapping

from dashboard.road_geometry import parse_s3_uri

ZONE_MASTER_COLUMNS = ("location_id", "borough", "geometry")

# Degrees, roughly 22m. Dissolving the zones keeps every coastline vertex, which
# is ~2.9MB of GeoJSON inlined into the page; simplifying at this tolerance cuts
# that to ~350KB without a visible difference at borough scale.
_OUTLINE_SIMPLIFY_TOLERANCE = 0.0002


@dataclass(frozen=True, slots=True)
class Borough:
    name: str
    geometry: dict[str, Any]
    # (min_lon, min_lat, max_lon, max_lat), used to move the map to a selection.
    bounds: tuple[float, float, float, float]

    @property
    def center(self) -> tuple[float, float]:
        min_lon, min_lat, max_lon, max_lat = self.bounds
        return ((min_lat + max_lat) / 2, (min_lon + max_lon) / 2)


def load_zone_master(
    zone_master_s3_uri: str,
    aws_region: str | None = None,
) -> bytes:
    """Fetch one zone_master Parquet object using boto3's default credential chain."""
    bucket, key = parse_s3_uri(zone_master_s3_uri)
    client = boto3.client("s3", region_name=aws_region)
    response = client.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    try:
        return body.read()
    finally:
        body.close()


def _read_table(parquet_bytes: bytes):
    schema = pq.read_schema(BytesIO(parquet_bytes))
    missing_columns = set(ZONE_MASTER_COLUMNS).difference(schema.names)
    if missing_columns:
        raise ValueError(
            "zone_master Parquet is missing columns: "
            f"{', '.join(sorted(missing_columns))}"
        )
    return pq.read_table(BytesIO(parquet_bytes), columns=list(ZONE_MASTER_COLUMNS))


def zone_boroughs(parquet_bytes: bytes) -> dict[int, str]:
    """Map location_id to borough name."""
    boroughs: dict[int, str] = {}
    for row in _read_table(parquet_bytes).to_pylist():
        location_id = row["location_id"]
        borough = row["borough"]
        # A zone with no borough label cannot be offered as a filter choice.
        if location_id is None or borough is None:
            continue
        boroughs[int(location_id)] = str(borough)
    return boroughs


def borough_outlines(parquet_bytes: bytes) -> list[Borough]:
    """Dissolve the 265 zone polygons into one outline per borough.

    zone_master is keyed by zone, not borough, so the borough shapes the
    overview map needs do not exist until the zones are merged. location_id
    264/265 are the non-spatial TLC placeholders and carry no polygon.
    """
    geometries_by_borough: dict[str, list] = {}
    for row in _read_table(parquet_bytes).to_pylist():
        borough = row["borough"]
        geometry_wkb = row["geometry"]
        if borough is None or geometry_wkb is None:
            continue
        geometries_by_borough.setdefault(str(borough), []).append(
            shapely.from_wkb(bytes(geometry_wkb))
        )

    outlines = []
    for name, geometries in geometries_by_borough.items():
        merged = shapely.union_all(geometries)
        if merged.is_empty:
            continue
        # Bounds come from the full-detail shape: they position the map, so
        # they should not inherit the simplification error.
        bounds = tuple(merged.bounds)
        simplified = merged.simplify(
            _OUTLINE_SIMPLIFY_TOLERANCE, preserve_topology=True
        )
        outlines.append(
            Borough(
                name=name,
                geometry=dict(mapping(simplified)),
                bounds=bounds,
            )
        )
    return sorted(outlines, key=lambda borough: borough.name)
