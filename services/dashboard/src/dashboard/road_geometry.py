"""Read road-segment Parquet from S3 and convert its geometry for web maps."""

from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from typing import Any
from urllib.parse import urlparse

import boto3
import pyarrow.parquet as pq
import shapely
from pyproj import Transformer
from shapely.geometry import LineString, MultiLineString, mapping
from shapely.ops import transform as reproject_geometry

ROAD_SEGMENT_CRS = "EPSG:32118"
MAP_CRS = "EPSG:4326"
ROAD_SEGMENT_COLUMNS = ("segment_id", "street_name", "geometry_wkb", "location_id")

_TO_MAP_CRS = Transformer.from_crs(ROAD_SEGMENT_CRS, MAP_CRS, always_xy=True)


@dataclass(frozen=True, slots=True)
class RoadSegment:
    segment_id: str
    street_name: str | None
    geometry: dict[str, Any]
    # Null for segments the road environment build could not place in a taxi
    # zone; those cannot be filtered by borough.
    location_id: int | None = None


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split an S3 object URI into bucket and object key."""
    parsed = urlparse(uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    if parsed.scheme != "s3" or not bucket or not key:
        raise ValueError(f"expected an S3 object URI, got {uri!r}")
    return bucket, key


def wkb_to_map_geometry(geometry_wkb: bytes) -> dict[str, Any]:
    """Convert EPSG:32118 WKB to EPSG:4326 GeoJSON geometry."""
    geometry = shapely.from_wkb(geometry_wkb)
    if geometry.is_empty or not geometry.is_valid:
        raise ValueError("road segment geometry must be non-empty and valid")
    if not isinstance(geometry, (LineString, MultiLineString)):
        raise TypeError(
            "road segment geometry must be a LineString or MultiLineString, "
            f"got {geometry.geom_type}"
        )
    projected = reproject_geometry(_TO_MAP_CRS.transform, geometry)
    return dict(mapping(projected))


def load_road_segments(
    road_segment_s3_uri: str,
    aws_region: str | None = None,
) -> list[RoadSegment]:
    """Load one snapshot Parquet object using boto3's default credential chain."""
    bucket, key = parse_s3_uri(road_segment_s3_uri)
    client = boto3.client("s3", region_name=aws_region)
    response = client.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    try:
        parquet_bytes = body.read()
    finally:
        body.close()
    return load_road_segments_from_parquet(parquet_bytes)


def load_road_segments_from_parquet(parquet_bytes: bytes) -> list[RoadSegment]:
    """Build map-ready segment objects from an in-memory Parquet file."""
    schema = pq.read_schema(BytesIO(parquet_bytes))
    missing_columns = set(ROAD_SEGMENT_COLUMNS).difference(schema.names)
    if missing_columns:
        raise ValueError(
            "road_segment Parquet is missing columns: "
            f"{', '.join(sorted(missing_columns))}"
        )
    table = pq.read_table(BytesIO(parquet_bytes), columns=list(ROAD_SEGMENT_COLUMNS))

    segments: list[RoadSegment] = []
    seen_segment_ids: set[str] = set()
    for row in table.to_pylist():
        raw_segment_id = row["segment_id"]
        if raw_segment_id is None or not str(raw_segment_id).strip():
            raise ValueError("road_segment Parquet contains a blank segment_id")
        segment_id = str(raw_segment_id)
        if segment_id in seen_segment_ids:
            raise ValueError(f"duplicate road segment_id in snapshot: {segment_id}")
        seen_segment_ids.add(segment_id)

        geometry_wkb = row["geometry_wkb"]
        if geometry_wkb is None:
            raise ValueError(f"geometry_wkb is null for segment_id={segment_id}")
        segments.append(
            RoadSegment(
                segment_id=segment_id,
                street_name=(
                    str(row["street_name"]) if row["street_name"] is not None else None
                ),
                geometry=wkb_to_map_geometry(bytes(geometry_wkb)),
                location_id=(
                    int(row["location_id"])
                    if row["location_id"] is not None
                    else None
                ),
            )
        )
    return segments
