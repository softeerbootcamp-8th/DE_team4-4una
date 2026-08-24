"""Environment-driven configuration for the road-comfort dashboard."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

DEFAULT_SERVING_API_URL = "http://localhost:8000"
DEFAULT_VEHICLE_PROFILE_ID = 0

# This mirrors the serving API's current MAX_BATCH_ITEMS contract. Services must
# not import one another, so operators can override it when that API limit changes.
DEFAULT_BATCH_CHUNK_SIZE = 300
DEFAULT_REQUEST_TIMEOUT_SECONDS = 30.0

# The API caps a request at a few hundred segments, so a map-sized query becomes
# hundreds of round trips. They are issued concurrently, bounded here: the
# serving API's own connection pool is small, and out-running it would just move
# the queueing to its side.
DEFAULT_MAX_PARALLEL_REQUESTS = 8

ROAD_SEGMENT_CACHE_TTL_SECONDS = 24 * 60 * 60
SCORE_CACHE_TTL_SECONDS = 5 * 60

NYC_MAP_CENTER = (40.7128, -74.0060)
NYC_MAP_ZOOM = 11

# Zoom used after a borough is picked. A borough covers a small part of the
# city, so staying at the city-wide zoom would leave the selection a speck.
BOROUGH_MAP_ZOOM = 12

# Sentinel for "no borough picked": the map then shows borough outlines rather
# than road segments, which is also what makes the first render cheap.
ALL_BOROUGHS = "All boroughs"

# Folium inlines every rendered segment into the page as GeoJSON, and each one
# carries a six-field tooltip, so cost grows with the number drawn rather than
# the number in the snapshot. Only the current viewport is drawn, and this caps
# even that: zoomed out far enough, the viewport still covers a whole borough.
MAX_RENDERED_SEGMENTS = 3000


@dataclass(frozen=True, slots=True)
class DashboardConfig:
    """Configuration needed by the dashboard.

    Database settings are intentionally absent. Comfort scores always cross the
    serving API boundary.
    """

    road_segment_s3_uri: str
    zone_master_s3_uri: str | None
    serving_api_url: str
    vehicle_profile_id: int
    batch_chunk_size: int
    request_timeout_seconds: float
    aws_region: str | None

    @property
    def batch_endpoint(self) -> str:
        return f"{self.serving_api_url.rstrip('/')}/api/v1/comfort-scores/batch"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> DashboardConfig:
        source = env if env is not None else os.environ
        road_segment_s3_uri = _require(source, "DASHBOARD_ROAD_SEGMENT_S3_URI")
        if not road_segment_s3_uri.startswith("s3://"):
            raise ValueError("DASHBOARD_ROAD_SEGMENT_S3_URI must start with s3://")

        # Optional: the map still works without it, minus the borough filter.
        zone_master_s3_uri = source.get("DASHBOARD_ZONE_MASTER_S3_URI") or None
        if zone_master_s3_uri and not zone_master_s3_uri.startswith("s3://"):
            raise ValueError("DASHBOARD_ZONE_MASTER_S3_URI must start with s3://")

        vehicle_profile_id = int(
            source.get("DASHBOARD_VEHICLE_PROFILE_ID") or DEFAULT_VEHICLE_PROFILE_ID
        )
        if vehicle_profile_id < 0:
            raise ValueError("DASHBOARD_VEHICLE_PROFILE_ID must be >= 0")

        batch_chunk_size = int(
            source.get("DASHBOARD_BATCH_CHUNK_SIZE") or DEFAULT_BATCH_CHUNK_SIZE
        )
        if batch_chunk_size < 1:
            raise ValueError("DASHBOARD_BATCH_CHUNK_SIZE must be >= 1")

        request_timeout_seconds = float(
            source.get("DASHBOARD_REQUEST_TIMEOUT_SECONDS")
            or DEFAULT_REQUEST_TIMEOUT_SECONDS
        )
        if request_timeout_seconds <= 0:
            raise ValueError("DASHBOARD_REQUEST_TIMEOUT_SECONDS must be > 0")

        return cls(
            road_segment_s3_uri=road_segment_s3_uri,
            zone_master_s3_uri=zone_master_s3_uri,
            serving_api_url=(
                source.get("DASHBOARD_SERVING_API_URL") or DEFAULT_SERVING_API_URL
            ),
            vehicle_profile_id=vehicle_profile_id,
            batch_chunk_size=batch_chunk_size,
            request_timeout_seconds=request_timeout_seconds,
            aws_region=(source.get("AWS_REGION") or source.get("AWS_DEFAULT_REGION")),
        )


def _require(source: Mapping[str, str], key: str) -> str:
    value = source.get(key)
    if not value:
        raise ValueError(f"{key} must be set")
    return value
