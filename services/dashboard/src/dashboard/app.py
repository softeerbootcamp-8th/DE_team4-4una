"""Streamlit assembly for the NYC road comfort-score map."""

from __future__ import annotations

from collections.abc import Sequence

import httpx
import streamlit as st
from botocore.exceptions import BotoCoreError, ClientError
from streamlit_folium import st_folium

from dashboard.config import (
    MAX_RENDERED_SEGMENTS,
    NYC_MAP_CENTER,
    NYC_MAP_ZOOM,
    ROAD_SEGMENT_CACHE_TTL_SECONDS,
    SCORE_CACHE_TTL_SECONDS,
    DashboardConfig,
)
from dashboard.map_view import build_map, join_road_segments_with_scores
from dashboard.road_geometry import RoadSegment, load_road_segments
from dashboard.serving_api_client import (
    ComfortScoreBatchResult,
    fetch_comfort_scores,
)

# (min_lon, min_lat, max_lon, max_lat) per segment, and (south, west, north,
# east) for a viewport -- the two orders differ because the first follows
# GeoJSON coordinate order and the second follows Leaflet's bounds payload.
SegmentBounds = tuple[float, float, float, float]
Viewport = tuple[float, float, float, float]

# Leaflet reports bounds as full float precision, so panning by a pixel yields a
# different value every frame. Rounding to ~1m keeps that from triggering an
# endless rerun loop while still tracking real movement.
_VIEWPORT_PRECISION = 5


@st.cache_data(ttl=ROAD_SEGMENT_CACHE_TTL_SECONDS, show_spinner=False)
def _load_road_segments_cached(
    road_segment_s3_uri: str,
    aws_region: str | None,
) -> list[RoadSegment]:
    return load_road_segments(road_segment_s3_uri, aws_region)


@st.cache_data(ttl=ROAD_SEGMENT_CACHE_TTL_SECONDS, show_spinner=False)
def _load_segment_bounds_cached(
    road_segment_s3_uri: str,
    aws_region: str | None,
) -> list[SegmentBounds]:
    """Bounding box per segment, in the same order as the loaded segments.

    Keyed on the snapshot location rather than the segment list so it is
    computed once per snapshot: the segments themselves come from another cached
    call, so this does not re-read S3.
    """
    segments = _load_road_segments_cached(road_segment_s3_uri, aws_region)
    return [_geometry_bounds(segment.geometry) for segment in segments]


def _geometry_bounds(geometry: dict) -> SegmentBounds:
    coordinates = geometry["coordinates"]
    points = (
        coordinates
        if geometry["type"] == "LineString"
        else [point for line in coordinates for point in line]
    )
    longitudes = [point[0] for point in points]
    latitudes = [point[1] for point in points]
    return min(longitudes), min(latitudes), max(longitudes), max(latitudes)


def _parse_viewport(bounds: dict | None) -> Viewport | None:
    """Convert st_folium's Leaflet bounds payload to (south, west, north, east)."""
    if not bounds:
        return None
    south_west = bounds.get("_southWest")
    north_east = bounds.get("_northEast")
    if not south_west or not north_east:
        return None
    return (
        south_west["lat"],
        south_west["lng"],
        north_east["lat"],
        north_east["lng"],
    )


def _intersects(segment_bounds: SegmentBounds, viewport: Viewport) -> bool:
    min_lon, min_lat, max_lon, max_lat = segment_bounds
    south, west, north, east = viewport
    return not (
        max_lon < west or min_lon > east or max_lat < south or min_lat > north
    )


def _segments_in_viewport(
    segments: Sequence[RoadSegment],
    bounds: Sequence[SegmentBounds],
    viewport: Viewport,
) -> list[RoadSegment]:
    return [
        segment
        for segment, segment_bounds in zip(segments, bounds, strict=True)
        if _intersects(segment_bounds, viewport)
    ]


def _rounded(viewport: Viewport | None) -> tuple[float, ...] | None:
    if viewport is None:
        return None
    return tuple(round(value, _VIEWPORT_PRECISION) for value in viewport)


@st.cache_data(ttl=SCORE_CACHE_TTL_SECONDS, show_spinner=False)
def _load_scores_cached(
    endpoint: str,
    vehicle_profile_id: int,
    segment_ids: tuple[str, ...],
    batch_chunk_size: int,
    timeout_seconds: float,
) -> ComfortScoreBatchResult:
    return fetch_comfort_scores(
        endpoint=endpoint,
        vehicle_profile_id=vehicle_profile_id,
        segment_ids=segment_ids,
        batch_size=batch_chunk_size,
        timeout_seconds=timeout_seconds,
    )


def main() -> None:
    st.set_page_config(page_title="NYC Road Comfort Score", layout="wide")
    st.title("NYC Road Comfort Score Map")
    st.caption(
        "Road geometry comes from the configured S3 snapshot. Comfort scores "
        "come only from the Serving API."
    )

    try:
        config = DashboardConfig.from_env()
        with st.spinner("Loading NYC road segments from S3..."):
            road_segments = _load_road_segments_cached(
                config.road_segment_s3_uri,
                config.aws_region,
            )
            segment_bounds = _load_segment_bounds_cached(
                config.road_segment_s3_uri,
                config.aws_region,
            )
        segment_ids = tuple(segment.segment_id for segment in road_segments)
        with st.spinner("Loading comfort scores from the Serving API..."):
            score_result = _load_scores_cached(
                config.batch_endpoint,
                config.vehicle_profile_id,
                segment_ids,
                config.batch_chunk_size,
                config.request_timeout_seconds,
            )
    except ValueError as exc:
        st.error(f"Dashboard configuration or data is invalid: {exc}")
        st.stop()
    except (BotoCoreError, ClientError) as exc:
        st.error(f"Unable to load road segments from S3: {exc}")
        st.stop()
    except httpx.HTTPError as exc:
        st.error(f"Unable to load comfort scores from the Serving API: {exc}")
        st.stop()

    scored_count = len(score_result.scores)
    total_count = len(road_segments)
    missing_count = total_count - scored_count
    coverage = scored_count / total_count * 100 if total_count else 0.0

    metric_columns = st.columns(4)
    metric_columns[0].metric("Road segments", f"{total_count:,}")
    metric_columns[1].metric("Scored", f"{scored_count:,}")
    metric_columns[2].metric("No score", f"{missing_count:,}")
    metric_columns[3].metric("Coverage", f"{coverage:.1f}%")

    if score_result.vehicle_profile_fallback:
        st.warning(
            "Serving API used vehicle profile "
            f"{score_result.effective_vehicle_profile_id} instead of requested "
            f"profile {score_result.requested_vehicle_profile_id}."
        )

    # The viewport is only known after the map has rendered once, so the first
    # pass draws nothing and the reported bounds drive every pass after it.
    viewport: Viewport | None = st.session_state.get("viewport")
    center = st.session_state.get("center", NYC_MAP_CENTER)
    zoom = st.session_state.get("zoom", NYC_MAP_ZOOM)

    if viewport is None:
        visible_segments: list[RoadSegment] = []
        in_viewport_count = 0
    else:
        visible_segments = _segments_in_viewport(
            road_segments, segment_bounds, viewport
        )
        in_viewport_count = len(visible_segments)
        visible_segments = visible_segments[:MAX_RENDERED_SEGMENTS]

    if in_viewport_count > MAX_RENDERED_SEGMENTS:
        st.info(
            f"{in_viewport_count:,} segments are in view and the first "
            f"{MAX_RENDERED_SEGMENTS:,} are drawn. Zoom in to see all of them."
        )

    joined_segments = join_road_segments_with_scores(
        visible_segments,
        score_result.scores,
    )

    map_state = st_folium(
        build_map(joined_segments, center=center, zoom=zoom),
        width=None,
        height=720,
        center=center,
        zoom=zoom,
        returned_objects=["bounds", "zoom"],
    )

    st.caption(
        f"Rendered: {len(visible_segments):,} of {total_count:,} segments · "
        f"Road geometry cache: {ROAD_SEGMENT_CACHE_TTL_SECONDS // 3600}h · "
        f"Comfort score cache: {SCORE_CACHE_TTL_SECONDS // 60}m · "
        f"Serving API chunk size: {config.batch_chunk_size:,}"
    )

    _sync_viewport(map_state, viewport, zoom)


def _sync_viewport(
    map_state: dict | None,
    viewport: Viewport | None,
    zoom: int,
) -> None:
    """Store the viewport the user is actually looking at, then redraw for it.

    st_folium reports the viewport only after the map has rendered, so the map
    on screen always lags one pass behind. Rerunning once the reported viewport
    differs from the one just drawn closes that gap; comparing rounded values
    keeps sub-metre jitter from rerunning forever.
    """
    if not map_state:
        return
    reported = _parse_viewport(map_state.get("bounds"))
    if reported is None:
        return
    reported_zoom = map_state.get("zoom") or zoom
    if _rounded(reported) == _rounded(viewport) and reported_zoom == zoom:
        return

    south, west, north, east = reported
    st.session_state["viewport"] = reported
    st.session_state["center"] = ((south + north) / 2, (west + east) / 2)
    st.session_state["zoom"] = reported_zoom
    st.rerun()


if __name__ == "__main__":
    main()
