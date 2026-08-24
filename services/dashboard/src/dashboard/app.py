"""Streamlit assembly for the NYC road comfort-score map."""

from __future__ import annotations

from collections.abc import Sequence

import httpx
import streamlit as st
from botocore.exceptions import BotoCoreError, ClientError
from streamlit_folium import st_folium

from dashboard.config import (
    ALL_BOROUGHS,
    BOROUGH_MAP_ZOOM,
    MAX_RENDERED_SEGMENTS,
    NYC_MAP_CENTER,
    NYC_MAP_ZOOM,
    ROAD_SEGMENT_CACHE_TTL_SECONDS,
    SCORE_CACHE_TTL_SECONDS,
    DashboardConfig,
)
from dashboard.map_view import (
    build_borough_map,
    build_map,
    join_road_segments_with_scores,
)
from dashboard.road_geometry import RoadSegment, load_road_segments
from dashboard.serving_api_client import (
    ComfortScoreBatchResult,
    fetch_comfort_scores,
)
from dashboard.zone_master import (
    Borough,
    borough_outlines,
    load_zone_master,
    zone_boroughs,
)

# (min_lon, min_lat, max_lon, max_lat) per segment, and (south, west, north,
# east) for a viewport -- the two orders differ because the first follows
# GeoJSON coordinate order and the second follows Leaflet's bounds payload.
SegmentBounds = tuple[float, float, float, float]
Viewport = tuple[float, float, float, float]

# Leaflet reports bounds at full float precision, so panning by a pixel yields a
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


@st.cache_data(ttl=ROAD_SEGMENT_CACHE_TTL_SECONDS, show_spinner=False)
def _load_zone_master_cached(
    zone_master_s3_uri: str,
    aws_region: str | None,
) -> bytes:
    """The raw object, so the outlines and the zone lookup share one download."""
    return load_zone_master(zone_master_s3_uri, aws_region)


@st.cache_data(ttl=ROAD_SEGMENT_CACHE_TTL_SECONDS, show_spinner=False)
def _load_boroughs_cached(
    zone_master_s3_uri: str,
    aws_region: str | None,
) -> list[Borough]:
    return borough_outlines(_load_zone_master_cached(zone_master_s3_uri, aws_region))


@st.cache_data(ttl=ROAD_SEGMENT_CACHE_TTL_SECONDS, show_spinner=False)
def _borough_segment_indices_cached(
    road_segment_s3_uri: str,
    zone_master_s3_uri: str,
    aws_region: str | None,
    borough: str,
) -> list[int]:
    """Positions of the segments in one borough.

    Indices rather than segments so the bounding boxes stay aligned, and cached
    per borough because panning reruns the whole script.
    """
    segments = _load_road_segments_cached(road_segment_s3_uri, aws_region)
    boroughs = zone_boroughs(_load_zone_master_cached(zone_master_s3_uri, aws_region))
    return [
        index
        for index, segment in enumerate(segments)
        if segment.location_id is not None
        and boroughs.get(segment.location_id) == borough
    ]


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
        boroughs: list[Borough] = []
        if config.zone_master_s3_uri:
            with st.spinner("Loading TLC zones from S3..."):
                boroughs = _load_boroughs_cached(
                    config.zone_master_s3_uri,
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
        st.error(f"Unable to load reference data from S3: {exc}")
        st.stop()
    except httpx.HTTPError as exc:
        st.error(f"Unable to load comfort scores from the Serving API: {exc}")
        st.stop()

    # Snapshot-wide, deliberately not narrowed by the borough or the viewport:
    # coverage that moved as the map moved would be more confusing than useful.
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

    if not boroughs:
        st.info(
            "Set DASHBOARD_ZONE_MASTER_S3_URI to pick a borough. Showing every "
            "segment in the snapshot instead."
        )
        _render_segments(config, road_segments, segment_bounds, score_result, None)
        return

    selected = _borough_selector(boroughs)
    if selected == ALL_BOROUGHS:
        _render_borough_overview(boroughs)
    else:
        _render_segments(config, road_segments, segment_bounds, score_result, selected)


def _borough_selector(boroughs: Sequence[Borough]) -> str:
    """Dropdown mirroring the clickable outlines.

    Kept alongside the map because clicking depends on st_folium reporting the
    click, and a selection that cannot be undone would strand the user.
    """
    names = [ALL_BOROUGHS, *(borough.name for borough in boroughs)]
    current = st.session_state.get("borough", ALL_BOROUGHS)
    if current not in names:
        current = ALL_BOROUGHS
    choice = st.selectbox("Borough", names, index=names.index(current))
    if choice != current:
        _select_borough(choice, boroughs)
    return current


def _select_borough(name: str, boroughs: Sequence[Borough]) -> None:
    """Move the map onto the selection and redraw.

    The viewport is cleared rather than kept: it still describes wherever the
    user was looking, and filtering the new borough by it would usually leave
    nothing on screen.
    """
    st.session_state["borough"] = name
    st.session_state["viewport"] = None
    if name == ALL_BOROUGHS:
        st.session_state["center"] = NYC_MAP_CENTER
        st.session_state["zoom"] = NYC_MAP_ZOOM
    else:
        selected = next((b for b in boroughs if b.name == name), None)
        if selected is not None:
            st.session_state["center"] = selected.center
            st.session_state["zoom"] = BOROUGH_MAP_ZOOM
    st.rerun()


def _render_borough_overview(boroughs: Sequence[Borough]) -> None:
    center = st.session_state.get("center", NYC_MAP_CENTER)
    zoom = st.session_state.get("zoom", NYC_MAP_ZOOM)

    st.caption("Click a borough to load its road segments.")
    map_state = st_folium(
        build_borough_map(boroughs, center=center, zoom=zoom),
        width=None,
        height=720,
        center=center,
        zoom=zoom,
        # No viewport tracking here: six outlines are cheap to draw in full, so
        # nothing needs to be filtered by what is on screen.
        returned_objects=["last_object_clicked_tooltip"],
        key="borough_map",
    )

    clicked = _clicked_borough(map_state, boroughs)
    if clicked is not None:
        _select_borough(clicked, boroughs)


def _clicked_borough(
    map_state: dict | None,
    boroughs: Sequence[Borough],
) -> str | None:
    """Match st_folium's clicked-tooltip text back to a borough.

    The payload is the rendered tooltip rather than the feature's properties,
    so the name is matched inside it. No borough name contains another, so a
    substring match cannot pick the wrong one.
    """
    if not map_state:
        return None
    tooltip = map_state.get("last_object_clicked_tooltip")
    if not tooltip:
        return None
    for borough in boroughs:
        if borough.name in tooltip:
            return borough.name
    return None


def _render_segments(
    config: DashboardConfig,
    road_segments: Sequence[RoadSegment],
    segment_bounds: Sequence[SegmentBounds],
    score_result: ComfortScoreBatchResult,
    borough: str | None,
) -> None:
    if borough is None:
        candidate_indices: Sequence[int] = range(len(road_segments))
    else:
        candidate_indices = _borough_segment_indices_cached(
            config.road_segment_s3_uri,
            config.zone_master_s3_uri,
            config.aws_region,
            borough,
        )

    # The viewport is only known after the map has rendered once, so the first
    # pass draws nothing and the reported bounds drive every pass after it.
    viewport: Viewport | None = st.session_state.get("viewport")
    center = st.session_state.get("center", NYC_MAP_CENTER)
    zoom = st.session_state.get("zoom", NYC_MAP_ZOOM)

    if viewport is None:
        in_viewport: list[int] = []
    else:
        in_viewport = [
            index
            for index in candidate_indices
            if _intersects(segment_bounds[index], viewport)
        ]

    visible_segments = [road_segments[i] for i in in_viewport[:MAX_RENDERED_SEGMENTS]]
    if len(in_viewport) > MAX_RENDERED_SEGMENTS:
        st.info(
            f"{len(in_viewport):,} segments are in view and the first "
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
        key="segment_map",
    )

    scope = borough if borough is not None else "the snapshot"
    st.caption(
        f"Rendered: {len(visible_segments):,} of {len(candidate_indices):,} "
        f"segments in {scope} · "
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
