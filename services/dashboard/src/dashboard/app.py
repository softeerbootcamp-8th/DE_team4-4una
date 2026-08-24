"""Streamlit assembly for the NYC road comfort-score map."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

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
# different value every frame. Every change costs a rerun and a full redraw, so
# this rounds to ~100m: small enough to follow real panning, coarse enough that
# nudging the map does not rebuild it.
_VIEWPORT_PRECISION = 3


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


def _group_indices_by_borough(
    segments: Sequence[RoadSegment],
    boroughs: Mapping[int, str],
) -> dict[str, tuple[int, ...]]:
    """모든 segment를 한 번만 순회해 borough 이름 -> segment 위치로 묶는다(#414).

    이전에는 borough별로 따로 캐시해서, 처음 보는 borough를 고를 때마다
    전체 segment를 다시 훑었다(Manhattan 처음 선택 시 O(N), Brooklyn 처음
    선택 시 또 O(N), ...). 여기서 한 번만 O(N)으로 묶어두면 이후 어떤
    borough를 골라도 dict lookup 하나로 끝난다.
    """
    grouped: dict[str, list[int]] = {}
    for index, segment in enumerate(segments):
        if segment.location_id is None:
            continue
        borough = boroughs.get(segment.location_id)
        if borough is not None:
            grouped.setdefault(borough, []).append(index)
    return {name: tuple(indices) for name, indices in grouped.items()}


@st.cache_data(ttl=ROAD_SEGMENT_CACHE_TTL_SECONDS, show_spinner=False)
def _borough_segment_indices_cached(
    road_segment_s3_uri: str,
    zone_master_s3_uri: str,
    aws_region: str | None,
) -> dict[str, tuple[int, ...]]:
    segments = _load_road_segments_cached(road_segment_s3_uri, aws_region)
    boroughs = zone_boroughs(_load_zone_master_cached(zone_master_s3_uri, aws_region))
    return _group_indices_by_borough(segments, boroughs)


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
    """Convert st_folium's Leaflet bounds payload to (south, west, north, east).

    Returns None until the payload carries real coordinates: before Leaflet has
    laid the map out it reports the corner keys with null values, so a present
    key is not proof of a usable coordinate.
    """
    if not bounds:
        return None
    south_west = bounds.get("_southWest") or {}
    north_east = bounds.get("_northEast") or {}
    corners = (
        south_west.get("lat"),
        south_west.get("lng"),
        north_east.get("lat"),
        north_east.get("lng"),
    )
    if any(corner is None for corner in corners):
        return None
    return corners


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


def _visible_segments(
    candidate_indices: Sequence[int],
    road_segments: Sequence[RoadSegment],
    segment_bounds: Sequence[SegmentBounds],
    viewport: Viewport | None,
    max_rendered: int,
) -> tuple[list[RoadSegment], int]:
    """그릴 segment이자 score를 조회할 segment — viewport로 걸러내고 상한을 적용한다(#414).

    viewport를 아직 모르면 ([], 0)을 반환한다. 두 번째 값은 상한 적용 전
    교차 개수로, "N개 더 있음, 확대하라" 안내에 쓴다.
    """
    if viewport is None:
        return [], 0
    in_viewport = [
        index
        for index in candidate_indices
        if _intersects(segment_bounds[index], viewport)
    ]
    visible = [road_segments[i] for i in in_viewport[:max_rendered]]
    return visible, len(in_viewport)


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
    except ValueError as exc:
        st.error(f"Dashboard configuration or data is invalid: {exc}")
        st.stop()
    except (BotoCoreError, ClientError) as exc:
        st.error(f"Unable to load reference data from S3: {exc}")
        st.stop()
    except httpx.HTTPError as exc:
        st.error(f"Unable to load comfort scores from the Serving API: {exc}")
        st.stop()

    if not boroughs:
        st.info(
            "Set DASHBOARD_ZONE_MASTER_S3_URI to pick a borough. Showing every "
            "segment in the snapshot instead."
        )
    selected = _borough_selector(boroughs)
    borough = None if selected == ALL_BOROUGHS else selected
    candidate_indices = _candidate_indices(config, road_segments, boroughs, borough)

    _render_metrics(len(candidate_indices), borough, len(road_segments))

    if boroughs and borough is None:
        st.caption("Click a borough to load its road segments.")

    _render_map(
        config,
        road_segments,
        segment_bounds,
        boroughs,
        borough,
        candidate_indices,
    )


def _candidate_indices(
    config: DashboardConfig,
    road_segments: Sequence[RoadSegment],
    boroughs: Sequence[Borough],
    borough: str | None,
) -> Sequence[int]:
    if borough is not None:
        all_indices = _borough_segment_indices_cached(
            config.road_segment_s3_uri,
            config.zone_master_s3_uri,
            config.aws_region,
        )
        return all_indices.get(borough, ())
    if boroughs:
        # Outlines only: drawing the whole network is what made the map unusable.
        return ()
    return range(len(road_segments))


def _render_metrics(
    total_count: int,
    borough: str | None,
    snapshot_count: int,
) -> None:
    """segment 개수만 표시한다 — Scored/No score/Coverage는 borough 전체 score
    조회가 있어야 계산됐는데, #414에서 그 조회 자체를 없앴다."""
    if borough is None:
        st.metric("Road segments in snapshot", f"{snapshot_count:,}")
        return
    st.metric(f"Road segments in {borough}", f"{total_count:,}")


def _borough_selector(boroughs: Sequence[Borough]) -> str:
    """Dropdown mirroring the clickable outlines.

    Kept alongside the map because clicking depends on st_folium reporting the
    click, and a selection that cannot be undone would strand the user.
    """
    if not boroughs:
        return ALL_BOROUGHS
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


def _clicked_borough(
    map_state: dict | None,
    boroughs: Sequence[Borough],
) -> str | None:
    """Match st_folium's clicked-tooltip text back to a borough.

    The payload is the rendered tooltip rather than the feature's properties,
    so the name is matched inside it. Segment tooltips are rejected first: a
    street such as MANHATTAN AVENUE would otherwise read as a borough click.
    """
    if not map_state:
        return None
    tooltip = map_state.get("last_object_clicked_tooltip")
    if not tooltip or "segment_id" in tooltip:
        return None
    for borough in boroughs:
        if borough.name in tooltip:
            return borough.name
    return None


@st.fragment
def _render_map(
    config: DashboardConfig,
    road_segments: Sequence[RoadSegment],
    segment_bounds: Sequence[SegmentBounds],
    boroughs: Sequence[Borough],
    borough: str | None,
    candidate_indices: Sequence[int],
) -> None:
    """One map holding both layers.

    The outlines are drawn on every pass, not only before a borough is picked,
    so zooming out and clicking a different borough stays possible. Segments are
    added only once a borough narrows them down -- or, with no zone reference at
    all, for the whole snapshot.
    """
    # The viewport is only known after the map has rendered once, so the first
    # pass draws nothing and the reported bounds drive every pass after it.
    viewport: Viewport | None = st.session_state.get("viewport")
    center = st.session_state.get("center", NYC_MAP_CENTER)
    zoom = st.session_state.get("zoom", NYC_MAP_ZOOM)

    visible_segments, in_viewport_count = _visible_segments(
        candidate_indices, road_segments, segment_bounds, viewport, MAX_RENDERED_SEGMENTS
    )
    if in_viewport_count > MAX_RENDERED_SEGMENTS:
        st.info(
            f"{in_viewport_count:,} segments are in view and the first "
            f"{MAX_RENDERED_SEGMENTS:,} are drawn. Zoom in to see all of them."
        )

    # borough 전체가 아니라 실제로 그리는 것만 조회한다(#414).
    score_result: ComfortScoreBatchResult | None = None
    if visible_segments:
        try:
            with st.spinner("Loading comfort scores from the Serving API..."):
                score_result = _load_scores_cached(
                    config.batch_endpoint,
                    config.vehicle_profile_id,
                    tuple(segment.segment_id for segment in visible_segments),
                    config.batch_chunk_size,
                    config.request_timeout_seconds,
                )
        except httpx.HTTPError as exc:
            st.error(f"Unable to load comfort scores from the Serving API: {exc}")
            st.stop()

    if score_result is not None and score_result.vehicle_profile_fallback:
        st.warning(
            "Serving API used vehicle profile "
            f"{score_result.effective_vehicle_profile_id} instead of requested "
            f"profile {score_result.requested_vehicle_profile_id}."
        )

    joined_segments = join_road_segments_with_scores(
        visible_segments,
        score_result.scores if score_result is not None else {},
    )

    map_state = st_folium(
        build_map(
            joined_segments,
            center=center,
            zoom=zoom,
            boroughs=boroughs,
            selected_borough=borough,
        ),
        width=None,
        height=720,
        center=center,
        zoom=zoom,
        returned_objects=["bounds", "zoom", "last_object_clicked_tooltip"],
        key="map",
        on_change=_sync_map_state,
    )

    scope = borough if borough is not None else "the snapshot"
    st.caption(
        f"Rendered: {len(visible_segments):,} of {len(candidate_indices):,} "
        f"segments in {scope} · "
        f"Road geometry cache: {ROAD_SEGMENT_CACHE_TTL_SECONDS // 3600}h · "
        f"Comfort score cache: {SCORE_CACHE_TTL_SECONDS // 60}m · "
        f"Serving API chunk size: {config.batch_chunk_size:,}"
    )

    clicked = _clicked_borough(map_state, boroughs)
    if clicked is not None and clicked != borough:
        _select_borough(clicked, boroughs)

    # 최초 부트스트랩 전용(#414 후속) — viewport를 아직 모르면 이번 실행에서 받은
    # bounds로 딱 한 번만 전체 rerun한다. 이후 pan/zoom은 on_change 콜백이 처리하므로
    # (Streamlit이 fragment만 다시 실행) 여기 다시 안 걸린다.
    if viewport is None:
        update = _resolve_viewport_update(map_state, viewport, zoom)
        if update is not None:
            new_viewport, new_center, new_zoom = update
            st.session_state["viewport"] = new_viewport
            st.session_state["center"] = new_center
            st.session_state["zoom"] = new_zoom
            st.rerun()


def _resolve_viewport_update(
    map_state: Mapping[str, object] | None,
    current_viewport: Viewport | None,
    current_zoom: int,
) -> tuple[Viewport, tuple[float, float], int] | None:
    """map_state에서 새 viewport/center/zoom을 뽑는다. 바뀐 게 없으면 None.

    bounds가 없거나(레이아웃 전) 기존과 같으면(반올림 기준) None을 돌려줘서
    미세한 pan 지터로 계속 갱신되는 걸 막는다.
    """
    if not map_state:
        return None
    reported = _parse_viewport(map_state.get("bounds"))
    if reported is None:
        return None
    reported_zoom = map_state.get("zoom") or current_zoom
    if _rounded(reported) == _rounded(current_viewport) and reported_zoom == current_zoom:
        return None

    south, west, north, east = reported
    center = ((south + north) / 2, (west + east) / 2)
    return reported, center, reported_zoom


def _sync_map_state() -> None:
    """st_folium의 on_change 콜백 — Session State만 갱신한다(#414 후속).

    fragment 안 위젯 상호작용은 Streamlit이 알아서 fragment만 다시 실행하므로
    여기서 rerun을 직접 호출하지 않는다. `scope="fragment"`는 이 콜백이 full-app
    실행의 일부로 처음 도는 상황(예: Borough 변경 직후)에서 부르면
    `StreamlitAPIException`이 난다 — 그래서 아예 호출하지 않는다.
    """
    map_state = st.session_state.get("map")
    update = _resolve_viewport_update(
        map_state,
        st.session_state.get("viewport"),
        st.session_state.get("zoom", NYC_MAP_ZOOM),
    )
    if update is None:
        return
    viewport, center, zoom = update
    st.session_state["viewport"] = viewport
    st.session_state["center"] = center
    st.session_state["zoom"] = zoom


if __name__ == "__main__":
    main()