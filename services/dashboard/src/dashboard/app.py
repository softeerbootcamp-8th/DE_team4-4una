"""Streamlit assembly for the NYC road comfort-score map."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import httpx
import streamlit as st
from botocore.exceptions import BotoCoreError, ClientError
from shapely.geometry import box
from shapely.strtree import STRtree
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
    ScoredRoadSegment,
    build_base_map,
    build_segment_feature_group,
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


@st.cache_resource(ttl=ROAD_SEGMENT_CACHE_TTL_SECONDS, show_spinner=False)
def _spatial_index_cached(
    road_segment_s3_uri: str,
    aws_region: str | None,
) -> STRtree:
    """segment bounding box들의 R-tree, segment 목록과 같은 순서로 만든다(#421 후속).

    뷰포트와 겹치는 segment를 O(log N + k)로 찾기 위한 것 -- 이전에는 pan/zoom마다
    borough 전체를 순회하며 bounding box를 하나씩 비교했다(borough가 클수록 느려짐).

    st.cache_data 대신 st.cache_resource를 쓰는 이유: st.cache_data는 캐시 히트마다
    값을 복사해서 돌려주는데, STRtree는 읽기 전용으로만 쓰이니 안전하게 그대로
    공유해도 되고, 매번 복사하기엔 크다.
    """
    bounds = _load_segment_bounds_cached(road_segment_s3_uri, aws_region)
    return STRtree(
        [box(min_lon, min_lat, max_lon, max_lat) for min_lon, min_lat, max_lon, max_lat in bounds]
    )


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


@st.cache_data(ttl=ROAD_SEGMENT_CACHE_TTL_SECONDS, show_spinner=False)
def _borough_segment_index_sets_cached(
    road_segment_s3_uri: str,
    zone_master_s3_uri: str,
    aws_region: str | None,
) -> dict[str, frozenset[int]]:
    """borough별 segment index를 frozenset으로 미리 만들어둔다(#421 후속).

    공간 인덱스 쿼리 결과가 이 borough 소속인지 pan/zoom마다 O(1)로 확인하기
    위한 것 -- borough를 고를 때 한 번만 만들고, 그 뒤로는 다시 만들지 않는다.
    """
    grouped = _borough_segment_indices_cached(
        road_segment_s3_uri, zone_master_s3_uri, aws_region
    )
    return {name: frozenset(indices) for name, indices in grouped.items()}


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
    road_segments: Sequence[RoadSegment],
    spatial_index: STRtree,
    candidate_set: frozenset[int] | None,
    viewport: Viewport | None,
    max_rendered: int,
) -> tuple[list[RoadSegment], int]:
    """그릴 segment이자 score를 조회할 segment(#421 후속으로 R-tree 사용).

    공간 인덱스로 뷰포트와 겹치는 segment를 먼저 찾는다 -- 도시 전체 기준으로
    O(log N + k)이고 borough 크기와 무관하다. 그 결과가 candidate_set(현재
    borough)에 속하는지는 frozenset 조회라 O(1)이다. candidate_set이 None이면
    전체 스냅샷 모드라는 뜻이라 걸러내지 않는다.

    viewport를 아직 모르면 ([], 0)을 반환한다. 두 번째 값은 상한 적용 전
    교차 개수로, "N개 더 있음, 확대하라" 안내에 쓴다.
    """
    if viewport is None:
        return [], 0
    south, west, north, east = viewport
    query_result = spatial_index.query(box(west, south, east, north), predicate="intersects")
    if candidate_set is None:
        matched = [int(index) for index in query_result]
    else:
        matched = [int(index) for index in query_result if int(index) in candidate_set]
    visible = [road_segments[index] for index in matched[:max_rendered]]
    return visible, len(matched)


SegmentRenderKey = tuple[tuple[str, float | None, float | None, str | None, str | None], ...]


def _segment_render_key(joined_segments: Sequence[ScoredRoadSegment]) -> SegmentRenderKey:
    """가벼운 값 튜플로 만든, 지금 그릴 segment+score 조합의 지문(#421 후속).

    Folium 객체는 매번 새로 만들면 내부 요소 id가 달라져서, 실제로는 똑같은
    segment를 그리는데도 프론트엔드가 레이어를 다시 그린다. 이 키가 직전과
    같으면 새로 만들지 않고 이전 FeatureGroup을 그대로 재사용한다.
    """
    return tuple(
        (
            segment.road_segment.segment_id,
            segment.score.comfort_score if segment.score is not None else None,
            segment.score.confidence_score if segment.score is not None else None,
            segment.score.source if segment.score is not None else None,
            segment.score.weather_time if segment.score is not None else None,
        )
        for segment in joined_segments
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
            spatial_index = _spatial_index_cached(
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
    candidate_set = _candidate_set(config, boroughs, borough)
    candidate_count = len(candidate_set) if candidate_set is not None else len(road_segments)

    _render_metrics(candidate_count, borough, len(road_segments))

    if boroughs and borough is None:
        st.caption("Click a borough to load its road segments.")

    _render_map(
        config,
        road_segments,
        spatial_index,
        boroughs,
        borough,
        candidate_set,
        candidate_count,
    )


def _candidate_set(
    config: DashboardConfig,
    boroughs: Sequence[Borough],
    borough: str | None,
) -> frozenset[int] | None:
    """현재 선택 기준으로 그릴 수 있는 segment index 집합(#421 후속).

    None이면 "전체가 후보"라는 뜻이라 뷰포트 쿼리 결과를 걸러낼 필요가 없다
    (zone master가 없어 borough 개념 자체가 없는 경우).
    """
    if borough is not None:
        sets = _borough_segment_index_sets_cached(
            config.road_segment_s3_uri,
            config.zone_master_s3_uri,
            config.aws_region,
        )
        return sets.get(borough, frozenset())
    if boroughs:
        # Outlines only: drawing the whole network is what made the map unusable.
        return frozenset()
    return None


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
    spatial_index: STRtree,
    boroughs: Sequence[Borough],
    borough: str | None,
    candidate_set: frozenset[int] | None,
    candidate_count: int,
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
        road_segments, spatial_index, candidate_set, viewport, MAX_RENDERED_SEGMENTS
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

    # base map은 borough outline과 legend만 담고, 항상 기본 center/zoom으로
    # 만든다 -- pan/zoom마다 바뀌면 base map 자체가 다시 만들어지면서 이 이슈가
    # 없애려는 재생성 비용이 그대로 남는다(#421). 실제 보고 있는 위치는
    # st_folium의 center/zoom 인자로만 전달한다.
    base_map = build_base_map(boroughs=boroughs, selected_borough=borough)

    # 드래그 중에는 실제 viewport가 안 바뀌었는데도 fragment가 자주 재실행된다
    # -- 그릴 segment 조합이 직전과 같으면 새로 만들지 않고 재사용해서, 바뀐 것도
    # 없는데 프론트엔드가 레이어를 다시 그리는 걸 막는다(#421 후속).
    render_key = _segment_render_key(joined_segments)
    if (
        st.session_state.get("_segment_render_key") == render_key
        and "_segment_feature_group" in st.session_state
    ):
        road_layer = st.session_state["_segment_feature_group"]
    else:
        road_layer = build_segment_feature_group(joined_segments)
        st.session_state["_segment_render_key"] = render_key
        st.session_state["_segment_feature_group"] = road_layer

    map_state = st_folium(
        base_map,
        feature_group_to_add=road_layer,
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
        f"Rendered: {len(visible_segments):,} of {candidate_count:,} "
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