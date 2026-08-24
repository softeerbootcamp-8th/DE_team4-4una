"""app.py의 viewport 기반 score 조회 흐름과 borough 인덱싱 테스트 (#414)."""

from __future__ import annotations

import inspect

import streamlit as st
from dashboard import app as app_module
from dashboard.app import (
    Viewport,
    _group_indices_by_borough,
    _render_metrics,
    _resolve_viewport_update,
    _sync_map_state,
    _visible_segments,
)
from dashboard.config import MAX_RENDERED_SEGMENTS
from dashboard.road_geometry import RoadSegment

# Leaflet의 bounds 페이로드 형태를 그대로 흉내낸다.
BOUNDS = {
    "_southWest": {"lat": 40.0, "lng": -74.1},
    "_northEast": {"lat": 40.1, "lng": -74.0},
}

# (south, west, north, east) 순서
VIEWPORT: Viewport = (40.0, -74.1, 40.1, -74.0)
# (min_lon, min_lat, max_lon, max_lat) 순서
INSIDE = (-74.05, 40.02, -74.04, 40.03)
OUTSIDE = (-73.0, 41.0, -72.9, 41.1)


def _segment(segment_id: str, location_id: int | None = None) -> RoadSegment:
    return RoadSegment(
        segment_id=segment_id, street_name=None, geometry={}, location_id=location_id
    )


def test_max_rendered_segments_is_1500() -> None:
    assert MAX_RENDERED_SEGMENTS == 1500


class TestVisibleSegments:
    def test_returns_nothing_before_the_first_viewport_is_known(self):
        # 첫 렌더는 Leaflet이 bounds를 보고하기 전에 일어난다.
        segments = [_segment("1"), _segment("2")]

        visible, in_viewport_count = _visible_segments(
            candidate_indices=[0, 1],
            road_segments=segments,
            segment_bounds=[INSIDE, INSIDE],
            viewport=None,
            max_rendered=10,
        )

        assert visible == []
        assert in_viewport_count == 0

    def test_only_segments_intersecting_the_viewport_are_returned(self):
        segments = [_segment(str(i)) for i in range(5)]
        bounds = [INSIDE, OUTSIDE, OUTSIDE, INSIDE, OUTSIDE]

        visible, in_viewport_count = _visible_segments(
            candidate_indices=list(range(5)),
            road_segments=segments,
            segment_bounds=bounds,
            viewport=VIEWPORT,
            max_rendered=10,
        )

        assert [segment.segment_id for segment in visible] == ["0", "3"]
        assert in_viewport_count == 2

    def test_a_borough_sized_selection_does_not_all_come_back(self):
        borough_size = 20_000
        segments = [_segment(str(i)) for i in range(borough_size)]
        bounds = [INSIDE if i % 1000 == 0 else OUTSIDE for i in range(borough_size)]
        expected_in_view = borough_size // 1000

        visible, in_viewport_count = _visible_segments(
            candidate_indices=list(range(borough_size)),
            road_segments=segments,
            segment_bounds=bounds,
            viewport=VIEWPORT,
            max_rendered=MAX_RENDERED_SEGMENTS,
        )

        assert in_viewport_count == expected_in_view
        assert len(visible) == expected_in_view
        assert len(visible) < borough_size

    def test_caps_at_max_rendered_even_when_more_are_in_view(self):
        segments = [_segment(str(i)) for i in range(10)]

        visible, in_viewport_count = _visible_segments(
            candidate_indices=list(range(10)),
            road_segments=segments,
            segment_bounds=[INSIDE] * 10,
            viewport=VIEWPORT,
            max_rendered=3,
        )

        assert in_viewport_count == 10
        assert [segment.segment_id for segment in visible] == ["0", "1", "2"]

    def test_candidate_order_and_indices_outside_the_candidate_set_are_ignored(self):
        segments = [_segment(str(i)) for i in range(4)]
        bounds = [INSIDE, INSIDE, INSIDE, INSIDE]

        visible, in_viewport_count = _visible_segments(
            candidate_indices=[2, 0],
            road_segments=segments,
            segment_bounds=bounds,
            viewport=VIEWPORT,
            max_rendered=10,
        )

        assert [segment.segment_id for segment in visible] == ["2", "0"]
        assert in_viewport_count == 2


class TestRenderMetrics:
    def test_shows_only_a_segment_count_with_no_borough_selected(self, monkeypatch):
        calls: list[tuple[str, str]] = []
        monkeypatch.setattr(
            "dashboard.app.st.metric", lambda label, value: calls.append((label, value))
        )

        _render_metrics(total_count=500, borough=None, snapshot_count=500)

        assert calls == [("Road segments in snapshot", "500")]

    def test_shows_only_a_segment_count_for_the_selected_borough(self, monkeypatch):
        calls: list[tuple[str, str]] = []
        monkeypatch.setattr(
            "dashboard.app.st.metric", lambda label, value: calls.append((label, value))
        )
        monkeypatch.setattr(
            "dashboard.app.st.columns",
            lambda *_a, **_k: (_ for _ in ()).throw(
                AssertionError("no multi-metric row should be built")
            ),
        )

        _render_metrics(total_count=42, borough="Manhattan", snapshot_count=999)

        assert calls == [("Road segments in Manhattan", "42")]


class TestGroupIndicesByBorough:
    def test_groups_every_segment_by_its_borough_in_one_pass(self):
        segments = [
            _segment("a", location_id=1),
            _segment("b", location_id=2),
            _segment("c", location_id=1),
        ]
        boroughs = {1: "Manhattan", 2: "Brooklyn"}

        grouped = _group_indices_by_borough(segments, boroughs)

        assert grouped == {"Manhattan": (0, 2), "Brooklyn": (1,)}

    def test_segments_without_a_zone_or_borough_are_skipped(self):
        segments = [
            _segment("a", location_id=None),
            _segment("b", location_id=99),
            _segment("c", location_id=1),
        ]
        boroughs = {1: "Manhattan"}

        grouped = _group_indices_by_borough(segments, boroughs)

        assert grouped == {"Manhattan": (2,)}

    def test_a_borough_with_no_segments_has_no_key(self):
        grouped = _group_indices_by_borough([], {})

        assert grouped == {}


class TestResolveViewportUpdate:
    def test_missing_bounds_returns_none(self):
        assert _resolve_viewport_update({"bounds": None}, None, 11) is None
        assert _resolve_viewport_update(None, None, 11) is None
        assert _resolve_viewport_update({}, None, 11) is None

    def test_valid_bounds_returns_viewport_center_and_zoom(self):
        map_state = {"bounds": BOUNDS, "zoom": 13}

        update = _resolve_viewport_update(map_state, None, 11)

        assert update == ((40.0, -74.1, 40.1, -74.0), (40.05, -74.05), 13)

    def test_an_unchanged_viewport_and_zoom_returns_none(self):
        # 미세한 부동소수점 차이는 _VIEWPORT_PRECISION 반올림으로 흡수돼야 한다.
        jittered_bounds = {
            "_southWest": {"lat": 40.0000001, "lng": -74.1000001},
            "_northEast": {"lat": 40.1000001, "lng": -74.0000001},
        }
        current_viewport = (40.0, -74.1, 40.1, -74.0)

        update = _resolve_viewport_update(
            {"bounds": jittered_bounds, "zoom": 11}, current_viewport, 11
        )

        assert update is None

    def test_a_zoom_only_change_still_returns_an_update(self):
        current_viewport = (40.0, -74.1, 40.1, -74.0)

        update = _resolve_viewport_update(
            {"bounds": BOUNDS, "zoom": 14}, current_viewport, 11
        )

        assert update is not None
        assert update[2] == 14


class TestSyncMapState:
    def teardown_method(self):
        for key in ("map", "viewport", "center", "zoom"):
            st.session_state.pop(key, None)

    def test_updates_session_state_from_the_map_key(self):
        st.session_state["map"] = {"bounds": BOUNDS, "zoom": 13}

        _sync_map_state()

        assert st.session_state["viewport"] == (40.0, -74.1, 40.1, -74.0)
        assert st.session_state["center"] == (40.05, -74.05)
        assert st.session_state["zoom"] == 13

    def test_does_nothing_when_bounds_are_missing(self):
        st.session_state["map"] = {"bounds": None}

        _sync_map_state()

        assert "viewport" not in st.session_state


class TestMapFragment:
    def test_render_map_is_wrapped_as_a_streamlit_fragment(self):
        source = inspect.getsource(app_module)
        assert "@st.fragment\ndef _render_map(" in source

    def test_the_on_change_callback_never_triggers_a_rerun(self):
        # 일반 pan/zoom은 fragment의 자동 rerun에 맡긴다 — 여기서 st.rerun()이나
        # st.rerun(scope="fragment")를 직접 부르면 안 된다.
        source = inspect.getsource(_sync_map_state)
        assert "st.rerun(" not in source
