"""app.py의 viewport 기반 score 조회 흐름과 borough 인덱싱 테스트 (#414)."""

from __future__ import annotations

from dashboard.app import (
    Viewport,
    _group_indices_by_borough,
    _render_metrics,
    _visible_segments,
)
from dashboard.config import MAX_RENDERED_SEGMENTS
from dashboard.road_geometry import RoadSegment

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
