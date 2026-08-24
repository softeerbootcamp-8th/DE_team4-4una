"""app.py에서 옮겨온 viewport/borough/score 로직 테스트 (#414, #421 후속)."""

from __future__ import annotations

import pytest
from dashboard import dashboard_service as service_module
from dashboard.config import (
    MAX_RENDERED_SEGMENTS,
    SCORE_CACHE_TTL_SECONDS,
    DashboardConfig,
)
from dashboard.dashboard_service import (
    DashboardService,
    UnknownBoroughError,
    Viewport,
    geometry_bounds,
    group_indices_by_borough,
    visible_segments,
)
from dashboard.road_geometry import RoadSegment
from dashboard.serving_api_client import ComfortScore, ComfortScoreBatchResult
from dashboard.zone_master import Borough
from shapely.geometry import box
from shapely.strtree import STRtree

# (south, west, north, east) 순서
VIEWPORT: Viewport = (40.0, -74.1, 40.1, -74.0)
# (min_lon, min_lat, max_lon, max_lat) 순서
INSIDE = (-74.05, 40.02, -74.04, 40.03)
OUTSIDE = (-73.0, 41.0, -72.9, 41.1)

CONFIG = DashboardConfig(
    road_segment_s3_uri="s3://bucket/road_segment.parquet",
    zone_master_s3_uri="s3://bucket/zone_master.parquet",
    serving_api_url="http://serving-api:8000",
    vehicle_profile_id=0,
    batch_chunk_size=1000,
    request_timeout_seconds=30.0,
    aws_region="ap-northeast-2",
)


def _segment(segment_id: str, location_id: int | None = None) -> RoadSegment:
    return RoadSegment(
        segment_id=segment_id,
        street_name="BROADWAY",
        geometry={"type": "LineString", "coordinates": [[-74.05, 40.02], [-74.04, 40.03]]},
        location_id=location_id,
    )


def _tree(bounds_list) -> STRtree:
    return STRtree([box(*bounds) for bounds in bounds_list])


def _score(segment_id: str) -> ComfortScore:
    return ComfortScore(segment_id, 72.5, 0.91, "current", "2026-08-25T10:00:00")


def _batch_result(segment_ids, *, not_found=()) -> ComfortScoreBatchResult:
    return ComfortScoreBatchResult(
        scores={sid: _score(sid) for sid in segment_ids},
        not_found_segment_ids=tuple(not_found),
        requested_vehicle_profile_id=0,
        effective_vehicle_profile_id=0,
        vehicle_profile_fallback=False,
    )


class TestVisibleSegments:
    """R-tree(공간 인덱스)로 뷰포트와 겹치는 segment를 찾고, candidate_set으로
    현재 borough 소속 여부만 확인한다(#421 후속)."""

    def test_returns_nothing_before_the_first_viewport_is_known(self):
        segments = [_segment("1"), _segment("2")]

        visible, in_viewport_count = visible_segments(
            segments, _tree([INSIDE, INSIDE]), frozenset({0, 1}), None, 10
        )

        assert visible == []
        assert in_viewport_count == 0

    def test_only_segments_intersecting_the_viewport_are_returned(self):
        segments = [_segment(str(i)) for i in range(5)]
        bounds = [INSIDE, OUTSIDE, OUTSIDE, INSIDE, OUTSIDE]

        visible, in_viewport_count = visible_segments(
            segments, _tree(bounds), frozenset(range(5)), VIEWPORT, 10
        )

        assert [segment.segment_id for segment in visible] == ["0", "3"]
        assert in_viewport_count == 2

    def test_none_candidate_set_means_everything_is_a_candidate(self):
        # zone master가 없어 borough 개념 자체가 없는 스냅샷 전체 모드.
        segments = [_segment(str(i)) for i in range(3)]

        visible, in_viewport_count = visible_segments(
            segments, _tree([INSIDE, OUTSIDE, INSIDE]), None, VIEWPORT, 10
        )

        assert [segment.segment_id for segment in visible] == ["0", "2"]
        assert in_viewport_count == 2

    def test_indices_outside_the_candidate_set_are_ignored(self):
        segments = [_segment(str(i)) for i in range(4)]

        visible, in_viewport_count = visible_segments(
            segments, _tree([INSIDE] * 4), frozenset({0, 2}), VIEWPORT, 10
        )

        assert [segment.segment_id for segment in visible] == ["0", "2"]
        assert in_viewport_count == 2

    def test_empty_candidate_set_returns_nothing(self):
        # boroughs는 로드됐지만 아직 아무것도 선택 안 한 상태.
        visible, in_viewport_count = visible_segments(
            [_segment("0")], _tree([INSIDE]), frozenset(), VIEWPORT, 10
        )

        assert visible == []
        assert in_viewport_count == 0

    def test_caps_at_max_rendered_even_when_more_are_in_view(self):
        segments = [_segment(str(i)) for i in range(10)]

        visible, in_viewport_count = visible_segments(
            segments, _tree([INSIDE] * 10), frozenset(range(10)), VIEWPORT, 3
        )

        assert in_viewport_count == 10
        assert len(visible) == 3

    def test_truncation_keeps_the_lowest_snapshot_indices(self):
        """STRtree 순회 순서가 아니라 스냅샷 순서로 자른다 -- 살짝 pan했을 때
        잘려나가는 집합이 통째로 바뀌며 지도가 깜빡이는 것을 막는다."""
        segments = [_segment(str(i)) for i in range(10)]

        visible, _ = visible_segments(
            segments, _tree([INSIDE] * 10), frozenset(range(10)), VIEWPORT, 4
        )

        assert [segment.segment_id for segment in visible] == ["0", "1", "2", "3"]

    def test_a_borough_sized_selection_does_not_all_come_back(self):
        borough_size = 20_000
        segments = [_segment(str(i)) for i in range(borough_size)]
        bounds = [INSIDE if i % 1000 == 0 else OUTSIDE for i in range(borough_size)]

        visible, in_viewport_count = visible_segments(
            segments, _tree(bounds), frozenset(range(borough_size)), VIEWPORT,
            MAX_RENDERED_SEGMENTS,
        )

        assert in_viewport_count == borough_size // 1000
        assert len(visible) == borough_size // 1000


def test_group_indices_by_borough_skips_segments_without_a_zone():
    segments = [_segment("0", 100), _segment("1", None), _segment("2", 200), _segment("3", 999)]

    grouped = group_indices_by_borough(segments, {100: "Manhattan", 200: "Manhattan"})

    assert grouped == {"Manhattan": frozenset({0, 2})}


def test_geometry_bounds_covers_every_point_of_a_multilinestring():
    geometry = {
        "type": "MultiLineString",
        "coordinates": [[[-74.0, 40.0], [-73.9, 40.1]], [[-74.2, 39.9], [-74.1, 40.05]]],
    }

    assert geometry_bounds(geometry) == (-74.2, 39.9, -73.9, 40.1)


class TestService:
    @pytest.fixture
    def clock(self):
        return {"now": 1000.0}

    @pytest.fixture
    def service(self, monkeypatch, clock):
        segments = [_segment(str(i), location_id=100) for i in range(3)]
        monkeypatch.setattr(
            service_module, "load_road_segments", lambda *_args, **_kwargs: segments
        )
        monkeypatch.setattr(
            service_module, "load_zone_master", lambda *_args, **_kwargs: b"zone"
        )
        monkeypatch.setattr(
            service_module,
            "borough_outlines",
            lambda _raw: [
                Borough(
                    name="Manhattan",
                    geometry={"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [0, 0]]]},
                    bounds=(-74.1, 40.0, -74.0, 40.1),
                )
            ],
        )
        monkeypatch.setattr(service_module, "zone_boroughs", lambda _raw: {100: "Manhattan"})
        return DashboardService(CONFIG, now=lambda: clock["now"])

    def test_bootstrap_reports_snapshot_and_borough_counts(self, service):
        bootstrap = service.bootstrap()

        assert bootstrap.total_segment_count == 3
        assert bootstrap.max_rendered_segments == MAX_RENDERED_SEGMENTS
        assert [borough.name for borough in bootstrap.boroughs] == ["Manhattan"]
        assert bootstrap.boroughs[0].segment_count == 3

    def test_the_snapshot_is_loaded_once_and_reused(self, monkeypatch, service):
        calls = []
        monkeypatch.setattr(
            service_module,
            "load_road_segments",
            lambda *args, **kwargs: calls.append(1) or [_segment("0", 100)],
        )

        service.bootstrap()
        service.bootstrap()

        assert len(calls) == 1

    def test_no_borough_selected_renders_nothing(self, monkeypatch, service):
        monkeypatch.setattr(
            service_module,
            "fetch_comfort_scores",
            lambda **_kwargs: pytest.fail("no borough selected must not hit the serving API"),
        )

        result = service.get_segments_in_viewport(None, *VIEWPORT)

        assert result.rendered_count == 0
        assert result.features["features"] == []

    def test_an_unknown_borough_is_rejected(self, service):
        with pytest.raises(UnknownBoroughError):
            service.get_segments_in_viewport("Atlantis", *VIEWPORT)

    def test_segments_are_returned_as_geojson_with_scores_joined(self, monkeypatch, service):
        monkeypatch.setattr(
            service_module,
            "fetch_comfort_scores",
            lambda **kwargs: _batch_result(kwargs["segment_ids"]),
        )

        result = service.get_segments_in_viewport("Manhattan", *VIEWPORT)

        assert result.rendered_count == 3
        assert result.truncated is False
        assert result.features["type"] == "FeatureCollection"
        properties = result.features["features"][0]["properties"]
        assert properties["segment_id"] == "0"
        assert properties["comfort_score"] == "72.50"
        assert properties["color"] == "yellow"

    def test_only_uncached_segments_are_fetched_again(self, monkeypatch, service):
        requested: list[list[str]] = []

        def _fetch(**kwargs):
            ids = list(kwargs["segment_ids"])
            requested.append(ids)
            return _batch_result(ids)

        monkeypatch.setattr(service_module, "fetch_comfort_scores", _fetch)

        service.get_segments_in_viewport("Manhattan", *VIEWPORT)
        service.get_segments_in_viewport("Manhattan", *VIEWPORT)

        # 두 번째 요청은 같은 segment를 보고 있으므로 serving API를 다시 부르지 않는다.
        assert requested == [["0", "1", "2"]]

    def test_expired_scores_are_fetched_again(self, monkeypatch, service, clock):
        requested: list[list[str]] = []

        def _fetch(**kwargs):
            ids = list(kwargs["segment_ids"])
            requested.append(ids)
            return _batch_result(ids)

        monkeypatch.setattr(service_module, "fetch_comfort_scores", _fetch)

        service.get_segments_in_viewport("Manhattan", *VIEWPORT)
        clock["now"] += SCORE_CACHE_TTL_SECONDS + 1
        service.get_segments_in_viewport("Manhattan", *VIEWPORT)

        assert requested == [["0", "1", "2"], ["0", "1", "2"]]

    def test_segments_the_api_could_not_find_are_cached_too(self, monkeypatch, service):
        requested: list[list[str]] = []

        def _fetch(**kwargs):
            ids = list(kwargs["segment_ids"])
            requested.append(ids)
            return _batch_result([], not_found=ids)

        monkeypatch.setattr(service_module, "fetch_comfort_scores", _fetch)

        first = service.get_segments_in_viewport("Manhattan", *VIEWPORT)
        service.get_segments_in_viewport("Manhattan", *VIEWPORT)

        assert len(requested) == 1
        assert first.features["features"][0]["properties"]["comfort_score"] == "N/A"
        assert first.features["features"][0]["properties"]["color"] == "gray"

    def test_profile_fallback_survives_a_full_cache_hit(self, monkeypatch, service):
        monkeypatch.setattr(
            service_module,
            "fetch_comfort_scores",
            lambda **kwargs: ComfortScoreBatchResult(
                scores={sid: _score(sid) for sid in kwargs["segment_ids"]},
                not_found_segment_ids=(),
                requested_vehicle_profile_id=0,
                effective_vehicle_profile_id=7,
                vehicle_profile_fallback=True,
            ),
        )

        service.get_segments_in_viewport("Manhattan", *VIEWPORT)
        cached = service.get_segments_in_viewport("Manhattan", *VIEWPORT)

        assert cached.vehicle_profile_fallback is True
        assert cached.effective_vehicle_profile_id == 7

    def test_truncation_is_reported(self, monkeypatch, service):
        monkeypatch.setattr(service_module, "MAX_RENDERED_SEGMENTS", 2)
        monkeypatch.setattr(
            service_module,
            "fetch_comfort_scores",
            lambda **kwargs: _batch_result(kwargs["segment_ids"]),
        )

        result = service.get_segments_in_viewport("Manhattan", *VIEWPORT)

        assert result.in_viewport_count == 3
        assert result.rendered_count == 2
        assert result.truncated is True
