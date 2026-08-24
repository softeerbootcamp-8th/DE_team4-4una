"""borough 단위 조회, 응답 캐시, 프리워밍 테스트."""

from __future__ import annotations

import gzip
import json

import pytest
from dashboard import dashboard_service as service_module
from dashboard.config import (
    SCORE_CACHE_TTL_SECONDS,
    SNAPSHOT_FALLBACK_MAX_SEGMENTS,
    DashboardConfig,
)
from dashboard.dashboard_service import (
    DashboardService,
    UnknownBoroughError,
    group_indices_by_borough,
)
from dashboard.road_geometry import RoadSegment
from dashboard.serving_api_client import ComfortScore, ComfortScoreBatchResult
from dashboard.zone_master import Borough

CONFIG = DashboardConfig(
    road_segment_s3_uri="s3://bucket/road_segment.parquet",
    zone_master_s3_uri="s3://bucket/zone_master.parquet",
    serving_api_url="http://serving-api:8000",
    vehicle_profile_id=0,
    batch_chunk_size=1000,
    request_timeout_seconds=30.0,
    aws_region="ap-northeast-2",
)

BOROUGH = Borough(
    name="Manhattan",
    geometry={"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [0, 0]]]},
    bounds=(-74.02, 40.70, -73.91, 40.88),
)


def _segment(segment_id: str, location_id: int | None = 100) -> RoadSegment:
    return RoadSegment(
        segment_id=segment_id,
        street_name="BROADWAY",
        geometry={"type": "LineString", "coordinates": [[-74.05, 40.02], [-74.04, 40.03]]},
        location_id=location_id,
    )


def _batch_result(segment_ids, *, not_found=(), effective=0, fallback=False):
    return ComfortScoreBatchResult(
        scores={
            sid: ComfortScore(sid, 72.5, 0.91, "current", "2026-08-25T10:00:00")
            for sid in segment_ids
        },
        not_found_segment_ids=tuple(not_found),
        requested_vehicle_profile_id=0,
        effective_vehicle_profile_id=effective,
        vehicle_profile_fallback=fallback,
    )


def _decode(payload) -> dict:
    return json.loads(gzip.decompress(payload.body))


def test_group_indices_by_borough_skips_segments_without_a_zone():
    segments = [_segment("0"), _segment("1", None), _segment("2", 200), _segment("3", 999)]

    grouped = group_indices_by_borough(segments, {100: "Manhattan", 200: "Manhattan"})

    assert grouped == {"Manhattan": frozenset({0, 2})}


@pytest.fixture
def clock():
    return {"now": 1000.0}


@pytest.fixture
def fetches(monkeypatch):
    """Serving API 호출을 가로채 요청된 segment_id를 기록한다."""
    calls: list[list[str]] = []

    def _fetch(**kwargs):
        ids = list(kwargs["segment_ids"])
        calls.append(ids)
        return _batch_result(ids)

    monkeypatch.setattr(service_module, "fetch_comfort_scores", _fetch)
    return calls


@pytest.fixture
def service(monkeypatch, clock):
    segments = [_segment(str(i)) for i in range(3)]
    monkeypatch.setattr(service_module, "load_road_segments", lambda *_a, **_k: segments)
    monkeypatch.setattr(service_module, "load_zone_master", lambda *_a, **_k: b"zone")
    monkeypatch.setattr(service_module, "borough_outlines", lambda _raw: [BOROUGH])
    monkeypatch.setattr(service_module, "zone_boroughs", lambda _raw: {100: "Manhattan"})
    return DashboardService(CONFIG, now=lambda: clock["now"])


class TestBootstrap:
    def test_reports_snapshot_and_borough_counts(self, service):
        bootstrap = service.bootstrap()

        assert bootstrap.total_segment_count == 3
        assert [b.name for b in bootstrap.boroughs] == ["Manhattan"]
        assert bootstrap.boroughs[0].segment_count == 3

    def test_the_snapshot_is_loaded_once_and_reused(self, monkeypatch, service):
        calls = []
        monkeypatch.setattr(
            service_module,
            "load_road_segments",
            lambda *a, **k: calls.append(1) or [_segment("0")],
        )

        service.bootstrap()
        service.bootstrap()

        assert len(calls) == 1


class TestGetSegments:
    def test_a_borough_returns_every_segment_in_it(self, service, fetches):
        payload = service.get_segments("Manhattan")
        body = _decode(payload)

        assert payload.segment_count == 3
        assert payload.truncated is False
        assert [f["properties"]["segment_id"] for f in body["features"]["features"]] == [
            "0",
            "1",
            "2",
        ]
        assert fetches == [["0", "1", "2"]]

    def test_scores_are_joined_into_the_features(self, service, fetches):
        body = _decode(service.get_segments("Manhattan"))

        properties = body["features"]["features"][0]["properties"]
        assert properties["comfort_score"] == "72.50"
        assert properties["color"] == "yellow"

    def test_segments_without_a_score_stay_gray(self, monkeypatch, service):
        monkeypatch.setattr(
            service_module,
            "fetch_comfort_scores",
            lambda **kwargs: _batch_result([], not_found=list(kwargs["segment_ids"])),
        )

        body = _decode(service.get_segments("Manhattan"))

        properties = body["features"]["features"][0]["properties"]
        assert properties["comfort_score"] == "N/A"
        assert properties["color"] == "gray"

    def test_no_borough_selected_draws_nothing(self, monkeypatch, service):
        monkeypatch.setattr(
            service_module,
            "fetch_comfort_scores",
            lambda **_k: pytest.fail("no borough selected must not hit the serving API"),
        )

        payload = service.get_segments(None)

        assert payload.segment_count == 0
        assert _decode(payload)["features"]["features"] == []

    def test_an_unknown_borough_is_rejected(self, service):
        with pytest.raises(UnknownBoroughError):
            service.get_segments("Atlantis")

    def test_the_profile_fallback_is_reported(self, monkeypatch, service):
        monkeypatch.setattr(
            service_module,
            "fetch_comfort_scores",
            lambda **kwargs: _batch_result(
                kwargs["segment_ids"], effective=7, fallback=True
            ),
        )

        body = _decode(service.get_segments("Manhattan"))

        assert body["vehicle_profile_fallback"] is True
        assert body["effective_vehicle_profile_id"] == 7
        assert body["requested_vehicle_profile_id"] == 0


class TestPayloadCache:
    def test_a_second_request_reuses_the_built_payload(self, service, fetches):
        service.get_segments("Manhattan")
        service.get_segments("Manhattan")

        assert len(fetches) == 1

    def test_an_expired_payload_is_rebuilt(self, service, fetches, clock):
        service.get_segments("Manhattan")
        clock["now"] += SCORE_CACHE_TTL_SECONDS + 1
        service.get_segments("Manhattan")

        assert len(fetches) == 2

    def test_a_new_snapshot_drops_the_cached_payloads(self, service, fetches, clock):
        from dashboard.config import ROAD_SEGMENT_CACHE_TTL_SECONDS

        service.get_segments("Manhattan")
        clock["now"] += ROAD_SEGMENT_CACHE_TTL_SECONDS + 1
        service.get_segments("Manhattan")

        assert len(fetches) == 2

    def test_the_body_is_gzipped(self, service, fetches):
        payload = service.get_segments("Manhattan")

        # gzip magic number. 엔드포인트가 이걸 그대로 흘려보낸다.
        assert payload.body[:2] == b"\x1f\x8b"
        assert _decode(payload)["features"]["type"] == "FeatureCollection"


class TestSnapshotFallback:
    """zone master가 없어 borough 개념 자체가 없는 배포."""

    @pytest.fixture
    def service(self, monkeypatch, clock):
        segments = [_segment(str(i)) for i in range(SNAPSHOT_FALLBACK_MAX_SEGMENTS + 5)]
        monkeypatch.setattr(
            service_module, "load_road_segments", lambda *_a, **_k: segments
        )
        config = DashboardConfig(
            road_segment_s3_uri=CONFIG.road_segment_s3_uri,
            zone_master_s3_uri=None,
            serving_api_url=CONFIG.serving_api_url,
            vehicle_profile_id=0,
            batch_chunk_size=1000,
            request_timeout_seconds=30.0,
            aws_region=CONFIG.aws_region,
        )
        return DashboardService(config, now=lambda: clock["now"])

    def test_the_snapshot_is_capped_and_reported_as_truncated(self, service, fetches):
        payload = service.get_segments(None)

        assert payload.segment_count == SNAPSHOT_FALLBACK_MAX_SEGMENTS
        assert payload.truncated is True


def test_prewarm_builds_every_borough_before_anyone_asks(monkeypatch, clock, fetches):
    """백그라운드 스레드 없이 한 tick만 직접 돌려 확인한다."""
    monkeypatch.setattr(service_module, "load_road_segments", lambda *_a, **_k: [_segment("0")])
    monkeypatch.setattr(service_module, "load_zone_master", lambda *_a, **_k: b"zone")
    monkeypatch.setattr(service_module, "borough_outlines", lambda _raw: [BOROUGH])
    monkeypatch.setattr(service_module, "zone_boroughs", lambda _raw: {100: "Manhattan"})
    service = DashboardService(CONFIG, now=lambda: clock["now"])

    for borough in service.bootstrap().boroughs:
        service.get_segments(borough.name)
    service.get_segments("Manhattan")

    # 프리워밍이 만들어둔 것을 사용자 요청이 그대로 받는다.
    assert len(fetches) == 1
