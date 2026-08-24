"""FastAPI 엔드포인트 계약 테스트."""

from __future__ import annotations

import gzip
import json

import httpx
import pytest
from botocore.exceptions import ClientError
from dashboard.api import app, get_service, resolve_static_dir
from dashboard.dashboard_service import (
    Bootstrap,
    BoroughSummary,
    SegmentPayload,
    UnknownBoroughError,
)
from fastapi.testclient import TestClient

BOOTSTRAP = Bootstrap(
    total_segment_count=166_222,
    boroughs=(
        BoroughSummary(
            name="Manhattan",
            center=(40.78, -73.96),
            bounds=(-74.02, 40.70, -73.91, 40.88),
            segment_count=30_000,
            geometry={"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [0, 0]]]},
        ),
    ),
)

BODY = {
    "segment_count": 30_000,
    "truncated": False,
    "requested_vehicle_profile_id": 0,
    "effective_vehicle_profile_id": 0,
    "vehicle_profile_fallback": False,
    "features": {"type": "FeatureCollection", "features": []},
}

PAYLOAD = SegmentPayload(
    segment_count=30_000,
    truncated=False,
    body=gzip.compress(json.dumps(BODY).encode(), 6),
    expires_at=0.0,
)


class _StubService:
    def __init__(self, **overrides) -> None:
        self.calls: list[str | None] = []
        self._overrides = overrides

    def bootstrap(self):
        if "bootstrap" in self._overrides:
            raise self._overrides["bootstrap"]
        return BOOTSTRAP

    def get_segments(self, borough):
        self.calls.append(borough)
        if "segments" in self._overrides:
            raise self._overrides["segments"]
        return PAYLOAD


@pytest.fixture
def stub():
    service = _StubService()
    app.dependency_overrides[get_service] = lambda: service
    yield service
    app.dependency_overrides.clear()


@pytest.fixture
def client(stub):
    with TestClient(app) as test_client:
        yield test_client


def _client_raising(**overrides) -> TestClient:
    app.dependency_overrides[get_service] = lambda: _StubService(**overrides)
    return TestClient(app, raise_server_exceptions=False)


def test_health_returns_plain_text_ok(client):
    """배포 스크립트가 이 경로로 health check를 하고 본문을 로그에 찍는다."""
    response = client.get("/_stcore/health")

    assert response.status_code == 200
    assert response.text == "ok"
    assert response.headers["content-type"].startswith("text/plain")


def test_bootstrap_exposes_boroughs_and_snapshot_size(client):
    payload = client.get("/api/bootstrap").json()

    assert payload["total_segment_count"] == 166_222
    assert payload["boroughs"][0]["name"] == "Manhattan"
    assert payload["boroughs"][0]["bounds"] == [-74.02, 40.70, -73.91, 40.88]
    assert payload["boroughs"][0]["segment_count"] == 30_000
    assert payload["boroughs"][0]["geometry"]["type"] == "Polygon"


def test_segments_returns_the_whole_borough(client, stub):
    payload = client.get("/api/segments", params={"borough": "Manhattan"}).json()

    assert stub.calls == ["Manhattan"]
    assert payload["segment_count"] == 30_000
    assert payload["truncated"] is False
    assert payload["features"]["type"] == "FeatureCollection"


def test_borough_is_optional(client, stub):
    client.get("/api/segments")

    assert stub.calls == [None]


def test_the_cached_gzip_body_is_sent_as_is(client):
    """서비스가 눌러둔 바이트를 그대로 흘려보낸다 -- 요청마다 다시 압축하지 않는다."""
    response = client.get("/api/segments", headers={"Accept-Encoding": "gzip"})

    assert response.headers["content-encoding"] == "gzip"
    # httpx가 자동으로 풀어준 결과가 원본과 같아야 한다.
    assert response.json()["segment_count"] == 30_000


def test_a_client_without_gzip_gets_plain_json(client):
    response = client.get("/api/segments", headers={"Accept-Encoding": "identity"})

    assert "content-encoding" not in response.headers
    assert response.json()["segment_count"] == 30_000


def test_an_unknown_borough_is_a_404():
    with _client_raising(segments=UnknownBoroughError("Atlantis")) as client:
        response = client.get("/api/segments", params={"borough": "Atlantis"})

    assert response.status_code == 404


def test_an_s3_failure_is_a_502():
    error = ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")
    with _client_raising(bootstrap=error) as client:
        assert client.get("/api/bootstrap").status_code == 502


def test_a_serving_api_failure_is_a_502():
    with _client_raising(segments=httpx.ConnectError("refused")) as client:
        assert client.get("/api/segments", params={"borough": "Manhattan"}).status_code == 502


def test_a_configuration_error_is_reported_in_the_response():
    """Streamlit이 화면에 띄워주던 설정 오류 안내를 API 응답이 대신한다."""
    with _client_raising(bootstrap=ValueError("DASHBOARD_ROAD_SEGMENT_S3_URI must be set")) as client:
        response = client.get("/api/bootstrap")

    assert response.status_code == 500
    assert "DASHBOARD_ROAD_SEGMENT_S3_URI must be set" in response.text


def test_static_files_are_optional(monkeypatch, tmp_path):
    """React를 아직 빌드하지 않았으면 API만 제공한다."""
    monkeypatch.setenv("DASHBOARD_STATIC_DIR", str(tmp_path))
    assert resolve_static_dir() is None

    (tmp_path / "index.html").write_text("<html></html>")
    assert resolve_static_dir() == tmp_path
