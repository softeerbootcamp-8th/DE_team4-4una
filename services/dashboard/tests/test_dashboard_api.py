"""FastAPI 엔드포인트 계약 테스트."""

from __future__ import annotations

import httpx
import pytest
from botocore.exceptions import ClientError
from dashboard.api import app, get_service, resolve_static_dir
from dashboard.dashboard_service import (
    Bootstrap,
    BoroughSummary,
    UnknownBoroughError,
    ViewportSegments,
)
from fastapi.testclient import TestClient

VIEWPORT_QUERY = {"south": 40.0, "west": -74.1, "north": 40.1, "east": -74.0}

BOOTSTRAP = Bootstrap(
    total_segment_count=166_222,
    max_rendered_segments=1000,
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

VIEWPORT_RESULT = ViewportSegments(
    in_viewport_count=1382,
    rendered_count=1000,
    truncated=True,
    requested_vehicle_profile_id=0,
    effective_vehicle_profile_id=0,
    vehicle_profile_fallback=False,
    features={"type": "FeatureCollection", "features": []},
)


class _StubService:
    def __init__(self, **overrides) -> None:
        self.calls: list[tuple] = []
        self._overrides = overrides

    def bootstrap(self):
        if "bootstrap" in self._overrides:
            raise self._overrides["bootstrap"]
        return BOOTSTRAP

    def get_segments_in_viewport(self, borough, south, west, north, east):
        self.calls.append((borough, south, west, north, east))
        if "segments" in self._overrides:
            raise self._overrides["segments"]
        return VIEWPORT_RESULT


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
    assert payload["max_rendered_segments"] == 1000
    assert payload["boroughs"][0]["name"] == "Manhattan"
    assert payload["boroughs"][0]["center"] == [40.78, -73.96]
    assert payload["boroughs"][0]["segment_count"] == 30_000
    assert payload["boroughs"][0]["geometry"]["type"] == "Polygon"


def test_segments_passes_the_viewport_through_and_reports_truncation(client, stub):
    payload = client.get("/api/segments", params={**VIEWPORT_QUERY, "borough": "Manhattan"}).json()

    assert stub.calls == [("Manhattan", 40.0, -74.1, 40.1, -74.0)]
    assert payload["in_viewport_count"] == 1382
    assert payload["rendered_count"] == 1000
    assert payload["truncated"] is True
    assert payload["vehicle_profile_fallback"] is False
    assert payload["features"]["type"] == "FeatureCollection"


def test_borough_is_optional(client, stub):
    client.get("/api/segments", params=VIEWPORT_QUERY)

    assert stub.calls == [(None, 40.0, -74.1, 40.1, -74.0)]


@pytest.mark.parametrize(
    ("params", "reason"),
    [
        ({"south": 40.1, "west": -74.1, "north": 40.0, "east": -74.0}, "south >= north"),
        ({"south": 40.0, "west": -74.0, "north": 40.1, "east": -74.1}, "west >= east"),
    ],
)
def test_an_inverted_viewport_is_rejected(client, params, reason):
    assert client.get("/api/segments", params=params).status_code == 400, reason


def test_a_latitude_outside_the_world_is_rejected(client):
    params = {**VIEWPORT_QUERY, "south": -120.0}

    assert client.get("/api/segments", params=params).status_code == 422


def test_a_missing_viewport_is_rejected(client):
    assert client.get("/api/segments").status_code == 422


def test_an_unknown_borough_is_a_404():
    with _client_raising(segments=UnknownBoroughError("Atlantis")) as client:
        response = client.get("/api/segments", params={**VIEWPORT_QUERY, "borough": "Atlantis"})

    assert response.status_code == 404


def test_an_s3_failure_is_a_502():
    error = ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")
    with _client_raising(bootstrap=error) as client:
        assert client.get("/api/bootstrap").status_code == 502


def test_a_serving_api_failure_is_a_502():
    with _client_raising(segments=httpx.ConnectError("refused")) as client:
        response = client.get("/api/segments", params={**VIEWPORT_QUERY, "borough": "Manhattan"})

    assert response.status_code == 502


def test_static_files_are_optional(monkeypatch, tmp_path):
    """React를 아직 빌드하지 않았으면 API만 제공한다."""
    monkeypatch.setenv("DASHBOARD_STATIC_DIR", str(tmp_path))
    assert resolve_static_dir() is None

    (tmp_path / "index.html").write_text("<html></html>")
    assert resolve_static_dir() == tmp_path


def test_a_configuration_error_is_reported_in_the_response():
    """Streamlit이 화면에 띄워주던 설정 오류 안내를 API 응답이 대신한다."""
    with _client_raising(bootstrap=ValueError("DASHBOARD_ROAD_SEGMENT_S3_URI must be set")) as client:
        response = client.get("/api/bootstrap")

    assert response.status_code == 500
    assert "DASHBOARD_ROAD_SEGMENT_S3_URI must be set" in response.text
