"""Tests for serving_api/metrics.py (#308).

`/health`처럼 실제 DB 연결 없이 동작해야 하는 경로만 필요하므로, test_api.py의
FakePool보다 훨씬 단순한 가짜 풀을 쓴다. 매 테스트가 독립된 `CollectorRegistry`를
주입해 metric 값이 테스트 사이에 섞이지 않게 한다.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Self

from fastapi.testclient import TestClient
from prometheus_client import CollectorRegistry
from serving_api.app import create_app
from serving_api.config import ServingApiConfig
from serving_api.metrics import (
    REQUEST_COUNT_METRIC_NAME,
    REQUEST_DURATION_METRIC_NAME,
    UNMATCHED_ROUTE,
    RequestMetrics,
)

CONFIG = ServingApiConfig.from_env(
    {
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "de4",
        "POSTGRES_USER": "de4",
        "POSTGRES_PASSWORD": "secret",
    }
)

COMFORT_SCORE_ROUTE = (
    "/api/v1/segments/{segment_id}/comfort-scores/{vehicle_profile_id}"
)


class _FakeConnection:
    """`/health`의 `SELECT 1`과 점수 조회(빈 결과)만 만족하면 되는 최소 stub."""

    def cursor(self) -> Self:
        return self

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *arguments: object) -> bool:
        return False

    def execute(self, sql: str, parameters: tuple[object, ...] | None = None) -> None:
        return None

    def fetchone(self) -> tuple[object, ...] | None:
        return None

    def fetchall(self) -> list[tuple[object, ...]]:
        return []


class _FakePool:
    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    @contextmanager
    def connection(self):
        yield _FakeConnection()


class _RaisingPool:
    """의도적으로 처리되지 않는 예외를 내는 풀 — 5xx 계측 경로를 재현한다."""

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    @contextmanager
    def connection(self):
        raise RuntimeError("boom")
        yield  # pragma: no cover - contextmanager 문법상 필요, 도달하지 않는다.


def build_client(
    metrics: RequestMetrics, pool_factory=lambda config: _FakePool()
) -> TestClient:
    app = create_app(CONFIG, pool_factory=pool_factory, metrics=metrics)
    return TestClient(app, raise_server_exceptions=False)


def test_request_metrics_counts_a_successful_request() -> None:
    metrics = RequestMetrics(CollectorRegistry())
    with build_client(metrics) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert (
        metrics.registry.get_sample_value(
            REQUEST_COUNT_METRIC_NAME,
            {"method": "GET", "route": "/health", "status": "200"},
        )
        == 1.0
    )


def test_request_metrics_use_the_route_template_not_the_raw_path() -> None:
    metrics = RequestMetrics(CollectorRegistry())
    with build_client(metrics) as client:
        response = client.get("/api/v1/segments/0032900/comfort-scores/0")

    assert response.status_code == 404
    assert (
        metrics.registry.get_sample_value(
            REQUEST_COUNT_METRIC_NAME,
            {"method": "GET", "route": COMFORT_SCORE_ROUTE, "status": "404"},
        )
        == 1.0
    )
    # segment_id 같은 path parameter 값 자체는 어떤 label에도 들어가지 않는다.
    for metric in metrics.registry.collect():
        for sample in metric.samples:
            assert "0032900" not in sample.labels.values()


def test_request_metrics_use_a_fixed_label_for_an_unmatched_path() -> None:
    metrics = RequestMetrics(CollectorRegistry())
    with build_client(metrics) as client:
        response = client.get("/does/not/exist")

    assert response.status_code == 404
    assert (
        metrics.registry.get_sample_value(
            REQUEST_COUNT_METRIC_NAME,
            {"method": "GET", "route": UNMATCHED_ROUTE, "status": "404"},
        )
        == 1.0
    )
    for metric in metrics.registry.collect():
        for sample in metric.samples:
            assert "/does/not/exist" not in sample.labels.values()


def test_request_metrics_record_a_duration_observation() -> None:
    metrics = RequestMetrics(CollectorRegistry())
    with build_client(metrics) as client:
        client.get("/health")

    count = metrics.registry.get_sample_value(
        f"{REQUEST_DURATION_METRIC_NAME}_count", {"method": "GET", "route": "/health"}
    )
    assert count == 1.0


def test_request_metrics_record_5xx_for_an_unhandled_exception() -> None:
    metrics = RequestMetrics(CollectorRegistry())
    with build_client(metrics, pool_factory=lambda config: _RaisingPool()) as client:
        response = client.get("/health")

    assert response.status_code == 500
    assert (
        metrics.registry.get_sample_value(
            REQUEST_COUNT_METRIC_NAME,
            {"method": "GET", "route": "/health", "status": "500"},
        )
        == 1.0
    )


def test_create_app_without_explicit_metrics_does_not_share_a_registry() -> None:
    # 인스턴스 전용 registry를 기본값으로 쓰지 않으면, create_app()을 반복 호출할 때마다
    # 같은 이름의 metric을 global registry에 다시 등록하려다 예외가 난다.
    create_app(CONFIG, pool_factory=lambda config: _FakePool())
    create_app(CONFIG, pool_factory=lambda config: _FakePool())
