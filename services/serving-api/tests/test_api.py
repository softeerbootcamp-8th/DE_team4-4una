"""Endpoint tests for the serving API (#160).

가짜 커넥션 풀을 끼워 실제 PostgreSQL 없이 상태 코드와 응답 형식을 검증한다.
쿼리가 실제 DB에서 도는지는 통합 테스트가 확인한다.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Self

import psycopg
import pytest
from fastapi.testclient import TestClient
from serving_api.app import create_app
from serving_api.config import MAX_BATCH_ITEMS, ServingApiConfig

CONFIG = ServingApiConfig.from_env(
    {
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "de4",
        "POSTGRES_USER": "de4",
        "POSTGRES_PASSWORD": "secret",
    }
)
FAILURE = psycopg.OperationalError("connection refused")


def row(segment_id: str, comfort_score: float) -> tuple[object, ...]:
    """repository.COLUMNS 순서의 한 행."""
    return (
        segment_id,
        0,
        datetime(2026, 8, 10, tzinfo=UTC),
        datetime(2026, 8, 17, tzinfo=UTC),
        comfort_score,
        1200,
        0.94,
        "1.0.0",
        datetime(2026, 8, 17, tzinfo=UTC),
    )


class FakeConnection:
    """커넥션과 커서를 한 객체로 흉내낸다.

    조회 계층이 쓰는 메서드(`cursor`, `execute`, `fetchone`, `fetchall`)와
    `/health`가 쓰는 `execute`만 있으면 충분하다.
    """

    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._rows = rows

    def cursor(self) -> Self:
        return self

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *arguments: object) -> bool:
        return False

    def execute(self, sql: str, parameters: tuple[object, ...] | None = None) -> None:
        return None

    def fetchone(self) -> tuple[object, ...] | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[tuple[object, ...]]:
        return list(self._rows)


class FakePool:
    """`open`/`close`/`connection`만 있으면 앱이 요구하는 계약을 만족한다."""

    def __init__(self, rows: list[tuple[object, ...]], failure: Exception | None) -> None:
        self._rows = rows
        self._failure = failure

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    @contextmanager
    def connection(self):
        if self._failure is not None:
            raise self._failure
        yield FakeConnection(self._rows)


def build_client(
    rows: list[tuple[object, ...]] | None = None, failure: Exception | None = None
) -> TestClient:
    pool = FakePool(rows or [], failure)
    return TestClient(create_app(CONFIG, pool_factory=lambda config: pool))


def test_single_lookup_returns_every_response_field() -> None:
    # 응답 본문 전체를 못 박는다 — 필드가 빠지거나 날짜 직렬화 형식이 바뀌면 깨진다.
    with build_client([row("0032900", 82.5)]) as client:
        response = client.get("/api/v1/segments/0032900/comfort-scores/0")

    assert response.status_code == 200
    assert response.json() == {
        "segment_id": "0032900",
        "vehicle_profile_id": 0,
        "data_period_start": "2026-08-10T00:00:00Z",
        "data_period_end": "2026-08-17T00:00:00Z",
        "comfort_score": 82.5,
        "sample_count": 1200,
        "confidence_score": 0.94,
        "score_version": "1.0.0",
        "calculated_at": "2026-08-17T00:00:00Z",
    }


def test_single_lookup_returns_404_when_the_row_is_missing() -> None:
    # 조회 계층은 None을 돌려줄 뿐이고, 404로 바꾸는 것은 HTTP 계층의 책임이다.
    with build_client() as client:
        response = client.get("/api/v1/segments/0032900/comfort-scores/0")

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "not_found"


def test_batch_lookup_keeps_request_order_and_reports_missing_segments() -> None:
    # 요청 순서 보존과 not_found 분리를 함께 확인한다. DB는 정렬을 보장하지 않으므로
    # 응답 순서는 애플리케이션이 만들어야 한다.
    rows = [row("0032900", 82.5), row("0032901", 74.1)]
    with build_client(rows) as client:
        response = client.post(
            "/api/v1/comfort-scores/batch",
            json={"vehicle_profile_id": 0, "segment_ids": ["0032901", "9999999", "0032900"]},
        )

    body = response.json()
    assert response.status_code == 200
    assert [score["segment_id"] for score in body["scores"]] == ["0032901", "0032900"]
    assert body["not_found_segment_ids"] == ["9999999"]


def test_batch_lookup_returns_a_repeated_segment_once() -> None:
    # 경로가 같은 구간을 두 번 지나도(회차·U턴) 점수는 구간당 하나만 내보낸다.
    with build_client([row("0032900", 82.5)]) as client:
        response = client.post(
            "/api/v1/comfort-scores/batch",
            json={"vehicle_profile_id": 0, "segment_ids": ["0032900", "0032900"]},
        )

    assert [score["segment_id"] for score in response.json()["scores"]] == ["0032900"]


@pytest.mark.parametrize(
    "segment_ids",
    [
        pytest.param([], id="empty"),
        pytest.param([f"{index:07d}" for index in range(MAX_BATCH_ITEMS + 1)], id="too-many"),
    ],
)
def test_batch_lookup_rejects_an_out_of_range_segment_count(segment_ids: list[str]) -> None:
    # 빈 요청은 의미가 없고, 상한을 넘는 요청은 커넥션을 오래 점유한다. 둘 다 422다.
    with build_client() as client:
        response = client.post(
            "/api/v1/comfort-scores/batch",
            json={"vehicle_profile_id": 0, "segment_ids": segment_ids},
        )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"


def test_a_database_failure_returns_503() -> None:
    # DB 예외가 500으로 새지 않고, 예외 메시지도 응답에 노출되지 않아야 한다.
    with build_client(failure=FAILURE) as client:
        response = client.get("/api/v1/segments/0032900/comfort-scores/0")

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "database_unavailable"


def test_health_reports_the_database_state() -> None:
    # /health는 DB가 죽어도 예외를 밖으로 내보내지 않고 본문으로 상태를 알린다.
    with build_client() as client:
        assert client.get("/health").json() == {"status": "ok", "database": "ok"}

    with build_client(failure=FAILURE) as client:
        response = client.get("/health")

    assert response.status_code == 503
    assert response.json() == {"status": "degraded", "database": "unavailable"}
