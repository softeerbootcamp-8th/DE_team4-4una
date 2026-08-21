"""Endpoint tests for the serving API (#160).

가짜 커넥션 풀을 끼워 실제 PostgreSQL 없이 상태 코드와 응답 형식을 검증한다.
쿼리가 실제 DB에서 도는지는 통합 테스트가 확인한다.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Self

import psycopg
import pytest
from fastapi.testclient import TestClient
from serving_api.app import create_app
from serving_api.config import MAX_BATCH_ITEMS, ServingApiConfig
from serving_api.repository import (
    ACTIVE_VEHICLE_PROFILE_SQL,
    CURRENT_TABLE,
    STANDARD_TABLE,
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
FAILURE = psycopg.OperationalError("connection refused")


PERIOD_START = datetime(2026, 8, 10, tzinfo=UTC)
AS_OF = datetime(2026, 8, 17, tzinfo=UTC)


def row(segment_id: str, comfort_score: float) -> tuple[object, ...]:
    """repository.ROW_FIELDS 순서의 current 행."""
    return (
        segment_id, 0, comfort_score, 90.0, 70.0, 80.0, 0.94, 1200,
        PERIOD_START, AS_OF, "1.0.0", AS_OF, "1.0.0", AS_OF,
    )


def standard_row(segment_id: str, comfort_score: float) -> tuple[object, ...]:
    """같은 순서의 standard 폴백 행 — 날씨 컬럼은 NULL이다."""
    return (
        segment_id, 0, comfort_score, 96.0, 97.0, 96.0, 0.17, 900,
        PERIOD_START, AS_OF, "1.0.0", None, None, AS_OF,
    )


class FakeConnection:
    """커넥션과 커서를 한 객체로 흉내낸다.

    조회 계층이 쓰는 메서드(`cursor`, `execute`, `fetchone`, `fetchall`)와
    `/health`가 쓰는 `execute`만 있으면 충분하다.
    """

    def __init__(
        self,
        rows: list[tuple[object, ...]],
        standard_rows: list[tuple[object, ...]],
        active_profile_ids: set[int] | None = None,
    ) -> None:
        self._current = rows
        self._standard = standard_rows
        # None은 "무엇을 물어도 활성"이라는 뜻이다 — 프로필과 무관한 테스트가
        # 매번 유효한 프로필 집합을 적지 않아도 되게 한다.
        self._active_profile_ids = active_profile_ids
        self._rows: list[tuple[object, ...]] = []

    def cursor(self) -> Self:
        return self

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *arguments: object) -> bool:
        return False

    def execute(self, sql: str, parameters: tuple[object, ...] | None = None) -> None:
        # 폴백 분기를 재현하려면 조회 대상 테이블에 따라 다른 행을 돌려줘야 한다.
        # 프로필 조회를 먼저 본다 — 점수 쿼리도 `vehicle_profile_id` 컬럼을
        # 담고 있어 테이블 이름만으로는 구분되지 않는다.
        if sql == ACTIVE_VEHICLE_PROFILE_SQL:
            requested = parameters[0] if parameters else None
            self._rows = [
                (
                    self._active_profile_ids is None
                    or requested in self._active_profile_ids,
                )
            ]
        elif CURRENT_TABLE in sql:
            self._rows = list(self._current)
        elif STANDARD_TABLE in sql:
            requested = set(parameters[1]) if parameters else set()
            self._rows = [row for row in self._standard if row[0] in requested]
        else:
            self._rows = []

    def fetchone(self) -> tuple[object, ...] | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[tuple[object, ...]]:
        return list(self._rows)


class FakePool:
    """`open`/`close`/`connection`만 있으면 앱이 요구하는 계약을 만족한다."""

    def __init__(
        self,
        rows: list[tuple[object, ...]],
        failure: Exception | None,
        standard_rows: list[tuple[object, ...]] | None = None,
        active_profile_ids: set[int] | None = None,
    ) -> None:
        self._rows = rows
        self._standard_rows = standard_rows or []
        self._failure = failure
        self._active_profile_ids = active_profile_ids

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    @contextmanager
    def connection(self):
        if self._failure is not None:
            raise self._failure
        yield FakeConnection(self._rows, self._standard_rows, self._active_profile_ids)


def build_client(
    rows: list[tuple[object, ...]] | None = None,
    failure: Exception | None = None,
    standard_rows: list[tuple[object, ...]] | None = None,
    active_profile_ids: set[int] | None = None,
) -> TestClient:
    pool = FakePool(rows or [], failure, standard_rows, active_profile_ids)
    return TestClient(create_app(CONFIG, pool_factory=lambda config: pool))


def test_single_lookup_returns_every_response_field() -> None:
    # 응답 본문 전체를 못 박는다 — 필드가 빠지거나 날짜 직렬화 형식이 바뀌면 깨진다.
    with build_client([row("0032900", 82.5)]) as client:
        response = client.get("/api/v1/segments/0032900/comfort-scores/0")

    assert response.status_code == 200
    assert response.json() == {
        "segment_id": "0032900",
        "vehicle_profile_id": 0,
        "comfort_score": 82.5,
        "vertical_score": 90.0,
        "longitudinal_score": 70.0,
        "lateral_score": 80.0,
        "confidence_score": 0.94,
        "sample_count": 1200,
        "data_period_start": "2026-08-10T00:00:00Z",
        "standard_score_as_of": "2026-08-17T00:00:00Z",
        "standard_score_version": "1.0.0",
        "weather_time": "2026-08-17T00:00:00Z",
        "weather_rule_version": "1.0.0",
        "calculated_at": "2026-08-17T00:00:00Z",
        "source": "current",
    }


def test_single_lookup_falls_back_to_the_standard_score() -> None:
    # zone이 없거나 아직 날씨를 못 받은 구간은 current에 행이 없다. 404 대신
    # 날씨 미보정 점수를 주고, source로 그 사실을 알린다.
    with build_client(standard_rows=[standard_row("0032900", 96.3)]) as client:
        response = client.get("/api/v1/segments/0032900/comfort-scores/0")

    body = response.json()
    assert response.status_code == 200
    assert body["source"] == "standard"
    assert body["comfort_score"] == 96.3
    assert body["weather_time"] is None
    assert body["weather_rule_version"] is None


def test_batch_lookup_mixes_current_and_fallback_scores() -> None:
    with build_client(
        [row("0032900", 82.5)], standard_rows=[standard_row("0032901", 96.3)]
    ) as client:
        response = client.post(
            "/api/v1/comfort-scores/batch",
            json={"vehicle_profile_id": 0, "segment_ids": ["0032900", "0032901", "9999999"]},
        )

    body = response.json()
    assert response.status_code == 200
    assert [(s["segment_id"], s["source"]) for s in body["scores"]] == [
        ("0032900", "current"),
        ("0032901", "standard"),
    ]
    assert body["not_found_segment_ids"] == ["9999999"]


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


def test_route_evaluation_ranks_candidates_and_names_the_recommended_route() -> None:
    # 명세의 예시를 그대로 옮긴 것이다. route_c는 평균이 route_b보다 높지만
    # 불편한 구간이 섞여 있어 최종 점수는 뒤로 밀려야 한다.
    rows = [
        row("1001", 85.0), row("1002", 82.0), row("1003", 40.0), row("1004", 45.0),
        row("2001", 75.0), row("2002", 73.0), row("2003", 71.0), row("2004", 69.0),
        row("3001", 90.0), row("3002", 88.0), row("3003", 55.0), row("3004", 52.0),
    ]
    with build_client(rows) as client:
        response = client.post(
            "/api/v1/routes/evaluate",
            json={
                "vehicle_profile_id": 2,
                "routes": [
                    {"route_id": "route_a", "segment_ids": ["1001", "1002", "1003", "1004"]},
                    {"route_id": "route_b", "segment_ids": ["2001", "2002", "2003", "2004"]},
                    {"route_id": "route_c", "segment_ids": ["3001", "3002", "3003", "3004"]},
                ],
            },
        )

    assert response.status_code == 200
    assert response.json() == {
        "requested_vehicle_profile_id": 2,
        "effective_vehicle_profile_id": 2,
        "vehicle_profile_fallback": False,
        "recommended_route_id": "route_b",
        "routes": [
            {
                "route_id": "route_b",
                "comfort_score": 71.1,
                "average_comfort_score": 72.0,
                "worst_quartile_comfort_score": 69.0,
            },
            {
                "route_id": "route_c",
                "comfort_score": 65.48,
                "average_comfort_score": 71.25,
                "worst_quartile_comfort_score": 52.0,
            },
            {
                "route_id": "route_a",
                "comfort_score": 56.1,
                "average_comfort_score": 63.0,
                "worst_quartile_comfort_score": 40.0,
            },
        ],
    }


def test_route_evaluation_falls_back_to_the_standard_score() -> None:
    # 구간 점수 조회는 기존 로직을 그대로 쓴다 — current에 없으면 standard로 채운다.
    with build_client(
        [row("1001", 80.0)], standard_rows=[standard_row("1002", 60.0)]
    ) as client:
        response = client.post(
            "/api/v1/routes/evaluate",
            json={
                "vehicle_profile_id": 0,
                "routes": [{"route_id": "route_a", "segment_ids": ["1001", "1002"]}],
            },
        )

    assert response.status_code == 200
    assert response.json()["routes"][0]["average_comfort_score"] == 70.0


def test_route_evaluation_fails_when_a_segment_has_no_score() -> None:
    # road universe 밖의 구간을 조용히 빼고 평균을 내면, 요청한 것보다 짧은
    # 경로를 평가한 점수가 정상 응답처럼 나간다.
    with build_client([row("1001", 80.0)]) as client:
        response = client.post(
            "/api/v1/routes/evaluate",
            json={
                "vehicle_profile_id": 0,
                "routes": [{"route_id": "route_a", "segment_ids": ["1001", "9999999"]}],
            },
        )

    assert response.status_code == 404
    body = response.json()
    assert body["error"]["code"] == "not_found"
    assert "9999999" in body["error"]["message"]


def test_route_evaluation_keeps_request_order_between_tied_routes() -> None:
    # 점수가 같으면 순위를 뒤집을 근거가 없으므로 요청 순서를 유지한다.
    with build_client([row("1001", 70.0), row("2001", 70.0)]) as client:
        response = client.post(
            "/api/v1/routes/evaluate",
            json={
                "vehicle_profile_id": 0,
                "routes": [
                    {"route_id": "route_b", "segment_ids": ["2001"]},
                    {"route_id": "route_a", "segment_ids": ["1001"]},
                ],
            },
        )

    body = response.json()
    assert [route["route_id"] for route in body["routes"]] == ["route_b", "route_a"]
    assert body["recommended_route_id"] == "route_b"


def test_route_evaluation_scores_a_repeated_segment_once_per_traversal() -> None:
    # 조회는 구간당 한 번이지만 평균에는 지나간 횟수만큼 들어가야 한다.
    with build_client([row("1001", 90.0), row("1002", 30.0)]) as client:
        response = client.post(
            "/api/v1/routes/evaluate",
            json={
                "vehicle_profile_id": 0,
                "routes": [
                    {"route_id": "once", "segment_ids": ["1001", "1002"]},
                    {"route_id": "twice", "segment_ids": ["1001", "1002", "1002"]},
                ],
            },
        )

    averages = {
        route["route_id"]: route["average_comfort_score"]
        for route in response.json()["routes"]
    }
    assert averages == {"once": 60.0, "twice": 50.0}


def test_route_evaluation_uses_an_active_vehicle_profile_as_requested() -> None:
    with build_client([row("1001", 80.0)], active_profile_ids={0, 2}) as client:
        response = client.post(
            "/api/v1/routes/evaluate",
            json={
                "vehicle_profile_id": 2,
                "routes": [{"route_id": "route_a", "segment_ids": ["1001"]}],
            },
        )

    body = response.json()
    assert response.status_code == 200
    assert body["requested_vehicle_profile_id"] == 2
    assert body["effective_vehicle_profile_id"] == 2
    assert body["vehicle_profile_fallback"] is False


@pytest.mark.parametrize(
    ("requested", "active_profile_ids"),
    [
        pytest.param(999, {0, 2}, id="unknown-profile"),
        # 비활성 프로필은 행이 있어도 그 프로필로 점수를 낼 수 없으니 없는 것과 같다.
        pytest.param(2, {0}, id="inactive-profile"),
    ],
)
def test_route_evaluation_falls_back_to_the_vehicle_agnostic_profile(
    requested: int, active_profile_ids: set[int]
) -> None:
    # 프로필 하나가 잘못됐다고 경로 비교 전체를 실패시키지 않는다. 대신 어느
    # 프로필로 계산했는지를 응답에 실어 호출자가 오해하지 않게 한다.
    with build_client([row("1001", 80.0)], active_profile_ids=active_profile_ids) as client:
        response = client.post(
            "/api/v1/routes/evaluate",
            json={
                "vehicle_profile_id": requested,
                "routes": [{"route_id": "route_a", "segment_ids": ["1001"]}],
            },
        )

    body = response.json()
    assert response.status_code == 200
    assert body["requested_vehicle_profile_id"] == requested
    assert body["effective_vehicle_profile_id"] == 0
    assert body["vehicle_profile_fallback"] is True
    assert body["recommended_route_id"] == "route_a"


def test_route_evaluation_logs_a_warning_when_it_falls_back(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # 200이 나가므로 잘못된 id가 계속 들어와도 응답만으로는 드러나지 않는다.
    with build_client([row("1001", 80.0)], active_profile_ids={0}) as client, caplog.at_level(
        logging.WARNING, logger="serving_api.routes"
    ):
        client.post(
            "/api/v1/routes/evaluate",
            json={
                "vehicle_profile_id": 999,
                "routes": [{"route_id": "route_a", "segment_ids": ["1001"]}],
            },
        )

    assert "vehicle_profile_id=999 is unavailable" in caplog.text


def test_route_evaluation_does_not_look_up_the_sentinel_profile() -> None:
    # sentinel 행은 마이그레이션이 보장하므로 확인 왕복을 아낀다. 활성 프로필이
    # 하나도 없다고 해도 0 요청은 폴백 없이 그대로 통해야 한다.
    with build_client([row("1001", 80.0)], active_profile_ids=set()) as client:
        response = client.post(
            "/api/v1/routes/evaluate",
            json={
                "vehicle_profile_id": 0,
                "routes": [{"route_id": "route_a", "segment_ids": ["1001"]}],
            },
        )

    assert response.json()["vehicle_profile_fallback"] is False


@pytest.mark.parametrize(
    "endpoint_call",
    [
        pytest.param(
            lambda client: client.get("/api/v1/segments/1001/comfort-scores/999"),
            id="single-lookup",
        ),
        pytest.param(
            lambda client: client.post(
                "/api/v1/comfort-scores/batch",
                json={"vehicle_profile_id": 999, "segment_ids": ["1001"]},
            ),
            id="batch-lookup",
        ),
    ],
)
def test_the_other_endpoints_keep_their_vehicle_profile_policy(endpoint_call) -> None:
    # 폴백은 /routes/evaluate에만 적용한다 (#272). 두 엔드포인트는 요청한
    # 프로필로만 조회하므로 프로필이 잘못되면 점수를 못 찾은 것으로 나타난다.
    with build_client(active_profile_ids={0}) as client:
        response = endpoint_call(client)

    body = response.json()
    assert response.status_code in (200, 404)
    if response.status_code == 200:
        assert body["not_found_segment_ids"] == ["1001"]
    else:
        assert body["error"]["code"] == "not_found"


@pytest.mark.parametrize(
    "routes",
    [
        pytest.param([], id="no-routes"),
        pytest.param([{"route_id": "route_a", "segment_ids": []}], id="empty-route"),
        pytest.param(
            [
                {"route_id": "route_a", "segment_ids": ["1001"]},
                {"route_id": "route_a", "segment_ids": ["2001"]},
            ],
            id="duplicate-route-id",
        ),
        pytest.param(
            [
                {
                    "route_id": "route_a",
                    "segment_ids": [f"{index:07d}" for index in range(MAX_BATCH_ITEMS + 1)],
                }
            ],
            id="too-many-segments",
        ),
        pytest.param(
            [
                {
                    "route_id": f"route_{index}",
                    "segment_ids": [f"{index}{offset:06d}" for offset in range(200)],
                }
                for index in range(2)
            ],
            id="too-many-segments-across-routes",
        ),
    ],
)
def test_route_evaluation_rejects_a_malformed_request(routes: list[dict[str, object]]) -> None:
    with build_client() as client:
        response = client.post(
            "/api/v1/routes/evaluate",
            json={"vehicle_profile_id": 0, "routes": routes},
        )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"


def test_route_evaluation_rejects_a_negative_vehicle_profile_id() -> None:
    # 폴백은 "없는 프로필"을 위한 것이지 형식 오류를 받아주기 위한 것이 아니다.
    # 음수는 프로필 id가 될 수 없으므로 조회 전에 Pydantic이 거른다.
    with build_client() as client:
        response = client.post(
            "/api/v1/routes/evaluate",
            json={
                "vehicle_profile_id": -1,
                "routes": [{"route_id": "route_a", "segment_ids": ["1001"]}],
            },
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
