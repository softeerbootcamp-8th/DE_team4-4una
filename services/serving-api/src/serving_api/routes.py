"""HTTP endpoints for the serving API (#160)."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Annotated

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Path
from fastapi.responses import JSONResponse
from psycopg_pool import ConnectionPool

from serving_api import repository, route_comfort
from serving_api.config import RouteComfortConfig
from serving_api.dependencies import get_connection, get_pool, get_route_comfort_config
from serving_api.schemas import (
    ComfortScore,
    ComfortScoreBatchRequest,
    ComfortScoreBatchResponse,
    RouteCandidate,
    RouteComfortScore,
    RouteEvaluationRequest,
    RouteEvaluationResponse,
)

logger = logging.getLogger(__name__)

# 점수를 못 찾은 구간을 오류 메시지에 몇 개까지 나열할지. 어느 id가 잘못됐는지
# 알기에는 이 정도면 충분하고, 메시지가 로그를 뒤덮지 않는다.
MAX_REPORTED_SEGMENT_IDS = 20

health_router = APIRouter(tags=["health"])
comfort_score_router = APIRouter(prefix="/api/v1", tags=["comfort-scores"])
route_router = APIRouter(prefix="/api/v1", tags=["routes"])


# 정상일 때는 dict, 장애일 때는 JSONResponse를 돌려주므로 응답 모델 추론을 끈다.
@health_router.get("/health", response_model=None)
def read_health(
    pool: Annotated[ConnectionPool, Depends(get_pool)],
) -> JSONResponse | dict[str, str]:
    """앱과 DB 연결 상태를 보고한다.

    여기서는 예외를 밖으로 내보내지 않는다 — 장애를 알리는 것이 이 엔드포인트의
    목적이므로, DB가 죽었을 때도 본문으로 상태를 돌려줘야 한다.
    """
    try:
        with pool.connection() as connection:
            connection.execute("SELECT 1")
    except psycopg.Error:
        logger.exception("health check failed to reach the database")
        return JSONResponse(
            status_code=503,
            content={"status": "degraded", "database": "unavailable"},
        )
    return {"status": "ok", "database": "ok"}


@comfort_score_router.get(
    "/segments/{segment_id}/comfort-scores/{vehicle_profile_id}",
    response_model=ComfortScore,
)
def read_comfort_score(
    segment_id: Annotated[str, Path(min_length=1, max_length=64)],
    vehicle_profile_id: Annotated[int, Path(ge=0)],
    connection: Annotated[psycopg.Connection, Depends(get_connection)],
) -> ComfortScore:
    score = repository.fetch_one(connection, segment_id, vehicle_profile_id)
    if score is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"no comfort score for segment_id={segment_id} "
                f"vehicle_profile_id={vehicle_profile_id}"
            ),
        )
    return score


@comfort_score_router.post("/comfort-scores/batch", response_model=ComfortScoreBatchResponse)
def read_comfort_scores(
    request: ComfortScoreBatchRequest,
    connection: Annotated[psycopg.Connection, Depends(get_connection)],
) -> ComfortScoreBatchResponse:
    segment_ids = _unique_segment_ids(request.segment_ids)
    found = {
        score.segment_id: score
        for score in repository.fetch_many(
            connection, request.vehicle_profile_id, segment_ids
        )
    }

    # 요청 순서를 그대로 유지한다 — 경로 위 구간 순서가 응답 순서가 되어야
    # 클라이언트가 다시 정렬하지 않는다.
    scores: list[ComfortScore] = []
    not_found_segment_ids: list[str] = []
    for segment_id in segment_ids:
        score = found.get(segment_id)
        if score is None:
            not_found_segment_ids.append(segment_id)
        else:
            scores.append(score)
    return ComfortScoreBatchResponse(
        scores=scores, not_found_segment_ids=not_found_segment_ids
    )


@route_router.post("/routes/evaluate", response_model=RouteEvaluationResponse)
def evaluate_routes(
    request: RouteEvaluationRequest,
    connection: Annotated[psycopg.Connection, Depends(get_connection)],
    config: Annotated[RouteComfortConfig, Depends(get_route_comfort_config)],
) -> RouteEvaluationResponse:
    """후보 경로들을 승차감으로 줄 세우고 가장 편안한 경로를 지목한다.

    경로를 만들지는 않는다 — 내비게이션이 이미 뽑아 둔 후보를 받아 점수만 매긴다.
    """
    segment_ids = _unique_segment_ids(
        [segment_id for route in request.routes for segment_id in route.segment_ids]
    )
    # 후보 경로가 몇 개든 조회는 한 번이다. 경로마다 따로 조회하면 경로 사이에
    # 겹치는 구간을 중복해서 읽게 된다.
    scores = {
        score.segment_id: score.comfort_score
        for score in repository.fetch_many(
            connection, request.vehicle_profile_id, segment_ids
        )
    }

    # 정상적인 road universe 안의 구간이라면 current나 standard 중 한쪽에는
    # 반드시 점수가 있다. 없다는 것은 잘못된 구간 id가 왔다는 뜻이므로, 그 구간을
    # 빼고 평균을 내면 실제보다 짧은 경로를 평가한 점수를 조용히 돌려주게 된다.
    missing = [segment_id for segment_id in segment_ids if segment_id not in scores]
    if missing:
        raise HTTPException(
            status_code=404,
            detail=(
                f"no comfort score for vehicle_profile_id={request.vehicle_profile_id} "
                f"segment_ids: {_summarize(missing)}"
            ),
        )

    evaluated = [_evaluate(route, scores, config) for route in request.routes]
    # 응답에 실리는 점수를 그대로 정렬 키로 쓴다. 같은 점수끼리는 파이썬 정렬이
    # 안정적이라 요청에 실려 온 순서가 유지된다.
    evaluated.sort(key=lambda route: route.comfort_score, reverse=True)
    return RouteEvaluationResponse(
        recommended_route_id=evaluated[0].route_id, routes=evaluated
    )


def _evaluate(
    route: RouteCandidate, scores: Mapping[str, float], config: RouteComfortConfig
) -> RouteComfortScore:
    breakdown = route_comfort.score_route(
        [scores[segment_id] for segment_id in route.segment_ids], config
    )
    return RouteComfortScore(
        route_id=route.route_id,
        comfort_score=breakdown.comfort_score,
        average_comfort_score=breakdown.average_comfort_score,
        worst_quartile_comfort_score=breakdown.worst_quartile_comfort_score,
    )


def _summarize(segment_ids: list[str]) -> str:
    """오류 메시지가 구간 수백 개로 불어나지 않게 앞쪽만 보여준다."""
    shown = ", ".join(segment_ids[:MAX_REPORTED_SEGMENT_IDS])
    if len(segment_ids) <= MAX_REPORTED_SEGMENT_IDS:
        return shown
    return f"{shown} (and {len(segment_ids) - MAX_REPORTED_SEGMENT_IDS} more)"


def _unique_segment_ids(segment_ids: list[str]) -> list[str]:
    """같은 구간이 여러 번 와도 한 번만 조회하고, 첫 등장 순서를 지킨다."""
    seen: set[str] = set()
    unique: list[str] = []
    for segment_id in segment_ids:
        if segment_id in seen:
            continue
        seen.add(segment_id)
        unique.append(segment_id)
    return unique
