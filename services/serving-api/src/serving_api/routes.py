"""HTTP endpoints for the serving API (#160)."""

from __future__ import annotations

import logging
from typing import Annotated

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Path
from fastapi.responses import JSONResponse
from psycopg_pool import ConnectionPool

from serving_api import repository
from serving_api.dependencies import get_connection, get_pool
from serving_api.schemas import (
    ComfortScore,
    ComfortScoreBatchRequest,
    ComfortScoreBatchResponse,
)

logger = logging.getLogger(__name__)

health_router = APIRouter(tags=["health"])
comfort_score_router = APIRouter(prefix="/api/v1", tags=["comfort-scores"])


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
