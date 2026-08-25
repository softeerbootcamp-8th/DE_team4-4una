"""HTTP endpoints for the serving API (#160)."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Annotated

import psycopg
from fastapi import APIRouter, Body, Depends, HTTPException, Path
from fastapi.responses import JSONResponse
from psycopg_pool import ConnectionPool

from serving_api import repository, route_comfort
from serving_api.config import (
    MAX_COMFORT_SCORE_BATCH_ITEMS,
    MAX_ROUTE_SEGMENTS,
    MAX_ROUTES_PER_REQUEST,
    RouteComfortConfig,
)
from serving_api.dependencies import get_connection, get_pool, get_route_comfort_config
from serving_api.errors import ErrorResponse
from serving_api.schemas import (
    VEHICLE_AGNOSTIC_VEHICLE_PROFILE_ID,
    ComfortScore,
    ComfortScoreBatchRequest,
    ComfortScoreBatchResponse,
    ComfortScoreResponse,
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

# Swagger UI의 "Try it out"은 파라미터/본문 레벨 examples를 입력칸에 미리 채워준다.
# 그래서 Execute만 눌러도 형식이 맞는 요청이 나간다. 다만 어떤 구간이 실제로
# 존재하는지는 스냅샷마다 다르므로, 아래 id로 200이 온다는 보장은 없다 — 형식을
# 보여주는 것이 목적이고, 없는 구간이면 404나 not_found_segment_ids로 응답한다.
_SEGMENT_ID_EXAMPLES = {
    "sample_1": {"summary": "0032900", "value": "0032900"},
    "sample_2": {"summary": "0032901", "value": "0032901"},
    "sample_3": {"summary": "0041250", "value": "0041250"},
    "sample_4": {"summary": "0125630", "value": "0125630"},
    "missing": {"summary": "9999999 — 점수가 없는 구간 (404)", "value": "9999999"},
}
# 0005_define_vehicle_profiles.sql의 vehicle_profile 행. OpenAPI는 DB에 닿지 않는
# 시점에 만들어지므로 목록을 조회해 채울 수 없다 -- 프로필이 늘면 여기도 고친다.
# 드롭다운 선택지일 뿐이라 뒤처져도 요청 자체는 어떤 id로든 보낼 수 있다.
_VEHICLE_PROFILE_EXAMPLES = {
    "all_vehicles": {"summary": "0 — 차량 구분 없음 (기본)", "value": 0},
    "sedan_compact": {"summary": "1 — 세단 소형", "value": 1},
    "sedan_large": {"summary": "2 — 세단 대형", "value": 2},
    "suv_compact": {"summary": "3 — SUV 소형", "value": 3},
    "suv_large": {"summary": "4 — SUV 대형", "value": 4},
    "mpv_large": {"summary": "5 — MPV 대형", "value": 5},
}
_BATCH_BODY_EXAMPLES = {
    "sample": {
        "summary": "구간 다건 조회",
        "description": "마지막 구간은 점수가 없다고 가정한 예시다. `not_found_segment_ids`로 돌아온다.",
        "value": {
            "vehicle_profile_id": 0,
            "segment_ids": ["0032900", "0032901", "9999999"],
        },
    },
}
_ROUTE_BODY_EXAMPLES = {
    "sample": {
        "summary": "후보 경로 비교",
        "value": {
            "vehicle_profile_id": 3,
            "routes": [
                {
                    "route_id": "route_a",
                    "segment_ids": [
                        "0032900", "0032901", "0032902", "0032903",
                        "0032904", "0032905", "0032906",
                    ],
                },
                {
                    "route_id": "route_b",
                    "segment_ids": [
                        "0041250", "0041251", "0041252", "0041253",
                        "0041254", "0041255",
                    ],
                },
            ],
        },
    },
}

# 세 endpoint가 같은 오류를 낸다. OpenAPI에 실을 설명도 한 곳에서 관리한다.
_VALIDATION_RESPONSE = {
    422: {
        "model": ErrorResponse,
        "description": "요청 형식이 스키마에 맞지 않는다. `error.details`에 어느 필드가 왜 거절됐는지 담긴다.",
    }
}
_DATABASE_RESPONSE = {
    503: {
        "model": ErrorResponse,
        "description": "DB에 닿지 못했다. 커넥션 풀 고갈도 여기에 포함된다. 클라이언트가 할 수 있는 일은 재시도뿐이다.",
    }
}


# 정상일 때는 dict, 장애일 때는 JSONResponse를 돌려주므로 응답 모델 추론을 끈다.
@health_router.get(
    "/health",
    response_model=None,
    summary="상태 확인",
    description=(
        "앱이 떠 있고 DB에 닿는지 확인한다. 배포 스크립트와 모니터링이 쓴다.\n\n"
        "장애를 오류가 아니라 상태 보고로 다루므로, 503일 때도 공통 오류 형식이"
        " 아니라 정상 응답과 같은 모양(`status`, `database`)을 돌려준다."
    ),
    responses={
        200: {
            "description": "DB 연결 정상.",
            "content": {"application/json": {"example": {"status": "ok", "database": "ok"}}},
        },
        503: {
            "description": "DB에 닿지 못했다.",
            "content": {
                "application/json": {
                    "example": {"status": "degraded", "database": "unavailable"}
                }
            },
        },
    },
)
def read_health(
    pool: Annotated[ConnectionPool, Depends(get_pool)],
) -> JSONResponse | dict[str, str]:
    """앱과 DB 연결 상태를 보고한다."""
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
    response_model=ComfortScoreResponse,
    summary="구간 승차감 점수 단건 조회",
    description=(
        "구간 하나의 최신 점수를 돌려준다.\n\n"
        "`current`에 행이 없으면 `standard` 점수로 대신 응답하고, 어느 쪽인지는"
        " `source`로 알린다."
    ),
    responses={
        404: {
            "model": ErrorResponse,
            "description": "그 구간 x 프로필로 계산된 점수가 current에도 standard에도 없다.",
        },
        **_VALIDATION_RESPONSE,
        **_DATABASE_RESPONSE,
    },
)
def read_comfort_score(
    segment_id: Annotated[
        str,
        Path(
            min_length=1,
            max_length=64,
            description="LION 도로 구간 식별자.",
            # schema의 examples가 아니라 파라미터 레벨 examples로 나가야 Swagger UI의
            # "Try it out" 입력칸이 이 값으로 미리 채워진다.
            openapi_examples=_SEGMENT_ID_EXAMPLES,
        ),
    ],
    vehicle_profile_id: Annotated[
        int,
        Path(
            ge=0,
            description=(
                "차량 프로필. `0`은 차량 구분 없는 전체 대표값이다. 없거나 비활성인 "
                "값은 거절하지 않고 `0`으로 대체 조회한다."
            ),
            openapi_examples=_VEHICLE_PROFILE_EXAMPLES,
        ),
    ],
    connection: Annotated[psycopg.Connection, Depends(get_connection)],
) -> ComfortScoreResponse:
    effective_vehicle_profile_id = _resolve_vehicle_profile_id(
        connection, vehicle_profile_id
    )
    score = repository.fetch_one(connection, segment_id, effective_vehicle_profile_id)
    if score is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"no comfort score for segment_id={segment_id} "
                f"vehicle_profile_id={effective_vehicle_profile_id}"
            ),
        )
    return ComfortScoreResponse(
        **score.model_dump(),
        requested_vehicle_profile_id=vehicle_profile_id,
        effective_vehicle_profile_id=effective_vehicle_profile_id,
        vehicle_profile_fallback=effective_vehicle_profile_id != vehicle_profile_id,
    )


@comfort_score_router.post(
    "/comfort-scores/batch",
    response_model=ComfortScoreBatchResponse,
    summary="구간 승차감 점수 일괄(다건) 조회",
    description=(
        "여러 구간의 점수를 한 번에 돌려준다.\n\n"
        "- **중복·순서**: 같은 `segment_id`가 여러 번 와도 한 번만 조회하고,"
        " `scores`는 중복 제거 후 첫 등장 순서를 따른다.\n"
        "- **점수 없는 구간**: 오류가 아니라 `not_found_segment_ids`로 담는다."
        " 전부 없어도 200이다.\n"
        f"- **상한**: 요청 하나에 구간 {MAX_COMFORT_SCORE_BATCH_ITEMS:,}개까지."
        " 넘으면 422다."
    ),
    responses={**_VALIDATION_RESPONSE, **_DATABASE_RESPONSE},
)
def read_comfort_scores(
    request: Annotated[
        ComfortScoreBatchRequest, Body(openapi_examples=_BATCH_BODY_EXAMPLES)
    ],
    connection: Annotated[psycopg.Connection, Depends(get_connection)],
) -> ComfortScoreBatchResponse:
    effective_vehicle_profile_id = _resolve_vehicle_profile_id(
        connection, request.vehicle_profile_id
    )
    segment_ids = _unique_segment_ids(request.segment_ids)
    found = {
        score.segment_id: score
        for score in repository.fetch_many(
            connection, effective_vehicle_profile_id, segment_ids
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
        requested_vehicle_profile_id=request.vehicle_profile_id,
        effective_vehicle_profile_id=effective_vehicle_profile_id,
        vehicle_profile_fallback=(
            effective_vehicle_profile_id != request.vehicle_profile_id
        ),
        scores=scores,
        not_found_segment_ids=not_found_segment_ids,
    )


# summary/description은 그대로 OpenAPI에 실린다. 독스트링에 두면 코드 독자에게
# 하는 말과 API 소비자에게 하는 말이 한 덩어리가 되므로 여기서 분리한다.
@route_router.post(
    "/routes/evaluate",
    response_model=RouteEvaluationResponse,
    summary="후보 경로 승차감 평가",
    description=(
        "후보 경로를 받아 승차감 점수를 매긴다. 경로 탐색은 하지 않고, 거리·소요"
        " 시간도 받지 않는다.\n\n"
        "- **점수**: 전체 평균과 하위 구간 평균의 가중 평균. 두 중간값도 함께 돌려준다.\n"
        "- **정렬**: `comfort_score` 내림차순, 맨 앞이 `recommended_route_id`."
        " 점수가 같으면 요청 순서를 유지한다.\n"
        "- **누락 구간**: 점수를 찾지 못한 구간이 하나라도 있으면 404다"
        " (`/comfort-scores/batch`와 다르다).\n"
        f"- **상한**: 경로 {MAX_ROUTES_PER_REQUEST}개, 중복 제거한 구간"
        f" {MAX_ROUTE_SEGMENTS}개. `route_id`는 요청 안에서 유일해야 한다."
    ),
    responses={
        404: {
            "model": ErrorResponse,
            "description": (
                "경로가 지나는 구간 중 점수를 찾지 못한 것이 있다. `error.message`에 "
                "해당 구간 id가 일부 나열된다."
            ),
        },
        **_VALIDATION_RESPONSE,
        **_DATABASE_RESPONSE,
    },
)
def evaluate_routes(
    request: Annotated[
        RouteEvaluationRequest, Body(openapi_examples=_ROUTE_BODY_EXAMPLES)
    ],
    connection: Annotated[psycopg.Connection, Depends(get_connection)],
    config: Annotated[RouteComfortConfig, Depends(get_route_comfort_config)],
) -> RouteEvaluationResponse:
    effective_vehicle_profile_id = _resolve_vehicle_profile_id(
        connection, request.vehicle_profile_id
    )
    segment_ids = _unique_segment_ids(
        [segment_id for route in request.routes for segment_id in route.segment_ids]
    )
    # 후보 경로가 몇 개든 조회는 한 번이다. 경로마다 따로 조회하면 경로 사이에
    # 겹치는 구간을 중복해서 읽게 된다.
    scores = {
        score.segment_id: score.comfort_score
        for score in repository.fetch_many(
            connection, effective_vehicle_profile_id, segment_ids
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
                f"no comfort score for vehicle_profile_id={effective_vehicle_profile_id} "
                f"segment_ids: {_summarize(missing)}"
            ),
        )

    evaluated = [_evaluate(route, scores, config) for route in request.routes]
    # 응답에 실리는 점수를 그대로 정렬 키로 쓴다. 같은 점수끼리는 파이썬 정렬이
    # 안정적이라 요청에 실려 온 순서가 유지된다.
    evaluated.sort(key=lambda route: route.comfort_score, reverse=True)
    return RouteEvaluationResponse(
        requested_vehicle_profile_id=request.vehicle_profile_id,
        effective_vehicle_profile_id=effective_vehicle_profile_id,
        vehicle_profile_fallback=(
            effective_vehicle_profile_id != request.vehicle_profile_id
        ),
        recommended_route_id=evaluated[0].route_id,
        routes=evaluated,
    )


def _resolve_vehicle_profile_id(connection: psycopg.Connection, requested: int) -> int:
    """활성 프로필이 아니면 차량 무관 sentinel로 내려간다 (#272).

    경로 비교는 프로필 하나가 잘못됐다고 통째로 실패시키는 것보다, 차량 구분
    없는 점수로라도 순위를 내주는 편이 쓸모 있다. 대신 그 사실을 응답과 로그
    양쪽에 남긴다.
    """
    if requested == VEHICLE_AGNOSTIC_VEHICLE_PROFILE_ID:
        # sentinel 행은 마이그레이션이 보장하므로(0003) 확인 왕복이 필요 없다.
        return requested
    if repository.is_active_vehicle_profile(connection, requested):
        return requested

    # 잘못된 id가 계속 들어오는 상황은 200 응답만 보고는 드러나지 않는다.
    logger.warning(
        "requested vehicle_profile_id=%s is unavailable; "
        "fallback to vehicle_profile_id=%s",
        requested,
        VEHICLE_AGNOSTIC_VEHICLE_PROFILE_ID,
    )
    return VEHICLE_AGNOSTIC_VEHICLE_PROFILE_ID


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
