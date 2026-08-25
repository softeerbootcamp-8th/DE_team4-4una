"""FastAPI application factory for the serving API (#160)."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI
from psycopg_pool import ConnectionPool

from serving_api import routes
from serving_api.config import ServingApiConfig
from serving_api.db import create_pool
from serving_api.errors import register_error_handlers
from serving_api.metrics import RequestMetrics, install_request_metrics

PoolFactory = Callable[[ServingApiConfig], ConnectionPool]

# Swagger UI 최상단에 표시된다. 개별 정책은 각 endpoint와 필드 설명에 적는다.
API_DESCRIPTION = (
    "NYC 도로 구간의 승차감 점수를 조회하고, 후보 경로를 승차감 기준으로 비교한다."
)

TAGS_METADATA = [
    {
        "name": "health",
        "description": "앱과 DB 연결 상태 확인. 배포 스크립트와 모니터링이 사용한다.",
    },
    {
        "name": "comfort-scores",
        "description": (
            "도로 구간의 최신 승차감 점수를 조회한다. 단건과 다건을 모두 지원한다."
        ),
    },
    {
        "name": "routes",
        "description": (
            "후보 경로를 승차감 기준으로 비교한다. "
            "경로에 포함되는 도로 구간을 리스트로 받아 점수를 산출한다."
        ),
    },
]


def create_app(
    config: ServingApiConfig | None = None,
    pool_factory: PoolFactory = create_pool,
    metrics: RequestMetrics | None = None,
) -> FastAPI:
    """`pool_factory`는 가짜 풀을, `metrics`는 격리된 registry를 끼우는 테스트용 자리다.

    `metrics`를 생략하면 인스턴스 전용 registry가 매번 새로 만들어져 실제
    포트를 바인딩하지 않는다 — metrics HTTP server를 시작하는 것은 `main()`의
    몫이다.
    """
    resolved_config = config if config is not None else ServingApiConfig.from_env()
    resolved_metrics = metrics if metrics is not None else RequestMetrics()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        pool = pool_factory(resolved_config)
        # 연결을 기다리지 않고 연다 — DB가 아직 안 떠 있어도 앱은 기동되어야 하고,
        # 그 상태는 /health가 503으로 보고한다.
        pool.open()
        app.state.pool = pool
        try:
            yield
        finally:
            pool.close()

    app = FastAPI(
        title="Road Comfort Score API",
        summary="내비게이션이 승차감을 고려한 경로를 추천할 수 있도록 경로별 승차감 점수를 제공하는 API",
        description=API_DESCRIPTION,
        version="0.1.0",
        openapi_tags=TAGS_METADATA,
        lifespan=lifespan,
    )
    # 경로 평가 정책값을 핸들러에서 읽어야 한다. 풀과 달리 기동 전에도 값이
    # 정해져 있으므로 lifespan을 기다리지 않고 여기서 붙인다.
    app.state.config = resolved_config
    register_error_handlers(app)
    install_request_metrics(app, resolved_metrics)
    app.include_router(routes.health_router)
    app.include_router(routes.comfort_score_router)
    app.include_router(routes.route_router)
    return app
