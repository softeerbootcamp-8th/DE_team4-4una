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
        title="DE4 ride-comfort serving API",
        version="0.1.0",
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
