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

PoolFactory = Callable[[ServingApiConfig], ConnectionPool]


def create_app(
    config: ServingApiConfig | None = None,
    pool_factory: PoolFactory = create_pool,
) -> FastAPI:
    """`pool_factory`는 테스트에서 가짜 풀을 끼우기 위한 자리다."""
    resolved_config = config if config is not None else ServingApiConfig.from_env()

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
    app.include_router(routes.health_router)
    app.include_router(routes.comfort_score_router)
    app.include_router(routes.route_router)
    return app
