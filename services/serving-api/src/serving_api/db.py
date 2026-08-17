"""PostgreSQL connection pool for the serving API (#160)."""

from __future__ import annotations

from psycopg_pool import ConnectionPool

from serving_api.config import ServingApiConfig


def create_pool(config: ServingApiConfig) -> ConnectionPool:
    """풀을 만들되 연결은 시작하지 않는다.

    `open=False`로 두고 애플리케이션 lifespan에서 명시적으로 열어, 풀의 수명이
    앱의 수명과 정확히 일치하게 한다.
    """
    return ConnectionPool(
        conninfo=config.conninfo,
        min_size=config.pool_min_size,
        max_size=config.pool_max_size,
        open=False,
    )
