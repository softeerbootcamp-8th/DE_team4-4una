"""FastAPI dependencies for the serving API (#160)."""

from __future__ import annotations

from collections.abc import Iterator

import psycopg
from fastapi import Request
from psycopg_pool import ConnectionPool


def get_pool(request: Request) -> ConnectionPool:
    return request.app.state.pool


def get_connection(request: Request) -> Iterator[psycopg.Connection]:
    """요청 하나가 커넥션 하나를 빌려 쓰고 반납한다.

    풀에서 꺼내는 책임을 여기 한 곳에 두면 조회 계층은 커넥션이 어디서 왔는지
    몰라도 되고, 테스트에서는 이 의존성만 바꿔 끼우면 된다.
    """
    with get_pool(request).connection() as connection:
        yield connection
