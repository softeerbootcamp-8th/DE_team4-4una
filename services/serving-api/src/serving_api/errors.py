"""Common error responses for the serving API (#160).

모든 오류를 `{"error": {"code": ..., "message": ...}}` 한 형태로 내보낸다.
FastAPI 기본 형식(`{"detail": ...}`)은 검증 오류와 그 밖의 오류가 서로 다른
모양이라 클라이언트가 두 가지를 따로 처리해야 한다.
"""

from __future__ import annotations

import logging

import psycopg
from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

logger = logging.getLogger(__name__)

NOT_FOUND = "not_found"
INVALID_REQUEST = "invalid_request"
DATABASE_UNAVAILABLE = "database_unavailable"
INTERNAL_ERROR = "internal_error"

_STATUS_CODES = {404: NOT_FOUND, 422: INVALID_REQUEST}


def error_response(status_code: int, code: str, message: str, **extra: object) -> JSONResponse:
    error: dict[str, object] = {"code": code, "message": message}
    error.update(extra)
    return JSONResponse(status_code=status_code, content={"error": error})


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(StarletteHTTPException)
    async def handle_http_exception(
        request: Request, exception: StarletteHTTPException
    ) -> JSONResponse:
        code = _STATUS_CODES.get(exception.status_code, INTERNAL_ERROR)
        return error_response(exception.status_code, code, str(exception.detail))

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(
        request: Request, exception: RequestValidationError
    ) -> JSONResponse:
        # 모델 검증기가 던진 ValueError는 `ctx`에 예외 객체 그대로 실려 온다.
        # 직렬화하지 않고 넘기면 응답을 만들다가 500이 난다.
        return error_response(
            422,
            INVALID_REQUEST,
            "request validation failed",
            details=jsonable_encoder(exception.errors()),
        )

    # psycopg_pool.PoolTimeout이 psycopg.Error 하위라, 커넥션 고갈도 이 핸들러가
    # 함께 받는다. 어느 쪽이든 클라이언트가 할 수 있는 일은 재시도뿐이라 503이다.
    @app.exception_handler(psycopg.Error)
    async def handle_database_error(request: Request, exception: psycopg.Error) -> JSONResponse:
        # 예외 내용은 로그에만 남긴다 — 접속 문자열이나 내부 스키마가 응답으로
        # 새어 나가면 안 된다.
        logger.exception("database error while serving %s", request.url.path)
        return error_response(503, DATABASE_UNAVAILABLE, "database is unavailable")
