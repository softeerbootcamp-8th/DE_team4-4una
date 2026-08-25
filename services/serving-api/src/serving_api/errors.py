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
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

logger = logging.getLogger(__name__)

NOT_FOUND = "not_found"
INVALID_REQUEST = "invalid_request"
DATABASE_UNAVAILABLE = "database_unavailable"
INTERNAL_ERROR = "internal_error"

_STATUS_CODES = {404: NOT_FOUND, 422: INVALID_REQUEST}


# 응답 모델을 schemas가 아니라 여기에 둔다 — 이 형식을 실제로 만들어내는 핸들러가
# 바로 아래에 있어서, 한쪽만 고치고 다른 쪽이 뒤처지는 일을 막는다.
class ErrorDetail(BaseModel):
    code: str = Field(
        description=(
            "오류 종류를 나타내는 식별자. 상태 코드별로 "
            f"404는 `{NOT_FOUND}`, 422는 `{INVALID_REQUEST}`, "
            f"503은 `{DATABASE_UNAVAILABLE}`, 그 밖은 `{INTERNAL_ERROR}`다."
        ),
        examples=[NOT_FOUND],
    )
    message: str = Field(
        description=(
            "사람이 읽을 설명. 형식이 고정되어 있지 않으므로 분기 조건으로 쓰지 않는다 "
            "— 분기는 `code`로 한다."
        ),
        examples=["no comfort score for segment_id=0012345 vehicle_profile_id=0"],
    )
    details: list[dict] | None = Field(
        default=None,
        description="422에만 실리는 검증 오류 목록. 어느 필드가 왜 거절됐는지 담는다.",
    )


# 검증 오류와 그 밖의 오류가 같은 모양이라, 클라이언트가 `error.code` 하나만 보고
# 분기할 수 있다. 예외가 하나 있다 -- `GET /health`는 장애를 오류가 아니라 상태
# 보고로 다루므로 503에서도 이 형식을 쓰지 않는다.
class ErrorResponse(BaseModel):
    """모든 오류의 공통 응답 형식."""

    error: ErrorDetail


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
