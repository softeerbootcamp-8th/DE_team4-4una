"""FastAPI application backing the React dashboard.

브라우저는 여기에만 붙는다. S3와 Serving API 접근은 전부 서버에서 일어나고,
프론트엔드는 GeoJSON만 받는다.
"""

from __future__ import annotations

import gzip
import logging
import os
from contextlib import asynccontextmanager
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Any

import httpx
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import Depends, FastAPI, Request, Response
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import PlainTextResponse
from fastapi.staticfiles import StaticFiles

from dashboard.config import DashboardConfig
from dashboard.dashboard_service import DashboardService, UnknownBoroughError

# 배포 시 React 빌드 산출물이 놓이는 경로. Docker 이미지에서는 패키지가
# site-packages에 설치되므로 소스 트리 기준 경로로는 찾을 수 없다.
STATIC_DIR_ENV = "DASHBOARD_STATIC_DIR"

logger = logging.getLogger(__name__)

# outline만 353KB라 압축이 확실히 이득이다. 이보다 작은 응답은 건드리지 않는다.
_GZIP_MINIMUM_SIZE = 1024


@lru_cache(maxsize=1)
def get_service() -> DashboardService:
    """프로세스에 하나만 둔다 -- 스냅샷과 STRtree를 여기에 들고 있다."""
    return DashboardService(DashboardConfig.from_env())


ServiceDep = Annotated[DashboardService, Depends(get_service)]

@asynccontextmanager
async def lifespan(_app: FastAPI):
    """기동하자마자 borough별 응답을 미리 만들어둔다.

    설정이 비어 있으면 그냥 넘어간다 -- 그 경우 어차피 요청이 들어올 때
    핸들러가 무엇이 없는지 알려준다.
    """
    try:
        get_service().start_prewarm()
    except ValueError as exc:
        logger.warning("prewarm skipped: %s", exc)
    yield


app = FastAPI(
    title="NYC Road Comfort Dashboard", docs_url=None, redoc_url=None, lifespan=lifespan
)
app.add_middleware(GZipMiddleware, minimum_size=_GZIP_MINIMUM_SIZE)


@app.exception_handler(UnknownBoroughError)
async def _unknown_borough(_request: Request, exc: UnknownBoroughError) -> PlainTextResponse:
    return PlainTextResponse(f"unknown borough: {exc}", status_code=404)


@app.exception_handler(BotoCoreError)
@app.exception_handler(ClientError)
async def _s3_unavailable(_request: Request, exc: Exception) -> PlainTextResponse:
    return PlainTextResponse(f"unable to load reference data from S3: {exc}", status_code=502)


# 설정 오류(필수 환경변수 누락 등)와 serving API 응답 계약 위반이 여기로 온다.
# Streamlit이 화면에 띄워주던 안내를 대신한다. UnknownBoroughError는 ValueError의
# 하위 타입이지만 Starlette이 MRO 순으로 찾으므로 위의 404 핸들러가 먼저 잡는다.
@app.exception_handler(ValueError)
async def _invalid_configuration(_request: Request, exc: ValueError) -> PlainTextResponse:
    return PlainTextResponse(
        f"dashboard configuration or data is invalid: {exc}", status_code=500
    )


@app.exception_handler(httpx.HTTPError)
async def _serving_api_unavailable(_request: Request, exc: httpx.HTTPError) -> PlainTextResponse:
    return PlainTextResponse(
        f"unable to load comfort scores from the Serving API: {exc}", status_code=502
    )


# Streamlit이 제공하던 경로를 그대로 쓴다. 배포 워크플로와 EC2 배포 스크립트가
# 이 주소로 health check를 하고 응답 본문을 로그에 찍는다 -- JSON이 아니라
# 평문 "ok"여야 한다.
@app.get("/_stcore/health", response_class=PlainTextResponse)
def health() -> str:
    return "ok"


@app.get("/api/bootstrap")
def bootstrap(service: ServiceDep) -> dict[str, Any]:
    """React가 처음 뜰 때 한 번만 부른다: borough outline과 selector용 정보."""
    payload = service.bootstrap()
    return {
        "total_segment_count": payload.total_segment_count,
        "boroughs": [
            {
                "name": borough.name,
                "center": list(borough.center),
                "bounds": list(borough.bounds),
                "segment_count": borough.segment_count,
                "geometry": borough.geometry,
            }
            for borough in payload.boroughs
        ],
    }


@app.get("/api/segments")
def segments(service: ServiceDep, request: Request, borough: str | None = None) -> Response:
    """borough 안의 segment 전부를 score까지 붙여 GeoJSON으로 돌려준다.

    서비스가 이미 gzip으로 눌러 캐시해둔 바이트를 그대로 흘려보낸다. 요청마다
    다시 압축하면 borough 하나에 수백 ms가 든다 -- GZipMiddleware는 응답에
    Content-Encoding이 이미 붙어 있으면 건드리지 않는다.
    """
    payload = service.get_segments(borough)
    if "gzip" in request.headers.get("accept-encoding", ""):
        return Response(
            content=payload.body,
            media_type="application/json",
            headers={"Content-Encoding": "gzip"},
        )
    return Response(content=gzip.decompress(payload.body), media_type="application/json")


def resolve_static_dir() -> Path | None:
    """React 빌드 산출물 위치. 아직 빌드하지 않았으면 None(API만 제공)."""
    configured = os.environ.get(STATIC_DIR_ENV)
    # 소스 트리에서 개발할 때의 기본값: services/dashboard/frontend/dist
    path = Path(configured) if configured else Path(__file__).parents[2] / "frontend" / "dist"
    return path if (path / "index.html").is_file() else None


def mount_static(application: FastAPI) -> None:
    """정적 파일은 마지막에 붙인다 -- "/"에 mount하므로 위의 라우트가 먼저 잡혀야 한다."""
    static_dir = resolve_static_dir()
    if static_dir is not None:
        application.mount("/", StaticFiles(directory=static_dir, html=True), name="static")


mount_static(app)
