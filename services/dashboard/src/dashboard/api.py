"""FastAPI application backing the React dashboard.

브라우저는 여기에만 붙는다. S3와 Serving API 접근은 전부 서버에서 일어나고,
프론트엔드는 GeoJSON만 받는다.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Any

import httpx
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import PlainTextResponse
from fastapi.staticfiles import StaticFiles

from dashboard.config import DashboardConfig
from dashboard.dashboard_service import DashboardService, UnknownBoroughError

# 배포 시 React 빌드 산출물이 놓이는 경로. Docker 이미지에서는 패키지가
# site-packages에 설치되므로 소스 트리 기준 경로로는 찾을 수 없다.
STATIC_DIR_ENV = "DASHBOARD_STATIC_DIR"

# outline만 353KB라 압축이 확실히 이득이다. 이보다 작은 응답은 건드리지 않는다.
_GZIP_MINIMUM_SIZE = 1024


@lru_cache(maxsize=1)
def get_service() -> DashboardService:
    """프로세스에 하나만 둔다 -- 스냅샷과 STRtree를 여기에 들고 있다."""
    return DashboardService(DashboardConfig.from_env())


ServiceDep = Annotated[DashboardService, Depends(get_service)]

app = FastAPI(title="NYC Road Comfort Dashboard", docs_url=None, redoc_url=None)
app.add_middleware(GZipMiddleware, minimum_size=_GZIP_MINIMUM_SIZE)


@app.exception_handler(UnknownBoroughError)
async def _unknown_borough(_request: Request, exc: UnknownBoroughError) -> PlainTextResponse:
    return PlainTextResponse(f"unknown borough: {exc}", status_code=404)


@app.exception_handler(BotoCoreError)
@app.exception_handler(ClientError)
async def _s3_unavailable(_request: Request, exc: Exception) -> PlainTextResponse:
    return PlainTextResponse(f"unable to load reference data from S3: {exc}", status_code=502)


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
        "max_rendered_segments": payload.max_rendered_segments,
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
def segments(
    service: ServiceDep,
    south: Annotated[float, Query(ge=-90, le=90)],
    west: Annotated[float, Query(ge=-180, le=180)],
    north: Annotated[float, Query(ge=-90, le=90)],
    east: Annotated[float, Query(ge=-180, le=180)],
    borough: str | None = None,
) -> dict[str, Any]:
    """뷰포트와 겹치는 segment를 score까지 붙여 GeoJSON으로 돌려준다."""
    if south >= north:
        raise HTTPException(status_code=400, detail="south must be less than north")
    if west >= east:
        raise HTTPException(status_code=400, detail="west must be less than east")

    result = service.get_segments_in_viewport(borough, south, west, north, east)
    return {
        "in_viewport_count": result.in_viewport_count,
        "rendered_count": result.rendered_count,
        "truncated": result.truncated,
        "requested_vehicle_profile_id": result.requested_vehicle_profile_id,
        "effective_vehicle_profile_id": result.effective_vehicle_profile_id,
        "vehicle_profile_fallback": result.vehicle_profile_fallback,
        "features": result.features,
    }


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
