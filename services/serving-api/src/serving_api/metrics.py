"""HTTP request Prometheus metrics for the serving API (#308).

`create_app()`은 middleware만 붙이고 아무 소켓도 열지 않는다 — metrics HTTP
server(9101)를 시작하는 것은 `serving_api.main()`의 책임이다. 그래야 테스트가
`create_app()`을 몇 번을 호출해도 실제 포트 바인딩 충돌이 생기지 않는다.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable

from fastapi import FastAPI, Request, Response
from prometheus_client import CollectorRegistry, Counter, Histogram

# 매칭되는 route가 없을 때(예: 존재하지 않는 경로로 온 요청) 쓰는 고정 label.
# raw request path를 그대로 label에 쓰면 요청마다 새 시계열이 생겨
# 카디널리티가 무한히 늘어난다.
UNMATCHED_ROUTE = "unmatched"

REQUEST_COUNT_METRIC_NAME = "serving_api_http_requests_total"
REQUEST_DURATION_METRIC_NAME = "serving_api_http_request_duration_seconds"


class RequestMetrics:
    """HTTP 요청 계측용 Counter/Histogram 묶음.

    `registry`를 지정하지 않으면 인스턴스마다 새 `CollectorRegistry`를 쓴다 —
    global 기본 registry에 등록하면 `create_app()`을 여러 번 호출하는 테스트가
    "Duplicated timeseries" 오류로 깨진다. 실제 서비스에서는 `main()`이 이
    registry를 `start_http_server`에 그대로 넘겨 노출한다.
    """

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry if registry is not None else CollectorRegistry()
        self.request_count = Counter(
            REQUEST_COUNT_METRIC_NAME,
            "Total HTTP requests handled by the serving API",
            labelnames=("method", "route", "status"),
            registry=self.registry,
        )
        self.request_duration = Histogram(
            REQUEST_DURATION_METRIC_NAME,
            "HTTP request duration in seconds",
            labelnames=("method", "route"),
            registry=self.registry,
        )

    def observe(
        self, method: str, route: str, status: int, duration_seconds: float
    ) -> None:
        self.request_count.labels(method=method, route=route, status=str(status)).inc()
        self.request_duration.labels(method=method, route=route).observe(
            duration_seconds
        )


def install_request_metrics(app: FastAPI, metrics: RequestMetrics) -> None:
    """`app`에 HTTP 요청 계측 middleware를 붙인다.

    등록된 exception handler(404/422/503 등)가 처리한 응답은 정상적인
    `Response`로 여기까지 돌아오므로 `try` 블록 밖에서 상태 코드를 그대로
    기록한다. handler가 없는 예외만 `except`로 떨어지며, 그 경우 5xx로 기록한
    뒤 다시 던져 Starlette의 기본 500 처리로 넘긴다.
    """

    @app.middleware("http")
    async def _record_request_metrics(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        start = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            metrics.observe(
                request.method, _route_label(request), 500, time.perf_counter() - start
            )
            raise
        metrics.observe(
            request.method,
            _route_label(request),
            response.status_code,
            time.perf_counter() - start,
        )
        return response


def _route_label(request: Request) -> str:
    """route template(`/segments/{segment_id}`)을 label로 쓴다. 매칭 안 되면 고정값."""
    route = request.scope.get("route")
    if route is None:
        return UNMATCHED_ROUTE
    return route.path
