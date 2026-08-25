# Grafana가 firing이라 보내도 조치 전 Prometheus로 다시 확인한다. PromQL은 spark-streaming.json의 Component Status 패널과 완전히 동일해야 한다(#437).

from __future__ import annotations

from dataclasses import dataclass

import requests

# spark-streaming.json의 "Component Status" 패널 target.expr과 문자 그대로 동일해야 한다.
STREAM_PROCESSOR_STATUS_QUERY = (
    '(4 * (up{job="stream-processor"} == bool 0) > 0) '
    'or (3 * (stream_processor_query_running{job="stream-processor"} == bool 0) > 0) '
    'or (2 * (((time() - stream_processor_last_progress_timestamp_seconds{job="stream-processor"}) '
    'and (stream_processor_last_progress_timestamp_seconds{job="stream-processor"} > 0)) > bool 330) > 0) '
    'or (1 * (stream_processor_event_time_lag_seconds{job="stream-processor"} > bool 330) > 0) '
    'or (0 * up{job="stream-processor"})'
)

# 위 query가 만드는 숫자 코드 -> 이름. 같은 dashboard 패널의 value mapping과 동일.
STREAM_PROCESSOR_STATUS_LABELS: dict[int, str] = {
    0: "RUNNING",
    1: "EVENT DATA STALE",
    2: "PROGRESS STALE",
    3: "QUERY STOPPED",
    4: "TARGET DOWN",
}

# RUNNING(0)만 정상이다. 그 외는 remediation을 검토할 후보다.
HEALTHY_STATUS_CODE = 0


class PrometheusQueryError(RuntimeError):
    """Prometheus가 non-2xx를 주거나 응답 형식이 예상과 다를 때."""


@dataclass(frozen=True, slots=True)
class StreamProcessorStatus:
    code: int | None
    label: str
    instance: str | None

    @property
    def is_healthy(self) -> bool:
        return self.code == HEALTHY_STATUS_CODE


class PrometheusClient:
    """Prometheus HTTP API(`/api/v1/query`)의 아주 얇은 wrapper."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout_seconds: float = 10.0,
        session: requests.Session | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds
        self._session = session or requests.Session()

    def instant_query(self, expr: str) -> list[dict]:
        response = self._session.get(
            f"{self._base_url}/api/v1/query",
            params={"query": expr},
            timeout=self._timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("status") != "success":
            raise PrometheusQueryError(f"Prometheus query did not succeed: {payload}")
        return payload["data"]["result"]

    def stream_processor_status(self) -> StreamProcessorStatus:
        """현재 stream-processor 상태 — instance가 여러 개면 가장 심각한 것을 고른다."""
        results = self.instant_query(STREAM_PROCESSOR_STATUS_QUERY)
        if not results:
            # metric 자체가 없으면 상태를 확정할 수 없으므로 "정상"으로 오판하지 않는다.
            return StreamProcessorStatus(code=None, label="NO DATA", instance=None)

        worst = max(results, key=lambda result: float(result["value"][1]))
        code = int(float(worst["value"][1]))
        return StreamProcessorStatus(
            code=code,
            label=STREAM_PROCESSOR_STATUS_LABELS.get(code, "UNKNOWN"),
            instance=(worst.get("metric") or {}).get("instance"),
        )
