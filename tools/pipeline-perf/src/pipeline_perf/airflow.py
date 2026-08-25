"""Airflow REST API v2 client for the L1 collection layer (#462).

Airflow가 아는 시간(task별 start/end/try, task 사이 gap, Asset 트리거 시각,
`report_processing_counts`가 남긴 XCom 행 수)을 읽는다. HTTP 세션을 주입 가능하게
두어 테스트에서 fake로 갈아끼운다.
"""

from __future__ import annotations

import itertools
import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

import requests

_DEFAULT_TIMEOUT_SECONDS = 30


class HttpSession(Protocol):
    def request(self, method: str, url: str, **kwargs: Any) -> Any: ...


@dataclass(frozen=True, slots=True)
class AirflowCredentials:
    base_url: str
    username: str | None = None
    password: str | None = None
    token: str | None = None

    @classmethod
    def from_env(cls, base_url: str | None = None) -> AirflowCredentials:
        resolved = base_url or os.environ.get("AIRFLOW_API_BASE_URL", "")
        if not resolved:
            raise ValueError(
                "Airflow base URL이 필요하다. --airflow-base-url 또는 "
                "AIRFLOW_API_BASE_URL 환경변수를 지정한다."
            )
        return cls(
            base_url=resolved.rstrip("/"),
            username=os.environ.get("AIRFLOW_API_USERNAME") or None,
            password=os.environ.get("AIRFLOW_API_PASSWORD") or None,
            token=os.environ.get("AIRFLOW_API_TOKEN") or None,
        )


class AirflowClient:
    """Airflow 3의 `/api/v2` 엔드포인트를 감싼 얇은 클라이언트."""

    def __init__(
        self,
        credentials: AirflowCredentials,
        session: HttpSession | None = None,
        timeout: int = _DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        self._credentials = credentials
        self._session = session or requests.Session()
        self._timeout = timeout
        self._token = credentials.token

    # --- 저수준 --------------------------------------------------------

    def _authorization(self) -> str:
        if self._token is None:
            if not (self._credentials.username and self._credentials.password):
                raise ValueError(
                    "Airflow 인증 정보가 없다. AIRFLOW_API_TOKEN 또는 "
                    "AIRFLOW_API_USERNAME/AIRFLOW_API_PASSWORD를 지정한다."
                )
            response = self._session.request(
                "POST",
                f"{self._credentials.base_url}/auth/token",
                json={
                    "username": self._credentials.username,
                    "password": self._credentials.password,
                },
                timeout=self._timeout,
            )
            response.raise_for_status()
            self._token = response.json()["access_token"]
        return f"Bearer {self._token}"

    def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        accept: str | None = None,
    ) -> dict[str, Any]:
        headers = {"Authorization": self._authorization()}
        if accept is not None:
            headers["Accept"] = accept
        response = self._session.request(
            "GET",
            f"{self._credentials.base_url}/api/v2{path}",
            params=params or {},
            headers=headers,
            timeout=self._timeout,
        )
        response.raise_for_status()
        return response.json()

    # --- 도메인 --------------------------------------------------------

    def dag_runs(
        self,
        dag_id: str,
        limit: int,
        since: str | None = None,
        until: str | None = None,
        state: str | None = None,
    ) -> list[dict[str, Any]]:
        """최신순 DAG run 목록. 시간 구간은 `run_after`를 기준으로 자른다.

        `run_after`가 `dag_run_id`에 박히는 시각이라 사람이 "9시 실행"이라고 부르는
        것과 일치한다. `logical_date`는 data interval의 시작이라 09:00 실행이
        08:00으로 잡혀 한 시간 어긋나고, `start_date`는 스케줄러 지연만큼 밀린다.
        """
        params: dict[str, Any] = {"limit": limit, "order_by": "-run_after"}
        if since:
            params["run_after_gte"] = since
        if until:
            params["run_after_lte"] = until
        if state:
            params["state"] = state
        payload = self.get(f"/dags/{dag_id}/dagRuns", params)
        return payload.get("dag_runs", [])

    def dag_run(self, dag_id: str, dag_run_id: str) -> dict[str, Any] | None:
        """DAG run 하나를 지목해 읽는다. 없는 실행은 예외 대신 None이다.

        오타나 이미 정리된 실행 하나 때문에 나머지 수집을 통째로 버릴 이유가 없다.
        호출자가 None을 받아 `notes`에 남긴다.
        """
        try:
            return self.get(f"/dags/{dag_id}/dagRuns/{dag_run_id}")
        except requests.HTTPError as error:
            if error.response is not None and error.response.status_code == 404:
                return None
            raise

    def task_instances(self, dag_id: str, dag_run_id: str) -> list[dict[str, Any]]:
        payload = self.get(f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances", {"limit": 500})
        return payload.get("task_instances", [])

    def xcom_value(self, dag_id: str, dag_run_id: str, task_id: str, key: str) -> Any:
        """XCom 하나를 읽는다. 만료·미존재는 예외 대신 None으로 돌려준다.

        XCom은 보존 기간이 지나면 사라지는데, 그 한 건 때문에 나머지 수집을 버릴
        이유가 없다.
        """
        try:
            payload = self.get(
                f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{key}"
            )
        except requests.HTTPError as error:
            if error.response is not None and error.response.status_code == 404:
                return None
            raise
        value = payload.get("value")
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        return value

    def task_log(self, dag_id: str, dag_run_id: str, task_id: str, try_number: int) -> str:
        """task 시도 하나의 로그 본문을 한 덩어리 텍스트로 돌려준다.

        Spark를 쓰지 않는 task(`current_score_pipeline`의 `run_current_score`)는
        PERF 로그가 S3 driver 로그가 아니라 여기에만 남는다. 로그가 만료·미존재면
        예외 대신 빈 문자열이다.
        """
        try:
            payload = self.get(
                f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}",
                accept="application/json",
            )
        except requests.HTTPError as error:
            if error.response is not None and error.response.status_code == 404:
                return ""
            raise
        return flatten_log_content(payload.get("content"))

    def asset_events(self, source_dag_id: str, limit: int = 100) -> list[dict[str, Any]]:
        """`source_dag_id`가 발행한 Asset 이벤트와 그 이벤트가 만든 DAG run 목록."""
        payload = self.get(
            "/assets/events",
            {"source_dag_id": source_dag_id, "limit": limit, "order_by": "-timestamp"},
        )
        return payload.get("asset_events", [])

    def variable(self, key: str) -> str | None:
        try:
            payload = self.get(f"/variables/{key}")
        except requests.HTTPError as error:
            if error.response is not None and error.response.status_code == 404:
                return None
            raise
        return payload.get("value")


def flatten_log_content(content: Any) -> str:
    """로그 API가 돌려주는 구조화 본문을 줄 단위 텍스트로 편다.

    Airflow 3는 `content`를 `{"event": "<한 줄>"}` 목록으로 준다. 구버전이나
    `text/plain` 응답처럼 문자열로 오는 경우도 그대로 받는다.
    """
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    lines = []
    for entry in content:
        if isinstance(entry, dict):
            lines.append(str(entry.get("event", "")))
        else:
            lines.append(str(entry))
    return "\n".join(lines)


def parse_timestamp(value: str | None) -> datetime | None:
    """Airflow가 돌려주는 ISO-8601 문자열을 datetime으로 바꾼다."""
    if not value:
        return None
    return datetime.fromisoformat(value)


def seconds_between(start: str | None, end: str | None) -> float | None:
    started = parse_timestamp(start)
    ended = parse_timestamp(end)
    if started is None or ended is None:
        return None
    return round((ended - started).total_seconds(), 3)


def task_gaps(task_instances: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """실행 순서대로 늘어놓은 task 사이의 빈 시간.

    Airflow의 스케줄링 간격이 실제 계산 시간과 얼마나 경쟁하는지를 보려는
    지표다. 앞 task가 끝나고 다음 task가 시작될 때까지의 간격을 잰다.
    """
    ordered = [
        instance
        for instance in sorted(
            task_instances, key=lambda item: (item.get("start_date") or "", item.get("task_id", ""))
        )
        if instance.get("start_date") and instance.get("end_date")
    ]
    gaps = []
    for previous, current in itertools.pairwise(ordered):
        gap = seconds_between(previous["end_date"], current["start_date"])
        if gap is None:
            continue
        gaps.append(
            {
                "from_task_id": previous["task_id"],
                "to_task_id": current["task_id"],
                "seconds": gap,
            }
        )
    return gaps
