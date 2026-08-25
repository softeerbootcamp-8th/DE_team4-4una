"""Injectable fakes shared by the collector tests."""

from __future__ import annotations

from typing import Any

import requests


class FakeResponse:
    def __init__(self, payload: Any = None, status_code: int = 200):
        self._payload = payload if payload is not None else {}
        self.status_code = status_code

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)


class FakeSession:
    """`(method, path)` -> 응답 매핑. 실제로 온 요청은 `calls`에 남는다."""

    def __init__(self, routes: dict[tuple[str, str], FakeResponse]):
        self._routes = routes
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append((method, url, kwargs))
        for (route_method, suffix), response in self._routes.items():
            if route_method == method and url.endswith(suffix):
                return response
        return FakeResponse({}, status_code=404)


class FakeEmrClient:
    def __init__(
        self,
        job_runs: dict[str, dict[str, Any]],
        listed: list[dict[str, Any]] | None = None,
    ):
        self._job_runs = job_runs
        self._listed = listed or []

    def get_job_run(self, applicationId: str, jobRunId: str) -> dict[str, Any]:
        return {"jobRun": self._job_runs[jobRunId]}

    def list_job_runs(self, **kwargs: Any) -> dict[str, Any]:
        return {"jobRuns": self._listed}


class FakeAirflowClient:
    """`AirflowClient`와 같은 표면만 가진 대역."""

    def __init__(
        self,
        dag_runs: dict[str, list[dict[str, Any]]],
        task_instances: dict[str, list[dict[str, Any]]],
        xcoms: dict[tuple[str, str], Any] | None = None,
        variables: dict[str, str] | None = None,
        asset_events: dict[str, list[dict[str, Any]]] | None = None,
        task_logs: dict[str, str] | None = None,
    ):
        self._dag_runs = dag_runs
        self._task_instances = task_instances
        self._xcoms = xcoms or {}
        self._variables = variables or {}
        self._asset_events = asset_events or {}
        self._task_logs = task_logs or {}
        self.dag_runs_calls: list[dict[str, Any]] = []

    def dag_runs(
        self,
        dag_id: str,
        limit: int,
        since: str | None = None,
        until: str | None = None,
        state: str | None = None,
    ) -> list[dict[str, Any]]:
        self.dag_runs_calls.append(
            {"dag_id": dag_id, "limit": limit, "since": since, "until": until, "state": state}
        )
        return self._dag_runs.get(dag_id, [])[:limit]

    def dag_run(self, dag_id: str, dag_run_id: str) -> dict[str, Any] | None:
        for dag_run in self._dag_runs.get(dag_id, []):
            if dag_run["dag_run_id"] == dag_run_id:
                return dag_run
        return None

    def task_instances(self, dag_id: str, dag_run_id: str) -> list[dict[str, Any]]:
        return self._task_instances.get(dag_run_id, [])

    def xcom_value(self, dag_id: str, dag_run_id: str, task_id: str, key: str) -> Any:
        return self._xcoms.get((dag_run_id, task_id))

    def task_log(self, dag_id: str, dag_run_id: str, task_id: str, try_number: int) -> str:
        return self._task_logs.get(task_id, "")

    def asset_events(self, source_dag_id: str, limit: int = 100) -> list[dict[str, Any]]:
        return self._asset_events.get(source_dag_id, [])

    def variable(self, key: str) -> str | None:
        return self._variables.get(key)
