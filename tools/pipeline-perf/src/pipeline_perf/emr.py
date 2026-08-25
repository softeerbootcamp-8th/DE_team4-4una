"""EMR Serverless Job Run facts for the L2 collection layer (#462).

Spark event log는 Spark 애플리케이션이 뜬 뒤부터를 담는다. 그 앞의 프로비저닝 대기
(`createdAt` -> `startedAt`)와 뒤의 정리 시간, 그리고 과금 기준이 되는
`billedResourceUtilization`은 EMR Serverless API에만 있다.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Protocol

_MILLISECONDS = 1000


class EmrServerlessClient(Protocol):
    def get_job_run(self, **kwargs: Any) -> dict[str, Any]: ...

    def list_job_runs(self, **kwargs: Any) -> dict[str, Any]: ...


def describe_job_run(
    client: EmrServerlessClient, application_id: str, job_run_id: str
) -> dict[str, Any]:
    """Job Run 하나의 시간·상태·과금 지표를 평평한 dict로 만든다.

    API 응답 필드는 릴리스마다 늘어나므로 없는 값은 None으로 흘려보낸다. 특히
    `startedAt`/`endedAt`은 오래된 API 버전에 없어, 그 경우 프로비저닝 대기는
    구할 수 없고 `total_execution_duration_s`만 남는다.
    """
    job_run = client.get_job_run(applicationId=application_id, jobRunId=job_run_id).get(
        "jobRun", {}
    )
    created_at = job_run.get("createdAt")
    started_at = job_run.get("startedAt")
    ended_at = job_run.get("endedAt")
    billed = job_run.get("billedResourceUtilization") or {}
    total = job_run.get("totalResourceUtilization") or {}
    queued_ms = job_run.get("queuedDurationMilliseconds")
    return {
        "job_run_id": job_run.get("jobRunId", job_run_id),
        "application_id": job_run.get("applicationId", application_id),
        "name": job_run.get("name"),
        "state": job_run.get("state"),
        "state_details": job_run.get("stateDetails"),
        "release_label": job_run.get("releaseLabel"),
        "created_at": _isoformat(created_at),
        "started_at": _isoformat(started_at),
        "ended_at": _isoformat(ended_at),
        "queued_duration_s": round(queued_ms / _MILLISECONDS, 3) if queued_ms else None,
        "provisioning_wait_s": _seconds_between(created_at, started_at),
        "run_duration_s": _seconds_between(started_at, ended_at),
        "total_execution_duration_s": job_run.get("totalExecutionDurationSeconds"),
        "billed_vcpu_hour": billed.get("vCPUHour"),
        "billed_memory_gb_hour": billed.get("memoryGBHour"),
        "billed_storage_gb_hour": billed.get("storageGBHour"),
        "total_vcpu_hour": total.get("vCPUHour"),
        "total_memory_gb_hour": total.get("memoryGBHour"),
        "total_storage_gb_hour": total.get("storageGBHour"),
    }


def find_job_run_id_by_name(
    client: EmrServerlessClient,
    application_id: str,
    name: str,
    task_started_at: datetime,
    window: timedelta = timedelta(minutes=10),
) -> str | None:
    """XCom이 만료됐을 때 쓰는 fallback 매칭.

    `submit_batch_jobs_command`가 Job Run `name`을 task_id로 채우므로(#292),
    task 시작 시각 주변에서 같은 이름의 Job Run을 찾는다. 같은 이름이 여러 건이면
    task 시작 시각에 가장 가까운 것을 고른다 — DAG가 시간당 한 번 도는 한 후보는
    사실상 하나다.
    """
    response = client.list_job_runs(
        applicationId=application_id,
        createdAtAfter=task_started_at - window,
        createdAtBefore=task_started_at + window,
        maxResults=50,
    )
    candidates = [
        job_run
        for job_run in response.get("jobRuns", [])
        if job_run.get("name") == name and job_run.get("createdAt") is not None
    ]
    if not candidates:
        return None
    closest = min(candidates, key=lambda run: abs(run["createdAt"] - task_started_at))
    return closest.get("id")


def job_log_prefix(log_uri: str, application_id: str, job_run_id: str) -> str:
    """EMR Serverless가 S3에 남기는 Job Run 로그 루트."""
    return f"{log_uri.rstrip('/')}/applications/{application_id}/jobs/{job_run_id}"


def _isoformat(value: datetime | None) -> str | None:
    return value.isoformat() if isinstance(value, datetime) else None


def _seconds_between(start: datetime | None, end: datetime | None) -> float | None:
    if not isinstance(start, datetime) or not isinstance(end, datetime):
        return None
    return round((end - start).total_seconds(), 3)
