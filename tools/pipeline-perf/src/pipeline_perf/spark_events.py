"""Streaming aggregation of one Job Run's Spark event log (#462 L3).

`run_sensor_processing` 한 건의 rolling event log가 19파일 237MB라, 전량을 메모리에
올리는 파서는 쓸 수 없다. 여기서는 `events_<N>_<app-id>` 파일을 N 순서로 이어 읽으며
한 줄씩 JSON으로 풀고, 필요한 `Event` 타입만 누적기에 반영한 뒤 즉시 버린다. 개별
task 레코드는 남기지 않고 스테이지별 누적합과 분위수용 duration 표본
(`DurationSummary`)만 유지한다.

필드 가용성은 EMR 8.0.0 / Spark 4.0.2-amzn-0의 실제 event log(#462 조사 기록)로
확인했다. 그래도 파서는 없는 필드를 0이나 None으로 흘려보낸다 — 릴리스가 바뀌어
필드가 빠져도 수집 전체가 죽지 않고 해당 지표만 비게 하기 위해서다.
"""

from __future__ import annotations

import gzip
import io
import json
import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from typing import Any, BinaryIO, Protocol

from pipeline_perf.quantiles import DurationSummary, skew_ratio

# S3 range 리더를 8KB 기본 버퍼로 줄 단위 순회하면 16MB 파일 하나에 GET이 2천 번
# 넘게 나간다. 4MB로 묶어 파일당 4~5회로 줄인다.
_READ_BUFFER_BYTES = 4 * 1024 * 1024

# rolling event log v2가 남기는 파일명: events_<index>_<application-id>
_EVENT_FILE_PATTERN = re.compile(r"/events_(\d+)_")

_SQL_START = "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart"
_SQL_END = "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd"

# 동시 태스크 수를 재는 시간 버킷. 태스크를 보관하지 않고 버킷에 task-second를
# 누적하므로 메모리가 실행 길이에만 비례한다.
_CONCURRENCY_BUCKET_MS = 10_000


class Reader(Protocol):
    def open_reader(self, uri: str) -> BinaryIO: ...

    def list_objects(self, uri: str) -> list[Any]: ...


@dataclass
class StageAccumulator:
    """스테이지 시도(stage attempt) 하나의 누적 통계."""

    stage_id: int
    attempt_id: int
    name: str = ""
    num_tasks: int = 0
    parent_ids: list[int] = field(default_factory=list)
    submission_time_ms: int | None = None
    completion_time_ms: int | None = None
    failure_reason: str | None = None
    task_count: int = 0
    failed_task_count: int = 0
    durations: DurationSummary = field(default_factory=DurationSummary)
    executor_run_time_ms: int = 0
    executor_cpu_time_ms: int = 0
    jvm_gc_time_ms: int = 0
    memory_bytes_spilled: int = 0
    disk_bytes_spilled: int = 0
    peak_execution_memory: int = 0
    shuffle_read_bytes: int = 0
    shuffle_read_records: int = 0
    shuffle_fetch_wait_ms: int = 0
    shuffle_write_bytes: int = 0
    shuffle_write_time_ms: int = 0
    input_bytes: int = 0
    input_records: int = 0
    output_bytes: int = 0
    output_records: int = 0

    def wall_time_ms(self) -> int | None:
        if self.submission_time_ms is None or self.completion_time_ms is None:
            return None
        return self.completion_time_ms - self.submission_time_ms

    def as_dict(self) -> dict[str, Any]:
        durations = self.durations.summary()
        return {
            "stage_id": self.stage_id,
            "attempt_id": self.attempt_id,
            "name": self.name,
            "num_tasks": self.num_tasks,
            "parent_ids": self.parent_ids,
            "submission_time_ms": self.submission_time_ms,
            "completion_time_ms": self.completion_time_ms,
            "wall_time_ms": self.wall_time_ms(),
            "failure_reason": self.failure_reason,
            "task_count": self.task_count,
            "failed_task_count": self.failed_task_count,
            "task_duration": durations,
            "skew_ratio": skew_ratio(durations["max_ms"], durations["p50_ms"]),
            "executor_run_time_ms": self.executor_run_time_ms,
            "executor_cpu_time_ms": self.executor_cpu_time_ms,
            "jvm_gc_time_ms": self.jvm_gc_time_ms,
            "gc_ratio": _ratio(self.jvm_gc_time_ms, self.executor_run_time_ms),
            "memory_bytes_spilled": self.memory_bytes_spilled,
            "disk_bytes_spilled": self.disk_bytes_spilled,
            "peak_execution_memory": self.peak_execution_memory,
            "shuffle_read_bytes": self.shuffle_read_bytes,
            "shuffle_read_records": self.shuffle_read_records,
            "shuffle_fetch_wait_ms": self.shuffle_fetch_wait_ms,
            "shuffle_write_bytes": self.shuffle_write_bytes,
            "shuffle_write_time_ms": self.shuffle_write_time_ms,
            "input_bytes": self.input_bytes,
            "input_records": self.input_records,
            "output_bytes": self.output_bytes,
            "output_records": self.output_records,
        }


@dataclass
class JobAccumulator:
    job_id: int
    submission_time_ms: int | None = None
    completion_time_ms: int | None = None
    stage_ids: list[int] = field(default_factory=list)
    result: str | None = None
    sql_execution_id: int | None = None

    def as_dict(self) -> dict[str, Any]:
        duration = None
        if self.submission_time_ms is not None and self.completion_time_ms is not None:
            duration = self.completion_time_ms - self.submission_time_ms
        return {
            "job_id": self.job_id,
            "submission_time_ms": self.submission_time_ms,
            "completion_time_ms": self.completion_time_ms,
            "duration_ms": duration,
            "stage_ids": self.stage_ids,
            "result": self.result,
            "sql_execution_id": self.sql_execution_id,
        }


@dataclass
class SqlExecutionAccumulator:
    execution_id: int
    description: str = ""
    details: str = ""
    start_time_ms: int | None = None
    end_time_ms: int | None = None
    error_message: str | None = None

    def duration_ms(self) -> int | None:
        if self.start_time_ms is None or self.end_time_ms is None:
            return None
        return self.end_time_ms - self.start_time_ms

    def as_dict(self) -> dict[str, Any]:
        return {
            "execution_id": self.execution_id,
            "description": self.description,
            "details_head": self.details.splitlines()[0] if self.details else "",
            "start_time_ms": self.start_time_ms,
            "end_time_ms": self.end_time_ms,
            "duration_ms": self.duration_ms(),
            "error_message": self.error_message,
        }


class EventLogAggregator:
    """이벤트를 한 건씩 받아 누적한다. 파일 입출력과 분리해 두어 테스트가 쉽다."""

    def __init__(self) -> None:
        self.app_id: str | None = None
        self.app_name: str | None = None
        self.spark_version: str | None = None
        self.app_start_ms: int | None = None
        self.app_end_ms: int | None = None
        self.event_counts: dict[str, int] = {}
        self.stages: dict[tuple[int, int], StageAccumulator] = {}
        self.jobs: dict[int, JobAccumulator] = {}
        self.sql_executions: dict[int, SqlExecutionAccumulator] = {}
        self.missing_metrics: set[str] = set()
        self._executor_cores: dict[str, int] = {}
        self._slot_deltas: list[tuple[int, int]] = []
        self._task_seconds_by_bucket: dict[int, float] = {}

    def add(self, event: dict[str, Any]) -> None:
        name = event.get("Event")
        if not isinstance(name, str):
            return
        self.event_counts[name] = self.event_counts.get(name, 0) + 1
        handler = _HANDLERS.get(name)
        if handler is not None:
            handler(self, event)

    # --- 이벤트 핸들러 ---------------------------------------------------

    def _on_log_start(self, event: dict[str, Any]) -> None:
        self.spark_version = event.get("Spark Version")

    def _on_application_start(self, event: dict[str, Any]) -> None:
        self.app_id = event.get("App ID")
        self.app_name = event.get("App Name")
        self.app_start_ms = event.get("Timestamp")

    def _on_application_end(self, event: dict[str, Any]) -> None:
        self.app_end_ms = event.get("Timestamp")

    def _on_executor_added(self, event: dict[str, Any]) -> None:
        executor_id = str(event.get("Executor ID"))
        cores = int((event.get("Executor Info") or {}).get("Total Cores") or 0)
        self._executor_cores[executor_id] = cores
        timestamp = event.get("Timestamp")
        if timestamp is not None and cores:
            self._slot_deltas.append((int(timestamp), cores))

    def _on_executor_removed(self, event: dict[str, Any]) -> None:
        executor_id = str(event.get("Executor ID"))
        cores = self._executor_cores.pop(executor_id, 0)
        timestamp = event.get("Timestamp")
        if timestamp is not None and cores:
            self._slot_deltas.append((int(timestamp), -cores))

    def _on_job_start(self, event: dict[str, Any]) -> None:
        job_id = event.get("Job ID")
        if job_id is None:
            return
        job = self.jobs.setdefault(int(job_id), JobAccumulator(job_id=int(job_id)))
        job.submission_time_ms = event.get("Submission Time")
        job.stage_ids = [int(stage_id) for stage_id in event.get("Stage IDs", [])]

    def _on_job_end(self, event: dict[str, Any]) -> None:
        job_id = event.get("Job ID")
        if job_id is None:
            return
        job = self.jobs.setdefault(int(job_id), JobAccumulator(job_id=int(job_id)))
        job.completion_time_ms = event.get("Completion Time")
        job.result = (event.get("Job Result") or {}).get("Result")

    def _on_stage_submitted(self, event: dict[str, Any]) -> None:
        self._stage_from_info(event.get("Stage Info") or {})

    def _on_stage_completed(self, event: dict[str, Any]) -> None:
        info = event.get("Stage Info") or {}
        stage = self._stage_from_info(info)
        if stage is None:
            return
        stage.completion_time_ms = info.get("Completion Time")
        stage.failure_reason = info.get("Failure Reason")

    def _on_task_end(self, event: dict[str, Any]) -> None:
        stage_id = event.get("Stage ID")
        if stage_id is None:
            return
        key = (int(stage_id), int(event.get("Stage Attempt ID") or 0))
        stage = self.stages.get(key)
        if stage is None:
            stage = StageAccumulator(stage_id=key[0], attempt_id=key[1])
            self.stages[key] = stage
        info = event.get("Task Info") or {}
        stage.task_count += 1
        if info.get("Failed"):
            stage.failed_task_count += 1
        launch = info.get("Launch Time")
        finish = info.get("Finish Time")
        if launch and finish and finish >= launch:
            stage.durations.add(int(finish) - int(launch))
            self._add_task_occupancy(int(launch), int(finish))
        metrics = event.get("Task Metrics")
        if not metrics:
            # Task Metrics는 실패·투기 실행 종료에서 비어 있을 수 있다. 지표가 아예
            # 없는 릴리스와 구분하려고 어떤 필드가 없었는지 기록해 둔다.
            self.missing_metrics.add("Task Metrics")
            return
        stage.executor_run_time_ms += _as_int(metrics, "Executor Run Time", self)
        stage.executor_cpu_time_ms += _as_int(metrics, "Executor CPU Time", self) // 1_000_000
        stage.jvm_gc_time_ms += _as_int(metrics, "JVM GC Time", self)
        stage.memory_bytes_spilled += _as_int(metrics, "Memory Bytes Spilled", self)
        stage.disk_bytes_spilled += _as_int(metrics, "Disk Bytes Spilled", self)
        stage.peak_execution_memory = max(
            stage.peak_execution_memory, _as_int(metrics, "Peak Execution Memory", self)
        )
        shuffle_read = metrics.get("Shuffle Read Metrics") or {}
        stage.shuffle_read_bytes += int(shuffle_read.get("Remote Bytes Read") or 0) + int(
            shuffle_read.get("Local Bytes Read") or 0
        )
        stage.shuffle_read_records += int(shuffle_read.get("Total Records Read") or 0)
        stage.shuffle_fetch_wait_ms += int(shuffle_read.get("Fetch Wait Time") or 0)
        shuffle_write = metrics.get("Shuffle Write Metrics") or {}
        stage.shuffle_write_bytes += int(shuffle_write.get("Shuffle Bytes Written") or 0)
        # Shuffle Write Time만 나노초 단위다.
        stage.shuffle_write_time_ms += int(shuffle_write.get("Shuffle Write Time") or 0) // 1_000_000
        input_metrics = metrics.get("Input Metrics") or {}
        stage.input_bytes += int(input_metrics.get("Bytes Read") or 0)
        stage.input_records += int(input_metrics.get("Records Read") or 0)
        output_metrics = metrics.get("Output Metrics") or {}
        stage.output_bytes += int(output_metrics.get("Bytes Written") or 0)
        stage.output_records += int(output_metrics.get("Records Written") or 0)

    def _on_sql_start(self, event: dict[str, Any]) -> None:
        execution_id = event.get("executionId")
        if execution_id is None:
            return
        execution = self.sql_executions.setdefault(
            int(execution_id), SqlExecutionAccumulator(execution_id=int(execution_id))
        )
        execution.description = event.get("description") or ""
        execution.details = event.get("details") or ""
        execution.start_time_ms = event.get("time")

    def _on_sql_end(self, event: dict[str, Any]) -> None:
        execution_id = event.get("executionId")
        if execution_id is None:
            return
        execution = self.sql_executions.setdefault(
            int(execution_id), SqlExecutionAccumulator(execution_id=int(execution_id))
        )
        execution.end_time_ms = event.get("time")
        execution.error_message = event.get("errorMessage") or None

    # --- 내부 도우미 -----------------------------------------------------

    def _stage_from_info(self, info: dict[str, Any]) -> StageAccumulator | None:
        stage_id = info.get("Stage ID")
        if stage_id is None:
            return None
        key = (int(stage_id), int(info.get("Stage Attempt ID") or 0))
        stage = self.stages.get(key)
        if stage is None:
            stage = StageAccumulator(stage_id=key[0], attempt_id=key[1])
            self.stages[key] = stage
        stage.name = info.get("Stage Name") or stage.name
        stage.num_tasks = int(info.get("Number of Tasks") or stage.num_tasks)
        stage.parent_ids = [int(parent) for parent in info.get("Parent IDs", [])] or stage.parent_ids
        if info.get("Submission Time"):
            stage.submission_time_ms = int(info["Submission Time"])
        return stage

    def _add_task_occupancy(self, launch_ms: int, finish_ms: int) -> None:
        """태스크가 점유한 시간을 버킷별 task-second로 누적한다.

        태스크를 리스트로 들고 있다가 sweep하지 않는 이유는 메모리다. 이 방식은
        버킷 수(=실행 길이/10초)에만 비례한다.
        """
        first_bucket = launch_ms // _CONCURRENCY_BUCKET_MS
        last_bucket = finish_ms // _CONCURRENCY_BUCKET_MS
        for bucket in range(first_bucket, last_bucket + 1):
            bucket_start = bucket * _CONCURRENCY_BUCKET_MS
            bucket_end = bucket_start + _CONCURRENCY_BUCKET_MS
            overlap = min(finish_ms, bucket_end) - max(launch_ms, bucket_start)
            if overlap > 0:
                self._task_seconds_by_bucket[bucket] = (
                    self._task_seconds_by_bucket.get(bucket, 0.0) + overlap / 1000
                )

    def _map_jobs_to_sql_executions(self) -> None:
        """job의 submission time이 들어가는 SQL execution 구간을 찾아 붙인다.

        `SparkListenerJobStart.Properties`에는 `spark.sql.execution.id`가 없다(#462
        조사에서 실제 event log로 확인). 대신 execution 구간이 서로 겹치지 않는다는
        점을 이용해 시간창으로 매칭한다. execution 시작 전의 스키마 조회 job처럼 어느
        구간에도 안 들어가는 job이 있어, 매칭 실패는 정상으로 취급한다.
        """
        windows = [
            (execution.start_time_ms, execution.end_time_ms, execution.execution_id)
            for execution in self.sql_executions.values()
            if execution.start_time_ms is not None
        ]
        for job in self.jobs.values():
            if job.submission_time_ms is None:
                continue
            for start, end, execution_id in windows:
                if start <= job.submission_time_ms and (end is None or job.submission_time_ms <= end):
                    job.sql_execution_id = execution_id
                    break

    def concurrency_timeline(self) -> list[dict[str, Any]]:
        """10초 버킷별 평균 동시 태스크 수와 가용 코어(슬롯) 수."""
        if not self._task_seconds_by_bucket:
            return []
        slot_deltas = sorted(self._slot_deltas)
        buckets = sorted(self._task_seconds_by_bucket)
        timeline = []
        cursor = 0
        available = 0
        for bucket in range(buckets[0], buckets[-1] + 1):
            bucket_end = (bucket + 1) * _CONCURRENCY_BUCKET_MS
            while cursor < len(slot_deltas) and slot_deltas[cursor][0] < bucket_end:
                available += slot_deltas[cursor][1]
                cursor += 1
            task_seconds = self._task_seconds_by_bucket.get(bucket, 0.0)
            timeline.append(
                {
                    "bucket_start_ms": bucket * _CONCURRENCY_BUCKET_MS,
                    "avg_concurrent_tasks": round(
                        task_seconds / (_CONCURRENCY_BUCKET_MS / 1000), 2
                    ),
                    "available_slots": available,
                }
            )
        return timeline

    def result(self) -> dict[str, Any]:
        self._map_jobs_to_sql_executions()
        stages = [stage.as_dict() for stage in self.stages.values()]
        stage_by_id = {stage.stage_id: stage for stage in self.stages.values()}
        totals = _totals(self.stages.values())
        timeline = self.concurrency_timeline()
        slot_seconds = sum(entry["available_slots"] for entry in timeline) * (
            _CONCURRENCY_BUCKET_MS / 1000
        )
        task_seconds = sum(entry["avg_concurrent_tasks"] for entry in timeline) * (
            _CONCURRENCY_BUCKET_MS / 1000
        )
        return {
            "application_id": self.app_id,
            "application_name": self.app_name,
            "spark_version": self.spark_version,
            "application_start_ms": self.app_start_ms,
            "application_end_ms": self.app_end_ms,
            "event_counts": dict(sorted(self.event_counts.items())),
            "job_count": len(self.jobs),
            "stage_count": len(stages),
            "task_count": totals["task_count"],
            "totals": totals,
            "stages": sorted(stages, key=lambda item: (item["stage_id"], item["attempt_id"])),
            "jobs": [job.as_dict() for job in sorted(self.jobs.values(), key=lambda j: j.job_id)],
            "sql_executions": [
                execution.as_dict()
                for execution in sorted(
                    self.sql_executions.values(), key=lambda item: item.execution_id
                )
            ],
            "stage_to_sql_execution": _stage_to_sql_execution(self.jobs, stage_by_id),
            "concurrency": {
                "bucket_seconds": _CONCURRENCY_BUCKET_MS // 1000,
                "timeline": timeline,
                "task_seconds": round(task_seconds, 1),
                "slot_seconds": round(slot_seconds, 1),
                "slot_utilization": _ratio(task_seconds, slot_seconds),
            },
            "missing_metrics": sorted(self.missing_metrics),
        }


_HANDLERS = {
    "SparkListenerLogStart": EventLogAggregator._on_log_start,
    "SparkListenerApplicationStart": EventLogAggregator._on_application_start,
    "SparkListenerApplicationEnd": EventLogAggregator._on_application_end,
    "SparkListenerExecutorAdded": EventLogAggregator._on_executor_added,
    "SparkListenerExecutorRemoved": EventLogAggregator._on_executor_removed,
    "SparkListenerJobStart": EventLogAggregator._on_job_start,
    "SparkListenerJobEnd": EventLogAggregator._on_job_end,
    "SparkListenerStageSubmitted": EventLogAggregator._on_stage_submitted,
    "SparkListenerStageCompleted": EventLogAggregator._on_stage_completed,
    "SparkListenerTaskEnd": EventLogAggregator._on_task_end,
    _SQL_START: EventLogAggregator._on_sql_start,
    _SQL_END: EventLogAggregator._on_sql_end,
}


def _as_int(metrics: dict[str, Any], key: str, aggregator: EventLogAggregator) -> int:
    value = metrics.get(key)
    if value is None:
        aggregator.missing_metrics.add(key)
        return 0
    return int(value)


def _ratio(numerator: float, denominator: float) -> float | None:
    if not denominator:
        return None
    return round(numerator / denominator, 4)


def _totals(stages: Iterable[StageAccumulator]) -> dict[str, int]:
    totals = {
        "task_count": 0,
        "executor_run_time_ms": 0,
        "executor_cpu_time_ms": 0,
        "jvm_gc_time_ms": 0,
        "memory_bytes_spilled": 0,
        "disk_bytes_spilled": 0,
        "shuffle_read_bytes": 0,
        "shuffle_fetch_wait_ms": 0,
        "shuffle_write_bytes": 0,
        "input_bytes": 0,
        "input_records": 0,
        "output_bytes": 0,
        "output_records": 0,
    }
    for stage in stages:
        totals["task_count"] += stage.task_count
        totals["executor_run_time_ms"] += stage.executor_run_time_ms
        totals["executor_cpu_time_ms"] += stage.executor_cpu_time_ms
        totals["jvm_gc_time_ms"] += stage.jvm_gc_time_ms
        totals["memory_bytes_spilled"] += stage.memory_bytes_spilled
        totals["disk_bytes_spilled"] += stage.disk_bytes_spilled
        totals["shuffle_read_bytes"] += stage.shuffle_read_bytes
        totals["shuffle_fetch_wait_ms"] += stage.shuffle_fetch_wait_ms
        totals["shuffle_write_bytes"] += stage.shuffle_write_bytes
        totals["input_bytes"] += stage.input_bytes
        totals["input_records"] += stage.input_records
        totals["output_bytes"] += stage.output_bytes
        totals["output_records"] += stage.output_records
    return totals


def _stage_to_sql_execution(
    jobs: dict[int, JobAccumulator], stage_by_id: dict[int, StageAccumulator]
) -> dict[str, int]:
    mapping: dict[str, int] = {}
    for job in jobs.values():
        if job.sql_execution_id is None:
            continue
        for stage_id in job.stage_ids:
            if stage_id in stage_by_id:
                mapping[str(stage_id)] = job.sql_execution_id
    return mapping


def iter_events(stream: Iterable[bytes | str]) -> Iterator[dict[str, Any]]:
    """줄 단위 바이트 스트림을 이벤트 dict로 푼다. 깨진 줄은 건너뛴다.

    rolling event log는 실행 중 잘린 마지막 줄을 남길 수 있다(Job Run이 죽은 경우).
    한 줄 때문에 수집 전체를 포기하지 않는다.
    """
    for line in stream:
        text = line.decode("utf-8", "replace") if isinstance(line, bytes) else line
        text = text.strip()
        if not text:
            continue
        try:
            event = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict):
            yield event


def event_log_file_uris(reader: Reader, root_uri: str) -> list[str]:
    """`root_uri` 아래의 `events_<N>_<app-id>` 파일을 N 오름차순으로 돌려준다.

    `.../jobs/<job-run-id>/sparklogs/` 를 그대로 넘길 수 있다. S3 목록은 재귀라
    `eventlog_v2_<...>` 디렉터리 이름을 몰라도 된다. 재시도가 있었으면 event log
    디렉터리가 여러 개 남는데, 그 경우 가장 최근에 쓰인 디렉터리 하나만 고른다 —
    서로 다른 시도의 스테이지를 한 집계에 섞으면 지표가 의미를 잃는다.

    같은 디렉터리의 `appstatus_<app-id>`는 이벤트가 없어 제외된다.
    """
    by_directory: dict[str, list[tuple[int, str]]] = {}
    newest: dict[str, Any] = {}
    for metadata in reader.list_objects(root_uri):
        match = _EVENT_FILE_PATTERN.search(metadata.uri)
        if match is None:
            continue
        directory = metadata.uri.rsplit("/", 1)[0]
        by_directory.setdefault(directory, []).append((int(match.group(1)), metadata.uri))
        last_modified = getattr(metadata, "last_modified", None)
        if last_modified is not None and (
            directory not in newest or last_modified > newest[directory]
        ):
            newest[directory] = last_modified
    if not by_directory:
        return []
    selected = max(by_directory, key=lambda directory: (_sort_key(newest.get(directory)), directory))
    return [uri for _, uri in sorted(by_directory[selected])]


def _sort_key(last_modified: Any) -> tuple[int, str]:
    """`last_modified`가 없는 리스팅도 정렬에 끼워 넣기 위한 키."""
    if last_modified is None:
        return (0, "")
    return (1, last_modified.isoformat())


def open_event_lines(reader: Reader, uri: str) -> Iterator[bytes]:
    """이벤트 파일 하나를 줄 단위로 흘려보낸다. `.gz`는 자동으로 푼다."""
    raw = reader.open_reader(uri)
    stream: Any = io.BufferedReader(raw, buffer_size=_READ_BUFFER_BYTES)  # type: ignore[arg-type]
    if uri.endswith(".gz"):
        stream = gzip.GzipFile(fileobj=stream)
    try:
        yield from stream
    finally:
        stream.close()
        raw.close()


def aggregate_event_log(reader: Reader, event_log_dir_uri: str) -> dict[str, Any]:
    """event log 디렉터리 하나를 스트리밍 파싱해 집계 결과를 돌려준다."""
    aggregator = EventLogAggregator()
    uris = event_log_file_uris(reader, event_log_dir_uri)
    for uri in uris:
        for event in iter_events(open_event_lines(reader, uri)):
            aggregator.add(event)
    result = aggregator.result()
    result["event_log_files"] = len(uris)
    result["event_log_dir_uri"] = event_log_dir_uri
    return result
