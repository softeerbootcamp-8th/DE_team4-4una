"""Spark event log 집계 고정 테스트.

fixture는 실제 EMR 8.0.0 / Spark 4.0.2-amzn-0 event log의 구조(#462 조사에서 필드
가용성을 확인한 그 구조)를 그대로 따르되, 이벤트 수만 스테이지 3개·태스크 5개로
줄인 것이다. 여기서 고정하는 것은 스테이지 집계, 분위수, skew 비율, job -> SQL
execution 시간창 매칭이다.
"""

from dataclasses import dataclass
from datetime import UTC, datetime

import pytest
from de4_core import ObjectStore
from pipeline_perf.spark_events import (
    EventLogAggregator,
    aggregate_event_log,
    event_log_file_uris,
    iter_events,
)


@dataclass(frozen=True)
class FakeObject:
    uri: str
    size: int = 0
    last_modified: datetime = datetime(2026, 8, 25, tzinfo=UTC)


class FakeReader:
    def __init__(self, objects):
        self._objects = objects

    def list_objects(self, uri):
        return [obj for obj in self._objects if obj.uri.startswith(uri)]

    def open_reader(self, uri):  # pragma: no cover - 목록 테스트에서는 안 쓴다
        raise NotImplementedError


@pytest.fixture
def aggregated(job_run_dir):
    return aggregate_event_log(ObjectStore(), str(job_run_dir / "sparklogs"))


def test_reads_every_event_file_and_skips_appstatus(aggregated):
    assert aggregated["event_log_files"] == 2
    assert aggregated["spark_version"] == "4.0.2-amzn-0"
    assert aggregated["application_id"] == "00g88h7uj9j8002r"
    # 2026-08-25T02:01:40Z ~ 02:02:06Z
    assert aggregated["application_start_ms"] == 1_787_623_300_000
    assert aggregated["application_end_ms"] == 1_787_623_326_000


def test_event_files_are_ordered_numerically_not_lexically():
    reader = FakeReader(
        [
            FakeObject("s3://b/logs/eventlog_v2_app/events_10_app"),
            FakeObject("s3://b/logs/eventlog_v2_app/events_2_app"),
            FakeObject("s3://b/logs/eventlog_v2_app/appstatus_app"),
        ]
    )

    uris = event_log_file_uris(reader, "s3://b/logs/")

    assert uris == [
        "s3://b/logs/eventlog_v2_app/events_2_app",
        "s3://b/logs/eventlog_v2_app/events_10_app",
    ]


def test_retry_leaves_two_event_log_dirs_and_the_newest_wins():
    reader = FakeReader(
        [
            FakeObject(
                "s3://b/logs/eventlog_v2_first/events_1_first",
                last_modified=datetime(2026, 8, 24, tzinfo=UTC),
            ),
            FakeObject(
                "s3://b/logs/eventlog_v2_second/events_1_second",
                last_modified=datetime(2026, 8, 25, tzinfo=UTC),
            ),
        ]
    )

    assert event_log_file_uris(reader, "s3://b/logs/") == [
        "s3://b/logs/eventlog_v2_second/events_1_second"
    ]


def test_truncated_last_line_does_not_lose_the_rest():
    events = list(iter_events(['{"Event": "SparkListenerApplicationEnd"}', '{"Event": ']))

    assert events == [{"Event": "SparkListenerApplicationEnd"}]


def test_stage_aggregation_holds_sums_quantiles_and_skew(aggregated):
    stages = {stage["stage_id"]: stage for stage in aggregated["stages"]}

    heavy = stages[0]
    assert heavy["task_count"] == 3
    assert heavy["wall_time_ms"] == 8_000
    assert heavy["task_duration"] == {
        "count": 3,
        "p50_ms": 2_000,
        "p95_ms": 7_400,
        "max_ms": 8_000,
        "sum_ms": 12_000,
    }
    assert heavy["skew_ratio"] == 4.0
    assert heavy["executor_run_time_ms"] == 11_600
    assert heavy["jvm_gc_time_ms"] == 1_400
    assert heavy["gc_ratio"] == pytest.approx(0.1207, abs=1e-4)
    assert heavy["memory_bytes_spilled"] == 268_435_456
    assert heavy["disk_bytes_spilled"] == 134_217_728
    assert heavy["input_records"] == 2_000_000
    assert heavy["shuffle_write_bytes"] == 4_194_304

    shuffled = stages[1]
    assert shuffled["shuffle_read_bytes"] == 3_145_728
    assert shuffled["shuffle_fetch_wait_ms"] == 250
    assert shuffled["output_records"] == 1_000


def test_totals_sum_every_stage(aggregated):
    totals = aggregated["totals"]

    assert totals["task_count"] == 5
    assert totals["input_records"] == 2_000_001
    assert totals["disk_bytes_spilled"] == 134_217_728


def test_jobs_map_to_sql_executions_by_time_window(aggregated):
    jobs = {job["job_id"]: job for job in aggregated["jobs"]}

    # execution이 시작되기 전에 도는 스키마 조회 job은 어디에도 속하지 않는다.
    assert jobs[9]["sql_execution_id"] is None
    assert jobs[0]["sql_execution_id"] == 0
    assert jobs[1]["sql_execution_id"] == 1
    assert aggregated["stage_to_sql_execution"] == {"0": 0, "1": 1}


def test_sql_executions_keep_plaintext_description_and_duration(aggregated):
    executions = {item["execution_id"]: item for item in aggregated["sql_executions"]}

    assert executions[0]["duration_ms"] == 10_000
    assert executions[0]["description"] == "count at NativeMethodAccessorImpl.java:0"
    assert executions[1]["duration_ms"] == 3_000
    assert executions[1]["description"] == "collect at batch_jobs/map_matching/candidates.py:111"


def test_concurrency_compares_task_seconds_with_available_slots(aggregated):
    concurrency = aggregated["concurrency"]

    assert concurrency["bucket_seconds"] == 10
    assert [entry["available_slots"] for entry in concurrency["timeline"]] == [4, 4, 2]
    assert [entry["avg_concurrent_tasks"] for entry in concurrency["timeline"]] == [0.05, 1.2, 0.15]
    assert concurrency["task_seconds"] == 14.0
    assert concurrency["slot_seconds"] == 100.0
    assert concurrency["slot_utilization"] == 0.14


def test_missing_task_metrics_are_recorded_instead_of_crashing():
    aggregator = EventLogAggregator()

    aggregator.add(
        {
            "Event": "SparkListenerTaskEnd",
            "Stage ID": 3,
            "Stage Attempt ID": 0,
            "Task Info": {"Launch Time": 10, "Finish Time": 20, "Failed": True},
            "Task Metrics": {"Executor Run Time": 5},
        }
    )
    result = aggregator.result()

    assert "JVM GC Time" in result["missing_metrics"]
    assert result["stages"][0]["failed_task_count"] == 1
