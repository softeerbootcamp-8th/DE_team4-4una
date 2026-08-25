"""One realistic DAG run wired to the fixture log tree, shared by the collector,
render, and compare tests."""

from __future__ import annotations

from datetime import UTC, datetime

from de4_core import ObjectStore
from fakes import FakeAirflowClient, FakeEmrClient
from pipeline_perf.collector import CollectConfig, Collector

APPLICATION_ID = "00app"
JOB_RUN_ID = "00g88h7uj9j8002r"
RUN_ID = "scheduled__2026-08-25T02:00:00+00:00"


DAG_RUN = {
    "dag_run_id": RUN_ID,
    "run_type": "scheduled",
    "state": "success",
    "logical_date": "2026-08-25T02:00:00Z",
    "data_interval_start": "2026-08-25T02:00:00Z",
    "data_interval_end": "2026-08-25T03:00:00Z",
    "queued_at": "2026-08-25T01:59:58Z",
    "start_date": "2026-08-25T02:00:00Z",
    "end_date": "2026-08-25T02:10:00Z",
}

TASK_INSTANCES = [
    {
        "task_id": "sensor_processing.resolve_road_snapshot_date",
        "operator": "PythonOperator",
        "state": "success",
        "try_number": 1,
        "start_date": "2026-08-25T02:00:05Z",
        "end_date": "2026-08-25T02:00:10Z",
        "duration": 5.0,
    },
    {
        "task_id": "sensor_processing.run_sensor_processing",
        "operator": "EmrServerlessStartJobOperator",
        "state": "success",
        "try_number": 1,
        "start_date": "2026-08-25T02:00:20Z",
        "end_date": "2026-08-25T02:05:00Z",
        "duration": 280.0,
    },
]

JOB_RUN = {
    "jobRunId": JOB_RUN_ID,
    "applicationId": APPLICATION_ID,
    "name": "run_sensor_processing",
    "state": "SUCCESS",
    "createdAt": datetime(2026, 8, 25, 2, 0, 25, tzinfo=UTC),
    "startedAt": datetime(2026, 8, 25, 2, 1, 35, tzinfo=UTC),
    "endedAt": datetime(2026, 8, 25, 2, 2, 16, tzinfo=UTC),
    "billedResourceUtilization": {"vCPUHour": 0.5, "memoryGBHour": 2.0, "storageGBHour": 0.1},
}



# Spark를 안 쓰는 task의 PERF 로그는 Airflow task 로그에만 남는다(#461).
TASK_LOG = (
    "[2026-08-25, 02:00:06 UTC] {logging_mixin.py:190} INFO - "
    'PERF {"phase": "sensor_processing.resolve_road_snapshot_date", '
    '"elapsed_s": 1.204, "ok": true}\n'
)


def build_collector(lake, *, xcoms=None, listed=None, with_spark=True, task_logs=None):
    airflow = FakeAirflowClient(
        dag_runs={"standard_score_pipeline": [DAG_RUN]},
        task_instances={RUN_ID: TASK_INSTANCES},
        xcoms=xcoms
        if xcoms is not None
        else {
            (RUN_ID, "sensor_processing.run_sensor_processing"): JOB_RUN_ID,
            (RUN_ID, "report_processing_counts"): {"feature_count": 184213},
        },
        task_logs=task_logs
        if task_logs is not None
        else {"sensor_processing.resolve_road_snapshot_date": TASK_LOG},
    )
    config = CollectConfig(
        dag_ids=["standard_score_pipeline"],
        last=5,
        application_id=APPLICATION_ID,
        log_uri=str(lake / "logs"),
        bronze_input_uri=str(lake / "bronze"),
        with_spark=with_spark,
    )
    return Collector(
        airflow=airflow,
        emr_client=FakeEmrClient({JOB_RUN_ID: JOB_RUN}, listed=listed),
        object_store=ObjectStore(),
        config=config,
    )
