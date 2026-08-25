"""네 계층을 DAG run 하나로 조립하는 부분의 고정 테스트.

Airflow/EMR/S3 클라이언트를 전부 대역으로 갈아끼우고, Job Run 로그 트리만 실제
파일시스템에 fixture로 깔아 `ObjectStore`가 진짜로 읽게 한다.
"""

from datetime import UTC, datetime

import pytest
from de4_core import ObjectStore
from fakes import FakeAirflowClient, FakeEmrClient
from pipeline_perf.collector import CollectConfig, Collector
from scenario import (
    APPLICATION_ID,
    DAG_RUN,
    JOB_RUN,
    JOB_RUN_ID,
    RUN_ID,
    TASK_INSTANCES,
    build_collector,
)


@pytest.fixture
def collected(lake):
    return build_collector(lake).collect()


def test_collect_walks_every_layer_for_an_emr_task(collected):
    task = collected["dags"][0]["runs"][0]["tasks"][1]

    assert task["job_run_id"] == JOB_RUN_ID
    assert task["job_run_id_source"] == "xcom"
    assert task["emr"]["provisioning_wait_s"] == 70.0
    assert task["spark"]["stage_count"] == 3
    assert [phase["phase"] for phase in task["perf_phases"]["phases"]] == [
        "standard_score.gold_snapshot_write",
        "standard_score.postgres_merge",
    ]


def test_non_emr_tasks_carry_no_job_run(collected):
    task = collected["dags"][0]["runs"][0]["tasks"][0]

    assert task["job_run_id"] is None
    assert task["emr"] is None
    assert task["spark"] is None


def test_non_emr_task_perf_comes_from_the_airflow_task_log(collected):
    """Spark를 안 쓰는 task는 S3 driver 로그가 없어 PERF가 task 로그에만 남는다."""
    task = collected["dags"][0]["runs"][0]["tasks"][0]

    assert task["perf_phases"]["available"] is True
    assert task["perf_phases"]["phases"] == [
        {
            "phase": "sensor_processing.resolve_road_snapshot_date",
            "elapsed_s": 1.204,
            "ok": True,
        }
    ]
    assert task["perf_phases"]["source_uris"][0].startswith("airflow-task-log:")


def test_absent_task_log_is_reported_as_unavailable(lake):
    task = build_collector(lake, task_logs={}).collect()["dags"][0]["runs"][0]["tasks"][0]

    assert task["perf_phases"] == {"source_uris": [], "available": False, "phases": []}


def test_overhead_splits_the_run_into_named_segments(collected):
    overhead = collected["dags"][0]["runs"][0]["overhead"]

    assert overhead["provisioning_s"] == 70.0
    assert overhead["spark_boot_s"] == 5.0
    assert overhead["spark_app_s"] == 26.0
    assert overhead["teardown_s"] == 10.0
    assert overhead["airflow_gap_s"] == 10.0
    assert overhead["other_task_s"] == 5.0
    assert overhead["dag_run_duration_s"] == 600.0
    assert overhead["unaccounted_s"] == 474.0
    assert overhead["compute_ratio"] == pytest.approx(0.0433, abs=1e-4)


def test_bronze_partition_and_counts_are_attached_to_the_run(collected):
    run = collected["dags"][0]["runs"][0]

    assert run["bronze_input"]["file_count"] == 2
    assert run["bronze_input"]["total_bytes"] == 4096
    assert run["bronze_input"]["avg_bytes"] == 2048
    assert run["processing_counts"] == {"feature_count": 184213}


def test_expired_xcom_falls_back_to_job_run_name_matching(lake):
    collector = build_collector(
        lake,
        xcoms={},
        listed=[
            {
                "id": JOB_RUN_ID,
                "name": "run_sensor_processing",
                "createdAt": datetime(2026, 8, 25, 2, 0, 25, tzinfo=UTC),
            }
        ],
    )

    task = collector.collect()["dags"][0]["runs"][0]["tasks"][1]

    assert task["job_run_id"] == JOB_RUN_ID
    assert task["job_run_id_source"] == "name_match"


def test_unresolvable_job_run_is_noted_rather_than_raised(lake):
    collector = build_collector(lake, xcoms={}, listed=[])

    payload = collector.collect()

    assert payload["dags"][0]["runs"][0]["tasks"][1]["job_run_id"] is None
    assert any("Job Run ID" in note for note in payload["notes"])


def test_no_spark_skips_the_event_log_but_keeps_the_job_run_duration(lake):
    collector = build_collector(lake, with_spark=False)

    run = collector.collect()["dags"][0]["runs"][0]

    assert run["tasks"][1]["spark"] is None
    # event log를 안 읽으면 Job Run 실행시간 전부를 계산 구간으로 본다.
    assert run["overhead"]["spark_app_s"] == 41.0
    assert run["overhead"]["spark_boot_s"] == 0.0


def test_asset_triggered_run_records_the_wait_from_the_trigger(lake):
    airflow = FakeAirflowClient(
        dag_runs={"current_score_pipeline": [{**DAG_RUN, "run_type": "asset_triggered"}]},
        task_instances={RUN_ID: []},
        asset_events={
            "current_score_pipeline": [
                {
                    "timestamp": "2026-08-25T01:59:30Z",
                    "source_dag_id": "standard_score_pipeline",
                    "source_task_id": "report_processing_counts",
                    "source_run_id": "upstream-run",
                    "created_dagruns": [{"dag_id": "current_score_pipeline", "run_id": RUN_ID}],
                }
            ]
        },
    )
    collector = Collector(
        airflow=airflow,
        emr_client=FakeEmrClient({}),
        object_store=ObjectStore(),
        config=CollectConfig(
            dag_ids=["current_score_pipeline"], application_id=APPLICATION_ID, log_uri="unused"
        ),
    )

    trigger = collector.collect()["dags"][0]["runs"][0]["asset_trigger"]

    assert trigger["source_dag_id"] == "standard_score_pipeline"
    assert trigger["wait_to_start_s"] == 30.0


def test_upstream_failed_task_is_not_matched_to_someone_elses_job_run(lake):
    """Airflow는 실행된 적 없는 task에도 start_date를 찍는다.

    그 시각으로 이름 매칭을 돌리면 그 시간대에 있던 남의 Job Run에 붙는다 —
    실제 수집에서 관측한 오탐이라 상태로 먼저 걸러야 한다.
    """
    never_ran = {
        "task_id": "standard_score.validate_standard_score",
        "operator": "EmrServerlessStartJobOperator",
        "state": "upstream_failed",
        "try_number": 0,
        "start_date": "2026-08-25T02:09:44Z",
        "end_date": "2026-08-25T02:09:44Z",
        "duration": 0.0,
    }
    collector = build_collector(lake)
    collector.airflow._task_instances[RUN_ID] = [*TASK_INSTANCES, never_ran]
    collector.emr_client = FakeEmrClient(
        {JOB_RUN_ID: JOB_RUN},
        listed=[
            {
                "id": "someone-elses-run",
                "name": "validate_standard_score",
                "createdAt": datetime(2026, 8, 25, 2, 9, 40, tzinfo=UTC),
            }
        ],
    )

    task = collector.collect()["dags"][0]["runs"][0]["tasks"][-1]

    assert task["job_run_id"] is None
    assert task["emr"] is None
    assert not collector.notes


def test_a_run_without_spark_reports_no_compute_ratio(lake):
    """Spark를 안 쓰는 DAG에서 계산 비율 0%는 사실이 아니라 미측정이다."""
    collector = build_collector(lake)
    collector.airflow._task_instances[RUN_ID] = [TASK_INSTANCES[0]]

    overhead = collector.collect()["dags"][0]["runs"][0]["overhead"]

    assert overhead["spark_app_s"] == 0.0
    assert overhead["compute_ratio"] is None


def test_in_flight_run_is_excluded_from_the_baseline(lake):
    """아직 도는 중인 실행은 구간이 반만 찍혀 있어 평균을 왜곡한다."""
    collector = build_collector(lake)
    running = {**DAG_RUN, "dag_run_id": "still-running", "state": "running", "end_date": None}
    collector.airflow._dag_runs["standard_score_pipeline"] = [running, DAG_RUN]

    payload = collector.collect()

    assert [run["dag_run_id"] for run in payload["dags"][0]["runs"]] == [RUN_ID]
    assert any("아직 끝나지 않아" in note for note in payload["notes"])
