"""hourly_pipeline DAG의 구조를 docker 없이 검증하는 테스트.

실제 task 실행(batch-jobs 컨테이너 기동)은 로컬 Airflow에서 수동으로 확인하고,
여기서는 DAG가 정상 파싱되는지와 각 TaskGroup의
골격이 의도대로 구성됐는지만 확인한다.
"""

from __future__ import annotations

import datetime
import importlib.util
import sys
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.timetables.base import TimeRestriction
from airflow.timetables.interval import CronDataIntervalTimetable

DAG_PATH = (
    Path(__file__).resolve().parents[1] / "dags" / "hourly_pipeline.py"
)


def _load_dag_module():
    spec = importlib.util.spec_from_file_location("hourly_pipeline", DAG_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_dag_parses_with_expected_schedule():
    module = _load_dag_module()

    assert module.dag.dag_id == "hourly_pipeline"
    assert isinstance(module.dag.timetable, CronDataIntervalTimetable)
    assert module.dag.timetable.summary == "0 * * * *"
    assert module.dag.catchup is False


def test_dag_uses_one_hour_utc_data_interval():
    module = _load_dag_module()

    run_info = module.dag.timetable.next_dagrun_info(
        last_automated_data_interval=None,
        restriction=TimeRestriction(
            earliest=pendulum.datetime(2026, 8, 18, 9, tz="UTC"),
            latest=pendulum.datetime(2026, 8, 18, 10, tz="UTC"),
            catchup=True,
        ),
    )

    assert run_info is not None
    assert run_info.logical_date == pendulum.datetime(2026, 8, 18, 9, tz="UTC")
    assert run_info.data_interval.start == pendulum.datetime(
        2026, 8, 18, 9, tz="UTC"
    )
    assert run_info.data_interval.end == pendulum.datetime(
        2026, 8, 18, 10, tz="UTC"
    )


def test_dag_preserves_retry_policy():
    module = _load_dag_module()

    for task in module.dag.tasks:
        assert task.retries == 1
        assert task.retry_delay == datetime.timedelta(minutes=5)


def test_sensor_processing_task_group_contains_the_combined_job_and_its_validation():
    module = _load_dag_module()

    task_ids = {task.task_id for task in module.dag.tasks}
    assert "sensor_processing.run_sensor_processing" in task_ids
    assert "sensor_processing.validate_sensor_processing" in task_ids


def test_scoring_task_group_contains_only_run_scoring():
    module = _load_dag_module()

    task_ids = {task.task_id for task in module.dag.tasks}
    assert "scoring.run_scoring" in task_ids


def test_run_sensor_processing_invokes_combined_job_with_required_arguments():
    module = _load_dag_module()

    run_sensor_processing = module.dag.get_task(
        "sensor_processing.run_sensor_processing"
    )
    command = run_sensor_processing.bash_command
    assert "cleanse-sensor-events" in command
    assert "--target-hour='{{ data_interval_start.isoformat() }}'" in command
    assert "HOURLY_SEGMENT_FEATURE_ROAD_SNAPSHOT_DATE" in command
    assert "HOURLY_SEGMENT_FEATURE_VERSION" in command
    assert "--bronze-input-path=" in command
    assert "CLEANSING_BRONZE_INPUT_PATH" in command
    assert "--quarantine-output-path=" in command
    assert "CLEANSING_QUARANTINE_OUTPUT_PATH" in command
    assert "--road-segment-path=" in command
    assert "HOURLY_SEGMENT_FEATURE_ROAD_SEGMENT_PATH" in command
    assert "--output-path=" in command
    assert "HOURLY_SEGMENT_FEATURE_OUTPUT_PATH" in command
    assert "-e CLEANSING_CONFIG_PATH" in command
    assert "-e HOURLY_SEGMENT_FEATURE_EVENT_CONFIG_PATH" in command
    assert "-e HOURLY_SEGMENT_FEATURE_STEERING_CONFIG_PATH" in command
    assert "-e HOURLY_SEGMENT_FEATURE_MAP_MATCHING_CONFIG_PATH" in command
    assert "--run-id='{{ run_id }}'" in command
    assert "build-hourly-segment-features" not in command
    assert "CLEANSING_SILVER_OUTPUT_PATH" not in command
    assert "HOURLY_SEGMENT_FEATURE_INPUT_PATH" not in command


def test_validate_sensor_processing_invokes_gx_validation_with_matching_paths():
    module = _load_dag_module()

    validate_sensor_processing = module.dag.get_task(
        "sensor_processing.validate_sensor_processing"
    )
    command = validate_sensor_processing.bash_command
    assert "validate-sensor-processing" in command
    assert "--target-hour='{{ data_interval_start.isoformat() }}'" in command
    # run_sensor_processing과 같은 env var를 참조해야 같은 파티션을 가리킨다.
    assert "--output-path=" in command
    assert "HOURLY_SEGMENT_FEATURE_OUTPUT_PATH" in command
    assert "--quarantine-output-path=" in command
    assert "CLEANSING_QUARANTINE_OUTPUT_PATH" in command


def test_run_scoring_invokes_score_hourly_comfort_with_templated_run_id():
    module = _load_dag_module()

    run_scoring = module.dag.get_task("scoring.run_scoring")
    assert "score-hourly-comfort" in run_scoring.bash_command
    assert "--run-id={{ run_id }}" in run_scoring.bash_command


def test_dag_contains_expected_pipeline_tasks_so_far():
    module = _load_dag_module()

    task_ids = {task.task_id for task in module.dag.tasks}
    assert task_ids == {
        "sensor_processing.run_sensor_processing",
        "sensor_processing.validate_sensor_processing",
        "scoring.run_scoring",
        "standard_score.run_standard_score",
        "current_score.run_current_score",
    }


def test_task_groups_follow_hourly_pipeline_order():
    module = _load_dag_module()

    run_sensor_processing = module.dag.get_task(
        "sensor_processing.run_sensor_processing"
    )
    validate_sensor_processing = module.dag.get_task(
        "sensor_processing.validate_sensor_processing"
    )
    run_scoring = module.dag.get_task("scoring.run_scoring")

    assert run_sensor_processing.downstream_task_ids == {
        "sensor_processing.validate_sensor_processing"
    }
    assert validate_sensor_processing.upstream_task_ids == {
        "sensor_processing.run_sensor_processing"
    }
    assert validate_sensor_processing.downstream_task_ids == {"scoring.run_scoring"}
    assert run_scoring.upstream_task_ids == {
        "sensor_processing.validate_sensor_processing"
    }
    run_standard_score = module.dag.get_task("standard_score.run_standard_score")
    run_current_score = module.dag.get_task("current_score.run_current_score")

    assert run_scoring.downstream_task_ids == {"standard_score.run_standard_score"}
    assert run_standard_score.upstream_task_ids == {"scoring.run_scoring"}
    assert run_standard_score.downstream_task_ids == {"current_score.run_current_score"}
    assert run_current_score.downstream_task_ids == set()


def test_run_standard_score_invokes_the_standard_load_with_templated_as_of():
    module = _load_dag_module()

    command = module.dag.get_task("standard_score.run_standard_score").bash_command

    assert "load-standard-segment-comfort-score" in command
    assert "--as-of='{{ data_interval_end.isoformat() }}'" in command
    assert "STANDARD_COMFORT_SCORE_DATA_LAKE_URI" in command
    assert "POSTGRES_PASSWORD" in command


def test_run_current_score_refreshes_every_row():
    module = _load_dag_module()

    task = module.dag.get_task("current_score.run_current_score")

    assert isinstance(task, PythonOperator)
    assert task.python_callable is module._refresh_current_scores
