"""standard_score_pipeline DAG의 구조를 docker 없이 검증하는 테스트.

실제 task 실행(EMR Serverless Job Run 제출)은 로컬 Airflow에서 수동으로
확인하고(README 참고, Airflow의 EC2 이전 이후 별도 검증 — #292), 여기서는
DAG가 정상 파싱되는지와 sensor_processing/hourly_scoring/standard_score
TaskGroup의 골격이 의도대로 구성됐는지, 각 task가 EmrServerlessStartJobOperator로
올바른 entry point arguments를 전달하는지, standard_score에 STANDARD_SCORE_ASSET
outlet이 붙어 있는지를 확인한다(#229 ADR-0007, #227, #292 ADR-0001).
"""

from __future__ import annotations

import datetime
import importlib.util
import sys
from pathlib import Path

import pendulum
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.timetables.base import TimeRestriction
from airflow.timetables.interval import CronDataIntervalTimetable

DAG_PATH = (
    Path(__file__).resolve().parents[1] / "dags" / "standard_score_pipeline.py"
)

# Airflow의 DagBag은 dags 폴더 자체를 sys.path에 넣어서 그 안의 sibling 모듈
# (comfort_score_assets.py, emr_serverless.py)을 top-level import로 가져올 수
# 있게 해준다. 여기서는 DagBag을 안 쓰고 파일을 직접 로드하므로, 같은 동작을
# 수동으로 재현한다.
_DAGS_DIR = str(DAG_PATH.parent)
if _DAGS_DIR not in sys.path:
    sys.path.insert(0, _DAGS_DIR)

from comfort_score_assets import STANDARD_SCORE_ASSET


def _load_dag_module():
    spec = importlib.util.spec_from_file_location("standard_score_pipeline", DAG_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _entry_point_arguments(task) -> list[str]:
    assert isinstance(task, EmrServerlessStartJobOperator)
    return task.job_driver["sparkSubmit"]["entryPointArguments"]


def _driver_env(task) -> str:
    return task.job_driver["sparkSubmit"].get("sparkSubmitParameters", "")


def test_dag_parses_with_expected_schedule():
    module = _load_dag_module()

    assert module.dag.dag_id == "standard_score_pipeline"
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


def test_all_emr_tasks_submit_with_the_shared_variables():
    module = _load_dag_module()

    # resolve_road_snapshot_date(#402)는 EMR Serverless가 아니라 이 컨테이너 안에서
    # 도는 PythonOperator라 아래 검증 대상에서 제외한다.
    emr_tasks = [
        task for task in module.dag.tasks if isinstance(task, EmrServerlessStartJobOperator)
    ]
    assert emr_tasks
    for task in emr_tasks:
        assert task.application_id == "{{ var.value.EMR_SERVERLESS_APPLICATION_ID }}"
        assert (
            task.execution_role_arn
            == "{{ var.value.EMR_SERVERLESS_EXECUTION_ROLE_ARN }}"
        )
        assert (
            task.job_driver["sparkSubmit"]["entryPoint"]
            == "{{ var.value.BATCH_JOBS_EMR_ENTRY_POINT }}"
        )


def test_sensor_processing_task_group_contains_the_combined_job_and_its_validation():
    module = _load_dag_module()

    task_ids = {task.task_id for task in module.dag.tasks}
    assert "sensor_processing.resolve_road_snapshot_date" in task_ids
    assert "sensor_processing.run_sensor_processing" in task_ids
    assert "sensor_processing.validate_sensor_processing" in task_ids


def test_resolve_road_snapshot_date_is_a_python_operator_not_an_emr_job():
    """road_environment_uri의 active pointer/manifest를 읽는 건 이 컨테이너에서
    바로 Python으로 처리하지, EMR Serverless Job Run을 거치지 않는다(#402)."""
    module = _load_dag_module()

    task = module.dag.get_task("sensor_processing.resolve_road_snapshot_date")
    assert isinstance(task, PythonOperator)


def test_hourly_scoring_task_group_contains_the_scoring_job_and_its_validation():
    module = _load_dag_module()

    task_ids = {task.task_id for task in module.dag.tasks}
    assert "hourly_scoring.run_hourly_scoring" in task_ids
    assert "hourly_scoring.validate_hourly_scoring" in task_ids


def test_run_sensor_processing_invokes_combined_job_with_required_arguments():
    module = _load_dag_module()

    args = _entry_point_arguments(
        module.dag.get_task("sensor_processing.run_sensor_processing")
    )
    assert args[0] == "cleanse-sensor-events"
    assert "--target-hour" in args
    assert "{{ data_interval_start.isoformat() }}" in args
    assert "--road-snapshot-date" in args
    assert (
        "{{ ti.xcom_pull(task_ids='sensor_processing.resolve_road_snapshot_date') }}"
        in args
    )
    assert "--feature-version" in args
    assert "{{ var.value.HOURLY_SEGMENT_FEATURE_VERSION }}" in args
    assert "--bronze-input-path" in args
    assert "--quarantine-output-path" in args
    assert "--road-segment-path" in args
    assert "--output-path" in args
    assert "--run-id" in args
    assert "{{ run_id }}" in args


def test_validate_sensor_processing_invokes_gx_validation_with_matching_paths():
    module = _load_dag_module()

    args = _entry_point_arguments(
        module.dag.get_task("sensor_processing.validate_sensor_processing")
    )
    assert args[0] == "validate-sensor-processing"
    assert "--target-hour" in args
    assert "{{ data_interval_start.isoformat() }}" in args
    # run_sensor_processing과 같은 Variable을 참조해야 같은 파티션을 가리킨다.
    run_args = _entry_point_arguments(
        module.dag.get_task("sensor_processing.run_sensor_processing")
    )
    assert args[args.index("--output-path") + 1] == run_args[
        run_args.index("--output-path") + 1
    ]
    assert args[args.index("--quarantine-output-path") + 1] == run_args[
        run_args.index("--quarantine-output-path") + 1
    ]


def test_run_hourly_scoring_invokes_score_hourly_comfort_with_templated_run_id():
    module = _load_dag_module()

    args = _entry_point_arguments(
        module.dag.get_task("hourly_scoring.run_hourly_scoring")
    )
    assert args[0] == "score-hourly-comfort"
    assert "--run-id" in args
    assert "{{ run_id }}" in args


def test_validate_hourly_scoring_invokes_gx_validation_with_matching_output_path():
    module = _load_dag_module()

    args = _entry_point_arguments(
        module.dag.get_task("hourly_scoring.validate_hourly_scoring")
    )
    assert args[0] == "validate-hourly-scoring"
    run_args = _entry_point_arguments(
        module.dag.get_task("hourly_scoring.run_hourly_scoring")
    )
    # run_hourly_scoring과 같은 Variable을 참조해야 같은 output을 가리킨다.
    assert args[args.index("--output-path") + 1] == run_args[
        run_args.index("--output-path") + 1
    ]


def test_dag_contains_expected_pipeline_tasks_so_far():
    module = _load_dag_module()

    task_ids = {task.task_id for task in module.dag.tasks}
    assert task_ids == {
        "sensor_processing.resolve_road_snapshot_date",
        "sensor_processing.run_sensor_processing",
        "sensor_processing.validate_sensor_processing",
        "hourly_scoring.run_hourly_scoring",
        "hourly_scoring.validate_hourly_scoring",
        "standard_score.run_standard_score",
        "standard_score.validate_standard_score",
        "report_processing_counts",
    }


def test_current_score_task_group_is_removed():
    module = _load_dag_module()

    task_ids = {task.task_id for task in module.dag.tasks}
    assert not any(task_id.startswith("current_score") for task_id in task_ids)


def test_task_groups_follow_standard_score_pipeline_order():
    module = _load_dag_module()

    resolve_road_snapshot_date = module.dag.get_task(
        "sensor_processing.resolve_road_snapshot_date"
    )
    run_sensor_processing = module.dag.get_task(
        "sensor_processing.run_sensor_processing"
    )
    validate_sensor_processing = module.dag.get_task(
        "sensor_processing.validate_sensor_processing"
    )
    run_hourly_scoring = module.dag.get_task("hourly_scoring.run_hourly_scoring")
    validate_hourly_scoring = module.dag.get_task("hourly_scoring.validate_hourly_scoring")
    run_standard_score = module.dag.get_task("standard_score.run_standard_score")

    assert resolve_road_snapshot_date.upstream_task_ids == set()
    assert resolve_road_snapshot_date.downstream_task_ids == {
        "sensor_processing.run_sensor_processing"
    }
    assert run_sensor_processing.upstream_task_ids == {
        "sensor_processing.resolve_road_snapshot_date"
    }
    assert run_sensor_processing.downstream_task_ids == {
        "sensor_processing.validate_sensor_processing"
    }
    assert validate_sensor_processing.upstream_task_ids == {
        "sensor_processing.run_sensor_processing"
    }
    assert validate_sensor_processing.downstream_task_ids == {
        "hourly_scoring.run_hourly_scoring"
    }
    assert run_hourly_scoring.upstream_task_ids == {
        "sensor_processing.validate_sensor_processing"
    }
    assert run_hourly_scoring.downstream_task_ids == {
        "hourly_scoring.validate_hourly_scoring"
    }
    assert validate_hourly_scoring.upstream_task_ids == {
        "hourly_scoring.run_hourly_scoring"
    }
    assert validate_hourly_scoring.downstream_task_ids == {
        "standard_score.run_standard_score"
    }
    validate_standard_score = module.dag.get_task("standard_score.validate_standard_score")
    assert run_standard_score.upstream_task_ids == {
        "hourly_scoring.validate_hourly_scoring"
    }
    assert run_standard_score.downstream_task_ids == {
        "standard_score.validate_standard_score"
    }
    assert validate_standard_score.upstream_task_ids == {
        "standard_score.run_standard_score"
    }
    assert validate_standard_score.downstream_task_ids == {"report_processing_counts"}


def test_run_standard_score_invokes_the_standard_load_with_templated_as_of():
    module = _load_dag_module()

    task = module.dag.get_task("standard_score.run_standard_score")
    args = _entry_point_arguments(task)

    assert args[0] == "load-standard-segment-comfort-score"
    assert "--as-of" in args
    assert "{{ data_interval_end.isoformat() }}" in args
    driver_env = _driver_env(task)
    assert "STANDARD_COMFORT_SCORE_DATA_LAKE_URI" in driver_env
    assert "REFERENCE_DATA_LAKE_URI" in driver_env
    assert "POSTGRES_PASSWORD" in driver_env


def test_validate_standard_score_invokes_gx_validation_with_templated_as_of():
    module = _load_dag_module()

    task = module.dag.get_task("standard_score.validate_standard_score")
    args = _entry_point_arguments(task)

    assert args[0] == "validate-standard-score"
    assert "--as-of" in args
    assert "{{ data_interval_end.isoformat() }}" in args
    assert "POSTGRES_PASSWORD" in _driver_env(task)


def test_run_standard_score_does_not_emit_standard_score_asset():
    module = _load_dag_module()

    task = module.dag.get_task("standard_score.run_standard_score")
    assert task.outlets == []


def test_validate_standard_score_emits_standard_score_asset():
    """검증을 통과한 데이터만 current_score_pipeline을 깨우도록 outlet을 옮긴다(#249)."""
    module = _load_dag_module()

    task = module.dag.get_task("standard_score.validate_standard_score")
    assert task.outlets == [STANDARD_SCORE_ASSET]


def test_report_processing_counts_runs_after_standard_score_group():
    module = _load_dag_module()

    task = module.dag.get_task("report_processing_counts")
    assert isinstance(task, PythonOperator)
    assert task.python_callable is module._report_processing_counts
    assert task.upstream_task_ids == {"standard_score.validate_standard_score"}


def test_report_processing_counts_templates_the_same_paths_as_upstream_tasks():
    module = _load_dag_module()

    task = module.dag.get_task("report_processing_counts")
    assert task.op_kwargs["quarantine_output_path"] == module._CLEANSING_QUARANTINE_OUTPUT_PATH
    assert task.op_kwargs["feature_output_path"] == module._HOURLY_SEGMENT_FEATURE_OUTPUT_PATH
    assert task.op_kwargs["hourly_comfort_output_path"] == module._HOURLY_COMFORT_OUTPUT_PATH
    assert task.op_kwargs["target_hour"] == "{{ data_interval_start.isoformat() }}"
    assert task.op_kwargs["as_of"] == "{{ data_interval_end.isoformat() }}"


def test_dag_wires_shared_slack_notification_callbacks():
    import notifications

    module = _load_dag_module()

    assert module.dag.default_args["on_failure_callback"] is notifications.on_failure_callback
    assert module.dag.on_success_callback is notifications.on_success_callback
