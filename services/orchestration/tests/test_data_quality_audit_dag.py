"""data_quality_audit DAG의 구조를 docker 없이 검증하는 테스트 (#253, #295).

실제 task 실행(EMR Serverless Job Run 제출, S3 업로드)은 로컬 Airflow에서
수동으로 확인한다(spec의 완료 조건 — 실제 EMR Serverless 트리거 검증은 #295의
제외 범위). 여기서는 DAG가 정상 파싱되고, 두 task가 서로 독립적(병렬)이며,
outlet이 없는지, 그리고 각 task가 EmrServerlessStartJobOperator로 올바른
entry point arguments와 driver_env를 전달하는지를 확인한다.
"""

from __future__ import annotations

import datetime
import importlib.util
import sys
from pathlib import Path

from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.standard.operators.python import PythonOperator

DAG_PATH = Path(__file__).resolve().parents[1] / "dags" / "data_quality_audit.py"

# DagBag은 dags 폴더 자체를 sys.path에 넣어서 그 안의 sibling 모듈
# (emr_serverless.py)을 top-level import로 가져올 수 있게 해준다. 여기서는
# DagBag을 안 쓰고 파일을 직접 로드하므로, 같은 동작을 수동으로 재현한다
# (test_standard_score_pipeline_dag.py와 동일한 패턴).
_DAGS_DIR = str(DAG_PATH.parent)
if _DAGS_DIR not in sys.path:
    sys.path.insert(0, _DAGS_DIR)


def _load_dag_module():
    spec = importlib.util.spec_from_file_location("data_quality_audit", DAG_PATH)
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

    assert module.dag.dag_id == "data_quality_audit"
    assert module.dag.schedule == "0 3 * * *"
    assert module.dag.catchup is False


def test_dag_contains_one_task_per_gold_table_plus_the_count_report():
    module = _load_dag_module()

    task_ids = {task.task_id for task in module.dag.tasks}
    assert task_ids == {
        "audit_standard_segment_comfort_score",
        "audit_current_segment_comfort_score",
        "report_audit_counts",
    }


def test_audit_tasks_are_independent_of_each_other():
    module = _load_dag_module()

    standard_task = module.dag.get_task("audit_standard_segment_comfort_score")
    current_task = module.dag.get_task("audit_current_segment_comfort_score")

    assert standard_task.upstream_task_ids == set()
    assert current_task.upstream_task_ids == set()


def test_report_audit_counts_runs_after_both_audits():
    module = _load_dag_module()

    report_task = module.dag.get_task("report_audit_counts")
    assert isinstance(report_task, PythonOperator)
    assert report_task.python_callable is module._report_audit_counts
    assert report_task.upstream_task_ids == {
        "audit_standard_segment_comfort_score",
        "audit_current_segment_comfort_score",
    }


def test_tasks_have_no_outlets():
    module = _load_dag_module()

    for task in module.dag.tasks:
        assert task.outlets == []


def test_tasks_submit_to_emr_serverless_with_the_shared_variables():
    module = _load_dag_module()

    for task_id in (
        "audit_standard_segment_comfort_score",
        "audit_current_segment_comfort_score",
    ):
        task = module.dag.get_task(task_id)
        assert isinstance(task, EmrServerlessStartJobOperator)
        assert task.application_id == "{{ var.value.EMR_SERVERLESS_APPLICATION_ID }}"
        assert (
            task.execution_role_arn
            == "{{ var.value.EMR_SERVERLESS_EXECUTION_ROLE_ARN }}"
        )
        assert (
            task.job_driver["sparkSubmit"]["entryPoint"]
            == "{{ var.value.BATCH_JOBS_EMR_ENTRY_POINT }}"
        )


def test_audit_standard_task_targets_standard_table():
    module = _load_dag_module()

    args = _entry_point_arguments(
        module.dag.get_task("audit_standard_segment_comfort_score")
    )
    assert args == ["audit-gold", "--table=standard_segment_comfort_score"]


def test_audit_current_task_targets_current_table():
    module = _load_dag_module()

    args = _entry_point_arguments(
        module.dag.get_task("audit_current_segment_comfort_score")
    )
    assert args == ["audit-gold", "--table=current_segment_comfort_score"]


def test_tasks_pass_postgres_and_gold_audit_bucket_via_driver_env():
    module = _load_dag_module()

    for task_id in (
        "audit_standard_segment_comfort_score",
        "audit_current_segment_comfort_score",
    ):
        driver_env = _driver_env(module.dag.get_task(task_id))
        for env_var in (
            "POSTGRES_HOST",
            "POSTGRES_PORT",
            "POSTGRES_DB",
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
            "GOLD_AUDIT_S3_BUCKET",
        ):
            assert env_var in driver_env


def test_dag_preserves_retry_policy():
    module = _load_dag_module()

    for task in module.dag.tasks:
        assert task.retries == 1
        assert task.retry_delay == datetime.timedelta(minutes=5)


def test_dag_wires_shared_slack_notification_callbacks():
    import notifications

    module = _load_dag_module()

    assert module.dag.default_args["on_failure_callback"] is notifications.on_failure_callback
    assert module.dag.on_success_callback is notifications.on_success_callback
