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
    # 뉴욕 04:40 EDT. Bronze 입력량이 03:00 UTC 1.7 GiB에서 07:00 UTC 257 MiB로
    # 단조 감소하고(뉴욕 23시 → 03시), 08:00 hourly run이 끝난 뒤 09:00 run 전
    # 틈에 들어간다. 이전 값 `0 3 * * *`은 측정 구간 중 데이터가 가장 많은 시각이면서
    # standard_score_pipeline과 정확히 동시 시작이었다(#508).
    assert module.dag.schedule == "40 8 * * *"
    assert module.dag.catchup is False


def test_dag_contains_one_task_per_gold_table_plus_the_count_report():
    module = _load_dag_module()

    task_ids = {task.task_id for task in module.dag.tasks}
    assert task_ids == {
        "audit_standard_segment_comfort_score",
        "audit_current_segment_comfort_score",
        "report_audit_counts",
    }


def test_audit_tasks_run_one_after_another():
    # 병렬로 두면 이 DAG 혼자서 동시 job run 2건을 만들어 Application 용량을
    # 초과한다 — 08-25 03시에 audit_standard가 두 번 연속 실패했다(#508).
    # 감사 결과는 서로 독립이라 순서 자체는 상관없다.
    module = _load_dag_module()

    standard_task = module.dag.get_task("audit_standard_segment_comfort_score")
    current_task = module.dag.get_task("audit_current_segment_comfort_score")

    assert standard_task.upstream_task_ids == set()
    assert current_task.upstream_task_ids == {"audit_standard_segment_comfort_score"}


def test_report_audit_counts_runs_after_both_audits():
    # 두 audit을 직렬로 이으면서(#508) report의 직접 upstream은 체인의 마지막인
    # audit_current 하나가 됐다. standard -> current -> report 체인이라 report가
    # 두 audit 모두의 뒤에 온다는 성질은 전이적으로 유지된다 — 직접 간선과
    # 전이 관계를 함께 확인한다.
    module = _load_dag_module()

    report_task = module.dag.get_task("report_audit_counts")
    assert isinstance(report_task, PythonOperator)
    assert report_task.python_callable is module._report_audit_counts
    assert report_task.upstream_task_ids == {"audit_current_segment_comfort_score"}

    current_task = module.dag.get_task("audit_current_segment_comfort_score")
    assert current_task.upstream_task_ids == {"audit_standard_segment_comfort_score"}


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


def test_audit_tasks_use_the_serialising_pool_and_the_audit_profile():
    # audit job은 executor를 거의 쓰지 않고(실측: audit_current의 executor 존재
    # 시간 7초) driver가 exit 137로 죽는다 — Great Expectations가
    # gold_audit_validation.py:112의 `SELECT * FROM {table}`로 997,332행을 driver의
    # pandas에 전량 적재한다. driver를 2 vCPU / 16 GB로 올리고 executor는 1대로
    # 줄인다(#508).
    from emr_serverless import EMR_SERVERLESS_POOL

    module = _load_dag_module()

    emr_tasks = [
        task
        for task in module.dag.tasks
        if isinstance(task, EmrServerlessStartJobOperator)
    ]
    assert len(emr_tasks) == 2
    for task in emr_tasks:
        params = task.job_driver["sparkSubmit"]["sparkSubmitParameters"]
        assert task.pool == EMR_SERVERLESS_POOL, task.task_id
        assert "spark.driver.cores=2" in params, task.task_id
        assert "spark.driver.memory=4g" in params, task.task_id
        assert "spark.driver.memoryOverhead=12g" in params, task.task_id
        assert "spark.executor.instances=1" in params, task.task_id
