"""data_quality_audit DAG의 구조를 docker 없이 검증하는 테스트 (#253).

실제 task 실행(batch-jobs 컨테이너 기동, S3 업로드)은 로컬 Airflow에서
수동으로 확인한다(spec의 완료 조건). 여기서는 DAG가 정상 파싱되고, 두
task가 서로 독립적(병렬)이며, outlet이 없는지를 확인한다.
"""

from __future__ import annotations

import datetime
import importlib.util
import sys
from pathlib import Path

DAG_PATH = Path(__file__).resolve().parents[1] / "dags" / "data_quality_audit.py"


def _load_dag_module():
    spec = importlib.util.spec_from_file_location("data_quality_audit", DAG_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_dag_parses_with_expected_schedule():
    module = _load_dag_module()

    assert module.dag.dag_id == "data_quality_audit"
    assert module.dag.schedule == "0 3 * * *"
    assert module.dag.catchup is False


def test_dag_contains_one_task_per_gold_table():
    module = _load_dag_module()

    task_ids = {task.task_id for task in module.dag.tasks}
    assert task_ids == {
        "audit_standard_segment_comfort_score",
        "audit_current_segment_comfort_score",
    }


def test_tasks_have_no_upstream_or_downstream_dependencies():
    module = _load_dag_module()

    for task in module.dag.tasks:
        assert task.upstream_task_ids == set()
        assert task.downstream_task_ids == set()


def test_tasks_have_no_outlets():
    module = _load_dag_module()

    for task in module.dag.tasks:
        assert task.outlets == []


def test_audit_standard_task_targets_standard_table():
    module = _load_dag_module()

    task = module.dag.get_task("audit_standard_segment_comfort_score")
    assert "audit-gold" in task.bash_command
    assert "--table=standard_segment_comfort_score" in task.bash_command


def test_audit_current_task_targets_current_table():
    module = _load_dag_module()

    task = module.dag.get_task("audit_current_segment_comfort_score")
    assert "audit-gold" in task.bash_command
    assert "--table=current_segment_comfort_score" in task.bash_command


def test_tasks_pass_postgres_and_aws_env_vars():
    module = _load_dag_module()

    for task_id in (
        "audit_standard_segment_comfort_score",
        "audit_current_segment_comfort_score",
    ):
        command = module.dag.get_task(task_id).bash_command
        for env_var in (
            "POSTGRES_HOST",
            "POSTGRES_PORT",
            "POSTGRES_DB",
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_REGION",
            "GOLD_AUDIT_S3_BUCKET",
        ):
            assert env_var in command


def test_dag_preserves_retry_policy():
    module = _load_dag_module()

    for task in module.dag.tasks:
        assert task.retries == 1
        assert task.retry_delay == datetime.timedelta(minutes=5)
