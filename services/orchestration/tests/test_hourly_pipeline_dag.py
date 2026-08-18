"""hourly_pipeline DAG의 구조를 docker 없이 검증하는 테스트.

실제 task 실행(batch-jobs 컨테이너 기동)은 로컬 Airflow에서 수동으로 확인하고,
여기서는 DAG가 정상 파싱되는지와 cleanse TaskGroup의 골격이 의도대로
구성됐는지만 확인한다.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

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
    assert module.dag.schedule == "0 * * * *"
    assert module.dag.catchup is False


def test_cleanse_task_group_contains_only_run_cleanse():
    module = _load_dag_module()

    task_ids = {task.task_id for task in module.dag.tasks}
    assert "cleanse.run_cleanse" in task_ids
    assert len(task_ids) == 1
