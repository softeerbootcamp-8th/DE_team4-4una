"""hourly_pipeline DAG의 구조를 docker 없이 검증하는 테스트.

실제 task 실행(batch-jobs 컨테이너 기동)은 로컬 Airflow에서 수동으로 확인하고,
여기서는 DAG가 정상 파싱되는지와 cleanse/scoring TaskGroup의 골격이 의도대로
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


def test_scoring_task_group_contains_only_run_scoring():
    module = _load_dag_module()

    task_ids = {task.task_id for task in module.dag.tasks}
    assert "scoring.run_scoring" in task_ids


def test_run_scoring_invokes_score_hourly_comfort_with_templated_run_id():
    module = _load_dag_module()

    run_scoring = module.dag.get_task("scoring.run_scoring")
    assert "score-hourly-comfort" in run_scoring.bash_command
    assert "--run-id={{ run_id }}" in run_scoring.bash_command


def test_dag_contains_only_cleanse_and_scoring_tasks_so_far():
    # features/publish TaskGroup이 추가되면 이 집합도 함께 넓어져야 한다(#157 후속 이슈).
    module = _load_dag_module()

    task_ids = {task.task_id for task in module.dag.tasks}
    assert task_ids == {"cleanse.run_cleanse", "scoring.run_scoring"}
