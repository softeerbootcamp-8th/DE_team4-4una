"""bronze_compaction DAG의 구조를 docker 없이 검증하는 테스트 (#271, ADR-0009).

실제 task 실행(S3 오브젝트 나열/삭제/쓰기)은 로컬 Airflow에서 수동으로 확인한다
(spec의 완료 조건). 여기서는 DAG가 정상 파싱되고, task에 상하위 의존관계가 없으며,
outlet이 없는지를 확인한다.
"""

from __future__ import annotations

import datetime
import importlib.util
import sys
from pathlib import Path

from airflow.providers.standard.operators.python import PythonOperator

DAG_PATH = Path(__file__).resolve().parents[1] / "dags" / "bronze_compaction.py"


def _load_dag_module():
    spec = importlib.util.spec_from_file_location("bronze_compaction", DAG_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_dag_parses_with_expected_schedule():
    module = _load_dag_module()

    assert module.dag.dag_id == "bronze_compaction"
    assert module.dag.schedule == "17 4 * * *"
    assert module.dag.catchup is False


def test_dag_contains_one_task_per_bronze_source():
    module = _load_dag_module()

    task_ids = {task.task_id for task in module.dag.tasks}
    assert task_ids == {"compact_zone_weather_snapshot"}


def test_tasks_have_no_upstream_or_downstream_dependencies():
    module = _load_dag_module()

    for task in module.dag.tasks:
        assert task.upstream_task_ids == set()
        assert task.downstream_task_ids == set()


def test_tasks_have_no_outlets():
    module = _load_dag_module()

    for task in module.dag.tasks:
        assert task.outlets == []


def test_tasks_are_python_operators_calling_the_expected_callables():
    module = _load_dag_module()

    weather_task = module.dag.get_task("compact_zone_weather_snapshot")

    assert isinstance(weather_task, PythonOperator)
    assert weather_task.python_callable is module._compact_zone_weather_snapshot


def test_callables_declare_data_interval_end_so_airflow_injects_it():
    import inspect

    module = _load_dag_module()

    assert (
        "data_interval_end"
        in inspect.signature(module._compact_zone_weather_snapshot).parameters
    )


def test_dag_preserves_retry_policy():
    module = _load_dag_module()

    for task in module.dag.tasks:
        assert task.retries == 1
        assert task.retry_delay == datetime.timedelta(minutes=5)
