"""bronze_sensor_events_compaction DAG의 구조 검증 (#585)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from airflow.providers.standard.operators.python import PythonOperator

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
DAG_PATH = DAGS_DIR / "bronze_sensor_events_compaction.py"
sys.path.insert(0, str(DAGS_DIR))


def _load_dag_module():
    spec = importlib.util.spec_from_file_location("bronze_sensor_events_compaction", DAG_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_dag_has_the_expected_schedule_and_single_soft_fail_task():
    module = _load_dag_module()
    from emr_serverless import EMR_SERVERLESS_POOL

    assert module.dag.dag_id == "bronze_sensor_events_compaction"
    assert module.dag.schedule == "47 3 * * *"
    assert module.dag.catchup is False
    assert module.dag.max_active_runs == 1
    task = module.dag.get_task("compact_sensor_events")
    assert isinstance(task, PythonOperator)
    assert task.python_callable is module._compact_sensor_events
    assert task.pool == EMR_SERVERLESS_POOL
    assert task.outlets == []
