# weather_pipeline DAG 구조 검증(실제 task 실행은 로컬에서 수동 확인, test_hourly_pipeline_dag.py와 동일 방식).

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.timetables.base import TimeRestriction
from airflow.timetables.interval import CronDataIntervalTimetable

DAG_PATH = Path(__file__).resolve().parents[1] / "dags" / "zone_weather_pipeline.py"


def _load_dag_module():
    spec = importlib.util.spec_from_file_location("weather_pipeline", DAG_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_dag_parses_with_expected_schedule():
    module = _load_dag_module()

    assert module.dag.dag_id == "weather_pipeline"
    assert isinstance(module.dag.timetable, CronDataIntervalTimetable)
    assert module.dag.timetable.summary == "*/15 * * * *"
    assert module.dag.catchup is False
    # 느려진 옛 실행이 새 실행과 겹쳐 latest_zone_weather를 역전시키지 않도록 한다.
    assert module.dag.max_active_runs == 1


def test_dag_uses_a_15_minute_utc_data_interval():
    module = _load_dag_module()

    run_info = module.dag.timetable.next_dagrun_info(
        last_automated_data_interval=None,
        restriction=TimeRestriction(
            earliest=pendulum.datetime(2026, 8, 19, 10, 0, tz="UTC"),
            latest=pendulum.datetime(2026, 8, 19, 10, 30, tz="UTC"),
            catchup=True,
        ),
    )

    assert run_info is not None
    assert run_info.data_interval.start == pendulum.datetime(2026, 8, 19, 10, 0, tz="UTC")
    assert run_info.data_interval.end == pendulum.datetime(2026, 8, 19, 10, 15, tz="UTC")


def test_dag_collects_then_recomputes_changed_zones():
    module = _load_dag_module()

    task_ids = {task.task_id for task in module.dag.tasks}
    assert task_ids == {"run_weather_collection", "run_changed_zone_recompute"}

    collection = module.dag.get_task("run_weather_collection")
    recompute = module.dag.get_task("run_changed_zone_recompute")

    # 수집이 끝난 뒤에 비교해야 새 impact_signature가 이미 저장된 상태가 된다.
    assert collection.downstream_task_ids == {"run_changed_zone_recompute"}
    assert isinstance(recompute, PythonOperator)
    assert recompute.python_callable is module._recompute_changed_zone_scores


def test_run_weather_collection_is_a_python_task_calling_the_collector():
    module = _load_dag_module()

    task = module.dag.get_task("run_weather_collection")
    assert isinstance(task, PythonOperator)
    assert task.python_callable is module._collect_latest_zone_weather


def test_collector_declares_data_interval_end_so_airflow_injects_it():
    import inspect

    module = _load_dag_module()

    parameters = inspect.signature(module._collect_latest_zone_weather).parameters
    assert "data_interval_end" in parameters


def test_retries_faster_than_the_hourly_pipeline():
    module = _load_dag_module()

    # 15분 주기라 hourly_pipeline의 5분 재시도 간격은 너무 길다(주기의 1/3).
    assert module.dag.default_args["retries"] == 2
    assert module.dag.default_args["retry_delay"] == pendulum.duration(minutes=2)
