# zone_weather_pipeline DAG 구조 검증(실제 task 실행은 로컬에서 수동 확인,
# test_hourly_pipeline_dag.py와 동일 방식). #230: weather_pipeline에서 이름을
# 바꾸고, current 재계산 태스크를 ShortCircuit 게이팅 + Asset 발행으로 교체했다.

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import (
    PythonOperator,
    ShortCircuitOperator,
)
from airflow.timetables.base import TimeRestriction
from airflow.timetables.interval import CronDataIntervalTimetable

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
DAG_PATH = DAGS_DIR / "zone_weather_pipeline.py"

# zone_weather_pipeline.py가 모듈 최상단에서 `from assets import ZONE_WEATHER_ASSET`을
# 쓴다. 실제 Airflow는 dags_folder 전체를 sys.path에 등록해 주지만
# (airflow.dag_processing.dagbag), spec_from_file_location으로 파일 하나만 직접
# 로드하는 이 테스트에서는 그 동작을 흉내내야 한다.
sys.path.insert(0, str(DAGS_DIR))


def _load_dag_module():
    spec = importlib.util.spec_from_file_location("zone_weather_pipeline", DAG_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_dag_parses_with_expected_schedule():
    module = _load_dag_module()

    assert module.dag.dag_id == "zone_weather_pipeline"
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


def test_dag_has_no_current_score_recompute_task():
    module = _load_dag_module()

    task_ids = {task.task_id for task in module.dag.tasks}
    assert task_ids == {
        "run_weather_collection",
        "detect_changed_zones",
        "publish_zone_weather_asset",
    }


def test_dag_collects_then_gates_on_changed_zones():
    module = _load_dag_module()

    collection = module.dag.get_task("run_weather_collection")
    detect = module.dag.get_task("detect_changed_zones")
    publish = module.dag.get_task("publish_zone_weather_asset")

    # 수집이 끝난 뒤에 비교해야 새 impact_signature가 이미 저장된 상태가 된다.
    assert collection.downstream_task_ids == {"detect_changed_zones"}
    assert detect.downstream_task_ids == {"publish_zone_weather_asset"}

    assert isinstance(detect, ShortCircuitOperator)
    assert detect.python_callable is module._has_changed_zones

    assert isinstance(publish, EmptyOperator)


def test_only_the_gated_publish_task_declares_the_zone_weather_outlet():
    module = _load_dag_module()

    detect = module.dag.get_task("detect_changed_zones")
    publish = module.dag.get_task("publish_zone_weather_asset")

    # detect_changed_zones는 조건이 False여도 자기 자신은 SUCCESS로 끝나 하위
    # task만 SKIPPED시킨다. outlets를 여기 두면 변경 zone이 없는 tick에도 매번
    # 이벤트가 발행되므로, outlets는 detect_changed_zones가 SKIPPED시킬 수 있는
    # publish_zone_weather_asset에만 있어야 한다.
    assert detect.outlets == []
    assert publish.outlets == [module.ZONE_WEATHER_ASSET]


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
