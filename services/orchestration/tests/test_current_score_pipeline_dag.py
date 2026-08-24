# current_score_pipeline DAG 구조 검증(#231, ADR-0007). 실제 job 실행(psycopg2/road_segment
# 읽기)은 로컬 Airflow에서 수동으로 확인하고(README 참고), 여기서는 DAG가 정상 파싱되는지,
# schedule이 두 Asset의 AssetAny인지, max_active_runs=1인지, 트리거 Asset에 따른 모드 분기
# 로직(_changed_zones_only)이 의도대로 동작하는지를 확인한다.

from __future__ import annotations

import datetime
import importlib.util
import sys
from pathlib import Path

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import AssetAny

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
DAG_PATH = DAGS_DIR / "current_score_pipeline.py"

# current_score_pipeline.py가 모듈 최상단에서 `from assets import ZONE_WEATHER_ASSET`,
# `from comfort_score_assets import STANDARD_SCORE_ASSET`을 쓴다. 실제 Airflow는 dags_folder
# 전체를 sys.path에 등록해 주지만(airflow.dag_processing.dagbag), spec_from_file_location으로
# 파일 하나만 직접 로드하는 이 테스트에서는 그 동작을 흉내내야 한다.
sys.path.insert(0, str(DAGS_DIR))

from assets import ZONE_WEATHER_ASSET
from comfort_score_assets import STANDARD_SCORE_ASSET


def _load_dag_module():
    spec = importlib.util.spec_from_file_location("current_score_pipeline", DAG_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_dag_parses_with_expected_id():
    module = _load_dag_module()

    assert module.dag.dag_id == "current_score_pipeline"
    assert module.dag.catchup is False


def test_dag_schedules_from_both_producer_assets():
    module = _load_dag_module()

    assert isinstance(module.dag.schedule, AssetAny)
    assert set(module.dag.schedule.objects) == {STANDARD_SCORE_ASSET, ZONE_WEATHER_ASSET}


def test_dag_serializes_current_writes():
    module = _load_dag_module()

    # 두 producer가 겹쳐 트리거해도 current 쓰기가 동시에 두 번 돌지 않아야 한다.
    assert module.dag.max_active_runs == 1


def test_dag_has_a_single_python_task():
    module = _load_dag_module()

    task_ids = {task.task_id for task in module.dag.tasks}
    assert task_ids == {"run_current_score"}

    task = module.dag.get_task("run_current_score")
    assert isinstance(task, PythonOperator)
    assert task.python_callable is module._run_current_score


def test_task_declares_no_outlets():
    module = _load_dag_module()

    # current_score_pipeline은 유일한 writer이자 이 3-DAG 구조의 종착점이다 — 다른 DAG를
    # 더 트리거하지 않으므로 outlets가 없어야 한다.
    task = module.dag.get_task("run_current_score")
    assert task.outlets == []


def test_retries_match_zone_weather_pipeline_cadence():
    module = _load_dag_module()

    assert module.dag.default_args["retries"] == 2
    assert module.dag.default_args["retry_delay"] == datetime.timedelta(minutes=2)


def test_standard_score_asset_triggers_full_recompute():
    module = _load_dag_module()

    triggering_asset_events = {STANDARD_SCORE_ASSET: ["event"]}
    assert module._changed_zones_only(triggering_asset_events) is False


def test_zone_weather_asset_alone_triggers_changed_zones_only():
    module = _load_dag_module()

    triggering_asset_events = {ZONE_WEATHER_ASSET: ["event"]}
    assert module._changed_zones_only(triggering_asset_events) is True


def test_both_assets_triggering_prefers_full_recompute():
    module = _load_dag_module()

    # max_active_runs=1로 큐잉된 이벤트가 한 실행에 모여 소비되는 경우 — 전량 쪽이
    # 변경분을 포함하므로 안전한 쪽(전량)을 우선해야 한다.
    triggering_asset_events = {
        STANDARD_SCORE_ASSET: ["event"],
        ZONE_WEATHER_ASSET: ["event"],
    }
    assert module._changed_zones_only(triggering_asset_events) is False


def test_dag_wires_shared_slack_notification_callbacks():
    import notifications

    module = _load_dag_module()

    assert module.dag.default_args["on_failure_callback"] is notifications.on_failure_callback
    assert module.dag.on_success_callback is notifications.on_success_callback
