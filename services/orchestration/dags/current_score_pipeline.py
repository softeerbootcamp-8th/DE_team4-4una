# current_segment_comfort_score의 유일한 writer DAG(#231, ADR-0007). standard_score_pipeline/
# zone_weather_pipeline이 발행한 Asset에 따라 전량/변경-zone 모드를 스스로 결정하고,
# jobs.current_score.run_from_env()를 PythonOperator로 직접 실행한다(Spark 불필요).

from __future__ import annotations

import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, AssetAny
from assets import ZONE_WEATHER_ASSET
from comfort_score_assets import STANDARD_SCORE_ASSET
from notifications import (
    on_failure_callback,
    on_retry_callback,
    on_success_callback,
    on_task_success_callback,
)


def _changed_zones_only(triggering_asset_events) -> bool:
    """STANDARD_SCORE_ASSET이 트리거했으면 전량 모드, ZONE_WEATHER_ASSET만 트리거했으면
    변경 zone만 재계산한다. 둘 다 트리거되면 전량 쪽을 우선한다.

    `in`은 쓰지 않는다 — TriggeringAssetEventsAccessor는 KeyError 없이 항상 빈 리스트를
    돌려주는 defaultdict라 `in`이 항상 True가 된다(#245에서 실제로 발견된 버그). 대신
    조회 결과의 truthiness로 판단하고, plain dict를 넘기는 테스트와도 맞도록
    `.get(..., [])`을 쓴다.
    """
    return not triggering_asset_events.get(STANDARD_SCORE_ASSET, [])


def _compute_current_score(triggering_asset_events) -> dict:
    # dag-processor/webserver에는 마운트되지 않아 task 콜백 안에서 지연 import한다.
    from jobs.current_score import run_from_env

    changed_zones_only = _changed_zones_only(triggering_asset_events)
    summary = run_from_env(changed_zones_only=changed_zones_only)
    result = {
        "changed_zones_only": changed_zones_only,
        "zone_count": summary.zone_count,
        "upserted_count": summary.upserted_count,
        "skipped_unzoned_count": summary.skipped_unzoned_count,
        "quarantined_count": summary.quarantined_count,
    }
    print(result)
    return result


with DAG(
    dag_id="current_score_pipeline",
    description="current_segment_comfort_score의 유일한 writer — 트리거 Asset에 따라 전량/변경-zone 모드 결정",
    # 정기 cron이 아니라 두 producer 중 하나라도 Asset을 발행하면 깨어난다(ADR-0007).
    schedule=AssetAny(STANDARD_SCORE_ASSET, ZONE_WEATHER_ASSET),
    start_date=datetime.datetime(2026, 8, 20, tzinfo=datetime.UTC),
    catchup=False,
    # 두 producer가 겹쳐 트리거해도 동시에 두 번 쓰지 않게 한다(ADR-0007).
    max_active_runs=1,
    default_args={
        # UI 소유자 표시. config/dag_owners.yaml의 이름과 맞춘다.
        "owner": "RYUJIYOON",
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=2),
        "on_failure_callback": on_failure_callback,
        # 1차 실패를 즉시 알리고, 이후 전개는 그 알림의 스레드에 이어 붙인다.
        "on_retry_callback": on_retry_callback,
        # 재시도 끝에 성공했을 때만 스레드에 복구를 알린다(task 단위 — DAG 단위
        # on_success_callback과 별개다).
        "on_success_callback": on_task_success_callback,
    },
    on_success_callback=on_success_callback,
    tags=["pipeline", "comfort-score"],
) as dag:
    compute_current_score = PythonOperator(
        task_id="compute_current_score",
        python_callable=_compute_current_score,
    )
