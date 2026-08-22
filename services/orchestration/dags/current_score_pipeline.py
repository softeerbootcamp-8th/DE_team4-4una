# current_segment_comfort_score의 유일한 writer DAG(#231, ADR-0007). standard_score_pipeline
# 또는 zone_weather_pipeline이 발행한 Asset에 의해 트리거되며, 어떤 Asset이 트리거했는지에
# 따라 전량/변경-zone 모드를 스스로 결정한다. jobs.current_score.run_from_env(...)를 그대로
# 재사용하고(job 코드 변경 없음), zone_weather_pipeline과 같은 이유로 PythonOperator로 이
# airflow-scheduler 컨테이너 안에서 직접 실행한다(Spark 불필요, docker-outside-of-docker 불필요).

from __future__ import annotations

import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, AssetAny
from assets import ZONE_WEATHER_ASSET
from comfort_score_assets import STANDARD_SCORE_ASSET


def _changed_zones_only(triggering_asset_events) -> bool:
    """트리거한 Asset에 따라 전량/변경-zone 모드를 결정한다.

    STANDARD_SCORE_ASSET이 트리거에 포함되면(전량 재계산이 방금 끝났다는 뜻) 전량 모드로
    맞춰 반영한다. 그렇지 않고 ZONE_WEATHER_ASSET만 있으면(날씨만 바뀜) 변경된 zone만
    재계산한다. 두 Asset이 동시에 트리거에 걸려도(겹친 실행이 max_active_runs=1로 큐잉됐다가
    한 번에 소비되는 경우) 전량 쪽이 변경분을 포함하므로 안전한 쪽을 우선한다.

    `triggering_asset_events`(TriggeringAssetEventsAccessor)는 내부적으로
    `defaultdict(list)`를 감싸고 있어, 트리거하지 않은 Asset을 조회해도 KeyError 없이 빈
    리스트를 돌려준다. 그래서 `in`(멤버십 체크, __contains__)은 __getitem__이 KeyError를
    던지는지로 판단하는데 여기서는 절대 던지지 않아 어떤 Asset을 넣어도 항상 True가
    나온다 — 이 함수가 항상 전량 모드로 고정되는 실제 버그였다(#245에서 로컬 통합
    테스트로 발견). 대신 조회 결과 리스트의 진위값(빈 리스트는 falsy)으로 판단한다.
    `.get(..., [])`을 쓰는 이유는 테스트가 이 accessor 대신 plain dict를 넘기기
    때문이다 — plain dict는 없는 키를 `[]`로 직접 인덱싱하면 KeyError를 던진다.
    """
    return not triggering_asset_events.get(STANDARD_SCORE_ASSET, [])


def _run_current_score(triggering_asset_events) -> None:
    # jobs/current_score.py는 airflow-scheduler 컨테이너에만 마운트되어 있어(dag-processor/
    # webserver는 파싱 시점에 접근 못함), zone_weather_pipeline과 동일하게 task 콜백 안에서
    # 지연 import한다.
    from jobs.current_score import run_from_env

    changed_zones_only = _changed_zones_only(triggering_asset_events)
    summary = run_from_env(changed_zones_only=changed_zones_only)
    print(
        {
            "changed_zones_only": changed_zones_only,
            "zone_count": summary.zone_count,
            "upserted_count": summary.upserted_count,
            "skipped_unzoned_count": summary.skipped_unzoned_count,
            "quarantined_count": summary.quarantined_count,
        }
    )


with DAG(
    dag_id="current_score_pipeline",
    description="current_segment_comfort_score의 유일한 writer — 트리거 Asset에 따라 전량/변경-zone 모드 결정",
    # 정기 cron이 아니라 두 producer 중 하나라도 Asset을 발행하면 깨어난다(ADR-0007).
    schedule=AssetAny(STANDARD_SCORE_ASSET, ZONE_WEATHER_ASSET),
    start_date=datetime.datetime(2026, 8, 20, tzinfo=datetime.UTC),
    catchup=False,
    # 두 producer가 겹쳐 트리거해도 current 쓰기가 동시에 두 번 돌지 않게 한다 — 실행 중인
    # DagRun은 막지 않지만 새 트리거는 큐잉만 되고, 큐잉된 이벤트는 다음 실행에서 한 번에
    # 소비된다(ADR-0007 결과 절).
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=2),
    },
    tags=["current-score-pipeline", "comfort-score"],
) as dag:
    run_current_score = PythonOperator(
        task_id="run_current_score",
        python_callable=_run_current_score,
    )
