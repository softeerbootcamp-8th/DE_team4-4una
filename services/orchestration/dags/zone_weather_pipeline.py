# 15분마다 Open-Meteo 날씨를 수집하고, 재연산이 필요한 zone이 있는지 감시하는 DAG
# (#207 수집, #230 zone_weather_pipeline으로 개편 + 게이팅). current 재계산은 이 DAG의
# 책임이 아니다 — current_score_pipeline(#231, ADR-0007)이 이 DAG가 발행하는
# ZONE_WEATHER_ASSET 이벤트를 받아 변경된 zone만 다시 계산한다.
# jobs.weather(orchestration의 lightweight Python job, #209)를 PythonOperator로 직접 실행한다
# — batch-jobs/EMR과 달리 Spark가 필요 없어 docker-outside-of-docker 없이 이 컨테이너에서 바로 돈다.

from __future__ import annotations

import datetime

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sdk import DAG
from airflow.timetables.interval import CronDataIntervalTimetable

from assets import ZONE_WEATHER_ASSET


def _collect_latest_zone_weather(data_interval_end) -> None:
    import psycopg2
    from jobs.weather import LatestZoneWeatherJobConfig, run_latest_zone_weather_job

    config = LatestZoneWeatherJobConfig.from_env()
    connection = psycopg2.connect(
        host=config.postgres_host,
        port=config.postgres_port,
        dbname=config.postgres_db,
        user=config.postgres_user,
        password=config.postgres_password,
    )
    try:
        summary = run_latest_zone_weather_job(config, data_interval_end, connection)
    finally:
        connection.close()
    print(
        {
            "requested_zone_count": summary.requested_zone_count,
            "collected_count": summary.collected_count,
            "failed_zone_count": summary.failed_zone_count,
            "snapshot_uri": summary.snapshot_uri,
        }
    )


def _has_changed_zones() -> bool:
    # jobs/current_score.py의 기존 find_changed_zones()를 그대로 재사용한다 — 판정
    # 로직을 여기서 새로 만들지 않는다(#230). 수집 태스크 바로 다음이라, 비교 시점에는
    # impact_signature가 이미 새로 갱신되어 있다.
    import psycopg2
    from jobs.current_score import find_changed_zones
    from jobs.weather import LatestZoneWeatherJobConfig

    config = LatestZoneWeatherJobConfig.from_env()
    connection = psycopg2.connect(
        host=config.postgres_host,
        port=config.postgres_port,
        dbname=config.postgres_db,
        user=config.postgres_user,
        password=config.postgres_password,
    )
    try:
        zones = find_changed_zones(connection)
    finally:
        connection.close()
    print({"changed_zone_count": len(zones)})
    return bool(zones)


with DAG(
    dag_id="zone_weather_pipeline",
    description="Open-Meteo 15분 날씨를 latest_zone_weather에 수집하고 변경 zone을 감시",
    schedule=CronDataIntervalTimetable(
        "*/15 * * * *",
        timezone=pendulum.timezone("UTC"),
    ),
    start_date=datetime.datetime(2026, 8, 19, tzinfo=datetime.UTC),
    catchup=False,
    # 느려진 옛 실행과 새 실행이 겹쳐 latest_zone_weather를 역전시키는 걸 막는다(jobs/weather.py의 WHERE와 이중 방어).
    max_active_runs=1,
    default_args={
        # 15분 주기라 hourly_pipeline의 5분 재시도 간격은 너무 길다. retry_delay는 재시도 대기 시간일 뿐 실행 시간 제한이 아니다.
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=2),
    },
    tags=["zone-weather-pipeline"],
) as dag:
    run_weather_collection = PythonOperator(
        task_id="run_weather_collection",
        python_callable=_collect_latest_zone_weather,
    )

    detect_changed_zones = ShortCircuitOperator(
        task_id="detect_changed_zones",
        python_callable=_has_changed_zones,
    )

    # ShortCircuitOperator는 조건이 False여도 자기 자신은 SUCCESS로 끝나고 하위
    # task만 SKIPPED로 건너뛴다. Airflow는 TaskInstance가 SUCCESS로 끝나는 순간
    # 그 task에 선언된 outlets를 무조건 이벤트로 등록하므로(models/taskinstance.py
    # register_asset_changes_in_db), outlets를 detect_changed_zones에 직접 붙이면
    # 변경 zone이 없는 tick에도 매번 이벤트가 발행된다. 그래서 발행 전용 하위 task를
    # 두고 거기에만 outlets를 붙인다 — 이 task는 변경 zone이 없으면 SKIPPED로 끝나
    # 이벤트가 등록되지 않고, 있을 때만 SUCCESS로 끝나 이벤트를 등록한다.
    publish_zone_weather_asset = EmptyOperator(
        task_id="publish_zone_weather_asset",
        outlets=[ZONE_WEATHER_ASSET],
    )

    run_weather_collection >> detect_changed_zones >> publish_zone_weather_asset
