# 15분마다 Open-Meteo 날씨를 수집하고, 날씨가 바뀐 zone의 current 점수를 다시 만드는 DAG
# (#207 수집, #217 재계산).
# jobs.weather(orchestration의 lightweight Python job, #209)를 PythonOperator로 직접 실행한다
# — batch-jobs/EMR과 달리 Spark가 필요 없어 docker-outside-of-docker 없이 이 컨테이너에서 바로 돈다.

from __future__ import annotations

import datetime

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.timetables.interval import CronDataIntervalTimetable


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
        }
    )


def _recompute_changed_zone_scores() -> None:
    # 수집이 끝난 직후라, impact_signature가 달라진 zone만 다시 계산한다. 날씨가 그대로인
    # zone의 행은 손대지 않는다(#216 변경 감지).
    from jobs.current_score import run_from_env

    summary = run_from_env(changed_zones_only=True)
    print(
        {
            "zone_count": summary.zone_count,
            "upserted_count": summary.upserted_count,
            "skipped_unzoned_count": summary.skipped_unzoned_count,
        }
    )


with DAG(
    dag_id="weather_pipeline",
    description="Open-Meteo 15분 날씨를 latest_zone_weather에 수집",
    schedule=CronDataIntervalTimetable(
        "*/15 * * * *",
        timezone=pendulum.timezone("UTC"),
    ),
    start_date=datetime.datetime(2026, 8, 19, tzinfo=datetime.UTC),
    catchup=False,
    # 느려진 옛 실행과 새 실행이 겹쳐 latest_zone_weather를 역전시키는 걸 막는다(jobs/weather.py의 WHERE와 이중 방어).
    max_active_runs=1,
    default_args={
        # 15분 주기라 hourly_pipeline의 5분은 너무 길다. retry_delay는 재시도 대기 시간일 뿐 실행 시간 제한이 아니다.
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=2),
    },
    tags=["weather-pipeline"],
) as dag:
    run_weather_collection = PythonOperator(
        task_id="run_weather_collection",
        python_callable=_collect_latest_zone_weather,
    )

    run_changed_zone_recompute = PythonOperator(
        task_id="run_changed_zone_recompute",
        python_callable=_recompute_changed_zone_scores,
    )

    run_weather_collection >> run_changed_zone_recompute
