"""Bronze sensor-events 소파일을 정리하는 독립 저빈도 DAG (#585)."""

from __future__ import annotations

import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from emr_serverless import EMR_SERVERLESS_POOL
from notifications import (
    on_failure_callback,
    on_retry_callback,
    on_success_callback,
    on_task_success_callback,
)


def _compact_sensor_events(data_interval_end) -> dict:
    from jobs.sensor_events_compaction import (
        SensorEventsCompactionConfig,
        run_sensor_events_compaction,
    )

    summary = run_sensor_events_compaction(
        SensorEventsCompactionConfig.from_env(), data_interval_end
    )
    result = {
        "root_uri": summary.root_uri,
        "compacted_group_count": len(summary.compacted_groups),
        "skipped_group_count": summary.skipped_group_count,
    }
    print(result)
    return result


with DAG(
    dag_id="bronze_sensor_events_compaction",
    description="Bronze(sensor-events) 소파일 정리 — 매일 1회, soft fail",
    schedule="47 3 * * *",
    start_date=datetime.datetime(2026, 8, 26, tzinfo=datetime.UTC),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "JEONGKIJOON",
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=5),
        "on_failure_callback": on_failure_callback,
        "on_retry_callback": on_retry_callback,
        "on_success_callback": on_task_success_callback,
    },
    on_success_callback=on_success_callback,
    tags=["ops", "bronze", "sensor-events"],
) as dag:
    # 압축 결과를 올린 뒤 원본을 지우기 전 Spark가 읽으면 같은 row를 두 번 셀 수 있다
    # hourly EMR 작업과 같은 1-slot pool을 사용해 파일 교체와 읽기를 직렬화한다
    compact_sensor_events = PythonOperator(
        task_id="compact_sensor_events",
        python_callable=_compact_sensor_events,
        pool=EMR_SERVERLESS_POOL,
    )
