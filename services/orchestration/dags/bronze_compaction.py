"""Bronze 계층(sensor-events, zone_weather_snapshot) 소파일 정리 DAG (#271, ADR-0009).

`data_quality_audit`(#253, ADR-0004)와 같은 성격의 완전히 독립된 저빈도 유지보수
DAG다 — outlet이 없어 다른 DAG를 깨우거나 막지 않고, task가 실패해도(row count
불일치 등) 다른 DAG를 막지 않는다. 두 대상은 서로 의존관계가 없어 병렬로 돈다.

jobs.bronze_compaction은 pyarrow+boto3만 쓰고 Spark가 필요 없어, zone_weather_pipeline과
같은 방식으로 docker-outside-of-docker 없이 이 컨테이너(airflow-scheduler) 안에서
바로 PythonOperator로 실행한다.
"""

from __future__ import annotations

import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def _compact_sensor_events(data_interval_end) -> None:
    from jobs.bronze_compaction import (
        BronzeCompactionConfig,
        run_sensor_events_compaction,
    )

    config = BronzeCompactionConfig.from_env()
    summary = run_sensor_events_compaction(config, data_interval_end)
    print(
        {
            "root_uri": summary.root_uri,
            "compacted_group_count": len(summary.compacted_groups),
            "skipped_group_count": summary.skipped_group_count,
        }
    )


def _compact_zone_weather_snapshot(data_interval_end) -> None:
    from jobs.bronze_compaction import (
        BronzeCompactionConfig,
        run_zone_weather_snapshot_compaction,
    )

    config = BronzeCompactionConfig.from_env()
    summary = run_zone_weather_snapshot_compaction(config, data_interval_end)
    print(
        {
            "root_uri": summary.root_uri,
            "compacted_group_count": len(summary.compacted_groups),
            "skipped_group_count": summary.skipped_group_count,
        }
    )


with DAG(
    dag_id="bronze_compaction",
    description="Bronze(sensor-events/zone_weather_snapshot) 소파일 정리 — 매일 1회, soft fail",
    schedule="0 4 * * *",
    start_date=datetime.datetime(2026, 8, 20, tzinfo=datetime.UTC),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=5),
    },
    tags=["bronze-compaction"],
) as dag:
    compact_sensor_events = PythonOperator(
        task_id="compact_sensor_events",
        python_callable=_compact_sensor_events,
    )
    compact_zone_weather_snapshot = PythonOperator(
        task_id="compact_zone_weather_snapshot",
        python_callable=_compact_zone_weather_snapshot,
    )
