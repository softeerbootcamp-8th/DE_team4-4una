"""Bronze 계층 zone_weather_snapshot 소파일 정리 DAG (#271, ADR-0009).

sensor-events는 범위에서 제외됐다 — jobs/bronze_compaction.py 모듈 docstring과
ADR-0009 대안 참고(Spark FileStreamSink의 `_spark_metadata` 커밋 로그 때문에
제자리 압축이 안전하지 않음). `data_quality_audit`(#253, ADR-0004)와 같은
성격의 완전히 독립된 저빈도 유지보수 DAG다 — outlet이 없어 다른 DAG를 깨우거나
막지 않고, task가 실패해도(row count 불일치 등) 다른 DAG를 막지 않는다.

standard_score_pipeline이 매시 정각(`0 * * * *`)에 Bronze를 읽으므로, 이 DAG는
정각을 피해 스케줄한다 — 압축 중(원본 삭제~최종 키 쓰기 사이) 동시에 도는
리더가 겹칠 확률을 줄이기 위함이다(완전한 동시성 보장은 아님 — ADR-0009 결과
참고).

jobs.bronze_compaction은 pyarrow+boto3만 쓰고 Spark가 필요 없어, zone_weather_pipeline과
같은 방식으로 docker-outside-of-docker 없이 이 컨테이너(airflow-scheduler) 안에서
바로 PythonOperator로 실행한다.
"""

from __future__ import annotations

import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from notifications import (
    on_failure_callback,
    on_retry_callback,
    on_success_callback,
    on_task_success_callback,
)


def _compact_zone_weather_snapshot(data_interval_end) -> dict:
    from jobs.bronze_compaction import (
        BronzeCompactionConfig,
        run_zone_weather_snapshot_compaction,
    )

    config = BronzeCompactionConfig.from_env()
    summary = run_zone_weather_snapshot_compaction(config, data_interval_end)
    result = {
        "root_uri": summary.root_uri,
        "compacted_group_count": len(summary.compacted_groups),
        "skipped_group_count": summary.skipped_group_count,
    }
    print(result)
    return result


with DAG(
    dag_id="bronze_compaction",
    description="Bronze(zone_weather_snapshot) 소파일 정리 — 매일 1회, soft fail",
    schedule="17 4 * * *",
    start_date=datetime.datetime(2026, 8, 20, tzinfo=datetime.UTC),
    catchup=False,
    # concurrent DAG run으로 같은 그룹이 중복 압축되는 걸 막는다(PR #280 리뷰).
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=5),
        "on_failure_callback": on_failure_callback,
        # 1차 실패를 즉시 알리고, 이후 전개는 그 알림의 스레드에 이어 붙인다.
        "on_retry_callback": on_retry_callback,
        # 재시도 끝에 성공했을 때만 스레드에 복구를 알린다(task 단위 — DAG 단위
        # on_success_callback과 별개다).
        "on_success_callback": on_task_success_callback,
    },
    on_success_callback=on_success_callback,
    tags=["ops", "weather"],
) as dag:
    compact_zone_weather_snapshot = PythonOperator(
        task_id="compact_zone_weather_snapshot",
        python_callable=_compact_zone_weather_snapshot,
    )
