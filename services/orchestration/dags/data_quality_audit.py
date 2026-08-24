"""at-rest Gold 감시용 신규 DAG (#253, ADR-0004 롤아웃 ④번).

`standard_segment_comfort_score`/`current_segment_comfort_score`(Postgres)
전체 범위·freshness·`vehicle_profile_id` 참조 무결성을 매일 1회 독립
스케줄로 감사한다. in-flight 검증(#220, #249)과 달리 파이프라인 게이트가
아니라 완전히 독립된 DAG라 outlet이 없고, task가 실패해도 다른 DAG를
막지 않는다(soft fail — ADR-0004: "task 실패로 신호만 주고 다른 DAG는 막지
않음").

## EMR Serverless 실행 방식 (#295, ADR-0001)

standard_score_pipeline(#292)과 같은 공용 헬퍼(`emr_serverless.
submit_batch_jobs_command`)로, host의 docker socket을 마운트해 `docker run`으로
batch-jobs 컨테이너를 직접 띄우던(docker-outside-of-docker) 방식에서 미리
만들어진 EMR Serverless Application에 Job Run을 제출하는 방식으로 바뀌었다.
`audit-gold` CLI는 `--table` 외 옵션이 없어, Postgres 자격증명과
`GOLD_AUDIT_S3_BUCKET`은 driver_env로 넘긴다(standard_score_pipeline의 임시
방편과 동일한 이유 — GetJobRun API로 평문 조회 가능, #292 논의). AWS
access key는 넘기지 않고 EMR Serverless의 execution role(IAM)에 위임한다 —
GX Data Docs를 올리는 데 필요한 S3 PutObject 권한은 role 쪽에 미리 부여돼
있어야 한다(#295 논의).
"""

from __future__ import annotations

import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from emr_serverless import submit_batch_jobs_command
from notifications import on_failure_callback, on_success_callback

# standard_score_pipeline.py의 _POSTGRES_DRIVER_ENV와 같은 내용이다(#292). 그
# 파일은 이 이슈(#295)의 제외 범위라 공유 모듈로 뽑지 않고 그대로 복제한다.
_POSTGRES_DRIVER_ENV = {
    "POSTGRES_HOST": "{{ var.value.POSTGRES_HOST }}",
    "POSTGRES_PORT": "{{ var.value.POSTGRES_PORT }}",
    "POSTGRES_DB": "{{ var.value.POSTGRES_DB }}",
    "POSTGRES_USER": "{{ var.value.POSTGRES_USER }}",
    "POSTGRES_PASSWORD": "{{ var.value.POSTGRES_PASSWORD }}",
}

# audit-gold CLI는 --table 외 옵션이 없어 GOLD_AUDIT_S3_BUCKET도 driver_env로
# 넘긴다. 값이 없으면 batch-jobs 쪽 기본값(de4-data-quality-docs)과 동일한
# 값으로 fallback한다.
_GOLD_AUDIT_S3_BUCKET = (
    "{{ var.value.get('GOLD_AUDIT_S3_BUCKET', 'de4-data-quality-docs') }}"
)


def _audit_gold_driver_env() -> dict[str, str]:
    return {**_POSTGRES_DRIVER_ENV, "GOLD_AUDIT_S3_BUCKET": _GOLD_AUDIT_S3_BUCKET}


def _report_audit_counts() -> dict:
    import psycopg2
    from jobs.pipeline_counts import PostgresConfig, count_audit_gold_tables

    config = PostgresConfig.from_env()
    connection = psycopg2.connect(**config.as_connect_kwargs())
    try:
        counts = count_audit_gold_tables(connection=connection)
    finally:
        connection.close()
    print(counts)
    return counts


with DAG(
    dag_id="data_quality_audit",
    description="Gold(standard/current_segment_comfort_score) at-rest 품질 감시 — 매일 1회, soft fail",
    schedule="0 3 * * *",
    start_date=datetime.datetime(2026, 8, 20, tzinfo=datetime.UTC),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=5),
        "on_failure_callback": on_failure_callback,
    },
    on_success_callback=on_success_callback,
    tags=["data-quality-audit", "comfort-score"],
) as dag:
    # 두 task는 서로 독립이라(의존관계 없음) 병렬로 실행된다. outlet이 없어
    # 이 DAG의 성공/실패는 어떤 다른 DAG도 깨우거나 막지 않는다.
    audit_standard_segment_comfort_score = submit_batch_jobs_command(
        task_id="audit_standard_segment_comfort_score",
        entry_point_arguments=[
            "audit-gold",
            "--table=standard_segment_comfort_score",
        ],
        driver_env=_audit_gold_driver_env(),
    )
    audit_current_segment_comfort_score = submit_batch_jobs_command(
        task_id="audit_current_segment_comfort_score",
        entry_point_arguments=[
            "audit-gold",
            "--table=current_segment_comfort_score",
        ],
        driver_env=_audit_gold_driver_env(),
    )

    report_audit_counts = PythonOperator(
        task_id="report_audit_counts",
        python_callable=_report_audit_counts,
    )
    [
        audit_standard_segment_comfort_score,
        audit_current_segment_comfort_score,
    ] >> report_audit_counts
