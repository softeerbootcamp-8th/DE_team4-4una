"""at-rest Gold 감시용 신규 DAG (#253, ADR-0004 롤아웃 ④번).

`standard_segment_comfort_score`/`current_segment_comfort_score`(Postgres)
전체 범위·freshness·`vehicle_profile_id` 참조 무결성을 매일 1회 독립
스케줄로 감사한다. in-flight 검증(#220, #249)과 달리 파이프라인 게이트가
아니라 완전히 독립된 DAG라 outlet이 없고, task가 실패해도 다른 DAG를
막지 않는다(soft fail — ADR-0004: "task 실패로 신호만 주고 다른 DAG는 막지
않음").

## 로컬 실행 방식 (임시, EMR Serverless 전환 시 사라짐)

`standard_score_pipeline.py`와 동일하게 BashOperator로 batch-jobs 컨테이너를
docker-outside-of-docker로 띄운다(`infra/compose/airflow.yaml`의 docker
socket 마운트 필요). local-lake 마운트는 필요 없다 — Postgres만 조회한다.
"""

from __future__ import annotations

import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

_AUDIT_GOLD_ENV_FLAGS = (
    "-e POSTGRES_HOST -e POSTGRES_PORT -e POSTGRES_DB -e POSTGRES_USER -e POSTGRES_PASSWORD "
    "-e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_REGION -e GOLD_AUDIT_S3_BUCKET "
)


def _audit_gold_bash_command(table: str) -> str:
    return (
        "docker run --rm --network de4-local "
        + _AUDIT_GOLD_ENV_FLAGS
        + "batch-jobs:${BATCH_JOBS_IMAGE_TAG:?BATCH_JOBS_IMAGE_TAG must be set} "
        "uv run --no-sync --package batch-jobs batch-jobs "
        f"audit-gold --table={table}"
    )


with DAG(
    dag_id="data_quality_audit",
    description="Gold(standard/current_segment_comfort_score) at-rest 품질 감시 — 매일 1회, soft fail",
    schedule="0 3 * * *",
    start_date=datetime.datetime(2026, 8, 20, tzinfo=datetime.UTC),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=5),
    },
    tags=["data-quality-audit", "comfort-score"],
) as dag:
    # 두 task는 서로 독립이라(의존관계 없음) 병렬로 실행된다. outlet이 없어
    # 이 DAG의 성공/실패는 어떤 다른 DAG도 깨우거나 막지 않는다.
    audit_standard_segment_comfort_score = BashOperator(
        task_id="audit_standard_segment_comfort_score",
        bash_command=_audit_gold_bash_command("standard_segment_comfort_score"),
    )
    audit_current_segment_comfort_score = BashOperator(
        task_id="audit_current_segment_comfort_score",
        bash_command=_audit_gold_bash_command("current_segment_comfort_score"),
    )
