"""batch-jobs Spark 커맨드를 EMR Serverless Job Run으로 제출하는 공용 헬퍼 (#292).

standard_score_pipeline과 data_quality_audit(#295)이 이 헬퍼를 공유한다.
Application ID·실행 역할 ARN·entry point는 모두 Airflow Variable로 관리하며,
`AIRFLOW_VAR_*` 환경변수로 주입할 수 있다(`.env.example`, `infra/compose/airflow.yaml`
참고). entry point는 batch-jobs의 EMR Serverless 커스텀 이미지가 준비되기 전까지
플레이스홀더다.
"""

from __future__ import annotations

from typing import Any

from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator

_APPLICATION_ID_TEMPLATE = "{{ var.value.EMR_SERVERLESS_APPLICATION_ID }}"
_EXECUTION_ROLE_ARN_TEMPLATE = "{{ var.value.EMR_SERVERLESS_EXECUTION_ROLE_ARN }}"
_ENTRY_POINT_TEMPLATE = "{{ var.value.BATCH_JOBS_EMR_ENTRY_POINT }}"


def submit_batch_jobs_command(
    task_id: str,
    entry_point_arguments: list[str],
    *,
    driver_env: dict[str, str] | None = None,
    outlets: list[Any] | None = None,
) -> EmrServerlessStartJobOperator:
    """batch-jobs CLI 커맨드 하나를 EMR Serverless Job Run으로 제출하는 task를 만든다.

    `driver_env`는 `spark.emr-serverless.driverEnv.<KEY>` conf로 변환돼 Spark
    driver 프로세스의 환경변수가 된다. **주의**: 여기 넘긴 값은 EMR Serverless
    Job Run 설정에 평문으로 남아 GetJobRun API로 조회 가능하다 — Postgres
    비밀번호처럼 민감한 값을 이 경로로 넘기는 건 임시 방편이며(#292 논의,
    Secrets Manager 도입 전까지), 후속 이슈에서 IAM DB 인증 등으로 교체할 예정이다.
    """
    spark_submit: dict[str, Any] = {
        "entryPoint": _ENTRY_POINT_TEMPLATE,
        "entryPointArguments": entry_point_arguments,
    }
    spark_submit_parameters = " ".join(
        f'--conf spark.emr-serverless.driverEnv.{key}="{value}"'
        for key, value in (driver_env or {}).items()
    )
    if spark_submit_parameters:
        spark_submit["sparkSubmitParameters"] = spark_submit_parameters

    return EmrServerlessStartJobOperator(
        task_id=task_id,
        application_id=_APPLICATION_ID_TEMPLATE,
        execution_role_arn=_EXECUTION_ROLE_ARN_TEMPLATE,
        job_driver={"sparkSubmit": spark_submit},
        name=task_id,
        outlets=outlets or [],
    )
