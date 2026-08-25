"""batch-jobs Spark 커맨드를 EMR Serverless Job Run으로 제출하는 공용 헬퍼 (#292).

standard_score_pipeline과 data_quality_audit(#295)이 이 헬퍼를 공유한다.
Application ID·실행 역할 ARN·entry point는 모두 Airflow Variable로 관리하며,
`AIRFLOW_VAR_*` 환경변수로 주입할 수 있다(`.env.example`, `infra/compose/airflow.yaml`
참고). entry point는 batch-jobs의 EMR Serverless 커스텀 이미지가 준비되기 전까지
플레이스홀더다.

이슈 #432: Job Run 제출뿐 아니라 파이프라인이 끝난 뒤 Application을 명시적으로
내리는 task 팩토리(`check_emr_serverless_is_idle`,
`stop_emr_serverless_application`)도 여기서 함께 제공한다 — Application ID
Variable을 아는 곳이 이 모듈이라 stop 쪽도 같은 자리에 둔다.
"""

from __future__ import annotations

from typing import Any

from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessStartJobOperator,
    EmrServerlessStopApplicationOperator,
)
from airflow.providers.standard.operators.python import ShortCircuitOperator

_APPLICATION_ID_TEMPLATE = "{{ var.value.EMR_SERVERLESS_APPLICATION_ID }}"
_EXECUTION_ROLE_ARN_TEMPLATE = "{{ var.value.EMR_SERVERLESS_EXECUTION_ROLE_ARN }}"
_ENTRY_POINT_TEMPLATE = "{{ var.value.BATCH_JOBS_EMR_ENTRY_POINT }}"

# EMR Serverless Job Run의 driver/executor 로그를 영구 저장할 S3 위치(#409). 지금까지
# monitoringConfiguration이 없어 로그가 EMR 콘솔에 잠깐 노출됐다 사라졌다 — 실패
# 알림(dags/notifications.py)이 참조할 EmrServerlessS3LogsLink XCom도 이 설정이
# 있어야 채워진다. 실패 기록과 같은 관측 버킷을 쓴다(이전 기본값
# s3://de4-emr-serverless-logs/는 실재하지 않는 버킷이었다).
_DEFAULT_EMR_SERVERLESS_LOG_S3_URI = (
    "s3://de4-observability-473551908409-ap-northeast-2-an/emr-serverless/logs/"
)

# infra/compose/airflow.yaml이 이 env var를 항상 선언해서, 호스트 .env에 값이 없으면
# docker compose가 빈 문자열로 채운다 — 그러면 var.value.get의 default가 아니라 그
# 빈 문자열이 그대로 렌더링된다. notifications._failed_tasks_s3_root와 같은 이유로
# `or`를 한 번 더 둔다(#409).
_EMR_SERVERLESS_LOG_S3_URI_TEMPLATE = (
    f"{{{{ var.value.get('EMR_SERVERLESS_LOG_S3_URI', "
    f"'{_DEFAULT_EMR_SERVERLESS_LOG_S3_URI}') "
    f"or '{_DEFAULT_EMR_SERVERLESS_LOG_S3_URI}' }}}}"
)

# batch-jobs 커스텀 이미지는 python3.12 전용 경로에만 설치되는데, base 이미지의
# 기본 python3는 3.9라 PYSPARK_PYTHON을 명시하지 않으면 Spark driver/executor가
# batch_jobs를 못 찾는다(#360). Dockerfile의 ENV와 병행해 모든 Job Run에 강제한다.
_PYSPARK_PYTHON_PATH = "/usr/bin/python3.12"

# Application의 maximumCapacity는 12 vCPU / 48 GB / 200 GB disk이다(#372에서
# 처음 4 vCPU/16 GB로 시작, #471에서 현재 값으로 상향). Spark 기본값(driver
# 4 vCPU/8G, executor 2 vCPU/8G)은 driver 혼자 vCPU 예산을 많이 써버려 executor가
# 못 뜨고 ApplicationMaxCapacityExceededException으로 죽는 문제가 있었다(#372) —
# driver를 작게 고정해 executor가 안정적으로 뜨도록 한다.
_SPARK_DRIVER_CORES = "1"
_SPARK_DRIVER_MEMORY = "2g"
_SPARK_EXECUTOR_CORES = "2"
_SPARK_EXECUTOR_MEMORY = "8g"
# sensor processing을 포함한 batch job 처리 시간 단축을 위해 1 -> 2로 늘렸다(#471).
# dynamic allocation min/max/initial도 반드시 이 값과 동일하게 맞춘다(아래 참고,
# 어긋나면 #372처럼 ApplicationMaxCapacityExceededException이 재발한다).
_SPARK_EXECUTOR_INSTANCES = "2"

# Map Matching(find_segment_candidates의 mapInPandas)은 파티션마다 Python
# worker 프로세스에서 road_segment broadcast(약 17만 건)로 STRtree를 새로
# 만든다. cores=2라 이 무거운 작업이 최대 2개 동시에 뜨는데, JVM heap 밖
# overhead가 이전엔 기본값(~10%, 약 800MB)뿐이라 그 메모리를 못 버티고
# executor가 exit code 137(SIGKILL)로 반복해서 죽었다(#386, 그 시점 EMR Serverless
# Application maximumCapacity 8 vCPU/32 GB 기준으로 적용). executor 총 사용량은
# driver(1c/2g) + executor 2개(각 2c/8g+6g=14g) = 5 vCPU/30 GB로 현재
# maximumCapacity(12 vCPU/48 GB, #471) 안에 여유 있게 들어간다.
_SPARK_EXECUTOR_MEMORY_OVERHEAD = "6g"
_SPARK_DRIVER_MEMORY_OVERHEAD = "6g"

# 기본 disk로 standard_score_pipeline 실행 중 executor의 /tmp가 꽉 차 Job이
# 실패했다(#443). driver+executor 2개 합(20G + 2*60G = 140G)이 Application 최대
# disk(200GB, #471에서 상향) 안에 들어오게 잡는다.
_SPARK_DRIVER_DISK = "20G"
_SPARK_EXECUTOR_DISK = "60G"

# EMR Serverless는 dynamic allocation이 기본 켜져 있어서, 실제 목표 executor 수는
# spark.executor.instances가 아니라 max(dynamicAllocation.initialExecutors,
# minExecutors, executor.instances)로 계산된다. EMR 기본값이 1보다 커서 Spark가
# 계속 여분 executor를 요청하다 ApplicationMaxCapacityExceededException을
# 반복적으로 만나고, 실제 계산은 성공해도 이 이력만으로 Job Run이 FAILED
# 처리되는 것이 확인됐다(#372 재발 조사). min/max/initial을 전부 executor.instances와
# 맞춰 애초에 추가 요청이 발생하지 않게 한다.
_SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS = "2"
_SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS = "2"
_SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS = "2"


def submit_batch_jobs_command(
    task_id: str,
    entry_point_arguments: list[str],
    *,
    driver_env: dict[str, str] | None = None,
    outlets: list[Any] | None = None,
) -> EmrServerlessStartJobOperator:
    """batch-jobs CLI 커맨드 하나를 EMR Serverless Job Run으로 제출하는 task를 만든다.

    모든 Job Run에는 `driver_env` 유무와 무관하게 `PYSPARK_PYTHON` conf가
    driver/executor 양쪽에 강제로 붙는다(#360).

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
    # sparkSubmitParameters는 쉘을 거치지 않고 EMR Serverless API에 문자열
    # 그대로 전달되므로, 값을 따옴표로 감싸면 그 문자가 값의 일부로 들어가
    # 버린다(#368) — 값에 공백이 없는 한 따옴표 없이 그대로 이어붙인다.
    #
    # driverEnv/executorEnv(#360)는 컨테이너 환경변수만 세팅할 뿐이고, EMR
    # Serverless가 local:// entryPoint 스크립트를 실제로 어떤 인터프리터로
    # 부팅할지는 이 환경변수를 참조하지 않는 것으로 실제 Job Run 재현으로
    # 확인됐다(#368 재발 조사 — entryPoint의 site-packages import가 line 1에서
    # 바로 실패). Spark의 SparkSubmit/PythonRunner가 직접 읽는
    # spark.pyspark.python/spark.pyspark.driver.python conf를 병행한다.
    conf_flags = [
        f"--conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON={_PYSPARK_PYTHON_PATH}",
        f"--conf spark.executorEnv.PYSPARK_PYTHON={_PYSPARK_PYTHON_PATH}",
        f"--conf spark.pyspark.python={_PYSPARK_PYTHON_PATH}",
        f"--conf spark.pyspark.driver.python={_PYSPARK_PYTHON_PATH}",
        f"--conf spark.driver.cores={_SPARK_DRIVER_CORES}",
        f"--conf spark.driver.memory={_SPARK_DRIVER_MEMORY}",
        f"--conf spark.driver.memoryOverhead={_SPARK_DRIVER_MEMORY_OVERHEAD}",
        f"--conf spark.emr-serverless.driver.disk={_SPARK_DRIVER_DISK}",
        f"--conf spark.executor.cores={_SPARK_EXECUTOR_CORES}",
        f"--conf spark.executor.memory={_SPARK_EXECUTOR_MEMORY}",
        f"--conf spark.executor.memoryOverhead={_SPARK_EXECUTOR_MEMORY_OVERHEAD}",
        f"--conf spark.emr-serverless.executor.disk={_SPARK_EXECUTOR_DISK}",
        f"--conf spark.executor.instances={_SPARK_EXECUTOR_INSTANCES}",
        f"--conf spark.dynamicAllocation.minExecutors={_SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS}",
        f"--conf spark.dynamicAllocation.maxExecutors={_SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS}",
        f"--conf spark.dynamicAllocation.initialExecutors={_SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS}",
        *(
            f"--conf spark.emr-serverless.driverEnv.{key}={value}"
            for key, value in (driver_env or {}).items()
        ),
    ]
    spark_submit["sparkSubmitParameters"] = " ".join(conf_flags)

    return EmrServerlessStartJobOperator(
        task_id=task_id,
        application_id=_APPLICATION_ID_TEMPLATE,
        execution_role_arn=_EXECUTION_ROLE_ARN_TEMPLATE,
        job_driver={"sparkSubmit": spark_submit},
        # monitoringConfiguration은 job_driver가 아니라 configuration_overrides에
        # 둬야 EMR Serverless가 인식한다(#409 조사 기록 — provider의
        # is_monitoring_in_job_override()가 configuration_overrides를 검사).
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": _EMR_SERVERLESS_LOG_S3_URI_TEMPLATE}
            }
        },
        name=task_id,
        outlets=outlets or [],
    )


def emr_serverless_has_no_running_jobs(application_id: str) -> bool:
    """Application에 아직 끝나지 않은 Job Run이 하나도 없으면 True를 돌려준다(#432).

    `standard_score_pipeline`(hourly)과 `data_quality_audit`(daily 03:00 UTC)이
    같은 Application을 공유하므로, hourly가 끝났다고 무조건 stop을 걸면 audit의
    Job Run을 건드릴 수 있다. EMR Serverless의 StopApplication은
    "All scheduled and running jobs must be completed or cancelled before
    stopping an application"이라 이 상황에서 ValidationException으로 실패하는데,
    그러면 매일 03시대에 stop task가 실패하며 실패 알림만 울린다. 그래서 stop
    앞에 이 확인을 두고, 남의 Job Run이 돌고 있으면 stop을 건너뛰어 기존
    idle timeout(15분)에 맡긴다.

    이 DAG 자신의 Job Run들은 EmrServerlessStartJobOperator가 완료를 기다린 뒤
    성공해야 여기까지 오므로 이미 terminal 상태다 — 즉 여기서 잡히는 건 다른
    DAG의 Job Run이다.
    """
    from airflow.providers.amazon.aws.hooks.emr import EmrServerlessHook

    hook = EmrServerlessHook()
    paginator = hook.conn.get_paginator("list_job_runs")
    running_job_run_ids = [
        job_run["id"]
        for page in paginator.paginate(
            applicationId=application_id,
            states=list(EmrServerlessHook.JOB_INTERMEDIATE_STATES),
        )
        for job_run in page["jobRuns"]
    ]
    # 같은 파일의 다른 PythonOperator들처럼 판단 근거를 Log 탭에 남긴다(#406).
    # Application ID와 Job Run ID는 자격증명이 아니라 로그에 남겨도 안전하다.
    print(
        {
            "application_id": application_id,
            "running_job_run_ids": running_job_run_ids,
            "stop_application": not running_job_run_ids,
        }
    )
    return not running_job_run_ids


def check_emr_serverless_is_idle(task_id: str) -> ShortCircuitOperator:
    """실행 중 Job Run이 없을 때만 downstream(stop task)을 실행시키는 task를 만든다(#432).

    실행 중 Job Run이 있으면 downstream이 failed가 아니라 skipped가 되므로 DAG Run은
    성공으로 남고 실패 알림도 울리지 않는다.
    """
    return ShortCircuitOperator(
        task_id=task_id,
        python_callable=emr_serverless_has_no_running_jobs,
        # op_kwargs는 템플릿 필드라 Application ID Variable이 실행 시점에 렌더링된다.
        op_kwargs={"application_id": _APPLICATION_ID_TEMPLATE},
    )


def stop_emr_serverless_application(task_id: str) -> EmrServerlessStopApplicationOperator:
    """Application을 명시적으로 stop시키는 task를 만든다(#432).

    autoStopConfiguration의 idle timeout(15분)을 다 기다리지 않고 바로 내려서
    유휴 과금을 줄인다. idle timeout 자체는 그대로 두어, 이 task까지 오지 못한
    실패 실행에서는 기존대로 안전망 역할을 하게 한다.

    `force_stop`은 기본값 False를 유지한다 — True면 다른 DAG의 Job Run까지
    취소해버린다. 앞단 `check_emr_serverless_is_idle`이 이미 실행 중 Job Run이
    없음을 확인했으므로 취소할 대상도 없다.

    waiter는 기본값(60초 × 25회 = 25분) 대신 15초 × 20회(최대 5분)로 줄인다.
    STARTED에서 STOPPED까지는 보통 1분 안에 끝나므로, 5분을 넘기면 기다리기보다
    실패로 드러내는 편이 낫다.
    """
    return EmrServerlessStopApplicationOperator(
        task_id=task_id,
        application_id=_APPLICATION_ID_TEMPLATE,
        wait_for_completion=True,
        waiter_delay=15,
        waiter_max_attempts=20,
    )
