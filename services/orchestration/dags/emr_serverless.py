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

from dataclasses import dataclass, replace
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

# standard_score_pipeline과 data_quality_audit이 같은 Application을 공유하므로 job run
# 제출을 pool로 직렬화한다(#508). 겹치면 뒤에 온 job run이 executor를 못 받고
# ApplicationMaxCapacityExceededException을 반복하다 FAILED가 된다 — 실측으로 한
# job run이 10분 12초를 굶었다. EmrServerlessStartJobOperator는 deferrable이 아니라
# job run이 끝날 때까지 slot을 쥐고 있어, pool 하나로 DAG 간 직렬화가 성립한다.
#
# pool은 Airflow DB 객체라 DAG 코드로 선언할 수 없다 — infra/compose/airflow.yaml의
# airflow-init이 만들고, 나머지 컨테이너가 그 서비스에 의존하므로 DAG가 돌기 전에
# 반드시 존재한다.
EMR_SERVERLESS_POOL = "emr_serverless"


@dataclass(frozen=True)
class SparkResourceProfile:
    """job run 하나가 요청할 driver/executor 크기 (#508).

    EMR Serverless는 Application이 워커를 공유하지 않고 job run마다 전용
    driver·executor 세트를 새로 띄운다. 그래서 job의 성격에 따라 크기를 다르게
    요청할 수 있고, job run 개수가 그대로면 cold start 총량도 그대로다
    (프로비저닝 실측 84~91초는 워커 크기·Application 상태와 무관하게 일정하다).

    worker 하나의 실제 메모리는 `memory + memoryOverhead`이고 이 값이 과금과
    maximumCapacity 계산에 들어간다 — 경합 없는 job run의 실측
    GB-h/vCPU-h 7.20~7.22가 이를 확인해 준다.
    """

    driver_cores: str
    driver_memory: str
    driver_memory_overhead: str
    driver_disk: str
    executor_cores: str
    executor_memory: str
    executor_memory_overhead: str
    executor_disk: str
    executor_instances: str

    def conf_flags(self) -> list[str]:
        return [
            f"--conf spark.driver.cores={self.driver_cores}",
            f"--conf spark.driver.memory={self.driver_memory}",
            f"--conf spark.driver.memoryOverhead={self.driver_memory_overhead}",
            f"--conf spark.emr-serverless.driver.disk={self.driver_disk}",
            f"--conf spark.executor.cores={self.executor_cores}",
            f"--conf spark.executor.memory={self.executor_memory}",
            f"--conf spark.executor.memoryOverhead={self.executor_memory_overhead}",
            f"--conf spark.emr-serverless.executor.disk={self.executor_disk}",
            f"--conf spark.executor.instances={self.executor_instances}",
            # EMR Serverless는 dynamic allocation이 기본 켜져 있어 실제 목표
            # executor 수가 max(initialExecutors, minExecutors, executor.instances)로
            # 계산된다. EMR 기본값이 이보다 크면 Spark가 여분 executor를 계속
            # 요청하다 ApplicationMaxCapacityExceededException을 반복하고, 실제
            # 계산이 성공해도 Job Run이 FAILED로 판정된다(#372). 셋을 instances에서
            # 파생시켜 값이 어긋나는 것이 애초에 불가능하게 한다.
            f"--conf spark.dynamicAllocation.minExecutors={self.executor_instances}",
            f"--conf spark.dynamicAllocation.maxExecutors={self.executor_instances}",
            f"--conf spark.dynamicAllocation.initialExecutors={self.executor_instances}",
        ]


# 기본 프로파일 = 지금까지 모든 job run이 쓰던 값이다. 합계는 5 vCPU / 36 GB /
# 140 GB다 — 이전 주석이 30 GB로 적었던 것은 driver의 memoryOverhead 6g를 빼먹은
# 오기였다(#508에서 과금 실측으로 확인: GB-h / vCPU-h = 7.20~7.22 = 36 / 5).
#
# Spark 기본값(driver 4 vCPU/8G, executor 2 vCPU/8G)은 driver 혼자 vCPU 예산을 많이
# 써버려 executor가 못 뜨고 ApplicationMaxCapacityExceededException으로 죽었다(#372) —
# driver를 작게 고정해 executor가 안정적으로 뜨도록 한다.
#
# executor의 memoryOverhead 6g는 Map Matching(find_segment_candidates의 mapInPandas)이
# 파티션마다 Python worker 프로세스에서 road_segment broadcast(약 17만 건)로 STRtree를
# 만드는 데 필요하다. cores=2라 이 무거운 작업이 최대 2개 동시에 뜨는데, overhead가
# 기본값(~10%, 약 800MB)뿐이었을 때 executor가 exit code 137(SIGKILL)로 반복해서
# 죽었다(#386). disk 60G는 실행 중 executor의 /tmp가 꽉 차 Job이 실패한 이력
# 때문이다(#443).
_DEFAULT_PROFILE = SparkResourceProfile(
    driver_cores="1",
    driver_memory="2g",
    driver_memory_overhead="6g",
    driver_disk="20G",
    executor_cores="2",
    executor_memory="8g",
    executor_memory_overhead="6g",
    executor_disk="60G",
    executor_instances="2",
)

# run_sensor_processing 전용. 합계 9 vCPU / 64 GB / 260 GB.
# 이 job이 가장 무겁다 — 실측 0.783 vCPU-h, 2,953 tasks로 run_hourly_scoring
# (0.073 vCPU-h)의 10배다. driver는 줄이지 않는다: map_matching/candidates.py:109가
# road_segment를 driver로 collect해 broadcast payload를 만들기 때문에 driver도
# Python 메모리를 실제로 쓴다.
_HEAVY_PROFILE = replace(_DEFAULT_PROFILE, executor_instances="4")

# audit_* 전용. 합계 4 vCPU / 30 GB / 80 GB.
# audit은 executor를 거의 쓰지 않고(실측: audit_current의 executor 존재 시간 7초)
# driver가 exit 137로 죽는다 — Great Expectations가 gold_audit_validation.py:112의
# `SELECT * FROM {table}`로 테이블 전량(997,332행)을 driver의 pandas에 올린다.
# 1 vCPU는 EMR Serverless 허용 메모리 상한이 8 GB인데 그것이 지금 죽는 값이라,
# 더 주려면 2 vCPU로 가야 한다. 전량 적재 자체를 없애는 것이 근본 해결이며 별도
# 과제로 남겼다(#508 설계 문서).
_AUDIT_PROFILE = replace(
    _DEFAULT_PROFILE,
    driver_cores="2",
    driver_memory="4g",
    driver_memory_overhead="12g",
    executor_instances="1",
)

RESOURCE_PROFILES: dict[str, SparkResourceProfile] = {
    "default": _DEFAULT_PROFILE,
    "heavy": _HEAVY_PROFILE,
    "audit": _AUDIT_PROFILE,
}


def submit_batch_jobs_command(
    task_id: str,
    entry_point_arguments: list[str],
    *,
    driver_env: dict[str, str] | None = None,
    outlets: list[Any] | None = None,
    profile: str = "default",
) -> EmrServerlessStartJobOperator:
    """batch-jobs CLI 커맨드 하나를 EMR Serverless Job Run으로 제출하는 task를 만든다.

    모든 Job Run에는 `driver_env` 유무와 무관하게 `PYSPARK_PYTHON` conf가
    driver/executor 양쪽에 강제로 붙는다(#360).

    `driver_env`는 `spark.emr-serverless.driverEnv.<KEY>` conf로 변환돼 Spark
    driver 프로세스의 환경변수가 된다. **주의**: 여기 넘긴 값은 EMR Serverless
    Job Run 설정에 평문으로 남아 GetJobRun API로 조회 가능하다 — Postgres
    비밀번호처럼 민감한 값을 이 경로로 넘기는 건 임시 방편이며(#292 논의,
    Secrets Manager 도입 전까지), 후속 이슈에서 IAM DB 인증 등으로 교체할 예정이다.

    `profile`은 이 Job Run이 요청할 driver/executor 크기를 고른다(#508) —
    `RESOURCE_PROFILES`의 키여야 하고, 없는 이름을 넘기면 DAG 파싱 시점에
    KeyError로 바로 드러난다. job마다 필요한 자원이 10배까지 차이나는데
    (run_sensor_processing 0.783 vCPU-h 대 run_hourly_scoring 0.073 vCPU-h)
    지금까지는 전부 같은 크기를 썼다.
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
        *RESOURCE_PROFILES[profile].conf_flags(),
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
        pool=EMR_SERVERLESS_POOL,
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
