"""batch-jobs 3단계 배치 파이프라인을 오케스트레이션하는 시간배치 DAG.

이슈 #162: cleanse 단계를 TaskGroup으로 구현했다. 이슈 #169: 같은 패턴으로
scoring 단계를 추가했다. 이슈 #171: features 단계를 추가하고, 이슈 #176에서
publish 단계를 추가했다. 이슈 #189: 4단계 의존관계를 완성했다. 이슈 #205:
cleanse와 features를 동일 Spark 세션에서 실행하는 transform_sensor_readings
task로 통합한다. 이슈 #217: standard 점수 적재와 current 점수 전량 갱신 단계를 붙여
서빙 테이블까지 한 DAG에서 채운다. 이슈 #229(ADR-0007): current 전량 갱신
책임을 current_score_pipeline(#231)으로 넘기고, 이 DAG는
standard_segment_comfort_score까지만 담당한다. dag_id도
standard_score_pipeline으로 바꾼다. 이슈 #227: 구 segment_comfort_score 경로를
제거하면서 publish 단계도 함께 빠졌다. 이슈 #265: compute_standard_score 내부에서
PostgreSQL 적재 전에 S3 Gold snapshot을 먼저 저장하도록 바뀌었다(Task 구조는 그대로).
이슈 #402: road_snapshot_date를 하드코딩된 Variable 대신 road_environment_uri의
active pointer/manifest(#389)에서 읽도록 resolve_road_snapshot_date task를
추가했다. 이슈 #540: 그 task를 없애고 같은 조회를 transform_sensor_readings의
인자 렌더링에서 user_defined_macros로 수행하며,
active pointer 대신 run이 처리 중인 달의 build를 고르도록 바꿨다. 이슈 #432: 마지막에 EMR Serverless
Application을 명시적으로 stop시키는 task를 붙여 idle timeout(15분)을 기다리지
않게 했다.

## EMR Serverless 실행 방식 (#292, ADR-0001)

각 task는 host의 docker socket을 마운트해 `docker run`으로 batch-jobs
컨테이너를 직접 띄우던(docker-outside-of-docker) 방식에서, 미리 만들어진 EMR
Serverless Application에 Job Run을 제출하는 `EmrServerlessStartJobOperator`
(`emr_serverless.submit_batch_jobs_command`)로 바뀌었다. Application ID·실행
역할 ARN·entry point는 Airflow Variable로 관리한다(`AIRFLOW_VAR_*` 환경변수로
주입 가능 — `.env.example` 참고). entry point는 batch-jobs의 EMR Serverless
커스텀 이미지가 준비되기 전까지 플레이스홀더다.

**임시 방편(Postgres 자격증명)**: `compute_standard_score`는 Postgres
자격증명이 필요한데, CLI에 `--postgres-*` 플래그가 없어 `driver_env`
(`spark.emr-serverless.driverEnv.*`)로 넘긴다. 이 값은 EMR Serverless Job Run
설정에 평문으로 남아 GetJobRun API로 조회 가능하다 — Secrets Manager를 지금
못 쓰는 상황이라 감수하기로 했다(#292 논의). 후속 이슈에서 IAM DB 인증 등으로
교체할 예정이다.
"""

from __future__ import annotations

import datetime

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup, Variable
from airflow.timetables.interval import CronDataIntervalTimetable
from comfort_score_assets import STANDARD_SCORE_ASSET
from emr_serverless import (
    check_emr_serverless_is_idle,
    stop_emr_serverless_application,
    submit_batch_jobs_command,
)
from notifications import (
    on_failure_callback,
    on_retry_callback,
    on_success_callback,
    on_task_success_callback,
)

# compute_standard_score에 넘기는 Postgres 자격증명 driver_env.
# 모듈 docstring의 "임시 방편" 설명 참고.
_POSTGRES_DRIVER_ENV = {
    "POSTGRES_HOST": "{{ var.value.POSTGRES_HOST }}",
    "POSTGRES_PORT": "{{ var.value.POSTGRES_PORT }}",
    "POSTGRES_DB": "{{ var.value.POSTGRES_DB }}",
    "POSTGRES_USER": "{{ var.value.POSTGRES_USER }}",
    "POSTGRES_PASSWORD": "{{ var.value.POSTGRES_PASSWORD }}",
}

# 아래 경로 상수들은 run/validate task가 같은 파티션을 가리키도록 한 곳에서만
# 정의해 공유한다(#220, ADR-0004) — transform_sensor_readings/compute_hourly_score
# 두 task에서 재사용한다.
_CLEANSING_BRONZE_INPUT_PATH = (
    "{{ var.value.get('CLEANSING_BRONZE_INPUT_PATH', "
    "'data/local-lake/bronze/sensor-events') }}"
)
_CLEANSING_QUARANTINE_OUTPUT_PATH = (
    "{{ var.value.get('CLEANSING_QUARANTINE_OUTPUT_PATH', "
    "'data/local-lake/silver/sensor_event_quarantine') }}"
)
_HOURLY_SEGMENT_FEATURE_ROAD_SEGMENT_PATH = (
    "{{ var.value.get('HOURLY_SEGMENT_FEATURE_ROAD_SEGMENT_PATH', "
    "'data/processed/road_segment') }}"
)
_HOURLY_SEGMENT_FEATURE_OUTPUT_PATH = (
    "{{ var.value.get('HOURLY_SEGMENT_FEATURE_OUTPUT_PATH', "
    "'data/local-lake/silver/hourly_segment_features') }}"
)
# compute_hourly_score의 입력 경로다. transform_sensor_readings가 방금 쓴 것과
# 물리적으로 같은 위치를 가리키지만(로컬 기본값도 동일), CLI/설정상 별개의 Airflow
# Variable이라 별도 상수로 둔다 — 위 _HOURLY_SEGMENT_FEATURE_OUTPUT_PATH와
# 혼동하지 않는다.
_HOURLY_COMFORT_INPUT_PATH = (
    "{{ var.value.get('HOURLY_COMFORT_INPUT_PATH', "
    "'data/local-lake/silver/hourly_segment_features') }}"
)
_HOURLY_COMFORT_OUTPUT_PATH = (
    "{{ var.value.get('HOURLY_COMFORT_OUTPUT_PATH', "
    "'data/local-lake/silver/hourly_comfort_score') }}"
)
_HOURLY_COMFORT_REJECTED_OUTPUT_PATH = (
    "{{ var.value.get('HOURLY_COMFORT_REJECTED_OUTPUT_PATH', "
    "'data/local-lake/quarantine/hourly_comfort_score') }}"
)
# compute_standard_score가 쓴 Gold snapshot을 validate_standard_score가 그대로 읽어야
# 하므로(#495, ADR-0012) 위 경로 상수들과 같은 이유로 한 곳에서 정의해 공유한다.
# airflow.yaml이 이 env var를 항상 선언해서, 호스트 .env에 값이 없으면 docker
# compose가 빈 문자열로 채운다 — 그러면 var.value.get의 default가 아니라 그 빈
# 문자열이 그대로 렌더링된다. emr_serverless._EMR_SERVERLESS_LOG_S3_URI_TEMPLATE와
# 같은 이유로 `or`를 한 번 더 둔다(#409). 폴백 값은 StandardComfortScoreJobConfig
# .from_env()의 것과 같아야 한다.
_STANDARD_COMFORT_SCORE_DATA_LAKE_URI = (
    "{{ var.value.get('STANDARD_COMFORT_SCORE_DATA_LAKE_URI', 'data/local-lake') "
    "or 'data/local-lake' }}"
)
_STANDARD_COMFORT_SCORE_GOLD_OUTPUT_URI = (
    "{{ var.value.get('STANDARD_COMFORT_SCORE_GOLD_OUTPUT_URI', '') }}"
)


def _resolve_road_snapshot_date(data_interval_start: datetime.datetime) -> str:
    """이 run이 처리 중인 달의 road-environment build에서 road_snapshot_date를 읽는다.

    #402에서는 active pointer가 가리키는 build 하나만 봤지만, 그러면 백필로 과거
    달을 돌려도 항상 지금 활성화된(=최신) 도로 정보를 쓰게 된다. #540부터는
    data_interval_start가 속한 달의 build를 고르고, 그 달에 없으면 그 이전 중
    가장 최신으로 폴백한다.

    이 함수는 DAG의 user_defined_macros로 등록돼 transform_sensor_readings의
    entry_point_arguments를 렌더링할 때 워커에서 호출된다 — de4_core와 jobs는
    dag-processor에 설치돼 있지 않으므로(airflow.yaml 참고) 이 함수 안에서만
    import한다.
    """
    from jobs.road_environment import resolve_road_snapshot_date_for_month

    # 어느 snapshot을 골랐는지 남기지 않으면 Airflow Log 탭에서 확인할 길이 없다
    # (#406) — 같은 파일의 다른 PythonOperator처럼 print({...}) 요약을 남긴다.
    # S3 URI는 자격증명이 아니라 로그에 남겨도 안전하다(#406 논의).
    target_month = data_interval_start.date()
    road_environment_uri = Variable.get("REFERENCE_DATA_LAKE_URI", default="")
    if road_environment_uri:
        road_snapshot_date = resolve_road_snapshot_date_for_month(
            road_environment_uri, target_month
        ).isoformat()
        print(
            {
                "source": "REFERENCE_DATA_LAKE_URI",
                "road_environment_uri": road_environment_uri,
                "target_month": target_month.strftime("%Y-%m"),
                "road_snapshot_date": road_snapshot_date,
            }
        )
        return road_snapshot_date

    fallback = Variable.get("HOURLY_SEGMENT_FEATURE_ROAD_SNAPSHOT_DATE", default="")
    if not fallback:
        raise ValueError(
            "REFERENCE_DATA_LAKE_URI or HOURLY_SEGMENT_FEATURE_ROAD_SNAPSHOT_DATE "
            "must be set"
        )
    print(
        {
            "source": "HOURLY_SEGMENT_FEATURE_ROAD_SNAPSHOT_DATE",
            "road_environment_uri": None,
            "target_month": target_month.strftime("%Y-%m"),
            "road_snapshot_date": fallback,
        }
    )
    return fallback


def _validate_standard_score(data_lake_uri: str, gold_output_uri: str, as_of: str) -> dict:
    """compute_standard_score가 쓴 Gold snapshot을 검증한다 (#495, ADR-0012).

    Spark가 필요 없어 EMR Serverless Job Run 대신 여기서 직접 돈다. de4_core와
    jobs는 dag-processor에 없으므로(airflow.yaml 참고) 이 함수 안에서만 import한다.
    """
    import datetime as dt

    from de4_core import join_uri
    from jobs.standard_score_validation import validate_standard_score

    # 폴백은 StandardComfortScoreJobConfig와 같아야 한다 — 어긋나면 쓴 곳과 읽는 곳이
    # 달라진다(comfort_score/standard_job.py).
    root_uri = gold_output_uri or join_uri(
        data_lake_uri, "gold", "standard_segment_comfort_score"
    )
    summary = validate_standard_score(root_uri, dt.datetime.fromisoformat(as_of))

    result = {"row_count": summary.row_count, "success": summary.success}
    print(result)
    return result


def _report_pipeline_counts(
    target_hour: str,
    as_of: str,
    quarantine_output_path: str,
    feature_output_path: str,
    hourly_comfort_output_path: str,
) -> dict:
    import datetime as dt

    import psycopg2
    from jobs.pipeline_counts import (
        PostgresConfig,
        count_standard_score_pipeline_outputs,
    )

    config = PostgresConfig.from_env()
    connection = psycopg2.connect(**config.as_connect_kwargs())
    try:
        counts = count_standard_score_pipeline_outputs(
            target_hour=dt.datetime.fromisoformat(target_hour),
            as_of=dt.datetime.fromisoformat(as_of),
            quarantine_output_path=quarantine_output_path,
            feature_output_path=feature_output_path,
            hourly_comfort_output_path=hourly_comfort_output_path,
            connection=connection,
        )
    finally:
        connection.close()

    result = {
        "quarantine_count": counts.quarantine_count,
        "feature_count": counts.feature_count,
        "hourly_comfort_score_count": counts.hourly_comfort_score_count,
        "standard_segment_comfort_score_count": counts.standard_segment_comfort_score_count,
    }
    print(result)
    return result


with DAG(
    dag_id="standard_score_pipeline",
    description="sensor processing -> scoring -> standard 3단계 시간배치 파이프라인",
    # Airflow 3의 bare cron 기본값인 CronTriggerTimetable은 data interval의
    # 시작과 끝을 같은 시각으로 만든다. 이 DAG는 09시 실행을 [09:00, 10:00)으로
    # 처리하고 standard 적재의 as_of에 10:00을 넘겨야 하므로 interval timetable을
    # 명시한다.
    schedule=CronDataIntervalTimetable(
        "0 * * * *",
        timezone=pendulum.timezone("UTC"),
    ),
    start_date=datetime.datetime(2026, 8, 17, tzinfo=datetime.UTC),
    catchup=False,
    # DAG run이 1시간을 넘기면 다음 시각 run과 겹쳐 EMR Serverless job run이 동시에
    # 뜨고, 뒤에 온 쪽이 executor를 못 받아 굶는다(#508 — 실측으로 10분 12초).
    # emr_serverless pool이 제출 자체는 직렬화하지만 DAG run이 쌓이는 것은 별개다.
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
    tags=["pipeline", "comfort-score", "emr-serverless"],
    # entry_point_arguments 렌더링 중에 road_snapshot_date를 직접 구하기 위한
    # 매크로다(#540) — 이것 때문에 별도 resolve task를 두지 않아도 된다.
    user_defined_macros={"resolve_road_snapshot_date": _resolve_road_snapshot_date},
) as dag:
    # T1 cleansing과 T2 feature 계산은 하나의 batch-jobs 명령으로 실행하며,
    # cleansing 결과 DataFrame을 중간 저장 없이 T2에 직접 전달한다.
    transform_sensor_readings = submit_batch_jobs_command(
        task_id="transform_sensor_readings",
        # 파이프라인에서 가장 무거운 job이다 — 실측 0.783 vCPU-h로
        # compute_hourly_score(0.073 vCPU-h)의 10배다(#508).
        profile="heavy",
        entry_point_arguments=[
            "cleanse-sensor-events",
            "--run-id",
            "{{ run_id }}",
            "--target-hour",
            "{{ data_interval_start.isoformat() }}",
            "--road-snapshot-date",
            "{{ resolve_road_snapshot_date(data_interval_start) }}",
            "--feature-version",
            "{{ var.value.HOURLY_SEGMENT_FEATURE_VERSION }}",
            "--bronze-input-path",
            _CLEANSING_BRONZE_INPUT_PATH,
            "--quarantine-output-path",
            _CLEANSING_QUARANTINE_OUTPUT_PATH,
            "--road-segment-path",
            _HOURLY_SEGMENT_FEATURE_ROAD_SEGMENT_PATH,
            "--output-path",
            _HOURLY_SEGMENT_FEATURE_OUTPUT_PATH,
        ],
    )
    # 품질 검증은 별도 task가 아니라 transform_sensor_readings의 Job Run 안에서
    # 이어서 돈다(#495, ADR-0012) — 검증만을 위해 Job Run을 하나 더 띄우면
    # 검증 자체(약 30초)보다 콜드 스타트(약 1분 25초)가 더 든다. 실패하면
    # 이 task가 실패해 scoring으로 넘어가지 않는 것(hard fail)은 그대로다.

    compute_hourly_score = submit_batch_jobs_command(
        task_id="compute_hourly_score",
        entry_point_arguments=[
            "score-hourly-comfort",
            "--run-id",
            "{{ run_id }}",
            "--target-hour",
            "{{ data_interval_start.isoformat() }}",
            "--input-path",
            _HOURLY_COMFORT_INPUT_PATH,
            "--output-path",
            _HOURLY_COMFORT_OUTPUT_PATH,
            "--rejected-output-path",
            _HOURLY_COMFORT_REJECTED_OUTPUT_PATH,
        ],
    )
    # transform_sensor_readings와 같은 이유로 검증은 이 task의 Job Run 안에서
    # 이어서 돈다(#495, ADR-0012).

    # standard 점수는 hourly_comfort_score를 168시간 윈도우로 롤업해 S3 Gold
    # snapshot에 저장한 뒤(#265) PostgreSQL에 UPSERT한다. as_of는
    # `[as_of - window_hours, as_of)` 윈도우의 끝이므로 data_interval_end를 쓴다.
    # STANDARD_COMFORT_SCORE_*/POSTGRES_*는 CLI 플래그가 없어(from_env() 전용)
    # driver_env로 넘긴다.
    compute_standard_score = submit_batch_jobs_command(
        task_id="compute_standard_score",
        entry_point_arguments=[
            "load-standard-segment-comfort-score",
            "--as-of",
            "{{ data_interval_end.isoformat() }}",
        ],
        driver_env={
            **_POSTGRES_DRIVER_ENV,
            "STANDARD_COMFORT_SCORE_DATA_LAKE_URI": _STANDARD_COMFORT_SCORE_DATA_LAKE_URI,
            # road-environment(active pointer/manifest)는 gold/silver와 다른
            # reference 버킷에 있다(#389) — build-road-environment/run-monthly가
            # 이미 이 이름을 쓰고 있어 그대로 재사용한다. 비어 있으면 job이
            # STANDARD_COMFORT_SCORE_DATA_LAKE_URI로 폴백한다.
            "REFERENCE_DATA_LAKE_URI": (
                "{{ var.value.get('REFERENCE_DATA_LAKE_URI', '') }}"
            ),
            "STANDARD_COMFORT_SCORE_WINDOW_HOURS": (
                "{{ var.value.get('STANDARD_COMFORT_SCORE_WINDOW_HOURS', '168') }}"
            ),
            "STANDARD_COMFORT_SCORE_GOLD_OUTPUT_URI": _STANDARD_COMFORT_SCORE_GOLD_OUTPUT_URI,
        },
    )
    # validate_standard_score는 compute_standard_score가 이번 실행에 쓴 Gold
    # snapshot(기준 데이터셋)을 검증한다 — 서빙 테이블을 조회하던 방식은
    # ADR-0012에서 바뀌었다(#495). Spark가 필요 없어 Job Run을 내지 않는다.
    # 검증을 통과한 데이터만 current_score_pipeline을 깨우도록, outlet을
    # compute_standard_score가 아니라 여기 둔다(#249).
    validate_standard_score = PythonOperator(
        task_id="validate_standard_score",
        python_callable=_validate_standard_score,
        op_kwargs={
            "data_lake_uri": _STANDARD_COMFORT_SCORE_DATA_LAKE_URI,
            "gold_output_uri": _STANDARD_COMFORT_SCORE_GOLD_OUTPUT_URI,
            "as_of": "{{ data_interval_end.isoformat() }}",
        },
        outlets=[STANDARD_SCORE_ASSET],
    )

    report_pipeline_counts = PythonOperator(
        task_id="report_pipeline_counts",
        python_callable=_report_pipeline_counts,
        op_kwargs={
            "target_hour": "{{ data_interval_start.isoformat() }}",
            "as_of": "{{ data_interval_end.isoformat() }}",
            "quarantine_output_path": _CLEANSING_QUARANTINE_OUTPUT_PATH,
            "feature_output_path": _HOURLY_SEGMENT_FEATURE_OUTPUT_PATH,
            "hourly_comfort_output_path": _HOURLY_COMFORT_OUTPUT_PATH,
        },
    )

    # 파이프라인이 다 끝나면 EMR Serverless Application을 바로 내려서 idle
    # timeout(15분)만큼의 유휴 과금을 없앤다(#432). 기본 trigger_rule(all_success)
    # 이라 앞 task가 하나라도 실패하면 여기까지 오지 않고, 그 경우에는 기존
    # idle timeout이 그대로 안전망으로 남는다.
    #
    # task가 2개라 TaskGroup 기준(3개 이상)에는 못 미치지만, 본 파이프라인과
    # 성격이 다른 정리 블록이라 그룹으로 경계를 드러낸다.
    with TaskGroup(group_id="emr_teardown") as emr_teardown:
        check_idle = check_emr_serverless_is_idle(task_id="check_idle")
        stop_application = stop_emr_serverless_application(task_id="stop_application")
        check_idle >> stop_application

    (
        transform_sensor_readings
        >> compute_hourly_score
        >> compute_standard_score
        >> validate_standard_score
        >> report_pipeline_counts
        >> emr_teardown
    )
