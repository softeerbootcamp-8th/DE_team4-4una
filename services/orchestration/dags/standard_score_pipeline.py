"""batch-jobs 3단계 배치 파이프라인을 오케스트레이션하는 시간배치 DAG.

이슈 #162: cleanse 단계를 TaskGroup으로 구현했다. 이슈 #169: 같은 패턴으로
scoring 단계를 추가했다. 이슈 #171: features 단계를 추가하고, 이슈 #176에서
publish 단계를 추가했다. 이슈 #189: 4단계 의존관계를 완성했다. 이슈 #205:
cleanse와 features를 동일 Spark 세션에서 실행하는 sensor_processing 단계로
통합한다. 이슈 #217: standard 점수 적재와 current 점수 전량 갱신 단계를 붙여
서빙 테이블까지 한 DAG에서 채운다. 이슈 #229(ADR-0007): current 전량 갱신
책임을 current_score_pipeline(#231)으로 넘기고, 이 DAG는
standard_segment_comfort_score까지만 담당한다. dag_id도
standard_score_pipeline으로 바꾼다. 이슈 #227: 구 segment_comfort_score 경로를
제거하면서 publish 단계도 함께 빠졌다. 이슈 #265: run_standard_score 내부에서
PostgreSQL 적재 전에 S3 Gold snapshot을 먼저 저장하도록 바뀌었다(Task 구조는 그대로).
이슈 #402: road_snapshot_date를 하드코딩된 Variable 대신 road_environment_uri의
active pointer/manifest(#389)에서 읽도록, sensor_processing 맨 앞에
resolve_road_snapshot_date task를 추가했다.

## EMR Serverless 실행 방식 (#292, ADR-0001)

각 task는 host의 docker socket을 마운트해 `docker run`으로 batch-jobs
컨테이너를 직접 띄우던(docker-outside-of-docker) 방식에서, 미리 만들어진 EMR
Serverless Application에 Job Run을 제출하는 `EmrServerlessStartJobOperator`
(`emr_serverless.submit_batch_jobs_command`)로 바뀌었다. Application ID·실행
역할 ARN·entry point는 Airflow Variable로 관리한다(`AIRFLOW_VAR_*` 환경변수로
주입 가능 — `.env.example` 참고). entry point는 batch-jobs의 EMR Serverless
커스텀 이미지가 준비되기 전까지 플레이스홀더다.

**임시 방편(Postgres 자격증명)**: `standard_score` TaskGroup의 두 task는 Postgres
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
from emr_serverless import submit_batch_jobs_command
from notifications import on_failure_callback, on_success_callback

# standard_score TaskGroup의 두 task가 공유하는 Postgres 자격증명 driver_env.
# 모듈 docstring의 "임시 방편" 설명 참고.
_POSTGRES_DRIVER_ENV = {
    "POSTGRES_HOST": "{{ var.value.POSTGRES_HOST }}",
    "POSTGRES_PORT": "{{ var.value.POSTGRES_PORT }}",
    "POSTGRES_DB": "{{ var.value.POSTGRES_DB }}",
    "POSTGRES_USER": "{{ var.value.POSTGRES_USER }}",
    "POSTGRES_PASSWORD": "{{ var.value.POSTGRES_PASSWORD }}",
}

# 아래 경로 상수들은 run/validate task가 같은 파티션을 가리키도록 한 곳에서만
# 정의해 공유한다(#220, ADR-0004) — sensor_processing/hourly_scoring 두
# TaskGroup에서 재사용한다.
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
# hourly_scoring의 입력 경로다. sensor_processing이 방금 쓴 것과 물리적으로
# 같은 위치를 가리키지만(로컬 기본값도 동일), CLI/설정상 별개의 Airflow
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


def _resolve_road_snapshot_date() -> str:
    """road_environment_uri(#389)의 active pointer/manifest에서 최신 build의
    road_snapshot_date를 읽는다(#402). REFERENCE_DATA_LAKE_URI Variable이
    비어 있으면(로컬 개발 등) 기존 하드코딩 Variable로 폴백한다. de4_core는
    dag-processor에 설치돼 있지 않으므로(airflow.yaml 참고) 이 함수 안에서만
    import한다.
    """
    from jobs.road_environment import resolve_active_road_snapshot_date

    # 어느 snapshot을 골랐는지 XCom에만 담기면 Airflow Log 탭에서 확인할 길이
    # 없다(#406) — 같은 파일의 다른 PythonOperator처럼 print({...}) 요약을 남긴다.
    # S3 URI는 자격증명이 아니라 로그에 남겨도 안전하다(#406 논의).
    road_environment_uri = Variable.get("REFERENCE_DATA_LAKE_URI", default="")
    if road_environment_uri:
        road_snapshot_date = resolve_active_road_snapshot_date(road_environment_uri).isoformat()
        print(
            {
                "source": "REFERENCE_DATA_LAKE_URI",
                "road_environment_uri": road_environment_uri,
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
            "road_snapshot_date": fallback,
        }
    )
    return fallback


def _report_processing_counts(
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
    default_args={
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=5),
        "on_failure_callback": on_failure_callback,
    },
    on_success_callback=on_success_callback,
    tags=["standard-score-pipeline", "comfort-score"],
) as dag:
    with TaskGroup(group_id="sensor_processing") as sensor_processing:
        # road_snapshot_date는 사람이 관리하는 Variable이 아니라 road_environment_uri의
        # active pointer/manifest에서 매 실행마다 새로 읽는다(#402) — EMR Serverless
        # Job Run의 entry_point_arguments는 Jinja 템플릿 문자열일 뿐이라 이 안에서
        # 직접 pointer를 읽을 수 없으므로, 별도 PythonOperator로 미리 resolve해 XCom에
        # 남기고 run_sensor_processing이 그 값을 xcom_pull로 참조한다.
        resolve_road_snapshot_date = PythonOperator(
            task_id="resolve_road_snapshot_date",
            python_callable=_resolve_road_snapshot_date,
        )
        # T1 cleansing과 T2 feature 계산은 하나의 batch-jobs 명령으로 실행하며,
        # cleansing 결과 DataFrame을 중간 저장 없이 T2에 직접 전달한다.
        run_sensor_processing = submit_batch_jobs_command(
            task_id="run_sensor_processing",
            entry_point_arguments=[
                "cleanse-sensor-events",
                "--run-id",
                "{{ run_id }}",
                "--target-hour",
                "{{ data_interval_start.isoformat() }}",
                "--road-snapshot-date",
                "{{ ti.xcom_pull(task_ids='sensor_processing.resolve_road_snapshot_date') }}",
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
        # validate_sensor_processing은 run_sensor_processing이 방금 쓴
        # hourly_segment_features/quarantine 파티션만 읽으므로, 같은 Airflow
        # Variable(HOURLY_SEGMENT_FEATURE_OUTPUT_PATH, CLEANSING_QUARANTINE_OUTPUT_PATH)을
        # 재사용해 항상 같은 경로를 가리키게 한다(#220, ADR-0004). GX가 통계적/
        # 선언적 규칙(물리량 범위, quarantine 비율)을 검증하고 실패하면 이 task가
        # 실패해 scoring으로 넘어가지 않는다(hard fail). 스키마/필수값/PK 중복 같은
        # 하드 인바리언트는 run_sensor_processing이 쓰기 시점에 이미 강제하므로
        # 여기서 다시 다루지 않는다.
        validate_sensor_processing = submit_batch_jobs_command(
            task_id="validate_sensor_processing",
            entry_point_arguments=[
                "validate-sensor-processing",
                "--target-hour",
                "{{ data_interval_start.isoformat() }}",
                "--output-path",
                _HOURLY_SEGMENT_FEATURE_OUTPUT_PATH,
                "--quarantine-output-path",
                _CLEANSING_QUARANTINE_OUTPUT_PATH,
            ],
        )
        resolve_road_snapshot_date >> run_sensor_processing >> validate_sensor_processing

    with TaskGroup(group_id="hourly_scoring") as hourly_scoring:
        run_hourly_scoring = submit_batch_jobs_command(
            task_id="run_hourly_scoring",
            entry_point_arguments=[
                "score-hourly-comfort",
                "--run-id",
                "{{ run_id }}",
                "--input-path",
                _HOURLY_COMFORT_INPUT_PATH,
                "--output-path",
                _HOURLY_COMFORT_OUTPUT_PATH,
                "--rejected-output-path",
                _HOURLY_COMFORT_REJECTED_OUTPUT_PATH,
            ],
        )
        # validate_hourly_scoring은 run_hourly_scoring이 방금 overwrite한
        # hourly_comfort_score 전체(풀 리컴퓨트 결과)를 읽으므로(#249, ADR-0004),
        # 같은 Airflow Variable(HOURLY_COMFORT_OUTPUT_PATH)을 재사용해 항상 같은
        # 경로를 가리키게 한다. sensor_processing과 달리 hour 파티션이 없어
        # --target-hour는 필요 없다.
        validate_hourly_scoring = submit_batch_jobs_command(
            task_id="validate_hourly_scoring",
            entry_point_arguments=[
                "validate-hourly-scoring",
                "--output-path",
                _HOURLY_COMFORT_OUTPUT_PATH,
            ],
        )
        run_hourly_scoring >> validate_hourly_scoring

    with TaskGroup(group_id="standard_score") as standard_score:
        # standard 점수는 hourly_comfort_score를 168시간 윈도우로 롤업해 S3 Gold
        # snapshot에 저장한 뒤(#265) PostgreSQL에 UPSERT한다. as_of는
        # `[as_of - window_hours, as_of)` 윈도우의 끝이므로 data_interval_end를 쓴다.
        # STANDARD_COMFORT_SCORE_*/POSTGRES_*는 CLI 플래그가 없어(from_env() 전용)
        # driver_env로 넘긴다.
        run_standard_score = submit_batch_jobs_command(
            task_id="run_standard_score",
            entry_point_arguments=[
                "load-standard-segment-comfort-score",
                "--as-of",
                "{{ data_interval_end.isoformat() }}",
            ],
            driver_env={
                **_POSTGRES_DRIVER_ENV,
                "STANDARD_COMFORT_SCORE_DATA_LAKE_URI": (
                    "{{ var.value.get('STANDARD_COMFORT_SCORE_DATA_LAKE_URI', "
                    "'data/local-lake') }}"
                ),
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
                "STANDARD_COMFORT_SCORE_GOLD_OUTPUT_URI": (
                    "{{ var.value.get('STANDARD_COMFORT_SCORE_GOLD_OUTPUT_URI', '') }}"
                ),
            },
        )
        # validate_standard_score는 run_standard_score가 이번 실행에 UPSERT한
        # 행만(score_as_of=as_of) Postgres에서 직접 조회해 검증한다(#249,
        # ADR-0004: Gold/Postgres는 Spark가 아니라 SqlAlchemy 경로). 검증을
        # 통과한 데이터만 current_score_pipeline을 깨우도록, outlet을
        # run_standard_score가 아니라 여기 둔다(#249).
        validate_standard_score = submit_batch_jobs_command(
            task_id="validate_standard_score",
            entry_point_arguments=[
                "validate-standard-score",
                "--as-of",
                "{{ data_interval_end.isoformat() }}",
            ],
            driver_env=_POSTGRES_DRIVER_ENV,
            outlets=[STANDARD_SCORE_ASSET],
        )
        run_standard_score >> validate_standard_score

    report_processing_counts = PythonOperator(
        task_id="report_processing_counts",
        python_callable=_report_processing_counts,
        op_kwargs={
            "target_hour": "{{ data_interval_start.isoformat() }}",
            "as_of": "{{ data_interval_end.isoformat() }}",
            "quarantine_output_path": _CLEANSING_QUARANTINE_OUTPUT_PATH,
            "feature_output_path": _HOURLY_SEGMENT_FEATURE_OUTPUT_PATH,
            "hourly_comfort_output_path": _HOURLY_COMFORT_OUTPUT_PATH,
        },
    )

    sensor_processing >> hourly_scoring >> standard_score >> report_processing_counts
