"""batch-jobs 3단계 배치 파이프라인을 오케스트레이션하는 시간배치 DAG.

이슈 #162: cleanse 단계를 TaskGroup으로 구현했다. 이슈 #169: 같은 패턴으로
scoring 단계를 추가했다. 이슈 #171: features 단계를 추가하고, 이슈 #176에서
publish 단계를 추가했다. 이슈 #189: 4단계 의존관계를 완성했다. 이슈 #205:
cleanse와 features를 동일 Spark 세션에서 실행하는 sensor_processing 단계로
통합한다.

## 로컬 실행 방식 (임시, EMR Serverless 전환 시 사라짐)

Airflow는 공식 이미지를 그대로 쓰고(#70) pyspark를 섞지 않기 위해, 각 task는
BashOperator로 `docker run`을 호출해 별도의 batch-jobs 컨테이너를 host에
직접 띄운다("docker-outside-of-docker"). airflow-scheduler 컨테이너에 host의
docker socket을 마운트해야 동작한다(`infra/compose/airflow.yaml` 참고).

**보안 주의**: docker socket 마운트는 사실상 host docker에 대한 제어권을
컨테이너에 주는 것과 같다. 로컬 개발 환경 밖(공유 서버 등)으로 이 설정을
그대로 복사하지 않는다.

EMR Serverless로 연결되면(ADR 0001, 후속 이슈) 이 `docker run` 호출과
`infra/compose/airflow.yaml`의 docker socket 마운트는 통째로 삭제되고,
`EmrServerlessStartJobOperator`로 교체된다. TaskGroup 경계·task 의존관계·
`run_id` 템플릿 전달 방식은 그대로 유지된다.

"""

from __future__ import annotations

import datetime

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, TaskGroup
from airflow.timetables.interval import CronDataIntervalTimetable

# BATCH_JOBS_IMAGE_TAG는 `make build-batch-jobs-image`가 만든 git-SHA 태그를
# 가리켜야 한다(재현성). HOST_PROJECT_DIR와 BATCH_JOBS_IMAGE_TAG가 없으면
# 셸의 `:?` 문법으로 docker 실행 전에 즉시 실패시킨다.
#
# T1 cleansing과 T2 feature 계산은 하나의 batch-jobs 명령으로 실행하며,
# cleansing 결과 DataFrame을 중간 저장 없이 T2에 직접 전달한다.
_RUN_SENSOR_PROCESSING_BASH_COMMAND = (
    "docker run --rm --network de4-local "
    "-v ${HOST_PROJECT_DIR:?HOST_PROJECT_DIR must be set}/data/local-lake:/app/data/local-lake "
    "-v ${HOST_PROJECT_DIR:?HOST_PROJECT_DIR must be set}/data/processed:"
    "/app/data/processed:ro "
    "-e CLEANSING_CONFIG_PATH "
    "-e HOURLY_SEGMENT_FEATURE_EVENT_CONFIG_PATH "
    "-e HOURLY_SEGMENT_FEATURE_STEERING_CONFIG_PATH "
    "-e HOURLY_SEGMENT_FEATURE_MAP_MATCHING_CONFIG_PATH "
    "batch-jobs:${BATCH_JOBS_IMAGE_TAG:?BATCH_JOBS_IMAGE_TAG must be set} "
    # 이미지의 기본 CMD(`uv run --no-sync --package batch-jobs batch-jobs`)를 그대로
    # 반복해야 한다. `docker run <image> <cmd>`는 CMD를 완전히 덮어써서 셸 없이
    # 직접 exec하므로, `batch-jobs`(uv venv 안의 엔트리포인트)만 주면 PATH에서
    # 못 찾아 실패한다.
    "uv run --no-sync --package batch-jobs batch-jobs "
    "cleanse-sensor-events "
    "--target-hour='{{ data_interval_start.isoformat() }}' "
    '--road-snapshot-date="${HOURLY_SEGMENT_FEATURE_ROAD_SNAPSHOT_DATE'
    ':?HOURLY_SEGMENT_FEATURE_ROAD_SNAPSHOT_DATE must be set}" '
    '--feature-version="${HOURLY_SEGMENT_FEATURE_VERSION'
    ':?HOURLY_SEGMENT_FEATURE_VERSION must be set}" '
    '--bronze-input-path="${CLEANSING_BRONZE_INPUT_PATH:-data/local-lake/bronze/sensor-events}" '
    '--quarantine-output-path="${CLEANSING_QUARANTINE_OUTPUT_PATH:-data/local-lake/silver/'
    'sensor_event_quarantine}" '
    '--road-segment-path="${HOURLY_SEGMENT_FEATURE_ROAD_SEGMENT_PATH:-data/processed/'
    'road_segment}" '
    '--output-path="${HOURLY_SEGMENT_FEATURE_OUTPUT_PATH:-data/local-lake/silver/'
    'hourly_segment_features}" '
    "--run-id='{{ run_id }}'"
)

# run_id 외 나머지 설정은 HourlyComfortJobConfig.from_env()가 환경변수에서 읽는다.
_RUN_SCORING_BASH_COMMAND = (
    "docker run --rm --network de4-local "
    "-v ${HOST_PROJECT_DIR:?HOST_PROJECT_DIR must be set}/data/local-lake:/app/data/local-lake "
    "-e HOURLY_COMFORT_INPUT_PATH -e HOURLY_COMFORT_OUTPUT_PATH "
    "-e HOURLY_COMFORT_REJECTED_OUTPUT_PATH -e HOURLY_COMFORT_SCORING_CONFIG_PATH "
    "batch-jobs:${BATCH_JOBS_IMAGE_TAG:?BATCH_JOBS_IMAGE_TAG must be set} "
    "uv run --no-sync --package batch-jobs batch-jobs "
    "score-hourly-comfort --run-id={{ run_id }}"
)

# publish는 local-lake를 읽기만 하고(:ro) 쓰는 대상은 PostgreSQL이라 다른
# 단계와 달리 쓰기 마운트가 필요 없다. 나머지 설정(SegmentComfortScoreJobConfig)은
# SEGMENT_COMFORT_SCORE_*(옵션, 기본값 있음)와 POSTGRES_*(필수, 없으면 즉시
# 실패) 환경변수에서 읽는다. as_of는 `[as_of - window_hours, as_of)` 윈도우의
# 끝을 의미하므로, 방금 끝난 데이터 구간의 끝인 data_interval_end를 쓴다
# (features가 처리 대상 구간의 시작인 data_interval_start를 쓰는 것과 대칭).
_RUN_PUBLISH_BASH_COMMAND = (
    "docker run --rm --network de4-local "
    "-v ${HOST_PROJECT_DIR:?HOST_PROJECT_DIR must be set}/data/local-lake:"
    "/app/data/local-lake:ro "
    "-e SEGMENT_COMFORT_SCORE_DATA_LAKE_URI -e SEGMENT_COMFORT_SCORE_WINDOW_HOURS "
    "-e SEGMENT_COMFORT_SCORE_CONFIG_PATH "
    "-e POSTGRES_HOST -e POSTGRES_PORT -e POSTGRES_DB -e POSTGRES_USER -e POSTGRES_PASSWORD "
    "batch-jobs:${BATCH_JOBS_IMAGE_TAG:?BATCH_JOBS_IMAGE_TAG must be set} "
    "uv run --no-sync --package batch-jobs batch-jobs "
    "load-segment-comfort-score --as-of='{{ data_interval_end.isoformat() }}'"
)

with DAG(
    dag_id="hourly_pipeline",
    description="sensor processing -> scoring -> publish 3단계 시간배치 파이프라인",
    # Airflow 3의 bare cron 기본값인 CronTriggerTimetable은 data interval의
    # 시작과 끝을 같은 시각으로 만든다. 이 DAG는 09시 실행을 [09:00, 10:00)으로
    # 처리하고 publish의 as_of에 10:00을 넘겨야 하므로 interval timetable을
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
    },
    tags=["hourly-pipeline"],
) as dag:
    with TaskGroup(group_id="sensor_processing") as sensor_processing:
        run_sensor_processing = BashOperator(
            task_id="run_sensor_processing",
            bash_command=_RUN_SENSOR_PROCESSING_BASH_COMMAND,
        )

    with TaskGroup(group_id="scoring") as scoring:
        run_scoring = BashOperator(
            task_id="run_scoring",
            bash_command=_RUN_SCORING_BASH_COMMAND,
        )

    with TaskGroup(group_id="publish") as publish:
        run_publish = BashOperator(
            task_id="run_publish",
            bash_command=_RUN_PUBLISH_BASH_COMMAND,
        )

    sensor_processing >> scoring >> publish
