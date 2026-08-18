"""batch-jobs 4단계 배치 파이프라인을 오케스트레이션하는 시간배치 DAG.

이슈 #162: cleanse 단계를 TaskGroup으로 구현했다. 이슈 #169: 같은 패턴으로
scoring 단계를 추가했다. 이슈 #171: features 단계를 추가하고
cleanse >> features >> scoring 의존관계를 연결한다. 이슈 #176: publish 단계를
추가한다(#157의 마지막 진행). scoring >> publish 의존관계 연결은 이 이슈
범위 밖이라 아직 연결하지 않는다.

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

**알려진 한계(cleanse)**: `CleansingJobConfig.from_env()`(batch-jobs)가 누락된
환경변수를 조용히 기본값으로 대체하기 때문에, 아래 `-e` 목록과 batch-jobs가
기대하는 설정 키가 어긋나도 에러 없이 잘못된 경로로 실행될 수 있다. orchestration
서비스 범위 밖이라 이번 이슈에서는 고치지 않는다.

**scoring은 이 한계를 재현하지 않는다**: `HourlyComfortJobConfig.from_env()`도
동일하게 `or` 패턴으로 기본값을 대체하지만, 이는 batch-jobs가 의도적으로
설계한 동작이다(빈 값이 오면 로컬 기본 경로로 fallback).
"""

from __future__ import annotations

import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, TaskGroup

# BATCH_JOBS_IMAGE_TAG는 `make build-batch-jobs-image`가 만든 git-SHA 태그를
# 가리켜야 한다(재현성). 값이 없으면 어떤 코드가 실행되는지 보장할 수 없으므로
# `:?`로 즉시 실패시킨다.
_RUN_CLEANSE_BASH_COMMAND = (
    "docker run --rm --network de4-local "
    "-v ${HOST_PROJECT_DIR:?HOST_PROJECT_DIR must be set}/data/local-lake:/app/data/local-lake "
    "-e CLEANSING_BRONZE_INPUT_PATH -e CLEANSING_SILVER_OUTPUT_PATH "
    "-e CLEANSING_QUARANTINE_OUTPUT_PATH -e CLEANSING_SILVER_PARTITION_COLUMN "
    "-e CLEANSING_QUARANTINE_PARTITION_COLUMN "
    "batch-jobs:${BATCH_JOBS_IMAGE_TAG:?BATCH_JOBS_IMAGE_TAG must be set} "
    # 이미지의 기본 CMD(`uv run --no-sync --package batch-jobs batch-jobs`)를 그대로
    # 반복해야 한다. `docker run <image> <cmd>`는 CMD를 완전히 덮어써서 셸 없이
    # 직접 exec하므로, `batch-jobs`(uv venv 안의 엔트리포인트)만 주면 PATH에서
    # 못 찾아 실패한다.
    "uv run --no-sync --package batch-jobs batch-jobs "
    "cleanse-sensor-events --run-id={{ run_id }}"
)

# 경로·feature 설정은 HourlySegmentFeatureJobConfig.from_env()가 환경변수에서
# 읽는다. target_hour/run_id는 Airflow 실행 컨텍스트에서, road snapshot과 feature
# version은 orchestration 환경의 필수 설정에서 CLI 인자로 전달한다.
_RUN_FEATURES_BASH_COMMAND = (
    "docker run --rm --network de4-local "
    "-v ${HOST_PROJECT_DIR:?HOST_PROJECT_DIR must be set}/data/local-lake:"
    "/app/data/local-lake "
    "-v ${HOST_PROJECT_DIR:?HOST_PROJECT_DIR must be set}/data/processed:"
    "/app/data/processed:ro "
    "-e HOURLY_SEGMENT_FEATURE_INPUT_PATH "
    "-e HOURLY_SEGMENT_FEATURE_ROAD_SEGMENT_PATH "
    "-e HOURLY_SEGMENT_FEATURE_OUTPUT_PATH "
    "-e HOURLY_SEGMENT_FEATURE_EVENT_CONFIG_PATH "
    "-e HOURLY_SEGMENT_FEATURE_STEERING_CONFIG_PATH "
    "-e HOURLY_SEGMENT_FEATURE_MAP_MATCHING_CONFIG_PATH "
    "batch-jobs:${BATCH_JOBS_IMAGE_TAG:?BATCH_JOBS_IMAGE_TAG must be set} "
    "uv run --no-sync --package batch-jobs batch-jobs "
    "build-hourly-segment-features "
    "--target-hour='{{ data_interval_start.isoformat() }}' "
    '--road-snapshot-date="${HOURLY_SEGMENT_FEATURE_ROAD_SNAPSHOT_DATE'
    ':?HOURLY_SEGMENT_FEATURE_ROAD_SNAPSHOT_DATE must be set}" '
    '--feature-version="${HOURLY_SEGMENT_FEATURE_VERSION'
    ':?HOURLY_SEGMENT_FEATURE_VERSION must be set}" '
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
    description="cleanse -> features -> scoring -> publish 4단계 시간배치 파이프라인",
    schedule="0 * * * *",
    start_date=datetime.datetime(2026, 8, 17, tzinfo=datetime.UTC),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=5),
    },
    tags=["hourly-pipeline"],
) as dag:
    with TaskGroup(group_id="cleanse") as cleanse:
        # 후속 이슈에서 Great Expectations 검증 task가 이 TaskGroup 안에 추가될 자리.
        run_cleanse = BashOperator(
            task_id="run_cleanse",
            bash_command=_RUN_CLEANSE_BASH_COMMAND,
        )

    with TaskGroup(group_id="features") as features:
        run_features = BashOperator(
            task_id="run_features",
            bash_command=_RUN_FEATURES_BASH_COMMAND,
        )

    with TaskGroup(group_id="scoring") as scoring:
        run_scoring = BashOperator(
            task_id="run_scoring",
            bash_command=_RUN_SCORING_BASH_COMMAND,
        )

    with TaskGroup(group_id="publish") as publish:
        # scoring >> publish 의존관계 연결은 이슈 #176 범위 밖(후속 이슈에서 연결).
        run_publish = BashOperator(
            task_id="run_publish",
            bash_command=_RUN_PUBLISH_BASH_COMMAND,
        )

    cleanse >> features >> scoring
