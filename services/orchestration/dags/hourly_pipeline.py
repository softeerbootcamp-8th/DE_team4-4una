"""batch-jobs 4단계 배치 파이프라인을 오케스트레이션하는 시간배치 DAG.

이슈 #162: cleanse 단계만 TaskGroup으로 먼저 구현한다. features/scoring/publish는
같은 패턴을 따라 후속 이슈에서 순차 추가한다(#157).

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

**알려진 한계**: `CleansingJobConfig.from_env()`(batch-jobs)가 누락된 환경변수를
조용히 기본값으로 대체하기 때문에, 아래 `-e` 목록과 batch-jobs가 기대하는 설정
키가 어긋나도 에러 없이 잘못된 경로로 실행될 수 있다. orchestration 서비스
범위 밖이라 이번 이슈에서는 고치지 않는다.
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
) as dag, TaskGroup(group_id="cleanse") as cleanse:
    # 후속 이슈에서 Great Expectations 검증 task가 이 TaskGroup 안에 추가될 자리.
    run_cleanse = BashOperator(
        task_id="run_cleanse",
        bash_command=_RUN_CLEANSE_BASH_COMMAND,
    )
