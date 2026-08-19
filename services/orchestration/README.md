# orchestration

Apache Airflow(LocalExecutor)를 로컬 개발 환경에서 부트스트랩하는 서비스다.
`hello_world`(부트스트랩 동작 확인용)에 이어, `hourly_pipeline` DAG가
batch-jobs 4단계 배치 파이프라인 중 cleanse(#162)·features(#171)·scoring(#169)·
publish(#176) 단계를 오케스트레이션한다.

## 준비

저장소 루트의 `.env`에 다음 키를 채운다 (`.env.example` 참고). 값은 로컬
개발용으로 자유롭게 정하면 된다.

- `AIRFLOW_HOME` — Airflow 컨테이너 내부 경로다. 공식 이미지의 기본값인
  `/opt/airflow`를 그대로 쓰는 것을 권장한다(호스트 경로가 아니다).
- `AIRFLOW_POSTGRES_DB`, `AIRFLOW_POSTGRES_USER`, `AIRFLOW_POSTGRES_PASSWORD`
- `AIRFLOW_JWT_SECRET` — scheduler와 api-server(webserver)가 내부 인증에
  함께 쓰는 서명 시크릿이다. 충분히 긴 임의 문자열이면 되고, 예를 들어
  `openssl rand -hex 32`로 생성할 수 있다.
- `AIRFLOW_SECRET_KEY` — webserver/scheduler/dag-processor가 로그 서버 인증에
  공유해야 하는 서명 키(`[api] secret_key`). 컴포넌트마다 다른 값이면(기본은
  각자 랜덤 생성) 웹 UI가 task 로그를 못 가져오고 "secret_key... time
  synchronized..." 경고만 뜬다. `AIRFLOW_JWT_SECRET`과 마찬가지로
  `openssl rand -hex 32`로 생성.
- `BATCH_JOBS_IMAGE_TAG` — `hourly_pipeline`의 batch-jobs task가 실행할
  batch-jobs 이미지 태그. 아래 "hourly_pipeline 실행하기"에서 만든다.
- `CLEANSING_BRONZE_INPUT_PATH` 등 `CLEANSING_*` 5개 키 — batch-jobs의
  `cleanse-sensor-events` 커맨드가 읽는 입출력 경로다. 기존 값을 그대로 쓰면 된다.
- `HOURLY_SEGMENT_FEATURE_ROAD_SNAPSHOT_DATE` — feature job이 읽을 road segment의
  `snapshot_date`다. 실제 road segment Parquet의 값과 일치해야 한다.
- `HOURLY_SEGMENT_FEATURE_VERSION` — 생성할 feature 데이터의 버전이다
  (예: `hourly-features-v1`).
- `HOURLY_COMFORT_INPUT_PATH` 등 `HOURLY_COMFORT_*` 4개 키 — batch-jobs의
  `score-hourly-comfort` 커맨드가 읽는 입출력 경로다. 비워두면
  `HourlyComfortJobConfig.from_env()`가 `data/local-lake` 하위 기본 경로로
  대체하므로(의도된 동작이다), 로컬 개발에서는 채우지 않아도 된다.
- `SEGMENT_COMFORT_SCORE_DATA_LAKE_URI`, `SEGMENT_COMFORT_SCORE_WINDOW_HOURS`,
  `SEGMENT_COMFORT_SCORE_CONFIG_PATH` — batch-jobs의
  `load-segment-comfort-score` 커맨드(publish 단계)가 읽는 값이다. 비워두면
  `SegmentComfortScoreJobConfig.from_env()`가 로컬 기본값으로 대체한다(scoring과
  동일하게 의도된 동작).
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`,
  `POSTGRES_PASSWORD` — publish 단계가 Gold 결과를 적재할 서빙 Postgres 접속
  정보다. `SegmentComfortScoreJobConfig.from_env()`가 필수로 요구하며, 비어
  있으면(기본값 대체 없이) 즉시 실패한다. 로컬 개발에서는 `infra/compose/postgres.yaml`의
  `postgres` 서비스를 그대로 가리키면 된다(`POSTGRES_HOST=postgres`). 이 값이
  가리키는 서빙 DB에 마이그레이션(`migrate-database`)이 먼저 적용돼 있어야 한다.

## hourly_pipeline 로컬 전용 배선

`hourly_pipeline`은 UTC 기준 매시 정각에 `[logical_date, logical_date + 1시간)`
구간을 처리하며, 아래 순서로 실행된다.

```text
cleanse >> features >> scoring >> publish
```

각 TaskGroup의 BashOperator는 `docker run`으로 host에 별도의 `batch-jobs`
컨테이너를 띄운다("docker-outside-of-docker"). Airflow 공식 이미지에 pyspark를
섞지 않기 위한 로컬 임시 배선이며, `airflow-scheduler` 컨테이너에 host의
docker socket을 마운트해 동작한다(`infra/compose/airflow.yaml`).

| 단계 | 실행 커맨드 | 주요 입출력 |
| --- | --- | --- |
| cleanse | `cleanse-sensor-events` | Bronze → `processed_sensor_event`, `sensor_event_quarantine` |
| features | `build-hourly-segment-features` | processed events + road snapshot → `hourly_segment_features` |
| scoring | `score-hourly-comfort` | features → `hourly_comfort_score`, rejected |
| publish | `load-segment-comfort-score` | scoring 결과 → 서빙 PostgreSQL |

publish의 `--as-of`에는 `data_interval_end`가 전달된다. 예를 들어 logical date가
`2026-08-18 09:00 UTC`이면 cleanse/features는 09시 구간을 처리하고, publish는
해당 구간의 끝인 `2026-08-18T10:00:00+00:00`을 기준으로 집계한다.

> ⚠️ **보안 주의**: docker socket 마운트는 그 컨테이너에 host docker에 대한
> 사실상의 제어권을 준다. 로컬 개발 환경 밖(공유 서버, 운영 환경 등)으로 이
> compose 설정을 그대로 옮기지 않는다. EMR Serverless로 연결되면(ADR 0001,
> 후속 이슈) 이 소켓 마운트와 `docker run` 호출은 `EmrServerlessStartJobOperator`로
> 대체되며 통째로 사라진다.

**알려진 한계**: batch-jobs의 `CleansingJobConfig.from_env()`는 누락된
환경변수를 에러 없이 기본값으로 대체한다. 그래서 `infra/compose/airflow.yaml`의
`CLEANSING_*` 전달 목록과 batch-jobs가 실제로 기대하는 설정 키가 어긋나도
조용히 잘못된 경로로 cleanse job이 실행될 수 있다. 이 검증 로직은 batch-jobs
서비스 범위라 이번 이슈에서는 고치지 않았다.

**문제 해결**: `docker run` 단계에서 `permission denied`가 나면, host의
`/var/run/docker.sock` 권한(그룹)과 컨테이너 안 `airflow` 유저의 그룹이
맞는지 확인한다(호스트 OS/도커 설정에 따라 다르다).

## 통합 테스트 (#189)

### 테스트 데이터

`data/`는 gitignore 대상이므로 통합 테스트 전에 로컬에 아래 fixture를 준비한다.
같은 seed로 재생성했을 때 식별자와 이벤트 시간이 같도록 결정론적으로 만든다.

| 경로 | 내용 |
| --- | --- |
| `data/local-lake/bronze/sensor-events` | 대상 시간 정상 100,000건, 비정상 100건, 시간 범위 밖 1,000건 |
| `data/processed/road_segment/snapshot_date=2026-08-11` | 매칭 대상 road segment 20개 |

검증한 fixture는 500개 trip, 4개 vehicle profile을 포함한다. 비정상 100건은
`2026-08-18 09:30 UTC` 구간에 있고, 범위 밖 1,000건은 09시 구간의 양쪽
경계에 둔다. 실행 전에는 Bronze와 road snapshot만 남기고 이전
`processed_sensor_event`, cleansing quarantine, features, scoring 산출물은
제거하거나 별도 경로로 이동한다.

### 사전 준비

1. batch-jobs 이미지를 git SHA로 태깅해 빌드하고, 출력된 태그를 `.env`의
   `BATCH_JOBS_IMAGE_TAG`에 넣는다.

   ```bash
   make build-batch-jobs-image
   ```

2. Postgres와 Airflow를 실행한다.

   ```bash
   make up-postgres
   make up-airflow
   ```

3. 최초 실행이거나 마이그레이션이 추가됐다면 서빙 Postgres에 batch-jobs
   마이그레이션을 적용한다.

   ```bash
   docker compose --env-file "$PWD/.env" -f infra/compose/airflow.yaml run --rm \
     airflow-scheduler bash -c '
       docker run --rm --network de4-local \
         -e POSTGRES_HOST -e POSTGRES_PORT -e POSTGRES_DB \
         -e POSTGRES_USER -e POSTGRES_PASSWORD \
         batch-jobs:${BATCH_JOBS_IMAGE_TAG:?BATCH_JOBS_IMAGE_TAG must be set} \
         uv run --no-sync --package batch-jobs batch-jobs migrate-database
     '
   ```

### 09시 구간 backfill

수동 trigger 시각이 아니라 정확한 logical date를 쓰기 위해 backfill로 실행한다.
일반 scheduler가 현재 시각의 scheduled run을 함께 만들지 않도록 중지하고,
정규 스케줄 생성을 끈 테스트 전용 scheduler만 사용한다.

```bash
docker compose --env-file "$PWD/.env" -f infra/compose/airflow.yaml \
  stop airflow-scheduler

docker compose --env-file "$PWD/.env" -f infra/compose/airflow.yaml exec \
  airflow-webserver airflow backfill create \
  --dag-id hourly_pipeline \
  --from-date 2026-08-18T09:00:00+00:00 \
  --to-date 2026-08-18T09:00:00+00:00 \
  --max-active-runs 1

docker compose --env-file "$PWD/.env" -f infra/compose/airflow.yaml run -d \
  --name de4-airflow-backfill-scheduler \
  -e AIRFLOW__SCHEDULER__USE_JOB_SCHEDULE=False airflow-scheduler
```

Airflow 3에서 이 구간의 run ID는
`backfill__2026-08-18T10:00:00+00:00`이고 logical date는 09시다. 상태 확인과
테스트 전용 scheduler 종료는 다음과 같이 한다.

```bash
docker compose --env-file "$PWD/.env" -f infra/compose/airflow.yaml exec \
  airflow-webserver airflow tasks states-for-dag-run \
  hourly_pipeline backfill__2026-08-18T10:00:00+00:00

docker stop de4-airflow-backfill-scheduler
```

같은 logical date의 멱등성을 확인할 때는 테스트 전용 scheduler를 다시 시작하고
`--reprocess-behavior completed`로 같은 backfill을 재처리한다.

```bash
docker start de4-airflow-backfill-scheduler

docker compose --env-file "$PWD/.env" -f infra/compose/airflow.yaml exec \
  airflow-webserver airflow backfill create \
  --dag-id hourly_pipeline \
  --from-date 2026-08-18T09:00:00+00:00 \
  --to-date 2026-08-18T09:00:00+00:00 \
  --max-active-runs 1 \
  --reprocess-behavior completed

docker stop de4-airflow-backfill-scheduler
```

### 검증 결과

2026-08-18 09시 fixture로 전체 DAG와 동일 logical date 재실행을 검증했다.

| 검증 대상 | 결과 |
| --- | --- |
| cleanse processed | 100,000건, `event_date=2026-08-18/event_hour=09` |
| cleanse quarantine | 100건, `target_date=2026-08-18/target_hour=09`, `OUT_OF_RANGE` |
| 시간 범위 밖 이벤트 | 1,000건 모두 09시 결과에서 제외 |
| features | 80건 = 20 segment × 4 profile, unmatched 0건 |
| scoring | 80건, rejected 0건 |
| 첫 publish | 100건 insert = 20 segment × (4 profile + 대표 profile 0) |
| 동일 시간 재실행 | 0건 insert, 100건 update, 전체 행 수 증가 없음 |


## 로컬에서 실행하기

1. Airflow 메타데이터 DB용 Postgres를 띄운다.

   ```bash
   make up-postgres
   ```

2. Airflow를 띄운다. `airflow-init`이 `airflow db migrate`를 먼저 실행하고
   종료하면 나머지 서비스(`airflow-dag-processor`, `airflow-scheduler`,
   `airflow-webserver`)가 뜬다.

   ```bash
   make up-airflow
   ```

3. 버전과 executor를 확인한다.

   ```bash
   docker compose -f infra/compose/airflow.yaml exec airflow-webserver airflow version
   docker compose -f infra/compose/airflow.yaml exec airflow-scheduler airflow config get-value core executor
   ```

4. 예시 DAG를 트리거하고 성공하는지 확인한다.

   ```bash
   docker compose -f infra/compose/airflow.yaml exec airflow-webserver airflow dags trigger hello_world
   docker compose -f infra/compose/airflow.yaml exec airflow-webserver airflow dags list-runs hello_world
   ```

   가장 최근 run의 `state`가 `success`인지 확인한다.

## 웹 UI

`http://localhost:8080`에서 접속할 수 있다. 기본 인증 방식은
`SimpleAuthManager`이며, `airflow-webserver` 컨테이너가 처음 시작할 때
`admin` 계정의 비밀번호를 무작위로 생성해 로그에 출력한다:

```bash
docker compose -f infra/compose/airflow.yaml logs airflow-webserver | grep "Password for user"
```

이 방식은 로컬 개발용이며, 컨테이너를 다시 만들 때마다 비밀번호가 바뀐다.
운영 환경에서는 FAB 등 별도 인증 관리자를 사용해야 한다.

## 종료

```bash
docker compose -f infra/compose/airflow.yaml down
docker compose -f infra/compose/postgres.yaml down
```

## 범위 밖

- Great Expectations 검증 task, Slack 실패 알림, EMR Serverless 실제 연결
  (#157 후속 이슈)
- Kafka -> Bronze 오케스트레이션
- CeleryExecutor/KubernetesExecutor 등 분산 실행 지원
- 운영 배포, 인증/RBAC 설정
