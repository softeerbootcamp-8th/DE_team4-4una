# orchestration

Apache Airflow(LocalExecutor)를 로컬 개발 환경에서 부트스트랩하는 서비스다.
`hello_world`(부트스트랩 동작 확인용)에 이어, `hourly_pipeline` DAG가
batch-jobs의 sensor processing(#205)·scoring(#169)·publish(#176) 3단계를
오케스트레이션한다. Sensor processing은 cleansing과 feature 계산을 같은 Spark
세션에서 실행한다.

`weather_pipeline` DAG(#207)는 다른 방식이다 — batch-jobs(EMR/Spark 전용)로
docker를 띄우는 대신, `jobs/weather.py`(#209, Open-Meteo 수집 + `latest_zone_weather`
UPSERT)를 `airflow-scheduler` 컨테이너 안에서 PythonOperator로 직접 실행하는
lightweight job이다. Spark가 필요 없어 이 편이 더 가볍다.

## comfort score 적재 (#217)

두 DAG가 `current_segment_comfort_score`를 갱신한다.

- `hourly_pipeline`: `standard_score`(batch-jobs Spark, `standard_segment_comfort_score`
  적재) 다음에 `current_score`가 **전량**을 다시 만든다. 날씨가 그대로여도 standard
  스냅샷이 새로 생겼으므로 갱신 대상이다.
- `weather_pipeline`: 15분 수집 뒤 `run_changed_zone_recompute`가 `impact_signature`가
  달라진 zone의 segment만 다시 만든다.

두 경로는 같은 테이블을 쓰므로 job이 PostgreSQL advisory lock으로 직렬화한다. 한쪽이
돌고 있으면 다른 쪽 task는 락을 기다린다. Airflow pool을 따로 두지 않은 이유이기도 하다.

`current_score`는 segment -> zone 매핑을 `road_segment` Parquet에서 읽는다. 이 매핑은
PostgreSQL에 없다. compose가 `data/processed`를 `:ro`로 마운트하고
`CURRENT_SCORE_ROAD_SEGMENT_PATH`/`CURRENT_SCORE_ROAD_SNAPSHOT_DATE`를 채워 준다.
`zone`이 없는 segment는 `current_segment_comfort_score.location_id`가 NOT NULL이라
행이 만들어지지 않는다 — `standard_segment_comfort_score`에만 남는다.

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
- `CLEANSING_BRONZE_INPUT_PATH`, `CLEANSING_QUARANTINE_OUTPUT_PATH`,
  `HOURLY_SEGMENT_FEATURE_ROAD_SEGMENT_PATH`,
  `HOURLY_SEGMENT_FEATURE_OUTPUT_PATH` — 통합 `cleanse-sensor-events` 커맨드의
  Bronze 입력, quarantine 출력, road segment 입력, feature 출력 경로다. 비우면
  DAG에 선언된 로컬 기본 경로를 사용한다.
- `CLEANSING_CONFIG_PATH`, `HOURLY_SEGMENT_FEATURE_EVENT_CONFIG_PATH`,
  `HOURLY_SEGMENT_FEATURE_STEERING_CONFIG_PATH`,
  `HOURLY_SEGMENT_FEATURE_MAP_MATCHING_CONFIG_PATH` — cleansing과 feature 계산
  설정 파일 경로다. 비우면 batch-jobs의 패키지 기본 설정을 사용한다.
- `HOURLY_SEGMENT_FEATURE_ROAD_SNAPSHOT_DATE` — sensor processing이 읽을 road
  segment의 `snapshot_date`다. 실제 road segment Parquet의 값과 일치해야 한다.
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
sensor_processing >> scoring >> publish
```

각 TaskGroup의 BashOperator는 `docker run`으로 host에 별도의 `batch-jobs`
컨테이너를 띄운다("docker-outside-of-docker"). Airflow 공식 이미지에 pyspark를
섞지 않기 위한 로컬 임시 배선이며, `airflow-scheduler` 컨테이너에 host의
docker socket을 마운트해 동작한다(`infra/compose/airflow.yaml`). Cleansing과
feature 계산은 `sensor_processing`의 단일 컨테이너와 Spark 세션에서 실행되며,
중간 cleansed-event 데이터셋을 저장하거나 다시 읽지 않는다.

| 단계 | 실행 커맨드 | 주요 입출력 |
| --- | --- | --- |
| sensor processing | `cleanse-sensor-events` | Bronze + road snapshot → `sensor_event_quarantine`, `hourly_segment_features` |
| scoring | `score-hourly-comfort` | features → `hourly_comfort_score`, rejected |
| publish | `load-segment-comfort-score` | scoring 결과 → 서빙 PostgreSQL |

publish의 `--as-of`에는 `data_interval_end`가 전달된다. 예를 들어 logical date가
`2026-08-18 09:00 UTC`이면 sensor processing은 09시 구간을 처리하고, publish는
해당 구간의 끝인 `2026-08-18T10:00:00+00:00`을 기준으로 집계한다.

> ⚠️ **보안 주의**: docker socket 마운트는 그 컨테이너에 host docker에 대한
> 사실상의 제어권을 준다. 로컬 개발 환경 밖(공유 서버, 운영 환경 등)으로 이
> compose 설정을 그대로 옮기지 않는다. EMR Serverless로 연결되면(ADR 0001,
> 후속 이슈) 이 소켓 마운트와 `docker run` 호출은 `EmrServerlessStartJobOperator`로
> 대체되며 통째로 사라진다.

**문제 해결**: `docker run` 단계에서 `permission denied`가 나면, host의
`/var/run/docker.sock` 권한(그룹)과 컨테이너 안 `airflow` 유저의 그룹이
맞는지 확인한다(호스트 OS/도커 설정에 따라 다르다).

## weather_pipeline (#207)

UTC 기준 15분마다(`*/15 * * * *`) `run_weather_collection` task 하나가 실행되며,
`jobs.weather.run_latest_zone_weather_job`을 `airflow-scheduler` 컨테이너 안에서
직접 호출한다(PythonOperator) — 별도 컨테이너를 띄우지 않는다. `data_interval_end`가
날씨 조회 기준 시각으로 전달된다.

`jobs/`는 `dags/`와 나란히 있지만 별도로 `${AIRFLOW_HOME}/orchestration/jobs`에
마운트되고, `PYTHONPATH=${AIRFLOW_HOME}/orchestration`로 `from jobs.weather import ...`가
되게 한다. `jobs.weather`는 task 함수 안에서만 임포트되므로(지연 임포트)
`airflow-dag-processor`/`airflow-webserver`는 이 배선이 없어도 DAG를 정상
파싱한다 — `airflow-scheduler`에만 필요하다(`infra/compose/airflow.yaml` 참고).

`requests`/`psycopg2-binary`/`pyarrow`는 공식 이미지에 없어 `_PIP_ADDITIONAL_REQUIREMENTS`로
`airflow-scheduler` 기동 시에만 설치한다 — 로컬 개발 전용이며, 운영에서는 이미지를
다시 빌드해 이 방식을 없애야 한다. `zone_master.parquet`은 `data/reference`를
`airflow-scheduler`에 읽기 전용으로 마운트해서 읽는다.

Open-Meteo 호출 실패나 일부 zone의 날씨 누락 시 `run_latest_zone_weather_job`이
예외를 던져 task가 실패하고, `retries=2, retry_delay=2분`으로 Airflow가
재시도한다(hourly_pipeline의 5분 간격은 15분 주기에 비해 너무 길어 줄였다).
`latest_zone_weather`는 `location_id`만 갖고 UPSERT하므로, 순서가 뒤바뀐 실행이
최신 값을 옛 값으로 덮어쓰지 않도록 SQL에 `weather_time` 역전 방지 조건을 걸고,
DAG에도 `max_active_runs=1`을 둬 이전 실행이 끝나기 전에 다음 tick이 겹치지
않게 한다(둘 다 걸어야 안전하다). `current_segment_comfort_score` 재계산은 이
DAG의 범위 밖이다(후속 이슈).

## 통합 테스트 (#189, #205)

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
cleansing quarantine, features, scoring 산출물은 제거하거나 별도 경로로
이동한다. `processed_sensor_event`는 현재 DAG가 읽거나 생성하지 않는다.

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

### 검증 기준

아래 건수는 #189에서 2026-08-18 09시 fixture로 검증한 결과를 기준으로 한다.
#205의 통합 sensor processing DAG는 같은 fixture로 다시 실행해 중간
`processed_sensor_event` 없이 동일한 최종 결과를 만드는지 확인한다.

| 검증 대상 | 결과 |
| --- | --- |
| sensor processing quarantine | 100건, `target_date=2026-08-18/target_hour=09`, `OUT_OF_RANGE` |
| 중간 cleansed-event 데이터셋 | 생성되지 않음 |
| 시간 범위 밖 이벤트 | 1,000건 모두 09시 결과에서 제외 |
| sensor processing features | 80건 = 20 segment × 4 profile, unmatched 0건 |
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
