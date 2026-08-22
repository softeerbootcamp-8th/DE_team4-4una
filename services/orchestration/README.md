# orchestration

Apache Airflow(LocalExecutor)를 로컬 개발 환경에서 부트스트랩하는 서비스다.
`hello_world`(부트스트랩 동작 확인용)에 이어, `standard_score_pipeline` DAG가
batch-jobs의 sensor processing(#205)·hourly scoring(#169)·standard score(#217)
3단계를 오케스트레이션한다(publish는 #227에서 제거). Sensor processing은
cleansing과 feature 계산을 같은 Spark 세션에서 실행한다.

`zone_weather_pipeline` DAG(#207)는 다른 방식이다 — batch-jobs(EMR/Spark 전용)로
docker를 띄우는 대신, `jobs/weather.py`(#209, Open-Meteo 수집 + `latest_zone_weather`
UPSERT)를 `airflow-scheduler` 컨테이너 안에서 PythonOperator로 직접 실행하는
lightweight job이다. Spark가 필요 없어 이 편이 더 가볍다.

## comfort score 적재 (#217, ADR-0007로 3-DAG 분리)

`current_segment_comfort_score`는 ADR-0007에 따라 `current_score_pipeline`(#231)이
유일하게 쓴다. `standard_score_pipeline`/`zone_weather_pipeline`은 각자
`standard_segment_comfort_score`/`latest_zone_weather`만 쓰고, `current`는 직접
건드리지 않는다 — 대신 자기 Asset(`STANDARD_SCORE_ASSET`/`ZONE_WEATHER_ASSET`)만
발행한다. `current_score_pipeline`은 `schedule=AssetAny(STANDARD_SCORE_ASSET,
ZONE_WEATHER_ASSET)`으로 두 producer 중 하나만 발행해도 깨어나고,
`context["triggering_asset_events"]`로 어떤 Asset이 트리거했는지 봐서
`STANDARD_SCORE_ASSET`이 있으면 전량, 없이 `ZONE_WEATHER_ASSET`만 있으면 변경된
zone만 재계산한다(`jobs/current_score.py`는 이 이슈에서 변경하지 않고 그대로
재사용). 이렇게 writer를 하나로 모으고 `max_active_runs=1`을 둬서, 두 producer가
겹쳐 트리거해도 stale-overwrite 없이 순차 실행되게 한다 — 자세한 배경/대안은
`docs/adr/0007-split-comfort-score-pipeline-into-three-dags.md` 참고.

`max_active_runs=1`은 실행 중인 DagRun을 막지 않고 이후 트리거를 큐잉만 하므로,
`jobs/current_score.py`의 PostgreSQL advisory lock(`LOCK_KEY=1004`)은 정상 경로에서는
불필요해지지만 수동 트리거·백필 등 그 보장이 깨지는 경우를 대비해 defense-in-depth로
그대로 남아 있다.

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
- `AIRFLOW_VAR_EMR_SERVERLESS_APPLICATION_ID`, `AIRFLOW_VAR_EMR_SERVERLESS_EXECUTION_ROLE_ARN`,
  `AIRFLOW_VAR_BATCH_JOBS_EMR_ENTRY_POINT` — `standard_score_pipeline`이 EMR
  Serverless Job Run을 제출하는 데 필요한 설정이다(#292, ADR-0001). `AIRFLOW_VAR_*`
  환경변수는 Airflow가 Variable로 자동 인식한다. entry point는 batch-jobs의
  EMR Serverless 커스텀 이미지가 준비되기 전까지 플레이스홀더다.
- `AIRFLOW_VAR_CLEANSING_BRONZE_INPUT_PATH`, `AIRFLOW_VAR_CLEANSING_QUARANTINE_OUTPUT_PATH`,
  `AIRFLOW_VAR_HOURLY_SEGMENT_FEATURE_ROAD_SEGMENT_PATH`,
  `AIRFLOW_VAR_HOURLY_SEGMENT_FEATURE_OUTPUT_PATH` — 통합 `cleanse-sensor-events`
  커맨드의 Bronze 입력, quarantine 출력, road segment 입력, feature 출력 경로다.
  비우면 DAG에 선언된 로컬 기본 경로를 사용한다.
- `AIRFLOW_VAR_HOURLY_SEGMENT_FEATURE_ROAD_SNAPSHOT_DATE` — sensor processing이
  읽을 road segment의 `snapshot_date`다. 실제 road segment Parquet의 값과
  일치해야 한다.
- `AIRFLOW_VAR_HOURLY_SEGMENT_FEATURE_VERSION` — 생성할 feature 데이터의
  버전이다(예: `hourly-features-v1`).
- `AIRFLOW_VAR_HOURLY_COMFORT_INPUT_PATH` 등 `AIRFLOW_VAR_HOURLY_COMFORT_*`
  3개 키 — batch-jobs의 `score-hourly-comfort` 커맨드가 읽는 입출력 경로다.
  비워두면 DAG에 선언된 로컬 기본 경로를 사용한다.
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`,
  `POSTGRES_PASSWORD` — `current_score_pipeline`의 PythonOperator
  (`jobs/current_score.py`)가 `airflow-scheduler` 프로세스의 환경변수에서
  직접 읽는 서빙 Postgres 접속 정보다.
- `AIRFLOW_VAR_POSTGRES_HOST` 등 `AIRFLOW_VAR_POSTGRES_*` 5개 키,
  `AIRFLOW_VAR_STANDARD_COMFORT_SCORE_DATA_LAKE_URI`,
  `AIRFLOW_VAR_STANDARD_COMFORT_SCORE_WINDOW_HOURS` — `standard_score` 단계가
  Gold 결과를 적재할 서빙 Postgres 접속 정보와 롤업 윈도우다.
  `data_quality_audit`의 audit-gold task도 같은 `AIRFLOW_VAR_POSTGRES_*` 5개
  키를 driver_env로 재사용한다(#295). 위 `POSTGRES_*`와 같은 값을 가리켜야
  한다(EMR Serverless job의 driver_env로 넘기는 경로가 달라 값을 두 번
  채워야 한다 — **주의**: 이 값은 EMR Serverless Job Run 설정에 평문으로
  남는 임시 방편이다, #292 논의). 로컬 개발에서는
  `infra/compose/postgres.yaml`의 `postgres` 서비스를 그대로 가리키면 된다
  (`POSTGRES_HOST=postgres`). 이 값이 가리키는 서빙 DB에
  마이그레이션(`migrate-database`)이 먼저 적용돼 있어야 한다.
- `AIRFLOW_VAR_GOLD_AUDIT_S3_BUCKET` — `data_quality_audit`의 audit-gold
  task가 GX Data Docs를 올릴 S3 버킷이다(#295). 비워두면 batch-jobs 쪽
  기본값(`de4-data-quality-docs`)을 쓴다. AWS 자격증명은 여기 넘기지 않고
  EMR Serverless execution role(IAM)에 위임한다 — role에 이 버킷에 대한
  `s3:PutObject` 권한이 미리 부여돼 있어야 한다.

## standard_score_pipeline — EMR Serverless 실행 (#292, ADR-0001)

`standard_score_pipeline`은 UTC 기준 매시 정각에 `[logical_date, logical_date + 1시간)`
구간을 처리하며, 아래 순서로 실행된다.

```text
sensor_processing >> hourly_scoring >> standard_score
```

각 TaskGroup의 task는 `EmrServerlessStartJobOperator`(`dags/emr_serverless.py`의
`submit_batch_jobs_command`)로 미리 만들어진 EMR Serverless Application에 Job
Run을 제출한다. Application ID·실행 역할 ARN·entry point는 Airflow Variable로
관리하며, entry point는 batch-jobs의 EMR Serverless 커스텀 이미지가 준비되기
전까지 플레이스홀더다. Cleansing과 feature 계산은 `sensor_processing`의 단일
Job Run과 Spark 세션에서 실행되며, 중간 cleansed-event 데이터셋을 저장하거나
다시 읽지 않는다.

| 단계 | 실행 커맨드 | 주요 입출력 |
| --- | --- | --- |
| sensor processing | `cleanse-sensor-events` | Bronze + road snapshot → `sensor_event_quarantine`, `hourly_segment_features` |
| hourly scoring | `score-hourly-comfort` | features → `hourly_comfort_score`, rejected |
| standard score | `load-standard-segment-comfort-score` | `hourly_comfort_score` 168시간 롤업 → 서빙 PostgreSQL(`standard_segment_comfort_score`) |

standard score의 `--as-of`에는 `data_interval_end`가 전달된다.
예를 들어 logical date가 `2026-08-18 09:00 UTC`이면 sensor processing은 09시
구간을 처리하고, standard score는 해당 구간의 끝인
`2026-08-18T10:00:00+00:00`을 기준으로 집계한다.

`standard_score` TaskGroup의 두 task는 CLI 플래그가 없는 설정(Postgres
자격증명, `STANDARD_COMFORT_SCORE_*`)을 `driver_env`
(`spark.emr-serverless.driverEnv.*`)로 넘긴다. **주의**: 이 값은 EMR Serverless
Job Run 설정에 평문으로 남아 GetJobRun API로 조회 가능하다 — Secrets Manager를
지금 못 쓰는 상황이라 감수하기로 했다(#292 논의). 후속 이슈에서 IAM DB 인증
등으로 교체할 예정이다.

> ⚠️ 이 DAG의 실제 EMR Serverless 트리거 검증(entry point 완성, Job Run
> 정상 실행 확인)은 batch-jobs의 커스텀 이미지가 준비되고 Airflow가 EC2로
> 이전된 뒤 별도로 진행한다(#289). 아래 "통합 테스트" 절의 backfill/검증
> 절차는 옛 docker-run 방식 기준이라 지금은 그대로 재현할 수 없다.

## data_quality_audit — EMR Serverless 실행 (#295, ADR-0001)

`data_quality_audit`도 같은 공용 헬퍼(`dags/emr_serverless.py`의
`submit_batch_jobs_command`)로 `EmrServerlessStartJobOperator`를 쓴다(#292의
방식을 그대로 재사용). host의 docker socket을 마운트해 `docker run`으로
batch-jobs 컨테이너를 직접 띄우던("docker-outside-of-docker") 방식은
제거했다 — `infra/compose/airflow.yaml`에는 이제 docker socket 마운트가
없다.

`audit-gold` CLI는 `--table` 외 옵션이 없어, Postgres 자격증명과
`GOLD_AUDIT_S3_BUCKET`을 `driver_env`로 넘긴다(위 "준비" 절 참고). AWS
access key는 넘기지 않고 EMR Serverless의 execution role(IAM)에 위임한다 —
role에 대상 S3 버킷에 대한 `s3:PutObject` 권한이 미리 부여돼 있어야 한다.

> ⚠️ 이 DAG의 실제 EMR Serverless 트리거 검증도 `standard_score_pipeline`과
> 마찬가지로 batch-jobs의 커스텀 이미지가 준비되고 Airflow가 EC2로 이전된 뒤
> 별도로 진행한다(#289).

## zone_weather_pipeline (#207, #230)

UTC 기준 15분마다(`*/15 * * * *`) `run_weather_collection >> validate_weather_collection
>> detect_changed_zones` 순으로 실행되며, 둘 다
`jobs.weather`/`jobs.weather_validation`을 `airflow-scheduler` 컨테이너 안에서
직접 호출한다(PythonOperator) — 별도 컨테이너를 띄우지 않는다. `data_interval_end`가
날씨 조회 기준 시각으로 전달된다.

`validate_weather_collection`(#250)은 `run_weather_collection`이 이번 실행에
실제로 UPSERT한 행(`weather_time = data_interval_end`)만 조회해 관측값 범위,
`weather_state`/`impact_signature` 형식, freshness를 검사하고 위반 시 task를
hard fail시킨다(`context/data/quality-rules.md`의 "Zone weather quality" 절).
다른 파이프라인의 검증과 달리 Great Expectations가 아니라 인라인 Python/SQL로
구현했다 — GX가 엔진 선택과 무관하게 무거운 의존성(pandas/numpy/scipy/altair 등)을
끌고 와 이 DAG의 경량 설계와 맞지 않기 때문이다(`docs/adr/0004-...md`의 `#250`
수정 노트 참고). `detect_changed_zones`보다 앞에 둔 이유는, 이상 데이터로 인해
잘못된 zone이 "변경됨"으로 오판되는 걸 막기 위해서다.

`jobs/`는 `dags/`와 나란히 있지만 별도로 `${AIRFLOW_HOME}/orchestration/jobs`에
마운트되고, `PYTHONPATH=${AIRFLOW_HOME}/orchestration`로 `from jobs.weather import ...`가
되게 한다. `jobs.weather`는 task 함수 안에서만 임포트되므로(지연 임포트)
`airflow-dag-processor`/`airflow-webserver`는 이 배선이 없어도 DAG를 정상
파싱한다 — `airflow-scheduler`에만 필요하다(`infra/compose/airflow.yaml` 참고).

`requests`/`psycopg2-binary`/`pyarrow`는 공식 이미지에 없어 `_PIP_ADDITIONAL_REQUIREMENTS`로
`airflow-scheduler` 기동 시에만 설치한다 — 로컬 개발 전용이며, 운영에서는 이미지를
다시 빌드해 이 방식을 없애야 한다. `zone_master.parquet`은 `data/reference`를
`airflow-scheduler`에 읽기 전용으로 마운트해서 읽는다.

`jobs.weather.fetch_open_meteo`는 zone을 50개씩 batch로 나눠 요청하는데(#222),
한 batch가 HTTP 오류나 zone 수 불일치로 실패해도 그 batch의 zone만 실패
처리하고 나머지 batch는 계속 조회한다 — 일부 zone 실패는 task를 실패시키지
않는다. 그 zone들은 `latest_zone_weather` UPSERT에서 빠져 기존 행을 그대로
두고, `zone_weather_snapshot`에는 `fetch_status="failed"` 행으로만 남는다
(아래 참고). 반대로 **요청한 zone 전체**가 실패하면(Open-Meteo가 통째로
다운된 경우 등) `run_latest_zone_weather_job`이 snapshot을 남긴 뒤 예외를
던져 task를 실패시키고, `retries=2, retry_delay=2분`으로 Airflow가 재시도한다
(standard_score_pipeline의 5분 간격은 15분 주기에 비해 너무 길어 줄였다).
`latest_zone_weather`는 `location_id`만 갖고 UPSERT하므로, 순서가 뒤바뀐 실행이
최신 값을 옛 값으로 덮어쓰지 않도록 SQL에 `weather_time` 역전 방지 조건을 걸고,
DAG에도 `max_active_runs=1`을 둬 이전 실행이 끝나기 전에 다음 tick이 겹치지
않게 한다(둘 다 걸어야 안전하다). `current_segment_comfort_score` 재계산은 이
DAG의 범위 밖이다(후속 이슈).

같은 실행에서 `jobs.weather.write_zone_weather_snapshot`이 `latest_zone_weather`
UPSERT보다 먼저 `zone_weather_snapshot` Parquet 이력을 남긴다(#222). 경로는
`ZONE_WEATHER_SNAPSHOT_DATA_LAKE_URI`(비우면 `data/local-lake/bronze/zone_weather_snapshot`)
아래 `weather_date=YYYY-MM-DD/weather_time=....parquet`로, `weather_time`이 파일
키라 같은 tick을 재시도해도 파일을 덮어쓸 뿐 중복 snapshot이 생기지 않는다.
지금은 로컬 경로만 지원한다 — S3 연동은 #222 범위 밖이라, 나중에 이 값을
`s3://...`로 바꿀 때 `write_zone_weather_snapshot`의 내부 쓰기 방식만 바뀌고
config/호출부는 그대로 둘 수 있게만 이름을 지어뒀다(de4-core는 batch-jobs
이미지에만 설치돼 있어 이 lightweight 컨테이너에서는 못 쓴다).

`zone_weather_snapshot`은 요청한 zone 전체를 매번 한 행씩 남긴다 — target_time
데이터가 없던 zone도 측정값/`weather_state`/`impact_signature`는 NULL로,
`fetch_status="failed"`(`error_reason`에 사유)로 기록된다. 성공/실패 여부를
나중에 구분할 수 있는 곳은 이 테이블뿐이다 — `latest_zone_weather`는 실패한
zone을 아예 건드리지 않으므로 흔적이 남지 않는다.

## current_score_pipeline (#231)

`standard_score_pipeline`/`zone_weather_pipeline` 둘 다 직접 쓰지 않는
`current_segment_comfort_score`의 유일한 writer다. 정기 cron이 아니라
`schedule=AssetAny(STANDARD_SCORE_ASSET, ZONE_WEATHER_ASSET)`으로, 두 producer 중
하나라도 Asset을 발행하면 깨어난다. `run_current_score` task 하나뿐이며,
`context["triggering_asset_events"]`에 `STANDARD_SCORE_ASSET`이 있으면 전량
(`changed_zones_only=False`), 없이 `ZONE_WEATHER_ASSET`만 있으면 변경된 zone만
(`changed_zones_only=True`)으로 판단해 `jobs.current_score.run_from_env(...)`를
그대로 호출한다. `max_active_runs=1`로 두 producer가 겹쳐 트리거해도 동시에
두 번 쓰지 않는다.

### 수동 확인 절차 (두 producer 중 하나만 트리거됐을 때 올바른 모드로 도는지)

> ⚠️ 아래 절차는 로컬에서 실행해보지 않고 기록만 해 둔 것이다 — "통합 테스트"
> 절의 fixture(`data/processed/road_segment`, 실 데이터가 채워진
> `standard_segment_comfort_score`/`latest_zone_weather`)가 준비된 뒤에 실제로
> 따라 해보고 이 문구를 지운다.

전제: 위 "통합 테스트" 절의 fixture가 준비되어 있고, `standard_score_pipeline`용
`BATCH_JOBS_IMAGE_TAG`가 빌드되어 있어야 한다.

1. `current_score_pipeline`은 새 DAG라 기본적으로 paused다. unpause한다.

   ```bash
   docker compose -f infra/compose/airflow.yaml exec airflow-webserver \
     airflow dags unpause current_score_pipeline
   ```

2. **STANDARD_SCORE_ASSET 단독 트리거 → 전량 모드** 확인: `zone_weather_pipeline`은
   pause한 채로 `standard_score_pipeline`만 "09시 구간 backfill" 절차로 실행한다.
   `standard_score.run_standard_score`가 SUCCESS로 끝나면 `STANDARD_SCORE_ASSET`
   이벤트가 발행되고 `current_score_pipeline`이 자동으로 새 DagRun을 만든다.

   ```bash
   docker compose -f infra/compose/airflow.yaml exec airflow-webserver \
     airflow dags list-runs current_score_pipeline
   ```

   가장 최근 run의 `run_current_score` task 로그에서
   `"changed_zones_only": false`가 찍히는지 확인한다.

3. **ZONE_WEATHER_ASSET 단독 트리거 → 변경 zone만 모드** 확인:
   `standard_score_pipeline`을 다시 pause하고 `zone_weather_pipeline`을 unpause한
   뒤, 변경된 zone이 있는 15분 tick(또는 수동 trigger)에서
   `publish_zone_weather_asset`이 SUCCESS로 끝나는지 본다. 마찬가지로
   `current_score_pipeline`의 새 run에서 `"changed_zones_only": true`가 찍히는지
   확인한다.

4. 확인이 끝나면 세 DAG을 원래 pause 상태로 되돌린다.

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
  --dag-id standard_score_pipeline \
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
  standard_score_pipeline backfill__2026-08-18T10:00:00+00:00

docker stop de4-airflow-backfill-scheduler
```

같은 logical date의 멱등성을 확인할 때는 테스트 전용 scheduler를 다시 시작하고
`--reprocess-behavior completed`로 같은 backfill을 재처리한다.

```bash
docker start de4-airflow-backfill-scheduler

docker compose --env-file "$PWD/.env" -f infra/compose/airflow.yaml exec \
  airflow-webserver airflow backfill create \
  --dag-id standard_score_pipeline \
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
| 첫 publish (당시 단계, #227에서 제거) | 100건 insert = 20 segment × (4 profile + 대표 profile 0) |
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

- Great Expectations 검증 task, Slack 실패 알림
- `data_quality_audit`/`standard_score_pipeline` 모두, batch-jobs 커스텀
  이미지 완성 후 실제 EMR Serverless Job Run 트리거 검증과 Airflow의 EC2
  이전(#289 후속 이슈)
- Kafka -> Bronze 오케스트레이션
- CeleryExecutor/KubernetesExecutor 등 분산 실행 지원
- CD는 EC2에서 컨테이너를 기동하고 헬스체크까지 확인한다(#315). 인증 관리자
  교체(SimpleAuthManager → FAB 등)와 RBAC 설정, RDS의 Airflow용 DB(스키마)
  실제 생성은 범위 밖이다 — 사람이 사전에 수행한다
