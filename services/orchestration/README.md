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
  가리키는 서빙 DB에 마이그레이션(`migrate-database`)이 먼저 적용돼 있어야
  한다 — 이 서비스 범위 밖의 사전 조건이다.

## hourly_pipeline 실행하기 (cleanse 단계, 로컬 전용 배선)

`hourly_pipeline`의 `cleanse` TaskGroup은 BashOperator로 `docker run`을 호출해
host에 별도의 `batch-jobs` 컨테이너를 직접 띄운다("docker-outside-of-docker").
Airflow는 공식 이미지를 그대로 쓰고(#70) pyspark를 섞지 않기 위한 임시 배선이며,
`airflow-scheduler` 컨테이너에 host의 docker socket을 마운트해 동작한다
(`infra/compose/airflow.yaml`).

> ⚠️ **보안 주의**: docker socket 마운트는 그 컨테이너에 host docker에 대한
> 사실상의 제어권을 준다. 로컬 개발 환경 밖(공유 서버, 운영 환경 등)으로 이
> compose 설정을 그대로 옮기지 않는다. EMR Serverless로 연결되면(ADR 0001,
> 후속 이슈) 이 소켓 마운트와 `docker run` 호출은 `EmrServerlessStartJobOperator`로
> 대체되며 통째로 사라진다.

1. batch-jobs 이미지를 git SHA로 태깅해 빌드하고, 나온 태그를 `.env`의
   `BATCH_JOBS_IMAGE_TAG`에 넣는다(재현성 확보 — `latest` 등 버전 미고정 태그는
   쓰지 않는다).

   ```bash
   make build-batch-jobs-image
   # 출력된 태그(git SHA)를 .env의 BATCH_JOBS_IMAGE_TAG=... 에 채워 넣는다.
   ```

2. `make up-airflow`로 Airflow를 띄운 뒤, `hourly_pipeline`을 트리거하고
   `cleanse` TaskGroup이 `success`로 끝나는지 확인한다.

   ```bash
   docker compose -f infra/compose/airflow.yaml exec airflow-webserver airflow dags trigger hourly_pipeline
   docker compose -f infra/compose/airflow.yaml exec airflow-webserver airflow dags list-runs hourly_pipeline
   ```

**알려진 한계**: batch-jobs의 `CleansingJobConfig.from_env()`는 누락된
환경변수를 에러 없이 기본값으로 대체한다. 그래서 `infra/compose/airflow.yaml`의
`CLEANSING_*` 전달 목록과 batch-jobs가 실제로 기대하는 설정 키가 어긋나도
조용히 잘못된 경로로 cleanse job이 실행될 수 있다. 이 검증 로직은 batch-jobs
서비스 범위라 이번 이슈에서는 고치지 않았다.

**문제 해결**: `docker run` 단계에서 `permission denied`가 나면, host의
`/var/run/docker.sock` 권한(그룹)과 컨테이너 안 `airflow` 유저의 그룹이
맞는지 확인한다(호스트 OS/도커 설정에 따라 다르다).

## hourly_pipeline 실행하기 (features 단계, 로컬 전용 배선)

`features` TaskGroup은 `cleanse`와 동일한 방식으로
`build-hourly-segment-features`를 실행한다. `target_hour`와 `run_id`는 Airflow
실행 컨텍스트에서 전달하고, `road_snapshot_date`와 `feature_version`은 위의
필수 환경변수에서 전달한다.

입출력과 feature 설정은 `HourlySegmentFeatureJobConfig.from_env()`의 로컬
기본값을 사용한다.

- 입력: `data/local-lake/silver/processed_sensor_event`
- road segment: `data/processed/road_segment`
- 출력: `data/local-lake/silver/hourly_segment_features`
- event/steering/map-matching 설정: batch-jobs 패키지 기본 설정

`run_features`를 실행하기 전에 입력과 road segment Parquet가 위 경로에 있어야
한다. `make up-airflow`로 Airflow를 띄운 뒤 `hourly_pipeline`을 트리거하면
`cleanse >> features >> scoring` 순서로 실행된다.

```bash
docker compose -f infra/compose/airflow.yaml exec airflow-webserver airflow dags trigger hourly_pipeline
docker compose -f infra/compose/airflow.yaml exec airflow-webserver airflow dags list-runs hourly_pipeline
```

## hourly_pipeline 실행하기 (scoring 단계, 로컬 전용 배선)

`scoring` TaskGroup도 `cleanse`와 동일한 방식(BashOperator + docker-outside-of-docker)
으로 `score-hourly-comfort --run-id={{ run_id }}`를 실행한다. `run_id`는 Airflow
템플릿으로 전달되고, 나머지 설정은 `HourlyComfortJobConfig.from_env()`가
`HOURLY_COMFORT_*` 환경변수에서 읽는다.

scoring 입력인 `hourly_segment_features`는 바로 앞의 `features` TaskGroup이
생성한다. 따라서 별도의 샘플 Parquet를 심지 않고 전체 DAG를 순서대로 실행한다.

1. (cleanse 단계에서 이미 `BATCH_JOBS_IMAGE_TAG`를 채웠다면 생략) batch-jobs
   이미지를 빌드하고 `.env`의 `BATCH_JOBS_IMAGE_TAG`에 태그를 채운다.

   ```bash
   make build-batch-jobs-image
   ```

2. `make up-airflow`로 Airflow를 띄운 뒤, `hourly_pipeline`을 트리거하고
   `scoring` TaskGroup이 `success`로 끝나는지 확인한다.

   ```bash
   docker compose -f infra/compose/airflow.yaml exec airflow-webserver airflow dags trigger hourly_pipeline
   docker compose -f infra/compose/airflow.yaml exec airflow-webserver airflow dags list-runs hourly_pipeline
   ```

## hourly_pipeline 실행하기 (publish 단계, 로컬 전용 배선)

`publish` TaskGroup도 `cleanse`/`scoring`과 동일한 방식(BashOperator +
docker-outside-of-docker)으로 `load-segment-comfort-score
--as-of='{{ data_interval_end.isoformat() }}'`를 실행한다. `as_of`는 이
run의 데이터 구간이 끝나는 시점이며(Gold job은 `[as_of - window_hours,
as_of)` 윈도우를 집계), 나머지 설정은
`SegmentComfortScoreJobConfig.from_env()`가 `SEGMENT_COMFORT_SCORE_*`/
`POSTGRES_*` 환경변수에서 읽는다.

> ⚠️ **이 이슈(#176) 범위 밖**: `scoring >> publish` 의존관계는 아직 연결하지
> 않았다. 다른 작업(features 때와 동일한 조율)이 정리되는 시점에 후속
> 이슈에서 연결한다. 지금은 `publish`를 단독으로 트리거해서 검증한다.

1. 서빙 Postgres에 마이그레이션이 적용돼 있어야 한다(사전 조건, 이 서비스
   범위 밖). 아직이면 batch-jobs의 `migrate-database` 커맨드로 먼저 적용한다.
2. `hourly_comfort_score`가 아직 없다면(features/scoring을 아직 안 돌렸다면)
   검증용 샘플 Parquet를 `data/local-lake` 아래 임시로 심는다.
3. (다른 단계에서 이미 채웠다면 생략) batch-jobs 이미지를 빌드하고 `.env`의
   `BATCH_JOBS_IMAGE_TAG`를 채운다.

   ```bash
   make build-batch-jobs-image
   ```

4. `make up-airflow`로 Airflow를 띄운 뒤, `publish` TaskGroup만 골라 트리거하고
   성공 여부를 확인한다.

   ```bash
   docker compose -f infra/compose/airflow.yaml exec airflow-webserver \
     airflow tasks test hourly_pipeline publish.run_publish <run-date>
   docker compose -f infra/compose/airflow.yaml exec airflow-webserver \
     airflow dags trigger hourly_pipeline
   docker compose -f infra/compose/airflow.yaml exec airflow-webserver \
     airflow dags list-runs hourly_pipeline
   ```

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

- `hourly_pipeline`의 `scoring >> publish` 의존관계 연결, Great Expectations
  검증 task, Slack 실패 알림, EMR Serverless 실제 연결 (#157 후속 이슈)
- Kafka -> Bronze 오케스트레이션
- CeleryExecutor/KubernetesExecutor 등 분산 실행 지원
- 운영 배포, 인증/RBAC 설정
