# orchestration

Apache Airflow(LocalExecutor)를 로컬 개발 환경에서 부트스트랩하는 서비스다.
`hello_world`(부트스트랩 동작 확인용)에 이어, `hourly_pipeline` DAG가
batch-jobs 4단계 배치 파이프라인 중 cleanse(#162)·scoring(#169) 단계를
오케스트레이션한다.

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
- `BATCH_JOBS_IMAGE_TAG` — `hourly_pipeline`의 `cleanse` task가 실행할
  batch-jobs 이미지 태그. 아래 "hourly_pipeline 실행하기"에서 만든다.
- `CLEANSING_BRONZE_INPUT_PATH` 등 `CLEANSING_*` 5개 키 — batch-jobs의
  `cleanse-sensor-events` 커맨드가 읽는 입출력 경로다. 기존 값을 그대로 쓰면 된다.
- `HOURLY_COMFORT_INPUT_PATH` 등 `HOURLY_COMFORT_*` 4개 키 — batch-jobs의
  `score-hourly-comfort` 커맨드가 읽는 입출력 경로다. 비워두면
  `HourlyComfortJobConfig.from_env()`가 `data/local-lake` 하위 기본 경로로
  대체하므로(의도된 동작이다), 로컬 개발에서는 채우지 않아도 된다.

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

## hourly_pipeline 실행하기 (scoring 단계, 로컬 전용 배선)

`scoring` TaskGroup도 `cleanse`와 동일한 방식(BashOperator + docker-outside-of-docker)
으로 `score-hourly-comfort --run-id={{ run_id }}`를 실행한다. `run_id`는 Airflow
템플릿으로 전달되고, 나머지 설정은 `HourlyComfortJobConfig.from_env()`가
`HOURLY_COMFORT_*` 환경변수에서 읽는다.

`features` TaskGroup은 아직 없어서 `cleanse >> scoring` 의존관계도 아직 연결돼
있지 않다(다른 팀원과 조율해 나중에 연결). 그래서 로컬에서 `scoring`만 단독으로
확인하려면, `score-hourly-comfort`가 읽는 `HOURLY_COMFORT_INPUT_PATH`
(기본값 `data/local-lake/silver/hourly_segment_features`)에 최소 샘플
`hourly_segment_features` parquet를 임시로 심어둬야 한다. 스키마는
`batch_jobs.schemas.HOURLY_SEGMENT_FEATURE_SCHEMA`를 따르면 되고, 예시 행 구조는
`services/batch-jobs/tests/test_hourly_comfort_job.py`의 `feature_row` 헬퍼를
참고한다. 이 샘플 데이터는 검증용 임시 파일이며 `features` TaskGroup이 실제로
연결되면 더 이상 필요 없다.

1. (cleanse 단계에서 이미 `BATCH_JOBS_IMAGE_TAG`를 채웠다면 생략) batch-jobs
   이미지를 빌드하고 `.env`의 `BATCH_JOBS_IMAGE_TAG`에 태그를 채운다.

   ```bash
   make build-batch-jobs-image
   ```

2. 위 스키마에 맞는 샘플 parquet를 `data/local-lake/silver/hourly_segment_features`
   (또는 `.env`에 설정한 `HOURLY_COMFORT_INPUT_PATH`)에 심는다.

3. `make up-airflow`로 Airflow를 띄운 뒤, `hourly_pipeline`을 트리거하고
   `scoring` TaskGroup이 `success`로 끝나는지 확인한다.

   ```bash
   docker compose -f infra/compose/airflow.yaml exec airflow-webserver airflow dags trigger hourly_pipeline
   docker compose -f infra/compose/airflow.yaml exec airflow-webserver airflow dags list-runs hourly_pipeline
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

- `hourly_pipeline`의 features/publish TaskGroup, `cleanse >> features >> scoring`
  실제 의존관계 연결, Great Expectations 검증 task, Slack 실패 알림,
  EMR Serverless 실제 연결 (#157 후속 이슈)
- Kafka -> Bronze 오케스트레이션
- CeleryExecutor/KubernetesExecutor 등 분산 실행 지원
- 운영 배포, 인증/RBAC 설정
