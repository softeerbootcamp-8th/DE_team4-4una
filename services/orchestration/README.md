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
PostgreSQL에 없다. `road_segment`/`zone_master`는 reference S3 버킷에서 직접 읽는다(#400,
`jobs/weather.py`/`jobs/current_score.py`가 `de4_core.ObjectStore`로 local path/`file://`/
`s3://` URI를 모두 처리) — 로컬 compose는 `ZONE_MASTER_URI`/`CURRENT_SCORE_ROAD_SEGMENT_URI`
기본값을 볼륨 마운트된 로컬 경로로 채워 주고, 운영은 같은 키를 reference S3 URI로 덮어쓴다.
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
- `ZONE_MASTER_URI`, `CURRENT_SCORE_ROAD_SEGMENT_URI` — `zone_weather_pipeline`
  (`jobs/weather.py`)과 `current_score_pipeline`(`jobs/current_score.py`)이 각각
  reference 데이터(`zone_master.parquet`, `road_segment`)를 읽을 URI다(#400).
  `AIRFLOW_VAR_*`가 아니라 `POSTGRES_*`와 같은 방식으로 `airflow-scheduler`
  프로세스의 환경변수에서 직접 읽는다. 로컬에서는 비워 두면
  `infra/compose/airflow.yaml`이 볼륨 마운트된 로컬 경로
  (`data/reference/tlc_zone/zone_master.parquet`, `data/processed/road_segment`)를
  기본값으로 채운다. 운영에서는 reference S3 버킷을 가리키는 값을 넣는다.

  ```env
  ZONE_MASTER_URI=s3://<reference-bucket>/normalized/zone_master/zone_master.parquet
  CURRENT_SCORE_ROAD_SEGMENT_URI=s3://<reference-bucket>/normalized/road_segment
  ```

  `CURRENT_SCORE_ROAD_SEGMENT_URI`는 root를 가리키고, `jobs/current_score.py`가
  `CURRENT_SCORE_ROAD_SNAPSHOT_DATE`로 그 아래 `snapshot_date=<date>/` partition만
  골라 읽는다 — root 전체를 스캔하지 않는다(그 아래 여러 날짜가 함께 쌓여 있을 수
  있어서다). 두 URI 모두 local path/`file://`/`s3://` 형식을 다
  받는다(`de4_core.ObjectStore`가 처리). S3를 쓰려면 Monitoring EC2의 CloudWatch
  IAM 준비와 마찬가지로 Airflow(Project) EC2의 IAM Role에 **reference 버킷**에 대한
  `s3:GetObject`, `s3:ListBucket` 권한이 미리 부여돼 있어야 한다 — access
  key/secret은 여기에 넣지 않고 EC2 Instance Role의 boto3 기본 credential chain을
  쓴다.
- `ZONE_WEATHER_SNAPSHOT_DATA_LAKE_URI`, `BRONZE_COMPACTION_ZONE_WEATHER_SNAPSHOT_URI`
  — `zone_weather_pipeline`(`jobs/weather.py`)이 15분마다 쓰는 Bronze
  `zone_weather_snapshot` 이력의 root와, `bronze_compaction`(`jobs/bronze_compaction.py`,
  #271)이 그 소파일을 압축할 때 읽는 root다(#400). **두 값은 항상 같은 root를
  가리켜야 한다** — 하나만 바꾸면 compaction이 새 파일을 못 찾는다. 로컬에서는
  비워 두면 둘 다 같은 로컬 기본값
  (`data/local-lake/bronze/zone_weather_snapshot`)을 쓴다. 운영에서는 이 project의
  Data Lake bucket(`<data-lake-bucket>`), **Bronze
  계층**(Silver 아님 — raw collection history라서)을 가리키는 값을 넣는다.

  ```env
  ZONE_WEATHER_SNAPSHOT_DATA_LAKE_URI=s3://<data-lake-bucket>/bronze/weather-snapshots
  BRONZE_COMPACTION_ZONE_WEATHER_SNAPSHOT_URI=s3://<data-lake-bucket>/bronze/weather-snapshots
  ```

  `jobs/weather.py`의 `write_zone_weather_snapshot()`도 `de4_core.ObjectStore` +
  `join_uri()`로 쓴다 — local path/`file://`/`s3://` 모두 지원하고, 같은
  `target_time`으로 재실행되면 같은 object 키(`weather_date=D/weather_time=T.parquet`)를
  덮어써 중복 snapshot이 생기지 않는 기존 계약을 그대로 유지한다. S3를 쓰려면
  Airflow(Project) EC2의 IAM Role에 **Data Lake 버킷**에 대한 `s3:GetObject`,
  `s3:PutObject`, `s3:DeleteObject`(`bronze_compaction`이 병합 후 원본을 지우는 데
  필요), `s3:ListBucket` 권한이 미리 부여돼 있어야 한다 — reference 버킷과 권한
  요구사항이 다르다(reference는 읽기 전용, 이 버킷은 읽기/쓰기/삭제).
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
- `AIRFLOW_VAR_STANDARD_COMFORT_SCORE_GOLD_OUTPUT_URI` — `standard_score`
  단계가 PostgreSQL에 쓰기 전에 먼저 저장하는 S3 Gold snapshot 경로다(#265).
  비우면 `STANDARD_COMFORT_SCORE_DATA_LAKE_URI/gold/standard_segment_comfort_score`를
  기본값으로 쓴다. AWS 자격증명은 EMR Serverless execution role(IAM)에
  위임하므로, role에 이 경로가 속한 버킷의 `gold/*`에 대한 `s3:PutObject`
  권한(같은 as_of 재실행 시 기존 객체 교체까지 커버하려면 `s3:DeleteObject`도)이
  미리 부여돼 있어야 한다.
- `AIRFLOW_VAR_GOLD_AUDIT_S3_BUCKET` — `data_quality_audit`의 audit-gold
  task가 GX Data Docs를 올릴 S3 버킷이다(#295). 비워두면 batch-jobs 쪽
  기본값(`de4-data-quality-docs`)을 쓴다. AWS 자격증명은 여기 넘기지 않고
  EMR Serverless execution role(IAM)에 위임한다 — role에 이 버킷에 대한
  `s3:PutObject` 권한이 미리 부여돼 있어야 한다.
- `AIRFLOW_CONN_SLACK_API_DEFAULT`, `AIRFLOW_VAR_SLACK_ALERT_CHANNEL`,
  `AIRFLOW_VAR_EMR_SERVERLESS_LOG_S3_URI`,
  `AIRFLOW_VAR_OBSERVABILITY_FAILED_TASKS_S3_URI`, `AIRFLOW_API_BASE_URL` —
  DAG 실행 결과 Slack 알림(#409)에 필요하다.
  - Slack Bot Token 기반 App을 사전에 만들고(Incoming Webhook 아님 —
    담당자 이메일을 Slack 멘션으로 바꾸려면 Slack Web API 호출이 필요하다),
    `chat:write`, `users:read.email` 스코프를 부여한다. Bot User OAuth
    Token(`xoxb-...`)을 `AIRFLOW_CONN_SLACK_API_DEFAULT=slack://:xoxb-...@`
    형식으로 `.env`에 채운다.
  - `AIRFLOW_VAR_SLACK_ALERT_CHANNEL`은 알림을 보낼 채널(예: `#de4-alerts`).
  - `AIRFLOW_VAR_EMR_SERVERLESS_LOG_S3_URI`는 EMR Serverless Job Run의
    원본 Spark driver/executor 로그를 영구 저장할 S3 위치다. 비우면
    `dags/emr_serverless.py`의 기본값
    `s3://de4-observability-473551908409-ap-northeast-2-an/emr-serverless/logs/`
    (실패 기록과 같은 관측 버킷)을 쓴다. EMR execution role
    (IAM)에 이 버킷에 대한 `s3:PutObject` 권한이 미리 부여돼 있어야 한다
    (다른 EMR 관련 버킷과 마찬가지로 콘솔에서 사람이 준비).
  - `AIRFLOW_VAR_OBSERVABILITY_FAILED_TASKS_S3_URI`는 `on_failure_callback`이
    실패할 때마다 남기는 구조화된 요약 기록(JSON — dag_id/task_id/처리
    일자/담당자/심각도/예외/처리 건수)을 쓸 S3 버킷이다. 기본값은 사용자가
    사전에 만들어 둔 `s3://de4-observability-473551908409-ap-northeast-2-an/airflow/failed-tasks/`.
    `airflow-scheduler`가 쓰는 AWS 자격증명(로컬은 boto3 기본 체인, 운영은
    EC2 Instance Role)에 이 버킷에 대한 `s3:PutObject` 권한이 미리 부여돼
    있어야 한다. §6의 EMR 원본 로그와는 별개다 — 이건 5개 DAG 전부에서
    남고, 사람이 Slack에서 바로 읽을 수 있게 우리가 직접 구조화한
    요약이다.
  - `AIRFLOW_API_BASE_URL`은 DAG Run/Task Instance Slack 알림 링크가 가리킬
    기준 URL이다(Airflow 3.3.1은 `[api] base_url` 설정, `webserver` 섹션이
    아니다). 로컬 기본값은 `http://localhost:8080`, 운영(EC2)에서는 실제
    접속 URL로 채운다.

## hourly_comfort_score 파티션 전환 (#469) — 배포 시 1회

`hourly_comfort_score`는 원래 파티션 없이 루트에 평면으로 쌓였다. 파티션 writer를
배포하기 전에 기존 평면 데이터를 루트에서 치워야 한다 — 평면 파일과 파티션
디렉터리가 한 루트에 공존하면 `spark.read.parquet()`가
`Conflicting directory structures`로 실패한다.

재파티션하지 않고 reference 버킷으로 옮긴다. 삭제가 아니라 이동이므로 판단이
틀렸을 때 되꺼낼 수 있다.

```bash
# 1. standard_score_pipeline DAG 일시정지 (웹 UI 또는 CLI)

# 2. Silver3 평면 데이터를 아카이브로 이동
aws s3 mv --recursive \
    s3://<lake>/silver/hourly_comfort_score/ \
    s3://<reference>/raw/comfort_score_archive/hourly_comfort_score/

# 3. quarantine도 같이 (읽는 곳은 없지만 같은 문제를 갖는다)
aws s3 mv --recursive \
    s3://<lake>/quarantine/hourly_comfort_score/ \
    s3://<reference>/raw/comfort_score_archive/quarantine_hourly_comfort_score/

# 4. 코드 배포 (파티션 writer/reader)

# 5. DAG 재개, 첫 실행 확인
```

**4단계를 2~3단계보다 먼저 하면 안 된다.** 구 writer가 평면 파일을 다시 만들어
같은 문제가 재발한다.

### 전환 후 168시간은 점수가 눌린다

이동 직후 `run_standard_score`의 168시간 윈도우에는 방금 채점한 1시간만 들어 있다.

```
N(qualifying hours): 1
Confidence = N / (N + k) = 1 / 11 ≈ 0.091   (k=10, comfort_score.yaml)
Score = (N·observed + k·mu) / (N + k)       → 91%가 모집단 평균
```

구간 간 점수 차이가 사실상 사라진 상태가 윈도우를 다시 채울 때까지 이어진다.
`current_segment_comfort_score`도 `standard_segment_comfort_score`를 그대로 읽어
날씨 보정만 얹으므로(`jobs/current_score.py`) 같은 영향을 받는다. 개발 단계라
감수하기로 한 판단이다 — 배경은
`docs/superpowers/specs/2026-08-25-hourly-comfort-score-partitioning-design.md`
참고.

## standard_score_pipeline — EMR Serverless 실행 (#292, ADR-0001)

`standard_score_pipeline`은 UTC 기준 매시 정각에 `[logical_date, logical_date + 1시간)`
구간을 처리하며, 아래 순서로 실행된다.

```text
sensor_processing >> hourly_scoring >> standard_score >> report_processing_counts
  >> check_emr_serverless_idle >> stop_emr_serverless_application
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
| standard score | `load-standard-segment-comfort-score` | `hourly_comfort_score` 168시간 롤업 → S3 Gold `standard_segment_comfort_score` snapshot → 서빙 PostgreSQL(`standard_segment_comfort_score`) |

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

### 파이프라인 종료 후 Application 내리기 (#432)

Application의 `autoStopConfiguration`은 idle timeout 15분이라, 파이프라인이
끝나도 15분 동안 유휴 상태로 남는다. 이를 없애려고 마지막에 두 task를 둔다.

| task | 하는 일 |
| --- | --- |
| `check_emr_serverless_idle` | `ListJobRuns`로 아직 terminal 상태가 아닌 Job Run(`PENDING`/`RUNNING`/`SCHEDULED`/`SUBMITTED`)이 있는지 확인하는 `ShortCircuitOperator` |
| `stop_emr_serverless_application` | `EmrServerlessStopApplicationOperator`로 Application을 stop시키고 `STOPPED`까지 대기(15초 × 20회 = 최대 5분) |

idle 확인을 앞에 두는 이유는 **`data_quality_audit`(daily 03:00 UTC)이 같은
Application을 공유**하기 때문이다. EMR Serverless의 StopApplication은
"All scheduled and running jobs must be completed or cancelled before stopping
an application"이라, audit의 Job Run이 도는 중에 stop을 걸면
`ValidationException`으로 실패한다. 실행 중 Job Run이 있으면
`ShortCircuitOperator`가 stop task를 **skipped**로 만들어(실패가 아니다)
DAG Run은 성공으로 남고, 그 경우에는 기존 idle timeout이 그대로 Application을
내린다.

`force_stop`은 기본값 `False`를 유지한다 — `True`면 다른 DAG의 Job Run까지
취소해버린다.

앞선 task가 실패하면 기본 `trigger_rule`(`all_success`)에 따라 여기까지 오지
않으므로, 실패 실행에서는 기존 idle timeout(15분)이 그대로 안전망 역할을 한다.

> **IAM**: `airflow-scheduler`가 쓰는 AWS 자격증명(로컬은 boto3 기본 체인,
> 운영은 EC2 Instance Role — 현재 `de4-serving-api-ec2-role`)에 아래 두 권한이
> 있어야 한다. `emr-serverless:GetApplication`(stop 완료 대기용)은 이미
> 부여돼 있다.
>
> ```json
> {
>   "Effect": "Allow",
>   "Action": [
>     "emr-serverless:ListJobRuns",
>     "emr-serverless:StopApplication"
>   ],
>   "Resource": "arn:aws:emr-serverless:ap-northeast-2:473551908409:/applications/00g85ljahc0svj2p"
> }
> ```
>
> 권한이 없으면 `check_emr_serverless_idle`이 `AccessDeniedException`으로
> 실패하고 실패 알림이 울린다 — 다른 EMR 관련 권한과 마찬가지로 콘솔에서
> 사람이 먼저 준비한다.

> ⚠️ 이 DAG의 실제 EMR Serverless 트리거 검증(entry point 완성, Job Run
> 정상 실행 확인)은 batch-jobs의 커스텀 이미지가 준비되고 Airflow가 EC2로
> 이전된 뒤 별도로 진행한다(#289). 아래 "통합 테스트" 절의 backfill/검증
> 절차는 옛 docker-run 방식 기준이라 지금은 그대로 재현할 수 없다.

## EMR Serverless Job Run 로그 읽기 (#406, #409)

**Airflow Log 탭에는 Spark job 내부 로그가 나오지 않는다.** EMR Serverless로
제출한 task의 Airflow 로그에는 "Job Run을 제출하고 상태를 폴링했다"는 기록만
남는다. batch-jobs가 `logger.info`로 남기는 실제 처리 요약(입출력 경로, 대상
시간대, 처리 건수)은 Spark driver의 stdout으로 나가고, 그건 S3에 쌓인다.
로그가 없는 게 아니라 **다른 시스템에 분리돼 있다**.

로그 위치는 `AIRFLOW_VAR_EMR_SERVERLESS_LOG_S3_URI`가 정한다(비우면
`dags/emr_serverless.py`의 기본값). 경로 규칙은 EMR Serverless가 정한다:

```text
<로그 루트>/applications/<application-id>/jobs/<job-run-id>/SPARK_DRIVER/stdout.gz
```

`<job-run-id>`는 Airflow task의 XCom(`return_value`)에 남고, 실패 알림(#409)의
"EMR Serverless 원본 로그 열기" 링크가 이 경로로 바로 이동한다.

```bash
# 어떤 Job Run들이 있는지 (application-id는 AIRFLOW_VAR_EMR_SERVERLESS_APPLICATION_ID)
aws s3 ls s3://de4-observability-473551908409-ap-northeast-2-an/emr-serverless/logs/applications/<application-id>/jobs/

# driver stdout 읽기 — 여기에 각 job의 요약 한 줄이 있다
aws s3 cp s3://de4-observability-473551908409-ap-northeast-2-an/emr-serverless/logs/applications/<application-id>/jobs/<job-run-id>/SPARK_DRIVER/stdout.gz - \
  | gunzip

# 예: 요약 줄만 뽑기
aws s3 cp s3://.../SPARK_DRIVER/stdout.gz - | gunzip | grep "finished"
```

executor 로그가 필요하면(OOM으로 executor가 죽은 경우 등) 같은 Job Run 아래
`SPARK_EXECUTOR/<executor-id>/stderr.gz`를 본다. 로테이션된 이전 조각은
`SPARK_DRIVER/archived/stdout/stdout_0.gz`처럼 `archived/` 아래에 쌓인다.

> ⚠️ **위 `aws s3 cp`는 Airflow EC2에 SSH로 들어가서 실행하면 실패한다.** 그
> 인스턴스 롤에는 이 버킷에 대한 `s3:PutObject`/`ListBucket`만 있고
> `s3:GetObject`가 없다(로그를 쓰기만 하고 읽지는 않는 역할이라 의도된 최소
> 권한). 로그를 읽을 때는 본인 AWS 자격증명으로 로컬에서 받거나 S3 콘솔에서
> 연다.

각 요약 줄에 무엇이 들어 있는지:

| 커맨드 | 요약에 담기는 것 |
| --- | --- |
| `cleanse-sensor-events` | `target_hour`, feature 입력 윈도우, bronze/quarantine/features 경로, 건수 |
| (같은 Job Run의 feature 단계) | `target_hour`, `road_segment` 경로, 출력 경로, 건수 |
| `score-hourly-comfort` | `processed_at`, 입력/점수/격리 경로, 건수 |
| `load-standard-segment-comfort-score` | 집계 구간 `[as_of - window_hours, as_of)`, data_lake/road_environment/gold version URI, Postgres `host:port/db`, 건수 |
| `audit-gold` | 감사 테이블, Postgres `host:port/db`, row_count, Data Docs S3 위치 |

Postgres는 `host:port/db`만 남고 자격증명은 로그에 남지 않는다. S3 경로 자체는
자격증명이 아니라 로그에 남겨도 안전하다(presigned URL은 남기지 않는다).

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
`write_zone_weather_snapshot`은 이제 `de4_core.ObjectStore`로 쓴다(#400) — local
path/`file://`/`s3://` URI를 모두 받고, 반환값도 항상 URI다. 운영에서는
`ZONE_WEATHER_SNAPSHOT_DATA_LAKE_URI`를 `bronze/weather-snapshots`를 가리키는
`s3://...`로 채운다 — 이 project의 Data Lake 계약상 zone_weather_snapshot은
가공 전 raw collection history이므로 Silver가 아니라 **Bronze**다. `de4-core`는
`bronze_compaction`(#271)이 처음 쓴 이래로 이미 이 컨테이너에 볼륨 마운트 +
`PYTHONPATH`로 들어와 있다(공식 이미지에 설치돼 있는 게 아니다 — 위 compose
주석 참고) — 새 boto3 코드를 여기에 추가하지 않았다.

`bronze_compaction`(#271, `jobs.bronze_compaction`)은 이 `zone_weather_snapshot`을
날짜 파티션별로 소파일 병합하는 job이다 — `BRONZE_COMPACTION_ZONE_WEATHER_SNAPSHOT_URI`가
`ZONE_WEATHER_SNAPSHOT_DATA_LAKE_URI`와 **항상 같은 root**를 가리켜야 이 job이 weather
수집이 방금 쓴 파일을 찾는다. 두 값 다 비우면 같은 로컬 기본 경로를 쓰므로 로컬에서는
자동으로 맞다 — 운영에서 둘 중 하나만 바꾸는 실수를 주의한다.

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

## DAG 실행 결과 Slack 알림 + 담당자 레지스트리 (#409)

`standard_score_pipeline`, `current_score_pipeline`, `zone_weather_pipeline`,
`data_quality_audit`, `bronze_compaction` 5개 DAG(`hello_world` 제외)는
`dags/notifications.py`의 공용 콜백을 쓴다. task가 재시도까지 소진하고
최종 실패하면 담당자 멘션·심각도·처리 일자·처리 건수(이미 성공한 상위
task가 있을 때만, 없으면 정직하게 "집계되지 않음")·Task Instance URL을
담아 Slack에 알리고, 같은 정보를 구조화된 JSON으로도
`AIRFLOW_VAR_OBSERVABILITY_FAILED_TASKS_S3_URI`(기본값: 사용자가 준비한
관측 버킷)에 남긴 뒤 그 링크도 함께 붙인다(EMR 기반 task라면 원본 Spark
로그 링크도 추가). DagRun이 성공하면 처리 건수 요약과 함께 1회 알린다.

담당자/심각도는 저장소 루트의 `config/dag_owners.yaml`에서 관리한다
(`jobs/dag_owners.py`가 로드). 새 DAG나 task에 담당자를 지정하려면:

1. `users`에 담당자가 없으면 추가한다 — `email`(알림 시점에
   `users.lookupByEmail`로 Slack ID를 조회) 또는 `slack_id`(이미 알면 조회
   생략) 중 하나 이상 채운다.
2. `dags.<dag_id>`에 `owner`(위 `users`의 키)와 `severity`
   (`critical`/`high`/`medium`/`low` 중 하나)를 채운다 — DAG 전체의 기본값이다.
3. 특정 task/TaskGroup만 다른 담당자·심각도를 쓰려면 `dags.<dag_id>.tasks`에
   그 task의 전체 dotted id(예: `sensor_processing.run_sensor_processing`)
   또는 TaskGroup id(예: `sensor_processing`)를 키로 추가한다. 조회 순서는
   task_id -> task_group_id -> DAG 기본값이다.
4. `owner`가 `users`에 없거나 `severity`가 유효하지 않으면 DAG 파싱
   시점이 아니라 콜백이 처음 로드를 시도할 때(다음 실행) 에러가 난다 —
   `uv run --package orchestration pytest services/orchestration/tests/test_dag_owners.py`로
   미리 검증할 수 있다.

`report_processing_counts`(`standard_score_pipeline`)와
`report_audit_counts`(`data_quality_audit`)는 EMR Serverless로 제출된
task가 방금 쓴 output을 orchestration 프로세스에서 직접 다시 세어(S3
Parquet/Postgres COUNT) 성공 알림에 넣는 전용 task다 — EMR Job Run
자체는 XCom을 만들 수 없어서다(`jobs/pipeline_counts.py`).

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

- `data_quality_audit`/`standard_score_pipeline` 모두, batch-jobs 커스텀
  이미지 완성 후 실제 EMR Serverless Job Run 트리거 검증과 Airflow의 EC2
  이전(#289 후속 이슈)
- Kafka -> Bronze 오케스트레이션
- CeleryExecutor/KubernetesExecutor 등 분산 실행 지원
- CD는 EC2에서 컨테이너를 기동하고 헬스체크까지 확인한다(#315). 인증 관리자
  교체(SimpleAuthManager → FAB 등)와 RBAC 설정, RDS의 Airflow용 DB(스키마)
  실제 생성은 범위 밖이다 — 사람이 사전에 수행한다
