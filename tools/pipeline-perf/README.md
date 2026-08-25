# pipeline-perf

승차감 점수 파이프라인의 성능 베이스라인을 명령 한 번으로 수집하고, 같은 명령으로
최적화 전후를 비교하기 위한 오프라인 도구다(#460, #462, #492).

`services/*`가 아니라 `tools/`에 있는 이유는 런타임 경로가 아니기 때문이다. 배포되지
않고, 사람이 손으로 돌린다.

## 수집 계층

| 계층 | 소스 | 담는 것 |
| --- | --- | --- |
| L1 Airflow | REST API v2 | task별 시간·시도, DAG run 총시간, task 사이 gap, Asset 트리거 대기, `report_processing_counts` XCom |
| L2 EMR Serverless | `get-job-run` / `list-job-runs` | 프로비저닝 대기(`createdAt`→`startedAt`), 실행시간, `billedResourceUtilization` |
| L3 Spark | S3 event log | job/stage/task 수, 스테이지 wall time, task duration p50/p95/max와 skew, shuffle, spill, GC, I/O, SQL execution, 동시 태스크 수 대 가용 슬롯 |
| L4 PERF 로그 | `SPARK_DRIVER/stdout`·`stderr`, Airflow task 로그 | `de4_core.perf_phase`가 남긴 Spark 밖 구간(#461) |

L3은 Spark를 쓰는 `standard_score_pipeline`에만 해당한다.
`current_score_pipeline`은 L1 + L4로 본다.

L4의 위치는 실제 Job Run으로 확인했다. batch-jobs의 PERF 줄은 driver **stdout**에
남고 stderr에는 Spark 자신의 log4j 출력만 있다. EMR을 거치지 않는 task는 S3에 driver
로그가 아예 없어 Airflow task 로그에서 같은 줄을 읽는다.

event log는 스트리밍 파싱한다. `run_sensor_processing` 한 건이 19파일 237MB라
전량 적재가 불가능해서, 파일을 순서대로 이어 읽으며 필요한 이벤트만 즉시 집계하고
버린다. 개별 task는 보관하지 않고 스테이지별 누적합과 분위수용 duration 표본만
남긴다.

## 사용법

```bash
# 인증 (Airflow REST API v2)
export AIRFLOW_API_BASE_URL=http://<airflow-host>:8080
export AIRFLOW_API_USERNAME=... AIRFLOW_API_PASSWORD=...   # 또는 AIRFLOW_API_TOKEN

# 수집 — 원시 JSON은 out/perf/에 떨어지고 커밋하지 않는다
uv run --package pipeline-perf pipeline-perf collect \
    --dag-id standard_score_pipeline \
    --last 10 \
    --out out/perf/

# 리포트 — 커밋하는 것은 이 마크다운이다
uv run --package pipeline-perf pipeline-perf render out/perf/*.json \
    -o docs/perf/2026-08-25-comfort-score-pipeline-baseline.md

# 최적화 전후 비교
uv run --package pipeline-perf pipeline-perf compare \
    --before out/perf/collect-before.json \
    --after out/perf/collect-after.json
```

### 수집할 실행 고르기

`collect`의 선택자는 셋 중 하나다.

| 선택자 | 무엇을 가져오는가 |
| --- | --- |
| `--last N` (기본 5) | 최근 N건 |
| `--since` / `--until` | 그 시간 구간의 실행 (`--last`가 개수 상한) |
| `--run-id` (반복 가능) | 지목한 실행만. 목록 조회를 건너뛴다 |

```bash
# 실행 1건만 — 파이프라인 1회 실행에 대한 성능 검증
uv run --package pipeline-perf pipeline-perf collect \
    --dag-id standard_score_pipeline \
    --run-id 'scheduled__2026-08-25T09:00:00+00:00'

# 시간 구간 — 최적화 전후를 같은 시간대끼리 비교할 때
uv run --package pipeline-perf pipeline-perf collect \
    --dag-id standard_score_pipeline \
    --since 2026-08-25T09:00:00Z --until 2026-08-25T10:00:00Z
```

**단일 실행을 권장한다.** `compare`는 DAG run 1건당 평균으로 비교하는데, 수집 구간에
실패·재시도한 실행이 섞이면 평균이 크게 움직인다. 실제 베이스라인 수집에서 5건 평균의
Airflow gap은 25.4%였지만 정상 실행 1건만 보면 0.3%였다(#492). 수집도 그만큼 빠르다 —
`--last 5`는 event log 약 30건을 파싱해 4분이 걸리지만 1건이면 6건만 읽는다.

`--since` / `--until`의 기준 시각은 **`run_after`** 다. `run_after`가 `dag_run_id`에
박히는 시각이라 사람이 "9시 실행"이라고 부르는 것과 일치한다. `logical_date`는 data
interval의 시작이라 09:00 실행이 08:00으로 잡혀 한 시간 어긋나고, `start_date`는
스케줄러 지연만큼 밀린다. 시간대를 안 붙인 값은 UTC로 읽는다.

`--run-id`는 DAG 하나를 대상으로 쓴다. 존재하지 않는 실행을 넘기면 수집이 실패하지
않고 그 사실을 `notes`에 남긴 채 나머지를 이어간다.

AWS 자격증명은 boto3 기본 체인을 따른다. 프로필을 쓰면 `--aws-profile`,
`--aws-region`을 넘긴다. Application ID·로그 URI·Bronze 입력 경로는 지정하지 않으면
Airflow Variable(`EMR_SERVERLESS_APPLICATION_ID`, `EMR_SERVERLESS_LOG_S3_URI`,
`CLEANSING_BRONZE_INPUT_PATH`)에서 읽는다.

**주의**: 현재 배포는 이 Variable들을 `AIRFLOW_VAR_*` 환경변수로 주입한다. 환경변수
Variable은 Airflow 메타DB에 없어서 REST API의 `/variables`가 404를 준다 — 그래서
`--application-id`, `--log-uri`, `--bronze-input-uri`를 직접 넘겨야 한다. 안 넘기면
수집은 실패하지 않고 L2~L4를 건너뛴 채 그 사실을 `notes`에 남긴다.

`--no-spark`는 L3를 건너뛴다. 큰 event log를 안 읽어 빠르므로, 타임라인만 급히
볼 때 쓴다.

## 필요한 권한

- 관측 버킷의 `emr-serverless/logs/*`에 대한 `s3:GetObject`, `s3:ListBucket`
- `emr-serverless:GetJobRun`, `emr-serverless:ListJobRuns`
- Airflow REST API 읽기 권한

## 리포트 규칙

`render`가 만드는 리포트의 마지막 절("관찰된 병목 후보")은 측정된 사실만 나열한다.
원인 진단과 최적화 방안은 담지 않는다 — 검증되지 않은 추측이 리포트에 사실처럼
남는 것을 막기 위해서다(#462).
