# data_quality_audit DAG — at-rest Gold 감시 설계 (#253)

## 배경

ADR-0004 롤아웃 ④번(at-rest 감시용 신규 `data_quality_audit` DAG)의 구현
이슈다. 기존 in-flight 검증(#220 완료, #249 미머지 — `standard_score_pipeline`의
`validate_hourly_scoring`/`validate_standard_score`)은 이번 실행분만 검사하므로,
실행 자체가 조용히 멈추거나 과거 적재분이 사후에 깨지는 경우를 잡지 못한다.
v1은 Gold 두 테이블(`standard_segment_comfort_score`,
`current_segment_comfort_score`)의 전체 범위·freshness·`vehicle_profile_id`
참조 무결성을 매일 1회 독립 스케줄로 감시하는 새 DAG를 도입한다.

관련 ADR: `docs/adr/0004-data-quality-validation-with-great-expectations.md`
관련 이슈: #190, #248, Refs #253

> 이 문서는 브레인스토밍 과정에서 사용자와 세 가지 쟁점(Data Docs 저장소,
> task 구현 방식, freshness 임계값)을 놓고 두 차례 이상 재검토를 거쳐
> 정리됐다. 처음 제안이 뒤집힌 지점(§1, §2)에는 왜 뒤집혔는지 근거를
> 남긴다.

## 확정된 결정

### 1. Data Docs 저장소 — 실 AWS S3 (`TupleS3StoreBackend`)

**결론**: 실제 AWS S3 버킷(`de4-data-quality-docs`, `ap-northeast-2`, 콘솔에서
사용자가 직접 생성)에 Data Docs를 쓴다. 경로 접두사는
`data-quality-audit/gold/`.

**뒤집힌 근거**: 처음엔 이 repo 전체에 아직 실 AWS 연동이 하나도 없다는
점(Bronze조차 local-lake Parquet)과 "Local-first boundaries: AWS 없이 개발
가능해야 한다"는 `context/architecture.md` 원칙을 근거로 로컬
파일시스템(`TupleFilesystemStoreBackend` + host 볼륨 마운트)을 제안했다.
그런데 이 프로젝트는 곧 Airflow를 EC2로, Spark job을 EMR(Serverless
포함)로 옮길 예정이고, 그 순간 Airflow(EC2)와 Spark job(EMR)은 **물리적으로
분리된 별개의 호스트**가 된다. 지금 로컬에서 "컴퓨트보다 오래 남는
저장소" 문제를 host 볼륨 마운트로 눈속임할 수 있는 건 두 프로세스가 같은
host 위에 있기 때문일 뿐이다. EMR Serverless job은 그 자체가 매번
뜨고 사라지는 일회성 컴퓨트라 로컬 디스크에 남기면 job이 끝나는 순간
사라지고, EC2와 공유할 방법도 없다. 즉 AWS 전환 이후엔 S3(또는 이에
준하는 공유 저장소)가 구조적으로 필요해지며, GX 공식 문서도 이 시나리오를
`TupleS3StoreBackend`의 표준 용례로 문서화한다. 마이그레이션이 임박한
상태에서 로컬 shim을 만들었다가 곧 버리는 것보다 지금 바로 S3로 가는 게
낫다고 판단해 원래 방향으로 되돌렸다.

**버킷 설정**(사용자가 콘솔에서 생성):

| 항목 | 값 |
| --- | --- |
| 버킷 이름 | `de4-data-quality-docs` |
| 리전 | `ap-northeast-2` |
| Object Ownership | ACLs disabled (bucket owner enforced) |
| Block Public Access | 네 항목 모두 체크 |
| Versioning | 비활성화 |
| 암호화 | 기본값(SSE-S3) |

**자격증명**: `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_REGION`을
`.env`(직접 추가, git에 올리지 않음)에서 읽어 `docker run -e`로 전달한다.
IAM 정책은 이 버킷 ARN으로만 범위를 좁힌다. 버킷 이름은
`GOLD_AUDIT_S3_BUCKET` 환경변수로 override 가능하게 하되 기본값을
`de4-data-quality-docs`로 둔다.

### 2. Task 구현 방식 — `BashOperator` + `docker run` (batch-jobs 이미지 재사용)

**결론**: 기존 in-flight 검증(`validate_sensor_processing`,
`validate_standard_score`)과 동일하게 `BashOperator`로 `batch-jobs` 컨테이너를
띄워 실행한다. 검증 로직/Expectation Suite는 `services/batch-jobs`가 계속
소유한다.

**논의 과정**: "in-flight와 실행 스타일을 통일해야 한다"는 이유는 기각됐다
— at-rest 감사는 파이프라인 실행 중 게이트가 아니라 완전히 독립된 DAG라
스타일을 맞출 이유가 없다는 지적이 맞다. 실제 갈림길은 두 가지였다:

- **PythonOperator + `batch_jobs` import**: `services/orchestration`이
  지금까지 한 번도 `batch_jobs`를 import한 적이 없다(`jobs/weather.py`,
  `jobs/current_score.py` 모두 orchestration 자체 재구현). 이 경계를 깨는
  새로운 서비스 간 결합이 생긴다.
- **PythonOperator + orchestration에 검증 로직 독자 구현**: 서비스 경계는
  지키지만 Gold 검증 규칙(range/freshness/참조무결성)을 batch_jobs와
  orchestration 두 곳에 나눠 유지해야 한다.
  `infra/compose/airflow.yaml`의 `_PIP_ADDITIONAL_REQUIREMENTS`로 GX를
  Airflow 이미지에 추가하는 것 자체는 이미 있는 정식 경로(현재도
  `psycopg2-binary`/`pyarrow`/`requests`가 이렇게 설치됨)라 마찰이 크지
  않다는 것도 확인했다.

한 차례 PythonOperator+독자구현 쪽으로 기울었으나, 최종적으로는 GX의
전이 의존성(sqlalchemy/pydantic 등)이 Airflow 3.3.1 자체가 고정한 버전과
`_PIP_ADDITIONAL_REQUIREMENTS`(런타임 pip install, 클린 resolve 아님) 경로에서
충돌할 불확실성을 미리 피하는 쪽을 택해 `BashOperator`로 확정했다. 검증
로직은 batch_jobs 안에 있으므로 Gold 규칙의 단일 소유권도 그대로 유지된다.

### 3. 검증 대상·스코프 — 전체 테이블, 테이블당 1 task

이슈 본문이 명시한 "전체 범위" 그대로, `WHERE` 없이 테이블 전체를 매번
스캔한다(증분/워터마크 없음 — v1 단순화, 테이블 규모가 커지면 후속 과제).

테이블당 Airflow task 1개, 총 2개(`audit_standard_segment_comfort_score`,
`audit_current_segment_comfort_score`). 두 task는 서로 의존관계가 없다(병렬).

### 4. 검증 항목 — 테이블당 배치(batch) 2개

`SqlAlchemyExecutionEngine`(`context.data_sources.add_postgres` +
`add_query_asset` + `add_batch_definition_whole_table`, #249의
`standard_score_validation.py` 패턴 재사용)으로 테이블당 쿼리 2개를 각각
GX 배치로 검증한다:

1. **range 배치**: `SELECT * FROM <table>` — 전체 행 대상으로
   `comfort_score`/`vertical_score`/`longitudinal_score`/`lateral_score`가
   각각 `0~100` 범위인지 검사(`ExpectColumnValuesToBeBetween` × 4).
2. **summary 배치**: 한 행짜리 집계 쿼리 — freshness와 참조 무결성을 같은
   suite 안 두 expectation으로 묶는다(`sensor_processing_validation.py`의
   `quarantine_rate` 한-행 DataFrame 패턴과 동일한 발상을 SQL 쪽으로 옮김):

   ```sql
   SELECT
       EXTRACT(EPOCH FROM (now() - MAX(score_as_of))) AS age_seconds,
       (SELECT count(*) FROM <table> t
          LEFT JOIN vehicle_profile vp
            ON t.vehicle_profile_id = vp.vehicle_profile_id
          WHERE vp.vehicle_profile_id IS NULL) AS orphan_vehicle_profile_count
   FROM <table>
   ```

   (`current_segment_comfort_score`는 `score_as_of` 대신
   `calculated_at`을 freshness 기준 컬럼으로 쓴다 — 이 테이블엔
   `score_as_of` 컬럼이 없다.)

   - `age_seconds <= 10800`(**3시간**, 사용자 확정값) —
     `standard_segment_comfort_score`는 매시간 갱신되는 파이프라인의
     여유치, `current_segment_comfort_score`는 두 producer(시간별
     standard, 15분별 weather-change) 중 하나가 최소 그만큼의 주기로는
     돌아야 한다는 신호로 같은 값을 재사용한다.
   - `orphan_vehicle_profile_count = 0` — `standard_segment_comfort_score`는
     이미 DB `FOREIGN KEY`로 이 위반이 구조적으로 불가능하지만(0006
     마이그레이션), 이슈 본문이 두 테이블 모두를 명시했고 검증 비용이
     저렴하므로 그대로 포함한다(항상 통과하는 안전망). 반대로
     `current_segment_comfort_score.vehicle_profile_id`는 DB FK가 **없어서**
     (0006 마이그레이션에 `REFERENCES` 없음) 여기가 이 체크가 실제로
     의미를 갖는 지점이다.

스키마/PK/필수값 같은 하드 인바리언트는 각 writer(`standard_writer.py`,
`jobs/current_score.py`)가 쓰기 시점에 이미 강제하므로 다루지 않는다
(ADR-0004: 하드 인바리언트는 GX로 옮기지 않는다).

### 5. Suite 파일

`services/batch-jobs/src/batch_jobs/resources/expectations/`에 4개 추가
(#249의 in-flight suite와 이름이 겹치지 않도록 `_audit_` 포함):

- `standard_segment_comfort_score_audit_range_suite.json`
- `standard_segment_comfort_score_audit_summary_suite.json`
- `current_segment_comfort_score_audit_range_suite.json`
- `current_segment_comfort_score_audit_summary_suite.json`

### 6. 실패 정책 — soft fail

검증에 실패해도 **Data Docs는 항상 먼저 빌드/업로드한 뒤** 예외를 던져
Airflow task를 fail시킨다(`try`로 두 배치 검증 결과를 모으고 `finally`에서
`build_data_docs()` 호출 후, 실패 시 `GoldAuditValidationFailed`를 raise).
이 DAG는 outlet이 없어 다른 DAG를 구독하지 않으므로, task가 실패해도
`current_score_pipeline` 등 다른 파이프라인은 막히지 않는다(ADR-0004:
"task 실패로 신호만 주고 다른 DAG는 막지 않음").

## 전체 데이터 흐름

```
standard_segment_comfort_score (Postgres)  ─┐
current_segment_comfort_score (Postgres)   ─┤
                                             │  SqlAlchemyExecutionEngine
                                             ▼
                          batch_jobs.gold_audit_validation
                          (range 배치 + summary 배치, 테이블당)
                                             │
                          ┌──────────────────┴──────────────────┐
                          ▼                                     ▼
                  성공/실패 판정                       Data Docs 빌드
                  (task 성공/soft fail)         → S3(de4-data-quality-docs/
                                                   data-quality-audit/gold/)
```

## 컴포넌트

### `services/batch-jobs`

- `src/batch_jobs/gold_audit_validation.py`(신규) —
  `GoldAuditValidationConfig.from_env()`(`POSTGRES_*`,
  `GOLD_AUDIT_S3_BUCKET`, `AWS_REGION` 재사용 + suite 경로),
  `run_gold_audit(config, connection, table)` → 인자로 받은 테이블 하나에
  대해 range/summary 배치 검증 + Data Docs 빌드, `GoldAuditSummary`
  dataclass(row_count/age_seconds/orphan_count/성공 여부)를 반환.
  `GoldAuditValidationFailed` 예외로 실패 신호. `table`은
  `standard_segment_comfort_score`/`current_segment_comfort_score` 리터럴만
  허용(그 외 값은 즉시 `ValueError`).
- `resources/expectations/*_audit_*_suite.json`(신규 4개, §5).
- `cli.py`에 `audit-gold` 서브커맨드 추가. `--table` 필수 인자(값은
  `standard_segment_comfort_score` 또는 `current_segment_comfort_score`) —
  DAG의 task 2개가 각각 테이블 하나씩만 대상으로 호출한다(아래 orchestration
  절). `run_gold_audit_cli`가 psycopg2 connection을 열고
  `run_gold_audit(config, connection, table)` 호출, 결과를 JSON으로 출력.
- `pyproject.toml`에 `great-expectations[spark,postgresql,s3]` (S3 store
  backend용 `boto3` extra 추가 — `[spark]`는 기존 sensor_processing
  검증에서 이미 필요).

### `services/orchestration`

- `dags/data_quality_audit.py`(신규) — `dag_id="data_quality_audit"`,
  `schedule="0 3 * * *"`(UTC), `catchup=False`, outlet 없음. 두
  `BashOperator`(`audit_standard_segment_comfort_score`,
  `audit_current_segment_comfort_score`)가 병렬로
  `docker run ... batch-jobs audit-gold --table <table_name>`을 실행 —
  `audit_standard_segment_comfort_score` task는
  `--table=standard_segment_comfort_score`,
  `audit_current_segment_comfort_score` task는
  `--table=current_segment_comfort_score`. `POSTGRES_*`,
  `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`,
  `GOLD_AUDIT_S3_BUCKET`를 env로 전달(local-lake 마운트 불필요 — Postgres만
  조회). 테이블당 task를 분리한 이유는 Airflow UI에서 어느 테이블이
  실패했는지 바로 보이게 하기 위함이다(§3).

## 테스트 전략

- **`test_gold_audit_validation.py`**(신규, batch-jobs): suite 로딩, summary
  쿼리 빌더(테이블명에 따라 freshness 컬럼이 `score_as_of`/`calculated_at`로
  갈리는지) 단위 테스트. 실제 DB 검증은
  `test_standard_score_validation.py`와 동일하게 `RUN_INTEGRATION=1` 게이트로
  로컬 Postgres에 붙어: 정상 데이터 success, 범위 밖 점수/존재하지 않는
  `vehicle_profile_id`/stale 값 각각 실제 실패를 확인.
- **`test_cli_dispatch.py`**: `audit-gold` 서브커맨드 파싱 확장.
- **`test_data_quality_audit_dag.py`**(신규, orchestration): DAG 구조
  파싱(task 존재, 스케줄, outlet 없음) — 기존 `test_standard_score_pipeline_dag.py`
  패턴.
- **로컬 Airflow 수동 확인**(완료 조건): 정상 데이터로 success, 의도적으로
  깨뜨린 데이터(범위 밖 점수, 존재하지 않는 `vehicle_profile_id`, stale
  `score_as_of`/`calculated_at`)로 실제 soft fail을 로컬 Airflow에서 확인.
  S3에 Data Docs가 쓰였는지, 리포트를 열람할 수 있는지 확인.

## 제외 범위

- Bronze/Silver(S3 local-lake Parquet) at-rest 감시 — 별도 이슈
- `schema_migrations` vs 실제 DB 드리프트 탐지 — 별도 이슈
- Slack 등 실시간 알림 연동
- 증분/워터마크 기반 스캔(테이블이 커졌을 때의 최적화) — v1은 항상 전체
  스캔
- `current_score_pipeline`의 행 단위 격리/서킷브레이커 로직 자체 — #251
- ADR-0004 본문의 "S3에 쓴다" 결정 자체를 바꾸는 것이 아니라, 이번 구현이
  그 결정을 실제 버킷으로 구체화한다는 것을 ADR-0004에 짧은 수정 노트로
  반영하는 작업(§1) — 구현 PR에서 함께 처리
