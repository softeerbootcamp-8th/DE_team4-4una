---
status: accepted
date: 2026-08-19
supersedes:
superseded_by:
---

# 0004. 데이터 품질 검증 도구로 Great Expectations 도입

## 배경

`context/data/quality-rules.md`에 Bronze/Silver/Gold 각 계층의 통계적·선언적
품질 규칙(범위, null 비율, freshness, 참조 무결성 등)이 이미 문서로 정의돼
있지만, 실행 가능한 검증 로직으로는 아직 연결되지 않았다. `hourly_pipeline`의
4개 TaskGroup(cleanse/features/scoring/publish, #157)이 막 완성됐고, `cleanse`
TaskGroup에는 "후속 이슈에서 데이터 품질 검증 task가 이 자리에 추가될 것"이라는
주석이 이미 예약돼 있다.

`publish` TaskGroup을 추가한 #176(PR #183) 로컬 검증 과정에서 오래된
batch-jobs 이미지 태그, 마이그레이션 체크섬 드리프트, Postgres 볼륨 초기화 후
DAG가 `dags_are_paused_at_creation` 기본값으로 다시 paused된 것 같은 사고를
실제로 겪었다. 이런 사고들은 파이프라인을 "통과하는 순간"의 검증만으로는
잡을 수 없고, 이미 적재된 데이터가 조용히 깨진 채로 남아 있을 수 있다는 것을
보여준다. 즉 in-flight 검증(파이프라인 각 단계 게이트)과 at-rest 검증(이미
쌓인 데이터에 대한 주기적 감시) 둘 다 필요하다.

TaskGroup마다 수작업 SQL/Python 검증(`cleansing/validate.py` 선례)을 반복
작성하는 대신, 규칙을 선언적으로 정의하고 in-flight/at-rest 양쪽에서 재사용할
수 있는 도구가 필요하다.

## 결정

데이터 품질 검증 도구로 **Great Expectations(GX)**를 도입한다.

- **실행 엔진은 데이터가 있는 저장소를 기준으로 갈라진다.**
  - **Gold**(`segment_comfort_score`, `vehicle_profile` — Postgres)만
    `SqlAlchemyExecutionEngine`으로 직접 조회한다.
  - **그 외 전부**(Bronze `sensor_event`, Silver `sensor_events_matched`,
    features 산출물 — 전부 S3/`local-lake`의 Parquet)는 in-flight든 at-rest든
    **Spark**(`SparkDFExecutionEngine`)로 검증한다. `cleanse`/`features`/
    `scoring`은 이미 메모리에 있는 DataFrame을 그 자리에서 검증하고, at-rest
    Parquet 감사는 새 CLI 서브커맨드가 파일을 다시 읽어 검증한다.
  - Parquet 감사에 DuckDB나 Pandas 같은 별도 엔진을 쓰지 않은 이유는 "대안"
    절 참고. 지금은 각 in-flight 단계가 이미 `docker run` 컨테이너 하나당
    로컬 Spark 세션을 새로 띄우고 끝나면 버리는 방식(#176 논의 참고, 영속
    Spark 클러스터 없음)이라, at-rest 감사도 같은 패턴을 한 번 더 반복하는
    것뿐이다. 이후 EMR Serverless로 전환되면(ADR 0001 후속) in-flight와
    at-rest 모두 같은 공유 Application으로 함께 옮겨가므로, 엔진을 두 번
    바꿀 필요가 없다.
- **Spark 세션은 새로 띄우지 않고 재사용한다.** GX의
  `SparkFilesystemDatasource`(`SparkDatasource`의 서브클래스)는
  `force_reuse_spark_context=True`가 기본값이라, 이미 활성화된 Spark
  세션/컨텍스트가 있으면 GX가 새로 띄우지 않고 그대로 재사용한다
  (`SparkSession.builder.getOrCreate()`와 동일한 동작). EMR 하나에서
  cleanse→features→scoring→publish 검증까지 전체 실행 흐름을 하나로 묶어
  돌릴 계획이라, Data Context·Checkpoint도 단계마다 새로 만들지 않고 EMR
  job 실행 전체에서 하나를 공유한다. Hadoop/S3 설정이 추가로 필요하면
  `spark_config` dict로 전달할 수 있으나, 이미 구성된 세션을 재사용하는
  경우 대부분 불필요하다. Parquet 자산은
  `SparkFilesystemDatasource.add_parquet_asset(...)`으로 확정한다(공식 API
  레퍼런스로 확인).
- **Batch 캐싱은 기본 동작에 맡긴다.** GX는 기본값(`persist=True`)으로
  검증 대상 Batch를 캐싱해 여러 Expectation이 반복 스캔할 때 재계산을
  막는다. Bronze처럼 대용량일 수 있는 계층에서 메모리 문제가 확인되면
  `persist=False`로 낮추는 것을 각 서브이슈에서 검토한다.
- **Data Docs는 로컬 경로가 아니라 S3에 쓴다.** in-flight 검증이든 at-rest
  감사든 실행이 끝나면 컴퓨트(컨테이너 또는 EMR job)가 사라지므로, 로컬
  파일시스템에 쓰면 리포트도 함께 사라진다. GX의 S3 store backend
  (`TupleS3StoreBackend`, 레거시 GX부터 있던 기능)로 Data Docs를 S3에
  호스팅한다. 이 설정의 정확한 방법은 이번 조사에서 공식 문서 페이지
  렌더링 실패로 직접 확인하지 못했고, `publish` 파일럿 서브이슈 착수 시
  재확인한다.
- **at-rest 감사도 같은 EMR Serverless Application을 공유한다.** ADR-0001
  에서 EMR Serverless Application은 유휴 시 auto-stop되도록 구성하기로
  했고 과금은 Job Run 단위이므로, 별도 Application을 새로 띄울 이유가
  없다. `data_quality_audit` DAG는 hourly_pipeline과 같은 Application에
  자기 몫의 독립적인 Job Run을 제출한다. 월간 road-environment 배치 등
  다른 스케줄에 얹어 같이 도는 것도 아니다 — 자신의 독립 스케줄(예: 매일
  1회)대로 별도 Job Run을 낸다. 같은 Application에 Job Run이 겹칠 수
  있다는 점은 Terraform 프로비저닝(ADR-0001에서 이미 후속 이슈로 남겨둔
  작업) 단계에서 최대 동시 실행 용량을 검토할 사항으로 남긴다.
- **실패 정책도 다르게 간다.** in-flight는 hard fail이다 — Airflow task가
  실패하면 다음 TaskGroup으로 진행하지 않는다(기존 BashOperator 실패
  메커니즘 그대로). at-rest는 알림만 하고 파이프라인을 막지 않는다. v1엔
  Slack 연동이 없으므로 Airflow task 실패와 GX Data Docs 리포트로만 신호를
  주고, Slack 연동은 후속 이슈로 남긴다.
- **Airflow 연결**: 각 기존 TaskGroup 안에 `run_X` 다음 순서로 `validate_X`
  task를 추가한다(`run_cleanse >> validate_cleanse`). TaskGroup 간
  의존관계는 그대로 유지한다. at-rest 감시는 독립 스케줄(예: 매일 1회)을
  갖는 신규 `data_quality_audit` DAG로 분리한다. 이 DAG가 정확히 어느
  계층(Bronze만인지, Silver·features까지 포함하는지)까지 감사할지는 하위
  이슈에서 확정한다.
- **Suite 저장 위치**: `services/batch-jobs/src/batch_jobs/resources/expectations/`
  아래에 JSON으로 저장한다. 기존 `resources/migrations/`와 같은 컨벤션을
  따른다 — git으로 버전 관리하고 배포 이미지에 함께 포함한다.
- **하드 인바리언트는 GX로 옮기지 않는다.** `gold_writer.py`의 PK 중복/NaN
  체크처럼 "절대 위반하면 안 되는" 것은 지금처럼 job 안 인라인 Python으로
  유지한다. GX는 `context/data/quality-rules.md`에 문서화된 통계적/선언적
  규칙(범위, null 비율, freshness, 참조 무결성)만 담당한다.
- **하위 이슈 롤아웃 순서**: ①`publish`(Gold) in-flight 검증(파일럿 —
  테이블이 작고 SQL 엔진이라 CLI+Airflow 연결 패턴을 가장 싸게 검증 가능)
  → ②`cleanse`(Bronze→Silver) in-flight(`quality-rules.md` 규칙이 가장
  많고 이미 `cleansing/validate.py` 선례가 있음) → ③`features`/`scoring`
  in-flight → ④신규 `data_quality_audit` DAG(at-rest 감시 — 예:
  `comfort_score` 범위/freshness, `vehicle_profile_id` 참조 무결성,
  local-lake 결측 시간 탐지, `schema_migrations` vs 실제 DB 드리프트 탐지.
  정확한 대상 계층·테이블은 이 서브이슈 본문에서 확정).

## 대안

| 대안 | 장점 | 단점 | 기각 이유 |
| --- | --- | --- | --- |
| TaskGroup마다 수작업 Python/SQL 검증 계속 작성(`cleansing/validate.py` 패턴 확장) | 새 의존성 없음, 이미 선례 있음 | 규칙이 늘수록 각 job에 검증 로직이 흩어지고 중복됨; 선언적 규칙(`quality-rules.md`)과 실행 코드가 분리돼 규칙 변경 시마다 코드 리뷰가 필요 | 규칙이 이미 여러 계층에 걸쳐 있고 계속 늘어날 예정이라 장기 유지비가 큼 |
| dbt test / dbt-expectations | SQL 기반이라 Postgres 대상(publish)엔 잘 맞음 | 프로젝트에 dbt가 없고, cleanse/features/scoring은 Spark DataFrame이라 SQL 웨어하우스 대상인 dbt가 커버 못 함 | 이미 있는 Spark 실행 경로와 안 맞아 도구를 두 개 병행해야 함 |
| pandera | 가볍고 Python 스키마 검증에 적합 | Spark DataFrame 네이티브 지원이 약함(주로 pandas/polars 대상); Data Docs 같은 사람이 볼 감사 리포트가 없음 | at-rest 감사에 사람이 볼 수 있는 리포트가 필요한데 기본 제공하지 않음 |
| Airflow 자체 SQL/Python sensor로만 at-rest 감시 | 이미 있는 오케스트레이터만으로 해결, 새 도구 없음 | in-flight 검증(Spark DataFrame 단계)엔 그대로 못 씀; 규칙을 선언적으로 재사용할 방법이 없음 | in-flight/at-rest를 하나의 규칙 정의로 묶으려는 목적과 안 맞음 |
| S3/local-lake Parquet at-rest 감사를 DuckDB(SQL 실행 엔진)로 | 임베디드라 JVM 없이 가볍게 뜸; Postgres(SQLAlchemy)와 같은 SQL 엔진 계열로 통일 가능 | 이 repo에 duckdb로 S3를 읽은 전례가 없음(현재 duckdb 용도는 월간 road-environment 파이프라인의 로컬 Parquet 쓰기·테스트 검증용 읽기뿐); EMR Serverless로 파이프라인 전체를 하나의 Spark 환경으로 합치려는 계획과 별개로 세 번째 데이터 처리 엔진이 추가됨 | EMR Serverless 전환 이후 in-flight/at-rest 모두 같은 공유 Application으로 합류시키려는 목표와 맞지 않아 장기 유지 부담이 커짐 |
| S3/local-lake Parquet at-rest 감사를 Pandas로 | 가장 가볍고 의존성 추가 없음(이미 pandas/pyarrow 보유) | Bronze처럼 대용량일 수 있는 계층 전체를 메모리에 올리면 리스크가 있고, EMR Serverless 통합 계획과도 안 맞음 | 데이터 볼륨이 커질수록 메모리 문제로 이어질 수 있어 감사 안정성이 떨어짐 |

## 결과

- `services/batch-jobs`에 `great-expectations[spark]` 의존성이 새로 추가된다.
  **`uv add --package batch-jobs "great-expectations[spark]"`로 실제 확인한
  결과, GX(1.20.0)의 `[spark]` extra가 `pyspark<4.2`를 요구해서 기존에
  잠겨 있던 `pyspark==4.2.0`이 `4.1.3`으로 자동 다운그레이드됐다** (기존
  `pyspark>=4.1.3,<5.0.0` 제약 범위 안이라 에러 없이 resolve됨). 이 상한은
  Spark 4.0이 ANSI SQL 모드를 기본값으로 켜면서 생긴 타입 처리 breaking
  change 때문에 GX가 걸어둔 것으로(GX 체인지로그 1.4.6 항목), 현재 코드가
  4.2 고유 기능(Spark Connect, Python Data Source API 등)에 의존하는
  흔적은 없어 기능적으로 잃는 건 없어 보이지만, **앞으로 pyspark나
  great-expectations를 업그레이드할 때마다 이 상한이 다시 걸리는지 확인이
  필요**하다. GX의 Spark 지원 자체도 공식 호환성 문서에서 "과거엔 됐지만
  계속 보장하지 않음"으로 분류돼 있어, Postgres(SQLAlchemy) 경로보다
  깨질 가능성이 더 높다고 보고 접근해야 한다.
- `hourly_pipeline`의 4개 TaskGroup 각각에 `validate_X` task가 추가돼 DAG가
  길어지고, task 실패 원인이 "실행 실패"인지 "품질 검증 실패"인지 구분해서
  봐야 한다.
- at-rest 알림 경로가 v1엔 Slack 없이 Airflow task 실패/Data Docs로만
  신호를 주므로, 사람이 능동적으로 확인하지 않으면 놓칠 수 있다. Slack
  연동은 후속 이슈로 남는다.
- `gold_writer.py` 같은 하드 인바리언트는 GX로 흡수하지 않으므로, 특정
  규칙을 어느 쪽(인라인 코드 vs GX suite)에 둘지 개별적으로 판단해야 하는
  경계가 생긴다.
- suite JSON이 배포 이미지에 포함되므로, 규칙 변경 시 이미지 재빌드가
  필요하다(마이그레이션 파일과 동일한 배포 특성).
- Data Docs를 S3에 쓰려면 버킷/경로, 그리고 batch-jobs 컨테이너(또는 EMR
  job)가 해당 버킷에 쓸 수 있는 IAM 권한이 추가로 필요하다. 정확한 버킷
  구조는 `publish` 파일럿 서브이슈에서 정한다.

## 영향 범위

- `services/batch-jobs` — `pyproject.toml`에 `great-expectations[spark]`
  의존성 추가(그에 따라 `pyspark`가 `4.1.x`대로 조정됨),
  `resources/expectations/`에 suite JSON 추가, `publish` job(gold writer
  근처)과 향후 `cleanse`/`features`/`scoring` job에 검증 실행 코드 추가,
  at-rest Parquet 감사를 위한 신규 CLI 서브커맨드 추가.
- `services/orchestration` — `hourly_pipeline.py`의 4개 TaskGroup에
  `validate_X` task 추가, 신규 `data_quality_audit` DAG 추가.
- `context/data/quality-rules.md` — 문서화된 규칙이 이 결정으로 실행
  가능한 GX suite와 연결됨을 명시(규칙 자체를 옮기는 작업은 각 하위
  이슈에서 진행).
- 루트 `uv.lock` — 의존성 추가 및 `pyspark` 버전 조정에 따라 갱신.

## 참고

- 관련 이슈: #157(상위), #176 / PR #183(이 결정의 배경이 된 로컬 검증 사고
  발생)
- 새 부모 이슈: #190(in-flight 검증 4개 + at-rest 감사 DAG 하위 이슈 롤아웃)
- 참고 문서: `context/data/quality-rules.md`
- Great Expectations 호환성 문서:
  https://docs.greatexpectations.io/docs/help/compatibility_reference/
- Great Expectations 문서(이번 결정에 직접 참고):
  - Connect to dataframe data:
    https://docs.greatexpectations.io/docs/core/connect_to_data/dataframes/
  - Connect to Filesystem data:
    https://docs.greatexpectations.io/docs/core/connect_to_data/filesystem_data/
  - `SparkFilesystemDatasource` API 레퍼런스:
    https://docs.greatexpectations.io/docs/reference/api/datasource/fluent/SparkFilesystemDatasource_class
  - Install additional dependencies (Spark):
    https://docs.greatexpectations.io/docs/core/set_up_a_gx_environment/install_additional_dependencies?dependencies=spark
