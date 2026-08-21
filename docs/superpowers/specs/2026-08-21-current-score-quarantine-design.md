# current_score_pipeline 행 단위 격리 & GX 서킷브레이커 설계

> 관련 이슈: #251 (feat: add row-level quarantine and gx circuit breaker to
> current_score_pipeline). 아키텍처 결정 근거는 [ADR-0008](../../adr/0008-current-score-row-level-quarantine-and-circuit-breaker.md)
> 참고.

## 1. 목표

`run_current_score_job`이 UPSERT하는 `current_segment_comfort_score`(서빙
API가 직접 읽는 테이블)에 in-flight 데이터 품질 보호를 추가한다.

- 평소(soft): 정상 행은 계속 UPSERT, 이상 행만 별도 격리 테이블로 분기한다.
- 파국적일 때만(hard): 정상 행이 0건이거나 격리율이 25%를 넘으면 이번 실행
  전체를 hard fail시켜 `current_segment_comfort_score`에 아무것도 쓰지 않는다.

## 2. 아키텍처 / 데이터 흐름

`run_current_score_job`의 기존 배치 루프(`_read_standard_rows` → `_build_row`
→ UPSERT, 최대 5000행씩, 전체가 하나의 트랜잭션) 구조를 유지한 채, 각 배치마다:

1. `_build_row`로 만든 행들을 pandas DataFrame으로 변환한다.
2. GX `PandasExecutionEngine`으로 그 배치 DataFrame을 Expectation Suite에
   대해 검증한다 — **UPSERT 이전**, in-memory로 수행한다. UPSERT 후 라이브
   테이블을 검증하는 방식은 채택하지 않는다: `current_segment_comfort_score`는
   `(segment_id, vehicle_profile_id)`당 단일 최신 행만 가지므로, 이상 값으로
   기존 정상 값을 먼저 덮어쓴 뒤 사후 검증하면 되돌릴 이전 값이 없어 더
   위험하다.
3. 각 Expectation의 `unexpected_index_list`를 모아 그 배치에서 격리 대상
   행 인덱스 집합을 만든다.
4. 정상 행만 기존 `_UPSERT_SQL`로 UPSERT, 격리 행은
   `current_segment_comfort_score_quarantine`에 별도 INSERT — 같은 커넥션·
   같은 트랜잭션.
5. 모든 배치 처리 후, 최종 `connection.commit()` 직전에 누적 카운트(정상/
   격리/전체)로 서킷브레이커를 판정한다. 트립되면 예외를 던져 기존
   `except Exception: connection.rollback(); raise` 경로를 그대로 태워
   UPSERT와 격리 INSERT 전부를 롤백한다.

서킷브레이커 신호(정상/격리 비율)는 위 1차 GX 검증에서 이미 나온
`unexpected_index_list` 집계로 계산한다. 이슈 본문이 언급한
`SqlAlchemyExecutionEngine`으로 UPSERT 직후 재검증하는 옵션은 채택하지
않는다 — 이미 UPSERT 이전에 같은 신호를 얻을 수 있고, "이상 행을 먼저 쓰지
않는다"는 원칙과도 맞지 않기 때문이다.

## 3. 격리 테이블 스키마

새 테이블 `current_segment_comfort_score_quarantine`을 `services/batch-jobs`의
마이그레이션(`0011_*.sql`)으로 추가한다. `sensor_event_quarantine`의 철학
(원본 페이로드 + 거부 사유 보존)을 Postgres/JSONB로 옮긴다. 메인 테이블과
컬럼을 1:1로 복제하지 않는다 — 규칙이 바뀔 때마다 두 테이블 스키마를 함께
마이그레이션해야 하는 부담을 피한다.

```sql
CREATE TABLE current_segment_comfort_score_quarantine (
    id BIGSERIAL PRIMARY KEY,
    segment_id TEXT NOT NULL,
    vehicle_profile_id INTEGER NOT NULL,
    calculated_at TIMESTAMPTZ NOT NULL,   -- 같은 실행의 정상 행과 동일한 값(조인 키)
    reject_reason TEXT NOT NULL,          -- 위반한 GX expectation 이름
    reject_detail JSONB NOT NULL,         -- GX 개별 expectation 결과(관측값 등)
    raw_row JSONB NOT NULL,               -- UPSERT됐어야 할 전체 계산 행
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ON current_segment_comfort_score_quarantine (segment_id, vehicle_profile_id);
CREATE INDEX ON current_segment_comfort_score_quarantine (calculated_at);
```

- `(segment_id, vehicle_profile_id)`에 유니크 제약을 걸지 않는다 — 메인
  테이블과 달리 "현재 상태"가 아니라 append-only 거부 로그라서, 같은 키가
  여러 실행에 걸쳐 반복 격리될 수 있다.
- 메인 테이블로 FK를 걸지 않는다(격리된 키가 메인 테이블에 아직 없을 수도,
  이전 값이 남아있을 수도 있다).
- 재처리/복구 워크플로는 이슈의 제외 범위이므로, 이 스키마는 감사·조회용
  최소 요건까지만 만족한다.

## 4. GX Expectation Suite

Suite는 `PandasExecutionEngine`으로 배치 DataFrame(정상/이상 판정 전, UPSERT
이전)에 대해 실행한다.

포함 규칙(quality-rules.md, `context/comfort-score.md` Step C 근거):

1. `comfort_score`/`vertical_score`/`longitudinal_score`/`lateral_score` ∈ [0, 100]
2. `confidence_score` ∈ [0, 1]
3. `sample_count` ≥ 0
4. 방향별 가중합 항등식(`low_visibility` 비활성 시
   `comfort_score == 0.5*vertical + 0.3*longitudinal + 0.2*lateral`, 허용오차
   내) — `low_visibility` 판정 자체는 job 코드가 미리 계산해 배치 DataFrame에
   `identity_diff` 파생 컬럼(비활성 행은 0)으로 넣고, GX는 그 컬럼이 허용오차
   이내인지만 선언적으로 검증한다.
5. `standard_score_as_of` NOT NULL

**제외한 규칙**: `weather_time`/`weather_rule_version`/`weather_impact_signature`
NULL 짝 제약은 GX Expectation으로 넣지 않는다. 이미 (a) `_build_row`가 코드
구조상 항상 셋을 함께 채우거나 함께 비우고, (b) DB `CHECK` 제약(0006/0009)이
어겨지면 INSERT 자체가 실패한다 — ADR-0004의 "하드 인바리언트는 GX로 옮기지
않는다" 원칙에 해당하는 중복 검증이라 제외한다.

**보류(open question)**: `standard_score_as_of` 신선도(몇 시간 이내여야
"참조 정합"으로 볼지)는 이번 서브이슈 범위에서 구체적 임계치를 정하지 않고
`context/open-questions.md`에 새 미해결 항목으로 남긴다.

## 5. 서킷브레이커 & 트랜잭션 처리

- 판정은 실제로 처리한 행이 있을 때만(`rows_seen = quarantined_count +
  upserted_count > 0`) 수행한다. `changed_zones_only=True`인데 변경된 zone이
  없어 조기 반환하는 기존 케이스(`CurrentScoreJobSummary(0,0,0)`)는 "격리율
  100%"가 아니라 "처리할 게 없었다"이므로 서킷브레이커 대상에서 제외한다.
- 트립 조건(전체 배치 처리 후, 최종 `commit()` 직전 1회 평가):
  - `upserted_count == 0` (rows_seen > 0인데 정상 행이 하나도 없음), 또는
  - `quarantined_count / rows_seen > 0.25`
- 트립 시 전용 예외 `CurrentScoreCircuitBreakerTripped`(카운트/비율을 메시지에
  포함)를 던진다. `hourly_segment_feature_storage`의 평범한 `ValueError`
  선례와 달리 전용 타입을 쓰는 이유는, 이 값이 서빙 API가 직접 읽는 안전장치라
  로그·테스트에서 실패 원인이 바로 구분돼야 하기 때문이다.
- 이 예외는 기존 `try/except Exception: connection.rollback(); raise` 경로를
  그대로 타서, 이번 실행에서 UPSERT한 정상 행과 격리 테이블에 INSERT한 행
  전부(같은 트랜잭션) 롤백된다.
- Airflow에서는 별도 task 분리 없이 기존 단일 `run_current_score`
  `PythonOperator`가 그대로 실패(hard fail)한다 — 이게 #249/#250(검증 task
  추가)과 스코프가 다른 지점: 쓰기 경로 자체에 행 단위 분기가 필요하다.

## 6. 모듈 / 코드 구조

- 신규 모듈 `services/orchestration/jobs/current_score_quarantine.py`: GX
  suite 로딩, `PandasExecutionEngine` 검증 호출, `unexpected_index_list` →
  행별 reject reason/detail 추출, 격리 INSERT SQL, 서킷브레이커 판정 함수
  (`CurrentScoreCircuitBreakerTripped` 포함)를 담는다.
- `current_score.py`는 배치마다 이 모듈을 호출해 정상/격리를 나누고, 루프
  종료 후 서킷브레이커를 평가하는 오케스트레이션만 담당한다.
- `CurrentScoreJobSummary`에 `quarantined_count: int` 필드를 추가해 XCom/로그로
  관측 가능하게 한다.

## 7. 의존성 / 서비스 경계 변경

- `services/orchestration/pyproject.toml`에 `great-expectations>=1.21.0`(
  `services/batch-jobs`와 같은 하한 버전, 단 `[spark,postgresql]` extra는
  불필요 — `SqlAlchemyExecutionEngine`/Spark를 쓰지 않으므로), `pandas>=2.0.0`
  (GX `PandasExecutionEngine`의 필수 의존성) 추가.
- `services/batch-jobs/src/batch_jobs/resources/migrations/0011_*.sql` 신규
  추가 — 마이그레이션 체계가 이 서비스에만 있어 불가피한 서비스 경계 침범.
  두 항목 모두 사용자 승인 완료.

## 8. 테스트 계획

기존 컨벤션(`test_current_score.py`가 `FakeConnection`/`FakeCursor`로 실제 DB
없이 단위 테스트, `test_current_score_pipeline_dag.py`는 DAG 구조만 검증)을
따른다.

- `current_score_quarantine.py` 단위 테스트: 정상 배치만 있는 경우(격리
  0건), 일부만 이상한 경우(정상 행은 UPSERT, 이상 행은 격리 INSERT, 서킷
  브레이커 미발동), 파국적인 경우(정상 0건 또는 격리율 >25% →
  `CurrentScoreCircuitBreakerTripped` 발생, `FakeConnection` 롤백 호출 여부
  확인) 세 갈래.
- `run_current_score_job` 통합 지점 테스트: 격리 로직이 배치 루프에 올바르게
  연결됐는지, `CurrentScoreJobSummary`에 격리 카운트가 반영되는지.
- `identity_diff`(방향 가중합 항등식) 파생 컬럼 계산과 `low_visibility`
  조건부 스킵 로직 별도 유닛 테스트.
- 완료조건에 명시된 "로컬 Airflow + 실제 DB로 확인"은 구현 후 로컬 Airflow
  웹 UI에서 직접 실행해 확인한다.

## 9. 컨텍스트 문서 갱신 대상

구현 완료 후 `context/data/quality-rules.md`(새 규칙 반영),
`context/data/schema-catalog.md`(격리 테이블 추가),
`context/open-questions.md`(`standard_score_as_of` 신선도 임계치 미해결 추가)를
갱신한다.
