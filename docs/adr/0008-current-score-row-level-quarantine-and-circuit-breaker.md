---
status: accepted
date: 2026-08-21
supersedes:
superseded_by:
---

# 0008. current_score_pipeline 행 단위 격리와 GX 서킷브레이커

## 배경

> `current_segment_comfort_score`는 두 개의 독립 스케줄 프로듀서(hourly
> `standard_score_pipeline`, 15분 `zone_weather_pipeline`)가 비동기로 갱신하는
> 유일한 서빙 진실원이고, `services/serving-api`가 이 값을 그대로 경로 추천
> 알고리즘에 노출한다. `standard_score_pipeline`은 `sensor_processing`/
> `hourly_scoring`/`standard_score` 세 TaskGroup 각각에 GX in-flight 검증을
> hard-fail로 연결해 뒀고(ADR-0004), `zone_weather_pipeline`도 인라인 검증을
> hard-fail로 붙였다(#250). 그러나 이 두 스코어를 최종 결합해 서빙 API가
> 직접 읽는 `current_score_pipeline`에는 in-flight 검증이 전혀 없다. 유일한
> 방어는 `data_quality_audit` DAG의 하루 1회 soft-fail 감사뿐이라, 오염된 값이
> 최대 24시간 동안 API 응답에 그대로 노출될 수 있다(#248, #251).
>
> 다른 두 서브이슈(#249, #250)와 달리 "검증 task 하나 추가"로는 부족하다 —
> `current_segment_comfort_score`는 `(segment_id, vehicle_profile_id)`당
> 단일 최신 행만 갖는 UPSERT 테이블이라, 정상 행까지 함께 막으면 서빙 API가
> 통째로 멈춘다. 정상 행은 계속 서빙되게 하면서 이상 행만 걸러내려면 UPSERT
> 쓰기 경로 자체에 행 단위 분기가 필요하다.

## 결정

> `jobs/current_score.py::run_current_score_job`의 UPSERT 직전, 배치로 계산된
> 행들을 GX `PandasExecutionEngine`으로 in-memory 검증한다. 정상 행은 기존
> `_UPSERT_SQL`로 UPSERT하고, 이상 행은 새 Postgres 테이블
> `current_segment_comfort_score_quarantine`에 같은 커넥션·같은 트랜잭션으로
> INSERT한다. 이번 실행에서 정상 행이 0건이거나(처리 대상이 있었는데도) 격리율이
> 25%를 넘으면 `CurrentScoreCircuitBreakerTripped`를 던져 트랜잭션 전체(정상
> UPSERT + 격리 INSERT 전부)를 롤백하고 Airflow task를 hard fail시킨다.
>
> GX Expectation Suite는 `comfort_score`/`vertical_score`/`longitudinal_score`/
> `lateral_score` 범위(0~100), `confidence_score` 범위(0~1), `sample_count`
> 음수 금지, 방향별 가중합 항등식(`low_visibility` 비활성 시)을 검증한다.
> `weather_time`/`weather_rule_version`/`weather_impact_signature` NULL 짝
> 제약은 GX로 옮기지 않는다 — 이미 코드 구조(`_build_row`)와 DB `CHECK`
> 제약(0006/0009)이 이중으로 강제하는 하드 인바리언트이기 때문이다
> (ADR-0004의 "하드 인바리언트는 GX로 옮기지 않는다" 원칙 유지).
> `standard_score_as_of`는 이번 서브이슈 범위에서 NOT NULL만 검증하고,
> 구체적 신선도 임계치는 `context/open-questions.md`에 미해결로 남긴다.

## 대안

| 대안 | 장점 | 단점 | 기각 이유 |
| --- | --- | --- | --- |
| `zone_weather_pipeline`처럼 GX 없이 인라인 Python 검증(ADR-0004 예외 패턴 유지) | 새 의존성 불필요, 기존 예외 패턴과 일관 | 이슈 본문이 명시한 GX API(`unexpected_index_list`)와 불일치, quality-rules.md → Suite 매핑 방식이 파이프라인마다 갈림 | 이슈에서 이미 GX 사용을 요청했고, 선언적 규칙 정의를 다른 파이프라인과 통일하는 이득이 큼 |
| UPSERT 후 `SqlAlchemyExecutionEngine`으로 라이브 테이블 검증 | 별도 in-memory 변환 불필요 | 이미 나쁜 값으로 기존 정상 값을 덮어쓴 뒤에야 검증 — 되돌릴 이전 값이 없어 오히려 더 위험 | 이 테이블은 키당 단일 최신 행만 가져 히스토리가 없음 |
| 격리를 Parquet/S3 로그로 저장(`sensor_event_quarantine` 방식 그대로) | 스키마 변경·`batch-jobs` 침범 불필요 | UPSERT와 별도 트랜잭션이라 원자적 롤백 불가, 서킷브레이커 트립 시 격리 로그가 고아로 남을 수 있음 | 정합성(consistency)이 이 파이프라인의 최우선 설계 축 — 원자적 롤백을 포기할 수 없음 |

## 결과

> `services/orchestration`에 `great-expectations`, `pandas` 신규 의존성이
> 추가된다. `services/batch-jobs`에 마이그레이션 파일이 1개 추가된다 —
> 마이그레이션은 이미 이 서비스에만 존재하는 유일한 체계이기 때문에, 쓰기
> 로직이 `services/orchestration`에 있음에도 서비스 경계를 넘는 변경이
> 불가피하다. 격리된 행의 재처리/복구 워크플로는 이번 결정 범위 밖으로 남겨
> 후속 이슈로 넘긴다. 격리율 임계치(25%)와 `standard_score_as_of` 신선도
> 미확정은 향후 운영 데이터를 보고 재조정할 수 있는 초기값이다.

## 영향 범위

- `services/orchestration/jobs/current_score.py`
- `services/orchestration/jobs/current_score_quarantine.py` (신규)
- `services/orchestration/dags/current_score_pipeline.py`
- `services/orchestration/pyproject.toml` (신규 의존성)
- `services/batch-jobs/src/batch_jobs/resources/migrations/0011_*.sql` (신규)
- `context/data/quality-rules.md`, `context/data/schema-catalog.md`, `context/open-questions.md`

## 참고

- Refs #248, #251
- ADR-0004 (Great Expectations 도입, 하드 인바리언트/통계 규칙 구분 원칙)
- ADR-0007 (`current_score_pipeline` 단일 writer 직렬화)
