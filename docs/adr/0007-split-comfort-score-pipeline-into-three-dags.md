---
status: accepted
date: 2026-08-20
supersedes:
superseded_by:
---

# 0007. Comfort score 파이프라인을 3개 DAG로 분리하고 Asset으로 트리거한다

## 배경

`hourly_pipeline`(시간별)과 `weather_pipeline`(15분)은 각자
`current_segment_comfort_score`에 쓰는 태스크를 갖고 있다 — `hourly_pipeline`은
매시간 전량 갱신(`current_score` 태스크), `weather_pipeline`은 15분마다
변경된 zone만 갱신(`run_changed_zone_recompute` 태스크). 두 경로 모두
`jobs/current_score.py`의 `run_current_score_job`을 호출하고, 이 함수는
`pg_advisory_lock`으로 쓰기 트랜잭션을 직렬화한다. 그러나
`load_latest_zone_weather()` 호출이 이 lock을 잡기 **이전**에 일어나므로,
두 DAG가 겹쳐 실행되면 advisory lock이 있어도 먼저 읽은 stale weather
스냅샷으로 나중에 쓰기가 이뤄지는 stale-overwrite가 구조적으로 가능하다.
`current`는 서빙 API가 직접 읽는 테이블이라 이 레이스는 실사용자에게
노출된다.

## 결정

`current_segment_comfort_score`에 쓰는 책임을 하나의 DAG로 모으고, Airflow
Asset으로 두 producer가 그 DAG를 트리거하게 한다. 3개 DAG로 재구성한다
(Airflow 3.3.1 고정, `Asset`/`AssetAny`/`outlets`는 모두 `airflow.sdk`에서
임포트).

| DAG | 소유 테이블 | 스케줄 | 책임 |
| --- | --- | --- | --- |
| `standard_score_pipeline` (구 `hourly_pipeline`) | `standard_segment_comfort_score` | `0 * * * *` | sensor_processing → scoring → publish → standard_score. `standard_score` 태스크에 `outlets=[STANDARD_SCORE_ASSET]`(**#249에서 `validate_standard_score`로 이동 — 아래 결과 섹션 참고**). **current 쓰기 태스크 제거.** |
| `zone_weather_pipeline` (구 `weather_pipeline`) | `latest_zone_weather` | `*/15 * * * *` | 날씨 수집 + 변경 zone 감지. `jobs/current_score.py`의 기존 `find_changed_zones()`를 재사용하는 ShortCircuitOperator로 게이팅하고, 변경 zone이 있을 때만 `outlets=[ZONE_WEATHER_ASSET]` 발행. **current 재계산 태스크 제거.** |
| `current_score_pipeline` (신규) | `current_segment_comfort_score` | `schedule=AssetAny(STANDARD_SCORE_ASSET, ZONE_WEATHER_ASSET)`, `max_active_runs=1` | `current`의 유일한 writer. `context["triggering_asset_events"]`로 트리거한 Asset을 보고 `changed_zones_only`를 결정(STANDARD_SCORE → 전량, ZONE_WEATHER만 → 변경 zone만). `jobs/current_score.run_current_score_job`을 그대로 재사용 — job 코드 변경 없음. |

## 대안

| 대안 | 장점 | 단점 | 기각 이유 |
| --- | --- | --- | --- |
| 구조 유지 + advisory lock 앞에서 weather 읽기 순서만 수정 | 최소 침습, DAG 구조 변경 없음 | 다중 writer라는 근본 원인이 남아 향후 새 트리거(참조 데이터 갱신 등)가 추가될 때마다 재발 가능. Airflow UI에 의존관계가 안 보임 | 근본 원인이 아니라 증상만 고침 |
| `TriggerDagRunOperator`로 두 DAG가 `current_score_pipeline`을 직접 트리거 | Asset 개념 학습 없이 구현 가능 | producer가 consumer 이름을 하드코딩, UI에 의존관계 그래프 없음, 두 producer가 동시에 트리거하면 여전히 동시 실행 가능(`wait_for_completion` 없이는 직렬화 보장 안 됨) | 원인은 그대로, Asset 대비 구조적 이점이 없음 |
| `zone_weather_pipeline`이 게이팅 없이 항상 Asset 발행, `current_score_pipeline`이 매번 스스로 no-op 판단 | 변경 감지 로직이 한 곳(`current_score_pipeline`)에만 존재 | 변경 없는 15분 tick도 매번 `current_score_pipeline`을 깨워 Airflow UI에 하루 최대 96회의 사실상 빈 실행이 쌓임. "재연산 필요한 zone 감시"라는 책임이 `zone_weather_pipeline` 쪽에 있다는 요구와도 어긋남 | 채택 안 함 — 대신 `find_changed_zones()`를 두 DAG가 함수 단위로 재사용해 로직 중복 없이 게이팅 |

## 결과

**긍정**: stale-overwrite 레이스가 구조적으로 제거된다(단일 writer +
`max_active_runs=1`). Airflow UI Asset 그래프에 producer→consumer
의존관계가 명시적으로 보인다. `standard_score_pipeline`/`zone_weather_pipeline`은
각자 생산에만 집중하는 단일 책임을 갖는다.

**부정**: 이 리포에서 Asset API를 처음 쓰는 것이라 학습 비용이 있다.
`current_score_pipeline`은 최대 하루 약 120회(24 + 최대 96) 트리거될 수
있어 스케줄러/UI에 잦은 실행 기록이 남는다. `max_active_runs=1`은 실행
중인 DagRun을 막지 않고 이후 트리거를 큐잉만 하므로, standard 트리거로
전량 재계산이 도는 동안 weather 트리거가 들어오면 그 반영은 지금 도는
실행이 끝날 때까지 지연된다 — 다만 Airflow의 asset 스케줄링은 그 사이
쌓인 이벤트를 다음 실행 한 번에 모아 소비하므로 밀린 실행이 누적되지는
않고, 지연 폭은 `current_score_pipeline` 자체 실행 시간(가벼운 단일
태스크)으로 한정된다. 이 실행 시간의 실측값은 아직 없어 구현 이슈에서
확인해야 한다. 기존 `pg_advisory_lock`(`LOCK_KEY=1004`)은 정상 경로에서는
불필요해지지만, 수동 트리거·백필 등 `max_active_runs=1`이 보장 못하는
경우를 대비해 제거하지 않고 유지한다.

## 수정 노트 (#249)

`standard_score` TaskGroup에 GX in-flight 검증(`validate_standard_score`)이
추가되면서(ADR-0004), `outlets=[STANDARD_SCORE_ASSET]`을 `run_standard_score`가
아니라 `validate_standard_score`로 옮겼다 — 검증을 통과한 데이터만
`current_score_pipeline`을 깨우게 해, 이 ADR이 없애려던 stale-overwrite류
문제를 검증 실패 데이터로부터도 동일하게 방지한다. 표의 원래 기술은
#249 이전 상태의 기록으로 남겨두고 여기 갱신한다.

## 번복 조건

zone별 재계산이 Spark 등 무거운 연산으로 바뀌어 트리거 빈도 자체가 비용
문제가 되거나, `current_score_pipeline` 실행 시간이 15분 주기 대비
유의미해지면, `zone_weather_pipeline`의 게이팅 기준을 다시 검토한다.

## 영향 범위

- `services/orchestration/dags/hourly_pipeline.py` → `standard_score_pipeline.py`로 이름 변경, `current_score` TaskGroup 제거, outlet 추가
- `services/orchestration/dags/weather_pipeline.py` → `zone_weather_pipeline.py`로 이름 변경, `run_changed_zone_recompute` 제거, ShortCircuit + outlet 추가
- `services/orchestration/dags/current_score_pipeline.py` 신규 생성
- `services/orchestration/tests/test_hourly_pipeline_dag.py`, `test_weather_pipeline_dag.py` → 이름/구조 대응 갱신, 신규 DAG 테스트 추가
- `context/architecture.md`의 DAG 다이어그램 갱신

## 참고

- 관련 이슈: #216, #217(기존 current 갱신 구현), #228(본 리팩터링 상위 이슈)
- Airflow 3.3.1 Asset 문서: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html , https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html
