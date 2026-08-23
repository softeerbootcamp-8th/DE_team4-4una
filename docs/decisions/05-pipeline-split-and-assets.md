# 05. 파이프라인을 3개 DAG로 분리하고 Asset으로 트리거한다

> 프레임워크 기본 동작을 정확히 파악해 우회한 사례를 포함합니다.

← [의사결정 목록](README.md)

## 트리거

날씨는 15분마다 바뀌지만 주행 이력은 시간 단위로만 늘어납니다. 두 주기를 한 DAG에 담아야 하는지 결정이 필요했습니다.

## 근본 원인

갱신 주기가 다른 두 데이터를 한 테이블·한 DAG에 묶으면, **빠른 쪽이 바뀔 때마다 느린 쪽의 무거운 재집계(168시간)가 함께 실행됩니다.** 날씨가 바뀌었을 뿐인데 일주일 분량을 다시 집계하는 낭비가 생깁니다.

## 결정

세 개로 분리하고, 시간 기반 스케줄 대신 **데이터 기반(Asset) 트리거**로 연결합니다.

| DAG | 스케줄 | 성격 | 산출 Asset |
| --- | --- | --- | --- |
| `standard_score_pipeline` | `0 * * * *` | 무거운 집계 (168시간) | `standard_segment_comfort_score` |
| `zone_weather_pipeline` | `*/15 * * * *` | 가벼운 수집 + 변경 감지 | `zone_weather_changed` |
| `current_score_pipeline` | `AssetAny(위 둘)` | 가벼운 보정 | — |

`zone_weather_pipeline`은 날씨 **영향 등급이 실제로 바뀐 존이 있을 때만** Asset 이벤트를 발행합니다. 비가 오지 않는 15분 tick에서는 Current Score를 재계산하지 않습니다.

## 최적화 대상과 포기한 것

불필요한 재집계를 없애는 대신 **DAG 수와 의존 관계의 복잡도**를 받아들였습니다. 대신 `current_segment_comfort_score`의 **writer를 한 DAG로 제한**해 경합을 차단했습니다.

## 구현 중 발견한 프레임워크 동작

변경된 존이 없는 tick에서 하위 실행을 막기 위해 `ShortCircuitOperator`를 썼는데, 그대로는 동작하지 않았습니다.

- Airflow는 TaskInstance가 SUCCESS로 끝나는 순간 그 task의 `outlets`를 **무조건** 이벤트로 등록한다 (`models/taskinstance.py`의 `register_asset_changes_in_db`).
- `ShortCircuitOperator`는 조건이 False여도 **자기 자신은 SUCCESS로 끝난다.** 하위 task만 SKIPPED가 된다.
- 따라서 `outlets`를 `ShortCircuitOperator`에 직접 붙이면 **변경 존이 없는 tick에도 매번 이벤트가 발행된다.**

그래서 **발행 전용 하위 task**(`publish_zone_weather_asset`, `EmptyOperator`)를 두고 거기에만 `outlets`를 붙였습니다. 이 task는 변경 존이 없으면 SKIPPED로 끝나 이벤트가 등록되지 않고, 있을 때만 SUCCESS로 끝나 이벤트를 등록합니다.

## 검증 방법

날씨 영향 등급이 바뀌지 않은 tick에서 `current_score_pipeline`이 트리거되지 않는지 확인.

## 재검토 조건

날씨 외에 Current Score를 움직이는 입력이 추가되면 트리거 조건을 다시 설계해야 합니다.

## 근거

- [ADR-0007](../adr/0007-split-comfort-score-pipeline-into-three-dags.md)
- #230 (`weather_pipeline` → `zone_weather_pipeline` 개명 및 변경 존 게이팅)
- `services/orchestration/dags/zone_weather_pipeline.py`, `current_score_pipeline.py`, `assets.py`
