# 08. 날씨를 히스토리 누적이 아니라 존별 최신 1건으로 저장한다

> 그레인이 용도와 맞지 않았음을 뒤늦게 발견하고 되돌린 사례입니다.

← [의사결정 목록](README.md)

## 트리거

15분 단위 날씨를 `zone_weather_snapshot`에 계속 누적하는 구조로 시작했습니다. PK는 `(location_id, weather_time)`이었습니다.

## 근본 원인

**그레인이 용도와 맞지 않았습니다.**

Current Score 계산에 필요한 것은 **현재 날씨 한 건**입니다. 그런데 이력은 누구도 조회하지 않는데도 15분마다 존 수(최대 263개)만큼 행이 무한히 늘어납니다. 하루 약 25,000행, 한 달 약 76만 행이 사용되지 않은 채 쌓입니다.

## 결정

| 항목 | 변경 전 | 변경 후 |
| --- | --- | --- |
| 테이블명 | `zone_weather_snapshot` | `latest_zone_weather` |
| 그레인 | 존 × 15분 시점 | **존당 1행** |
| PK | `(location_id, weather_time)` | `location_id` |
| 쓰기 방식 | INSERT (누적) | UPDATE (갱신) |

`weather_time`은 컬럼으로 유지합니다 — 현재 저장된 날씨의 기준 시각을 나타냅니다.

## 최적화 대상과 포기한 것

저장 증가와 조회 복잡도를 없애는 대신 **날씨 이력을 포기**했습니다.

**추적성은 다른 방식으로 지켰습니다.** `current_segment_comfort_score`가 계산에 **적용한 `weather_time`을 자기 행에 고정 저장**하므로, "이 점수가 어떤 시점 날씨로 계산됐는지"는 여전히 알 수 있습니다. 잃은 것은 "그 시점 날씨의 상세 측정값"입니다.

## 파급 — FK를 제거해야 했다

`current_segment_comfort_score`에 걸려 있던 `(location_id, weather_time)` 복합 FK를 제거해야 했습니다.

`latest_zone_weather`는 계속 UPDATE되는 테이블입니다. 점수 행이 FK로 그것을 따라가면, 날씨가 갱신될 때 **과거에 계산된 점수의 참조가 함께 움직이거나 FK 위반이 발생**합니다. 점수는 "계산 당시의 날씨"에 고정되어야 하므로, **논리적 참조로만 남기고 DB FK를 걸지 않는** 결정이 함께 필요했습니다.

## 검증 방법

- 날씨 갱신 시 행이 추가되지 않고 갱신되는지 (존당 행 수가 1을 유지)
- 과거에 적재된 점수 행의 `weather_time`이 날씨 갱신에 영향받지 않는지

## 재검토 조건

날씨와 점수의 상관을 **사후 분석**하려면 이력이 필요해집니다. 그때는 서빙 경로와 분리된 **분석용 이력 테이블**로 두는 것이 맞습니다 — 서빙 테이블에 이력을 다시 섞으면 같은 문제가 반복됩니다.

## 근거

- #209 (`refactor: change weather storage to latest zone state`)
- #230 (변경 존 게이팅 — [05번 결정](05-pipeline-split-and-assets.md)과 연결)
- `context/data/schema-catalog.md`의 `latest_zone_weather`, `current_segment_comfort_score`
