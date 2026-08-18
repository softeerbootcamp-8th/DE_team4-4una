---
status: accepted
date: 2026-08-18
supersedes:
superseded_by:
---

# 0002. vehicle_profile을 차체 유형 x 크기 등급으로 분류

## 배경

`vehicle_profile`은 원래 Genesis, Hyundai Grandeur, Hyundai Avante, EV5라는
실제 제조사/모델 4종으로 정의돼 있었다(`context/project.md` "Vehicle types",
초기 확정 요구사항). 그러나 어떤 Genesis 모델인지(`OQ-014`), EV5가 어느
제조사 모델인지(`OQ-015`), 정확한 트림·물성치 출처가 무엇인지(`OQ-013`)가
끝까지 확정되지 않아 구현이 막혀 있었다. `sensor_producer.domain.py`는
이 4종을 반응계수(`vertical_response`, `damping`,
`longitudinal_response`, `lateral_response`,
`steering_vibration_response`)로 근사해 시뮬레이션에 쓰고 있었지만,
`vehicle_profile` Postgres 테이블에는 이 4종이 실제로 seed된 적이 없었고
스키마도 `manufacturer`/`model_name`/`mass_kg`/`wheelbase_mm`/
`suspension_type` 같은 물성치 컬럼 위주였다.

이슈 #170에서 이 프로필 체계를 다시 정의하면서, 실제 모델명 대신 무엇으로
차량을 구분할지, DB 스키마를 물성치 기반으로 유지할지 반응계수 기반으로
바꿀지를 결정해야 했다.

## 결정

`vehicle_profile`을 **차체 유형(`body_type`) x 크기 등급(`size_class`)**
분류로 재정의한다. 실제 제조사/모델 식별자는 완전히 제거한다.

| `vehicle_profile_id` | `profile_name` | `body_type` | `size_class` |
| --- | --- | --- | --- |
| 0 | `ALL_VEHICLES` | `ALL` | `ALL` |
| 1 | `VP_SEDAN_COMPACT` | SEDAN | COMPACT |
| 2 | `VP_SEDAN_LARGE` | SEDAN | LARGE |
| 3 | `VP_SUV_COMPACT` | SUV | COMPACT |
| 4 | `VP_SUV_LARGE` | SUV | LARGE |
| 5 | `VP_MPV_LARGE` | MPV | LARGE |

`vehicle_profile` 테이블은 물성치 컬럼(`manufacturer`, `model_name`,
`mass_kg`, `wheelbase_mm`, `suspension_type`) 대신
`sensor_producer.domain.VehicleProfile`과 동일한 반응계수 5종
(`vertical_response_factor`, `longitudinal_response_factor`,
`lateral_response_factor`, `damping_factor`,
`steering_vibration_factor`)을 저장한다. `vehicle_profile_id=0`은 차량
구분 없는 집계용 sentinel로 그대로 유지한다(`OQ-038`).

## 대안

| 대안 | 장점 | 단점 | 기각 이유 |
| --- | --- | --- | --- |
| 실제 모델 유지, 트림만 확정 | 초기 요구사항 범위를 그대로 지킴 | `OQ-013`/`014`/`015`의 모호함이 근본적으로 해소되지 않고, 물성치 출처 확보가 계속 막힘 | 확정에 필요한 정보가 끝내 제공되지 않아 구현이 무기한 지연됨 |
| `mass_kg`/`wheelbase_mm`/`suspension_type` 등 물성치 기반 스키마 유지 | 더 "물리적으로" 근거 있어 보임 | 시뮬레이터(`sensor_producer.domain.VehicleProfile`)는 이미 반응계수 기반으로 신호를 생성하며, 물성치에서 반응계수를 도출하는 로직이 없음 | 아무 데서도 쓰이지 않는 별개의 물리 모델을 스키마에 유지하게 됨 |
| 시뮬레이터가 Postgres에서 프로필을 직접 조회 | 값이 한 곳(DB)에만 존재해 drift 위험이 없음 | sensor-producer에 DB 연결 의존성을 새로 추가해야 하는 더 큰 아키텍처 변경 | 이번 이슈 범위를 벗어남 — `OQ-028`로 남겨 둠 |

## 결과

- `vehicle_profile_id` 1~4의 의미가 완전히 바뀐다(과거: genesis/grandeur/
  avante/ev5, 이후: 위 5개 분류). 과거 의미를 가정한 더미 센서 데이터나
  테스트 산출물은 재생성해야 한다.
- `sensor_producer.domain.VEHICLE_PROFILES`와 `vehicle_profile` 테이블은
  같은 5개 프로필의 반응계수 값을 각자 들고 있다. 런타임 연동이 없어
  둘 중 하나만 바뀌면 값이 어긋날 수 있다(`OQ-028`, 여전히 미해결).
- `OQ-013`/`014`/`015`는 이 결정으로 해소된다(실제 모델을 아예 안 쓰므로
  질문 자체가 무의미해짐).
- 반응계수 값은 실측 보정값이 아니라 차체 특성에 대한 공학적 추정이다.
  정확도 개선이 필요해지면 이 ADR을 재검토한다.

## 영향 범위

- `services/batch-jobs` — `resources/migrations/0005_define_vehicle_profiles.sql`이
  `vehicle_profile` 스키마와 시드 데이터를 이 결정에 맞게 정의한다.
- `services/sensor-producer` — `domain.py`의 `VEHICLE_PROFILES`가 이
  분류를 반영한다.
- `context/project.md`, `context/data/schema-catalog.md`,
  `context/open-questions.md` — Vehicle types 요구사항과 스키마 문서,
  관련 미해결 질문(`OQ-013`/`014`/`015`/`028`)을 이 결정에 맞게 갱신했다.

## 참고

- 관련 이슈: #170
- 관련 미해결 질문: `OQ-013`, `OQ-014`, `OQ-015`, `OQ-028`, `OQ-038`
  (`context/open-questions.md`)
