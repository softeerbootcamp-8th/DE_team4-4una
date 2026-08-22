---
status: accepted
date: 2026-08-19
supersedes:
superseded_by:
---

# 0005. 존 geometry의 canonical 참조는 zone_master로 확정한다

## 배경

TLC 존(zone) 식별자 `location_id`를 물리적으로 들고 있는 테이블이 두 개
존재한다.

- `taxi_zone_lookup`: TLC가 배포하는 zone lookup CSV 그대로. `location_id`,
  `borough`, `zone_name`, `service_zone`만 있고 geometry는 없다.
- `zone_master`: `sensor-producer`의 zone-profile 파이프라인
  (`build_tlc_zone_base.py`)이 만드는 로컬 Parquet. 같은 `location_id`
  범위(1-265)를 쓰지만 폴리곤 `geometry`를 갖고 있다.

`context/data/schema-catalog.md`는 이 중복을 이미 "whether to unify them is
open (OQ-029)"로 기록해 두고 있었다. 이슈 #193(날씨 반영 comfort score
계약)에서 15분마다 zone 단위로 Open-Meteo를 호출하려면 zone별 대표 좌표가
필요해졌고, 이 좌표를 어느 테이블 기준으로 관리할지 정해야 이 좌표를 어디에
저장/참조하는지도 정할 수 있는 상황이 됐다. 

## 결정

**`zone_master`를 geometry를 갖는 canonical 존 참조 테이블로 확정한다.**
zone 폴리곤이나 대표 좌표가 필요한 신규 계약은 `taxi_zone_lookup`이 아니라
`zone_master`를 참조한다. `taxi_zone_lookup`은 이름/자치구 조회용 raw
lookup으로 역할을 유지한다.

이번 결정의 일부로 `zone_master`에 `representative_latitude`/
`representative_longitude`(폴리곤의 `representative_point()`) 두 컬럼을
추가하고, `zone_weather_snapshot.location_id`가 이를 논리적으로 참조한다.

## 대안

| 대안 | 장점 | 단점 | 기각 이유 |
| --- | --- | --- | --- |
| `taxi_zone_lookup`에 geometry/대표좌표 추가 | 이미 사용 중인(추정) Postgres 참조 테이블 하나로 유지, 논리적 참조(non-FK) 문제가 생기지 않음 | `zone_master`가 이미 하는 폴리곤 빌드(`taxi_zones.shp` 파싱)를 그대로 다시 만들어야 함; 두 테이블 모두 geometry를 갖게 되면 canonical이 뭔지는 여전히 안 정해짐 | 중복 작업만 만들고 근본 질문(어느 쪽이 기준이냐)은 해결하지 못함 |
| `zone_master`와 `taxi_zone_lookup`을 지금 하나로 물리 통합 | 중복 자체가 사라짐 | `taxi_zone_lookup`은 소스 적재(Bronze) 경로, `zone_master`는 `sensor-producer`의 zone-profile 파이프라인 산출물 — 서로 다른 빌드 파이프라인/소유권을 이번 이슈 범위에서 조정해야 함 | 이슈 #193(날씨 계약 정의) 범위를 벗어나는 별도 마이그레이션 작업; 필요성은 인정하되 지금 할 일은 아님 |
| 결정 보류, 좌표를 `zone_weather_snapshot`에 직접·독립적으로 저장 | 당장 결정 비용 없음 | 존별 정적 데이터(좌표)가 15분마다 매 관측 row에 중복 저장됨; canonical 참조 모호함은 다음 계약이 zone geometry를 필요로 할 때 다시 불거짐 | 이미 이번 이슈에서 필요해진 질문을 또 미루는 것일 뿐 |

## 결과

- `zone_weather_snapshot.location_id` -> `zone_master.location_id`,
  `current_segment_comfort_score.location_id` -> `zone_master.location_id`는
  **논리적 참조만** 가능하다 (`zone_master`가 PostgreSQL이 아니라 로컬
  Parquet이기 때문). 이는 이번 결정이 감수하는 트레이드오프이지 결함이
  아니다.
- `zone_master` 빌드(`sensor-producer` 소유)와 날씨 수집 잡(`batch-jobs`
  소유 예정)이 같은 Parquet 파일을 사이에 두고 서비스 경계를 넘어 결합된다.
  `zone_master`의 산출 경로나 소유권이 바뀌면 이 결정과
  `context/data/schema-catalog.md`의 관련 서술을 다시 봐야 한다.
- `zone_master`/`taxi_zone_lookup` 물리 통합은 여전히 계획되지 않은 미해결
  후속 작업으로 남는다 — 이번 ADR은 "둘 다 유지하되 canonical은 어느 쪽인지"만
  정한다.

## 영향 범위

- `context/data/schema-catalog.md` — `zone_master`에
  `representative_latitude`/`representative_longitude` 추가,
  `zone_weather_snapshot`/`current_segment_comfort_score`의 `location_id`
  참조 방식(논리적 참조) 서술
- `context/open-questions.md` — OQ-029 상태를 Accepted로 변경
- `services/sensor-producer/src/zone_profile/build_tlc_zone_base.py` (구현,
  이슈 #193 범위 밖) — `representative_latitude`/`representative_longitude`
  컬럼 생성 로직 추가 필요
- `services/batch-jobs`의 향후 날씨 수집 잡 (구현, 이슈 #193 범위 밖) —
  `zone_master.parquet`을 직접 읽어 좌표를 가져오는 교차 서비스 의존성 발생

## 참고

- 관련 이슈: #193 / 관련 오픈퀘스천: OQ-029
