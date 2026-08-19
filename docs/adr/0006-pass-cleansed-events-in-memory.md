---
status: accepted
date: 2026-08-19
supersedes:
superseded_by:
---

# 0006. 클렌징 결과를 T2에 인메모리로 전달한다

## 배경

기존 시간 단위 센서 처리 흐름은 T1이 Bronze `sensor_event`를 클렌징한 뒤
`processed_sensor_event`를 S3에 저장하고, T2가 그 경로를 다시 읽어 맵매칭과
피처 계산을 수행했다. 이 중간 저장은 동일 배치 흐름 안에서 즉시 소비되는
데이터를 다시 직렬화하고 읽는 I/O를 만들며, T1과 T2 사이에 실제로 필요하지
않은 영속 저장 경계를 두고 있었다.

T2는 대상 시간의 이벤트를 계산할 때 직전 이벤트와 다음 이벤트가 필요하므로
lookback/lookahead 구간을 읽는다. 중간 저장을 없애더라도 이 시간 경계 계산과
기존 시간별 중복 제거 의미는 유지해야 한다.

## 결정

T1과 T2를 하나의 Spark 애플리케이션과 세션에서 실행한다. T1이 T2의 정확한
lookback/lookahead 구간과 겹치는 Bronze 시간들을 읽어 시간별로 클렌징하고,
typed DataFrame을 T2 함수에 직접 전달한다.

`processed_sensor_event`는 S3에 저장하지 않으며 T2도 이를 다시 읽지 않는다.
영속 산출물은 대상 시간의 클렌징 quarantine과 최종
`hourly_segment_features`로 제한한다. T2는 전달받은 DataFrame에서 필요한
정확한 이벤트 시간 구간을 다시 필터링한다.

Airflow는 기존 T1/T2 개별 명령 대신 이 결합된 명령을 한 번 실행하도록 후속
이슈에서 변경한다. 이 ADR은 Airflow 코드 변경 자체를 포함하지 않는다.

## 대안

| 대안 | 장점 | 단점 | 기각 이유 |
| --- | --- | --- | --- |
| 기존 `processed_sensor_event` S3 저장 유지 | T1과 T2를 독립적으로 재실행할 수 있고 중간 결과를 직접 조사하기 쉽다 | 매시간 쓰기와 재읽기 I/O가 발생하고 임시 데이터의 저장 수명·파티션을 관리해야 한다 | T2가 같은 실행 흐름에서 즉시 소비하는 데이터라 영속 경계의 비용이 이점보다 크다 |
| T2가 Bronze를 직접 읽고 클렌징까지 수행 | T2 단독 실행 형태를 유지할 수 있다 | T1 책임이 T2에 중복되고 quarantine 처리 및 규칙 적용 지점이 불명확해진다 | 클렌징 책임을 T1에 유지해야 데이터 품질 경계가 명확하다 |
| 별도 상위 coordinator job을 추가 | T1과 T2 모듈을 감싸는 실행 흐름의 이름이 명확해진다 | 새 진입점과 배포·테스트 표면이 추가된다 | 기존 cleansing job이 동일 Spark 세션의 실행 진입점을 맡는 것으로 충분하다 |

## 결과

- 중간 Parquet 쓰기와 읽기, `event_hour` 파티션 관리가 제거된다.
- T1과 T2는 하나의 Spark 실행 단위가 되며 T2만 중간 결과에서 독립 재시작할
  수 없다. 실패 시 immutable Bronze부터 해당 시간을 다시 처리한다.
- `event_date`와 `event_hour`는 T1→T2 인메모리 계약에 포함하지 않는다. T2
  출력 경로의 날짜와 시간은 실행 인자인 `target_hour`에서 결정한다.
- T2의 lookback/lookahead를 충족하기 위해 T1은 대상 시간뿐 아니라 경계와
  겹치는 인접 Bronze 시간도 클렌징한다. 시간별 중복 제거 의미를 유지하고,
  quarantine 교체는 대상 시간에만 수행한다.
- 클렌징 성공 후 T2가 실패하면 quarantine은 이미 교체됐을 수 있다. 전체
  실행은 실패로 처리하고 같은 `run_id`/시간을 Bronze부터 재실행한다.
- Airflow DAG와 운영 문서는 결합된 CLI 계약에 맞추는 후속 변경이 필요하다.

## 영향 범위

- `services/batch-jobs` — T1이 Bronze 읽기와 클렌징 후 T2를 직접 호출하고,
  T2는 입력 DataFrame을 받는다. `processed_sensor_event` 저장 코드와 독립 T2
  CLI 명령은 제거된다.
- `services/orchestration` — 기존 cleanse/features 두 실행을 결합된 명령 한
  번으로 바꾸는 후속 작업이 필요하다.
- `context/architecture.md`, `context/data/lineage.md`,
  `context/data/schema-catalog.md`, `context/open-questions.md` — 영속 저장 경계
  제거와 인메모리 계약을 반영한다.

## 참고

- 관련 이슈: #201
- 실행 스키마:
  `services/batch-jobs/src/batch_jobs/schemas.py::PROCESSED_SENSOR_EVENT_SCHEMA`
