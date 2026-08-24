# 03. Bronze 읽기를 전체 스캔에서 파티션 프루닝으로 바꾼다

> 정확성을 위해 중복 방어를 의도적으로 남긴 사례입니다.

← [의사결정 목록](README.md)

## 트리거

`cleanse-sensor-events`가 매시간 실행마다 Bronze **전체**를 읽고 있었습니다.

## 관측된 사실

- 읽기 측이 `spark.read.parquet(root)`로 루트 전체를 스캔한 뒤 **메모리에서** target-hour를 필터링했다.
- 쓰기 측(`stream-processor`)은 이미 `event_date=` / `hour=`로 Hive 파티셔닝하고 있었다.
- 시간별 처리에서 **Bronze parquet scan이 3회** 발생했다.
- 저장소 안에 이미 올바른 패턴이 있었다 — `cleansing/hourly_storage.py`, `sensor_processing_validation.py`는 대상 파티션만 읽고 있었다. 읽기 경로만 뒤처져 있었다.

## 근본 원인

두 가지가 겹쳤습니다.

1. **쓰기 측 파티션 구조를 읽기 측이 활용하지 않는 비대칭.** 파티셔닝은 되어 있는데 pruning을 하지 않으니 효과가 없었다.
2. **lookahead 윈도우 설정** 때문에 필요 없는 인접 시간까지 반복 스캔했다.

## 결정

두 단계로 처리했습니다.

| 단계 | 내용 |
| --- | --- |
| #259 | 처리 윈도우를 `(target_hour, target_hour + 1h)`로 좁히고, 사용되지 않는 `lookahead_seconds`를 제거 |
| #345 | 읽기 함수가 `target_hour`를 받아 `event_date=YYYY-MM-DD/hour=HH` 파티션 경로만 globbing하도록 변경 |

파티션 컬럼·포맷은 `stream-processor/bronze_sink.py`의 상수와 동일하게 맞췄습니다.

## 최적화 대상과 포기한 것

스캔 범위를 줄이면서도 **파티션 프루닝 이후에도 in-memory 필터를 남겼습니다.**

늦게 도착한 이벤트가 다른 파티션에 적재될 수 있어 파티션 경계와 실제 이벤트 시간이 어긋날 수 있기 때문입니다. 중복 방어의 비용을 감수하고 정확성을 택했습니다.

## 검증 방법

Spark event log에서 scan time이 붙은 스테이지 수로 **3회 → 1회**를 확인.

## 결과

두 이슈 모두 완료. 읽기 경로에 `bronze_hour_partition_path()`가 추가되어 파티션 경로 규칙이 한 곳에서 관리됩니다. 대상 파티션이 아직 없을 때를 위한 빈 DataFrame 스키마도 함께 정의했습니다.

## 재검토 조건

늦게 도착한 이벤트 비율이 무의미할 만큼 낮다고 **측정되면**, 남겨둔 in-memory 필터를 제거할 수 있습니다. 측정 없이 지우면 조용한 데이터 유실이 됩니다.

## 근거

- #259 (윈도우 축소), #345 (파티션 프루닝)
- `services/batch-jobs/src/batch_jobs/cleansing/reader.py`
