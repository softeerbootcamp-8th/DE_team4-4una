---
owner: data-engineering
status: verified
executed_at: 2026-08-17
---

# 더미 데이터 기반 전체 파이프라인 로컬 실행 (#142)

시뮬레이터(sensor-producer) → Kafka → stream-processor(Bronze) → cleansing
(Silver1) → map matching + feature 집계(`build-hourly-segment-features`) →
`score-hourly-comfort` → Gold `segment_comfort_score`(PostgreSQL)까지, 손으로
만든 최소 더미 도로 환경/트립 데이터로 전 구간을 로컬에서 실제로 실행하고
각 단계 산출물을 캡처했다. Airflow는 검증 범위에서 제외했다.

이 실행은 `context/runs/2026-08-10-nyc-sensor-smoke.md`(실제 NYC 데이터
스모크 테스트)와 달리 **의도적으로 만든 더미 데이터**를 쓴다 — 실제 지리적
의미는 없고, 스키마·연결성만 유효하다.

## 사전 준비 — 더미 소스 데이터

`sensor-producer fetch-nyc-sample`과 `batch-jobs fetch-reference-data`가
정확히 같은 파일 포맷(`lion.geojson`/`pavement.geojson`/`speed_humps.geojson`/
`taxi_zones.zip`)을 쓴다는 걸 코드로 확인하고, 하나의 더미 소스 디렉터리에
아래를 1회성 스크립트로 만들었다 (스크립트 자체는 커밋하지 않음, `data/`는
`.gitignore` 대상):

- LION 세그먼트 2개(`DUMMY-SEG-1`: node 1001→1002, `DUMMY-SEG-2`: node
  1002→1003), 양방향(`TrafDir=T`), 총 길이 약 158m
- 빈 pavement/speed_humps GeoJSON
- taxi zone 1개(`location_id=1`, 세 노드를 모두 덮는 폴리곤), pyshp로 생성
- 트립 1건(`dummy-trip-0001`, zone 1→1, request 09:00 / pickup 09:02 /
  dropoff 09:03 America/New_York, `trip_miles=0.093`)

## 1. Extract/Generate — 시뮬레이터 → Kafka

```bash
uv run --package sensor-producer sensor-producer run \
  --input-dir data/dummy-source --publisher kafka \
  --topic sensor-events --run-id dummy-142-v1 \
  --sample-hz 10 --time-scale 0 --vehicle-profile-id 1
```

```json
{
  "events_published": 601,
  "trips_planned": 1,
  "unique_segments": 2,
  "vehicle_profile_id": 1
}
```

## 2. Load — Kafka → Bronze Parquet (stream-processor)

```
stream progress batchId=0 numInputRows=601 processedRowsPerSecond=244.8
```

```bash
$ python -c "pd.read_parquet('data/local-lake/bronze/sensor-events')"
rows: 601
```

Bronze `value` 컬럼(JSON 문자열) 샘플 1건:

```json
{"event_id":"7992a059-...","vehicle_id":"vehicle-1-dummy-tr","vehicle_profile_id":1,
 "trip_id":"dummy-trip-0001","trip_seq":0,"event_time":"2026-08-17T13:02:00+00:00",
 "latitude":40.7484,"longitude":-73.9857, "...":"...",
 "_run_id":"dummy-142-v1","_ingested_at":"2026-08-16T16:26:59.280Z"}
```

architecture.md의 설계대로 `segment_id`는 Bronze에 없다.

## 3. Transform (정제) — Bronze → Silver `processed_sensor_event`

```bash
uv run --package batch-jobs batch-jobs cleanse-sensor-events --run-id dummy-142-v1
```

```
cleansing.job input=601 passed=601 quarantined=0
```

## 4. Transform (맵매칭 + feature 집계) — `build-hourly-segment-features`

`road_segment`를 이 단계가 기대하는 스키마(`geometry_wkb` BinaryType,
`data/processed/road_segment/snapshot_date=.../data.parquet` 단일 파일)로
1회성 스크립트로 브리지했다 — [발견한 문제](#발견한-문제) 1번 참고.

```bash
TZ=UTC uv run --package batch-jobs batch-jobs build-hourly-segment-features \
  --target-hour 2026-08-17T13:00:00+00:00 --road-snapshot-date 2026-08-17 \
  --feature-version hourly-features-v1 --run-id dummy-142-v1
```

```
hourly segment feature job finished run_id=dummy-142-v1
  target_hour=2026-08-17T13:00:00+00:00 read=601 target=601 unmatched=0 result=2
```

```
  segment_id  vehicle_profile_id   data_period_start  trip_count  hard_brake_count  hard_accel_count  sharp_steer_count
0 DUMMY-SEG-2                   1 2026-08-17 13:00:00           1                 0                 0                  0
1 DUMMY-SEG-1                   1 2026-08-17 13:00:00           1                 0                 0                  0
```

`unmatched=0` — 두 세그먼트 모두 GPS 매칭 성공.

## 5. Transform (점수화) — `score-hourly-comfort`

```bash
TZ=UTC uv run --package batch-jobs batch-jobs score-hourly-comfort --run-id dummy-142-v1
```

```json
{"rejected_count": 0, "scored_count": 2}
```

```
  segment_id  vehicle_profile_id   data_period_start  vertical_score  longitudinal_score  lateral_score  trip_count
0 DUMMY-SEG-1                   1 2026-08-17 13:00:00           100.0           97.852728     100.000000           1
1 DUMMY-SEG-2                   1 2026-08-17 13:00:00           100.0           98.018290      88.022437           1
```

## 6. Load — Gold PostgreSQL 적재

```bash
TZ=UTC uv run --package batch-jobs batch-jobs load-segment-comfort-score \
  --as-of 2026-08-17T15:00:00+00:00
```

```sql
SELECT segment_id, vehicle_profile_id, comfort_score, confidence_score,
       sample_count, score_version, calculated_at
FROM segment_comfort_score ORDER BY segment_id, vehicle_profile_id;
```

```
 segment_id  | vehicle_profile_id | comfort_score | confidence_score | sample_count | score_version |     calculated_at
-------------+--------------------+---------------+------------------+--------------+---------------+------------------------
 DUMMY-SEG-1 |                  0 |     98.289526 |         0.090909 |          288 | 1.0.0         | 2026-08-17 15:00:00+00
 DUMMY-SEG-1 |                  1 |     98.289526 |         0.090909 |          288 | 1.0.0         | 2026-08-17 15:00:00+00
 DUMMY-SEG-2 |                  0 |     98.076267 |         0.090909 |          313 | 1.0.0         | 2026-08-17 15:00:00+00
 DUMMY-SEG-2 |                  1 |     98.076267 |         0.090909 |          313 | 1.0.0         | 2026-08-17 15:00:00+00
```

`vehicle_profile_id=0`(대표값) 행이 실제 차량(`1`)과 함께 적재됨 — #129
완료 조건과 일치.

**UPSERT 재확인**: `--as-of 2026-08-17T16:00:00+00:00`으로 재실행.

```json
{"staging_count": 4, "inserted_count": 0, "updated_count": 4}
```

```
 segment_id  | vehicle_profile_id | comfort_score |     calculated_at      | total_rows
-------------+--------------------+---------------+------------------------+------------
 DUMMY-SEG-1 |                  0 |     98.289526 | 2026-08-17 16:00:00+00 |          4
 DUMMY-SEG-1 |                  1 |     98.289526 | 2026-08-17 16:00:00+00 |          4
 DUMMY-SEG-2 |                  0 |     98.076267 | 2026-08-17 16:00:00+00 |          4
 DUMMY-SEG-2 |                  1 |     98.076267 | 2026-08-17 16:00:00+00 |          4
```

행 수 그대로(4행), `calculated_at`만 갱신 — 중복 삽입 없이 UPSERT됨.

## 7. 추적 확인

`DUMMY-SEG-1`/`DUMMY-SEG-2`가 1(Bronze, 601 rows)→3(Silver1, 601 rows)→
4(Silver2/3, 2 rows)→5(hourly_comfort_score, 2 rows)→6(Gold, 4 rows =
2 segment × {vehicle_profile_id 1, sentinel 0}) 전 구간에서 끊기지 않고
이어짐을 위 캡처들로 확인했다.

## 발견한 문제

검증 중 아래 5건의 실제 결함을 발견했다. 전부 더미 데이터와 무관하게
재현 가능하며, 프로덕션 코드는 수정하지 않고 1회성 스크립트/설정 사본으로
우회해 검증을 계속했다. **후속 이슈로 분리해 별도 수정이 필요하다.**

1. **`map_matching/candidates.py`의 좌표계 불일치.** 이벤트 GPS는
   `Transformer`로 EPSG:32118(NY State Plane, 미터)로 투영해 놓고,
   `road_segment_df.geometry_wkb`는 투영 없이 그대로 STRtree 생성과
   `shapely.distance` 계산에 쓴다. 실제 EPSG:4326(WGS84, 도) road_segment를
   넣으면 두 좌표계 스케일 차이 때문에 `dwithin`/거리 계산이 항상 실패해
   빈 매칭 결과가 나온다. `test_map_matching_candidates.py`의 fixture가
   테스트 지오메트리를 처음부터 EPSG:32118로 만들어서(`offset_line`) 이
   버그를 가려왔다. 검증 시 브리지 스크립트에서 `geometry_wkb`를 미리
   EPSG:32118로 투영해 우회했다.
2. **`hourly_segment_feature_storage.py::_require_single_target_hour`의
   호스트 타임존 의존.** naive `target_hour`를 `F.lit()`으로 Spark
   컬럼과 비교하는데, `spark.sql.session.timeZone=UTC` 설정과 무관하게
   호스트 OS 타임존이 개입해 UTC가 아닌 환경(예: KST, UTC+9)에서는 항상
   "result contains rows outside the requested target_hour"로 실패한다.
   `test_gold_job.py`가 이미 `os.environ["TZ"] = "UTC"`로 같은 종류의
   문제를 우회해 둔 전례가 있다 — 이 CLI 진입점에는 같은 보호가 없다.
   검증 시 `TZ=UTC` 환경변수로 우회했다.
3. **`batch_jobs/hourly_comfort.yaml`의 `scoring_version` 형식.** 커밋된
   기본값 `hourly-comfort-v1`이 세미버전이 아닌데
   `comfort_score/loader.py::_select_latest_scoring_version`은 무조건
   세미버전으로 가정하고 `.`로 나눠 `array<int>`로 캐스팅해서
   `CAST_INVALID_INPUT`으로 죽는다. 기본 설정 그대로는 Gold 적재가 항상
   실패한다. 검증 시 `scoring_version: "1.0.0"`으로 바꾼 설정 사본으로
   우회했다.
4. **`batch_jobs/cli.py::main()`의 `load-segment-comfort-score` 분기에
   `return` 누락.** 다른 모든 분기(`cleanse-sensor-events`,
   `score-hourly-comfort`, `migrate-database`, `build-hourly-segment-features`)는
   처리 후 `return`하는데 이 분기만 없어서, 적재 자체는 성공해도 이어서
   `build-road-environment`용 코드(`arguments.build_id`)로 흘러들어가
   `AttributeError`로 항상 비정상 종료한다(exit code 1). 실행 결과 자체는
   맞지만 CLI 종료 코드에 의존하는 자동화(예: `make`, CI)가 항상 실패로
   잡는다.
5. **`comfort_score/gold_job.py`가 `formula.py` 출력을 트리밍하지 않음.**
   `compute_segment_comfort_scores()`의 출력에는 staging 테이블에 없는
   중간 컬럼(`qualifying_hours`, `observed_score`, `population_mean`)이
   남아있는데, `gold_job.py`가 이를 select로 걸러내지 않고 그대로
   `gold_writer.write_segment_comfort_scores()`에 넘겨 JDBC write가
   `COLUMN_NOT_DEFINED_IN_TABLE`로 항상 실패한다. #127(formula.py)과
   #129(gold_writer)가 각자 유닛 테스트로는 통과했지만 실제로 이어붙여
   실행된 적이 없었다는 뜻 — 이 저장소에서 Gold 적재가 실제 formula.py
   출력으로 성공한 적은 이번이 처음이다. 검증 시 브리지 스크립트에서
   `EXPECTED_STAGING_COLUMNS`로 select해 우회했다.

버그는 아니지만 기록해 둘 점: `comfort_score.yaml`의
`min_traffic_threshold=5.0`(T_min, 시간당 최소 트립 수) 때문에 트립
1건짜리 더미 데이터로는 Gold 결과가 0건이었다(정상 동작 — "지나간 적
없는 조합은 만들지 않는다"는 설계 의도대로). 검증을 위해 이 값만
1.0으로 낮춘 설정 사본을 썼다.

## 미확인 상태로 남긴 것

- `TZ=UTC` 없이 실제 운영처럼(호스트 타임존 그대로) 돌렸을 때 2번 문제가
  실제 운영 환경(컨테이너 등 UTC 고정 환경)에서도 재현되는지는 미검증
  (이 로컬 macOS 환경은 Asia/Seoul).
- Airflow를 통한 스케줄링(제외 범위).
