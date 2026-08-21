---
status: accepted
date: 2026-08-22
supersedes:
superseded_by:
---

# 0009. Bronze 계층 소파일 정리를 위한 독립 S3 compaction DAG

## 배경

`de4-data-lake` 버킷의 Bronze 계층 두 대상(`sensor-events`, `zone_weather_snapshot`)에
소파일이 쌓인다.

- `sensor-events`는 `services/stream-processor`가 Kafka에서 받은 레코드를
  Structured Streaming으로 그대로 flush하며, `STREAM_TRIGGER_INTERVAL_SECONDS`
  기본값이 5초라 시간당 최대 약 720개 파일이 쌓일 수 있다. `origin/develop`
  (이 브랜치엔 아직 미반영)의 write-side fix가 `minOffsetsPerTrigger`/
  `maxTriggerDelay`(128MB 또는 5분 중 먼저 도달)로 배치를 지연시키고
  `coalesce(bronze_output_partitions)`로 배치당 파일 수를 강제해, develop과 합쳐지면
  신규 소파일 생성 자체가 막힌다. 그러나 그 이전에 이미 쌓인 과거 백로그는 이 fix의
  영향을 받지 않는다.
- `zone_weather_snapshot`은 `services/orchestration/jobs/weather.py`의
  `write_zone_weather_snapshot`이 15분마다 파일을 1개씩 쓴다. Structured Streaming이
  아니라 평범한 Python job이라 위 write-side fix의 영향을 전혀 받지 않고 계속 누적된다.

`services/batch-jobs/src/batch_jobs/cleansing/reader.py`의
`read_bronze_sensor_events`는 `spark.read.parquet(path)`로 Bronze 루트 전체를 매시간
통째로 읽고 이후 DataFrame 필터로 시간 범위를 좁힌다(파일 레벨 파티션 프루닝 없음).
압축하지 않으면 파일-오픈 오버헤드가 시간이 지날수록 선형으로 늘어난다.

Silver/Gold 계층의 소파일은 원인이 다르다(Spark `shuffle.partitions` 미조정,
`docs/pipeline-design-priorities.md` 3순위) — 이 ADR의 범위 밖이다.

## 결정

독립 저빈도(일 1회) Airflow DAG `bronze_compaction`을 신설한다. `data_quality_audit`
(#253, ADR-0004 롤아웃)과 같은 성격의 완전히 독립된 유지보수 DAG로 둔다 — outlet이
없어 다른 DAG를 깨우거나 막지 않고, task가 실패해도 파이프라인을 막지 않는다.
`sensor-events`/`zone_weather_snapshot`을 각각 독립 task로 처리한다(서로 의존관계
없음, 병렬 실행).

실행 엔진은 pyarrow + boto3다. `zone_weather_pipeline`의 PythonOperator와 같은 방식으로
Airflow 스케줄러 컨테이너 안에서 직접 돈다 — docker-outside-of-docker나 별도 Spark
세션이 필요 없다(Bronze 물리 스키마가 `value` JSON 문자열 컬럼뿐이라 Spark의 복잡한
변환 능력이 필요 없음).

각 대상은 다음 순서로 압축한다.

1. `libs/de4-core`의 `ObjectStore.list_objects()`로 대상 경로의 오브젝트를 나열한다
   (최종 수정 시각 포함).
2. 오브젝트를 상위 "디렉터리"로 그룹핑한다. `sensor-events`(파티션 없는 flat 출력)는
   전체가 한 그룹, `zone_weather_snapshot`(`weather_date=D/weather_time=T.parquet`)은
   날짜 파티션별로 한 그룹이 된다 — 소스별 특수 처리 없이 이 규칙 하나로 통일한다.
3. 그룹 안 오브젝트의 최종 수정 시각이 모두 안전 경계
   (`data_interval_end - SAFETY_MARGIN`, 기본 1시간) 이전인 그룹만 압축 대상으로
   삼는다 — 아직 쓰기가 진행 중일 수 있는 그룹은 건너뛴다.
4. 이미 목표 오브젝트 수(기본 1개) 이하인 그룹은 스킵한다(멱등성).
5. 그룹의 모든 Parquet 파일을 pyarrow로 읽어 병합하고, 임시 키에 쓴다.
6. 임시 키를 다시 읽어 병합 결과의 row 수가 원본 row 수 합과 일치하는지 검증한다.
   불일치하면 원본을 그대로 두고 예외를 던져 task를 hard-fail시킨다(Airflow task
   재시도/알림 대상, 다른 DAG는 안 막힘).
7. 검증을 통과하면 원본 오브젝트를 삭제하고, 병합 결과를 최종 키로 쓴 뒤 임시 키를
   지운다.

## 대안

| 대안 | 장점 | 단점 | 기각 이유 |
| --- | --- | --- | --- |
| `standard_score_pipeline`의 `sensor_processing` TaskGroup 안 선행 단계로 압축 | 같은 Spark 세션 재사용, 추가 세션 기동 비용 0 | `zone_weather_snapshot`엔 적용 불가(다른 DAG, 다른 실행 엔진). `sensor-events`도 develop 병합 후엔 압축 대상이 거의 안 남아 이 단계 자체가 대부분 no-op | 대상이 두 갈래인데 한쪽에만 적용 가능하고, 정상 경로에서 매시간 도는 태스크에 거의 항상 아무것도 안 하는 단계를 상시로 얹는 낭비 |
| `zone_weather_snapshot` write-side를 배치(예: 1시간에 1파일)로 변경 | 애초에 소파일이 안 생김, 별도 compaction 불필요 | `latest_zone_weather`가 요구하는 15분 신선도를 훼손 — 배치 주기 동안 수집된 관측을 한 번에 flush하면 그 사이 `latest_zone_weather` upsert가 지연되거나, snapshot 이력과 latest 갱신 타이밍이 어긋남 | 15분 신선도 요구사항과 직접 충돌 |
| 독립 DAG + Asset(ADR-0007 패턴)으로 producer/compaction 연결 | Airflow UI에 의존관계가 보임 | Asset은 "두 독립 producer 사이의 비동기 레이스"를 푸는 도구(ADR-0007이 고친 #228)다. compaction과 producer 사이엔 그런 레이스가 없다 — compaction은 안전 경계 필터로 이미 격리되어 있어 Asset이 주는 이점이 없다 | 문제 유형이 다름, 불필요한 복잡도 |
| 1회성 스크립트로 백로그만 정리 | 상시 DAG 불필요 | `zone_weather_snapshot`은 백로그가 아니라 지금도 계속 쌓임 — 1회성으로는 근본 해결이 안 됨 | 두 대상 중 하나(zone_weather_snapshot)의 근본 원인을 못 없앰 |
| Spark로 병합 | 이미 검증된 S3 쓰기 인프라 재사용 | 독립 DAG라 세션 공유 이점이 없고, Bronze 물리 스키마가 단순해(파싱 안 된 JSON 문자열) Spark의 복잡한 변환 능력이 불필요, JVM 기동 비용만 추가 | 세션 공유 없이는 pyarrow 대비 이점이 없음 |

## 결과

**긍정**: 두 Bronze 대상의 소파일이 매일 자동으로 정리된다. row count 검증으로 데이터
유실 위험을 hard-fail로 막는다. 안전 경계 필터와 그룹당 목표 오브젝트 수 체크로
멱등성을 가져 재실행이나 재시도가 안전하다. 소스별 특수 처리 없이 "상위 디렉터리로
그룹핑"이라는 규칙 하나로 두 대상을 모두 처리해 job 코드가 단순하다.

**부정**: 압축 직후 그 그룹을 읽는 다른 job이 있다면(현재는 없음) 원본 삭제~최종 키
쓰기 사이의 짧은 창에서 오브젝트가 일시적으로 안 보일 수 있다. 안전 경계
(`SAFETY_MARGIN`) 값은 로컬 PoC 데이터 규모 기준 임의로 1시간을 택했다 — 실제 트래픽
규모에서 재튜닝이 필요할 수 있다. `ObjectStore`에 `list_objects`/`delete_objects`가
추가되어 계약 표면이 넓어진다.

## 영향 범위

- `libs/de4-core/src/de4_core/storage.py`: `ObjectStore.list_objects()`,
  `ObjectStore.delete_objects()`, `ObjectMetadata` 추가
- `services/orchestration/jobs/bronze_compaction.py` 신규
- `services/orchestration/dags/bronze_compaction.py` 신규
- `services/orchestration/pyproject.toml`: `boto3` 의존성 추가
- `infra/compose/airflow.yaml`: `airflow-scheduler`에 압축 대상 경로 env var 추가
- `context/architecture.md`, `context/open-questions.md`(OQ-002)

## 참고

- 관련 이슈: #271
- `docs/adr/0004-data-quality-validation-with-great-expectations.md`(soft-fail 독립 DAG
  패턴의 최초 선례)
- `docs/adr/0007-split-comfort-score-pipeline-into-three-dags.md`(Asset이 풀도록
  설계된 문제 유형 참고)
- `services/stream-processor`의 write-side fix: `config.py`의
  `min_offsets_per_trigger`/`max_trigger_delay`, `bronze_sink.py`의
  `coalesce(bronze_output_partitions)`(`origin/develop`, 이 브랜치엔 미반영)
