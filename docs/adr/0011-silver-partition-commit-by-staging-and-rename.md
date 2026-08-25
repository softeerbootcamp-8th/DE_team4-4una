---
status: accepted
date: 2026-08-25
supersedes:
superseded_by:
---

# 0011. Silver 시간 파티션을 staging + rename으로 교체한다

## 배경

Silver 계층에는 시간 단위로 파티션을 교체하는 writer가 셋 있다.

| writer | 대상 |
| --- | --- |
| `cleansing/hourly_storage.py` | `sensor_event_quarantine` |
| `hourly_segment_feature_storage.py` | `hourly_segment_features` |
| `hourly_comfort_storage.py` (#469) | `hourly_comfort_score`, 그 quarantine |

셋 다 "재실행해도 행이 누적되지 않게 **해당 시간 파티션만** 교체한다"는 같은
요구를 갖는다.

### Spark의 기본 방식으로는 안전하게 못 한다

`df.write.mode("overwrite").parquet(path)`는 기존 데이터를 **먼저 지우고** 새로
쓴다. 쓰기 도중 실패하면 그 시간대가 비거나 반쪽만 남고, 되돌릴 원본은 이미
없다. 쓴 결과를 다시 읽어 행 수를 확인하고 **통과해야만 반영하는** 절차도
성립하지 않는다 — 확인 시점에는 이미 덮어쓴 뒤다.

`spark.sql.sources.partitionOverwriteMode=dynamic`도 사정은 같다. Spark가 내부
staging을 거치긴 하지만 커밋 단계에서 결국 같은 rename을 하고, 롤백 수단과
read-back 검증은 여전히 없다.

### EMRFS의 rename은 원자적이지 않다

로컬 파일시스템과 HDFS에서 `rename()`은 메타데이터만 바꾸는 원자적 연산이다.
그러나 **S3에는 디렉터리도 rename도 존재하지 않는다.** 키 접두어가 디렉터리처럼
보일 뿐이다. EMRFS는 `FileSystem.rename()`을 하위 객체를 하나씩 **copy한 뒤
delete**하는 방식으로 흉내 낸다.

따라서 rename 도중 프로세스가 죽으면 대상 경로에 **일부 객체만 옮겨진 상태**가
남을 수 있다. 이것이 위험한 구체적 이유는 복구 로직의 판단 기준에 있다 —
`_path_exists()`는 경로 아래 객체가 하나라도 있으면 "존재함"으로 본다. 그래서
다음 실행의 backup 복구 로직이 이 부분 상태를 "정상적으로 완료된 상태"로
오판하고 backup을 지워버릴 수 있다. row count 검증은 staging 단계에서 끝나므로
이 스왑 단계에는 적용되지 않는다.

이 판단은 지금까지 `cleansing/hourly_storage.py`의 코드 주석(#290)에만 있었고
결정 문서로 남은 적이 없다. #469에서 세 번째 writer를 추가하면서 문서화한다.

## 결정

Silver 시간 파티션 교체는 다음 절차를 따른다.

```
1. staging 경로에 쓴다                    (기존 파티션은 그대로)
2. 다시 읽어 스키마와 행 수를 검증한다      (통과해야만 다음으로)
3. 기존 파티션을 `_backup_<name>`으로 옮긴다
4. staging을 파티션 경로로 승격한다
5. backup을 삭제한다
```

어느 단계에서 실패하든 backup에서 원래 파티션을 되돌린다. 다음 실행 시작 시
남아 있는 backup을 발견하면 스왑 도중 죽은 것으로 보고 복구한다.

**EMRFS rename의 비원자성은 감수한다.**

**backup 디렉터리 이름은 반드시 `_`로 시작한다.** Spark 파티션 탐색이 `_`/`.`로
시작하는 디렉터리를 무시하기 때문이다.

## 대안

| 대안 | 장점 | 단점 | 기각 이유 |
| --- | --- | --- | --- |
| `mode("overwrite")`를 파티션 경로에 직접 | 한 줄로 끝나는 가장 단순한 방식 | 쓰기 도중 실패 시 파티션 소실, read-back 검증 불가 | 되돌릴 원본이 남지 않는다 |
| dynamic partition overwrite | Spark 표준 방식, 파티션 컬럼 기반이라 경로 조립이 불필요 | 커밋 프로토콜 내부에서 같은 rename이 일어나고, 롤백·검증 수단이 없다 | 원자성 문제를 해결하지 못하면서 안전장치만 잃는다 |
| Iceberg / Delta Lake 도입 | 메타데이터 포인터 교체라 커밋이 원자적, 스키마 진화와 시간 여행을 함께 얻는다 | 저장 포맷 전면 변경, 카탈로그 운영 부담, 기존 writer·reader 전부 재작성 | OQ-002(테이블 포맷 미확정)에 걸린 사안이다. 이 결정보다 큰 범위라 별도 ADR로 다뤄야 한다 |
| Gold식 manifest 포인터 | 원자적이고, 이 저장소에 이미 선례가 있다(`comfort_score/standard_storage.py`, #265·#343) | Silver writer 셋과 reader 전부를 재작성해야 한다 | 이번 범위를 크게 넘는다. 향후 전환 후보로 남긴다 |

## 결과

- **실패 창이 파티션 크기로 한정된다.** #469 이전의 `hourly_comfort_score`는 매
  실행 전량을 overwrite했으므로 노출 범위가 전체 이력이었다. 시간 파티셔닝은
  그 자체로 이 위험을 줄인다.
- **`_` 접두어 규칙을 어기면 읽기가 깨진다.** `hour=09.bak` 같은 이름은 Spark가
  `hour="09.bak"`이라는 파티션 값으로 읽어 컬럼 타입 추론을 int에서 string으로
  바꾼다. `hourly_segment_feature_storage.py`가 실제로 이 형태를 쓰고 있어
  **기존 결함으로 남아 있다** — 실패한 실행 직후에만 드러나므로 지금까지
  표면화되지 않았다. 별도 이슈로 정리한다.
- **세 writer가 같은 절차를 각자 복제한다.** Hadoop FileSystem 배관까지 세 벌이
  된다. 공용 모듈로 합치는 것은 이미 동작 중인 두 모듈을 건드리게 되므로 후속
  과제로 남긴다.
- 원자성이 실제 사고로 이어지면 manifest 포인터나 Iceberg로 전환한다. OQ-002가
  해소될 때 함께 재검토한다.

## 영향 범위

- `services/batch-jobs/src/batch_jobs/cleansing/hourly_storage.py` — 기존 구현,
  이 ADR이 사후 문서화한다
- `services/batch-jobs/src/batch_jobs/hourly_segment_feature_storage.py` — 기존
  구현. `.bak` 명명이 이 ADR의 규칙을 어기고 있어 후속 수정 대상
- `services/batch-jobs/src/batch_jobs/hourly_comfort_storage.py` — #469에서 이
  절차를 따라 신규 작성
- `context/open-questions.md` OQ-002 — 테이블 포맷 결정 시 이 ADR을 함께 재검토

## 참고

- [ADR-0001. batch-jobs Spark 실행을 EMR Serverless로 한다](0001-batch-jobs-spark-execution-emr-serverless.md)
- [ADR-0009. Bronze 압축 DAG](0009-bronze-compaction-dag.md) — 원자적 디렉터리
  스왑을 후속 과제로 언급한다
- `docs/superpowers/specs/2026-08-25-hourly-comfort-score-partitioning-design.md`
- 이슈 #290(EMR Serverless 전환), #469(`hourly_comfort_score` 시간 파티셔닝)
