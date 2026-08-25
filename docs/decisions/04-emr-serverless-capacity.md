# 04. EMR Serverless 용량 안에 driver와 executor를 코드로 고정한다

> 증상과 원인이 두 번 어긋난 사례입니다. "성공했는데 실패로 보고되는" 문제를 포함합니다.

← [의사결정 목록](README.md)

## 트리거

[01. Spark 실행 환경](01-spark-execution-environment.md)에서 EMR Serverless를 채택하고 실제 파이프라인을 붙이는 과정에서, job run이 연달아 실패했습니다.

---

## 1단계 — executor가 하나도 뜨지 않는다

### 관측된 사실

`run_sensor_processing`이 **20분간 정상 실행되다** 실패했습니다.

```
ApplicationMaxCapacityExceededException: Worker could not be allocated as the
  application has exceeded maximumCapacity settings: [cpu: 4 vCPU, memory: 16 GB]

ERROR EmrServerlessClusterSchedulerBackend: Abandoning job due to no executor
  being launched within 1200000ms after driver starts.
```

### 근본 원인

리소스 부족이 아니었습니다.

Application의 `maximumCapacity`가 `4 vCPU / 16 GB`인데, 코드가 Spark 리소스 conf를 **전혀 지정하지 않아** EMR Serverless 기본값(`driver.cores=4`, `driver.memory=8G`, `executor.cores=2`, `executor.memory=8G`)이 그대로 적용됐습니다.

**driver 하나가 vCPU 예산 전체(4 vCPU)를 소진**해 executor가 들어갈 자리가 없었습니다. 즉 "리소스가 모자란" 상황이 아니라 **driver + executor 조합이 원천적으로 이 용량에 들어갈 수 없는 구조적 문제**였습니다.

### 결정

driver·executor 크기를 **코드 레벨에서 고정**합니다. AWS 기본값이 바뀌어도 휘둘리지 않도록 상수로 둡니다.

### 기각한 대안

Application의 `maximumCapacity`를 늘리기 — **비용이 증가하므로 기각.** 이번에는 기존 용량 안에 맞추는 방향을 택했습니다.

---

## 2단계 — 전부 성공했는데 FAILED로 보고된다

### 관측된 사실

1단계를 반영하고 재실행한 결과:

- conf는 정확히 제출됐다 (`spark.driver.cores=1` 등)
- 데이터도 **끝까지 처리되어 S3에 커밋 완료** (`sensor_event_quarantine`, `hourly_segment_features`)
- Spark도 `SparkContext stopping with exitCode 0`으로 깔끔히 종료

**그런데 `get-job-run`은 `FAILED` / `ExitCode: 1`을 반환했습니다.**

드라이버 로그:

```
INFO Utils: Using initial executors = 3, min of effectiveMaxExecutors,
  (max of spark.dynamicAllocation.initialExecutors,
   spark.dynamicAllocation.minExecutors and spark.executor.instances)
INFO ExecutorContainerAllocator: Set total expected execs to {0=3}
```

### 근본 원인

EMR Serverless는 **dynamic allocation이 기본 활성화**되어 있습니다. `spark.executor.instances=1`을 지정해도 실제 목표 executor 수는

```
max(dynamicAllocation.initialExecutors, dynamicAllocation.minExecutors, executor.instances)
```

로 계산되고, EMR 기본값이 1보다 커서 최종 목표가 **3**이 됐습니다.

실제로는 executor 1개로 작업이 전부 성공했지만, Spark가 나머지 2개를 계속 요청하며 백오프(2s → 3s → 4s → 6s → 8s → 12s → 18s)로 `ApplicationMaxCapacityExceededException`을 반복했고, EMR Serverless가 **이 반복된 리소스 확보 실패 이력을 근거로 최종 상태를 FAILED로 판정**했습니다.

### 결정

`dynamicAllocation`의 min·max·initial을 모두 `executor.instances`와 같게 고정해, **애초에 추가 executor 요청이 발생하지 않게** 합니다.

---

## 최종 결정 — 8개 conf 고정

| 항목 | 값 |
| --- | --- |
| driver | `cores=1`, `memory=2g` |
| executor | `cores=2`, `memory=8g`, `instances=1` |
| dynamic allocation | `minExecutors=1`, `maxExecutors=1`, `initialExecutors=1` |

driver + executor 1개가 `4 vCPU / 16 GB` 안에 여유 있게 들어갑니다.

> 위 표는 **1·2단계 시점의 값**입니다. `instances`는 #471에서 2로, #508에서 job별
> 프로파일로 바뀌었습니다. 현재 값은 3단계와 설계 문서를 보세요.

## 검증 방법

- `submit_batch_jobs_command`가 생성하는 `sparkSubmitParameters`에 8개 conf가 포함되는지 **자동화 테스트로 확인** (TDD로 진행)
- `standard_score_pipeline`의 job run 3건이 모두 success로 완료

## 결과

두 단계 모두 코드에 반영됐습니다. `emr_serverless.py`가 8개 값을 모듈 상수로 고정합니다. (추적 이슈 #374는 아직 열려 있으나 코드는 반영된 상태입니다.)

## 여기서 얻은 것

**관리형 서비스에서는 애플리케이션의 종료 코드와 플랫폼의 성공 판정이 갈릴 수 있습니다.** `exitCode 0`이고 데이터가 S3에 정상 커밋됐는데도 job이 FAILED인 상황은, 원인이 애플리케이션이 아니라 **플랫폼의 리소스 협상 과정**에 있었기 때문입니다. 관리형 런타임의 로그는 "내 코드가 뭘 했나"와 "플랫폼이 뭘 하려다 실패했나"를 나눠서 읽어야 합니다.

---

## 3단계 — 겹치면 굶고, 감사 job은 driver가 죽는다 (#508)

> 1·2단계로부터 넉 달 뒤, 아래 "재검토 조건"에 적어둔 과제를 실제로 다룬 기록입니다.

### 관측된 사실

2026-08-25 03:00~08:00 UTC의 job run 40건과 driver 로그를 봤습니다.

`standard_score_pipeline` 06:00 run의 `run_sensor_processing`이 executor를 **10분 12초** 동안 받지 못했습니다.

```
06:01:42 ExecutorContainerAllocator: Going to request 2 executors ... already provisioned: 0.
ApplicationMaxCapacityExceededException: Worker could not be allocated as the application
  has exceeded maximumCapacity settings: [memory: 48 GB, disk: 200 GB]
   ... 같은 예외 48회
06:11:54 Registered executor ... with ID 33
```

경합 상대는 직전 DAG run이 남긴 `validate_standard_score`(05:59:51~06:12:12)였고, 그것이 끝나는 순간 executor가 붙었습니다.

그리고 `audit_standard_segment_comfort_score`가 `ExitCode: 137`로 실패하고 있었습니다. 과금이 `0.16 vCPU-h / 575초` = 정확히 1 vCPU라 **executor는 한 대도 뜨지 않았고 죽은 것은 driver**였습니다.

### 근본 원인

**용량이 모자란 것이 아니라 동시 제출을 막는 장치가 없었습니다.** 두 DAG가 Application 하나를 공유하는데 `standard_score_pipeline`에 `max_active_runs`가 없었고(Airflow 기본값 16), `data_quality_audit`은 두 task를 병렬 제출해 스스로 동시 2건을 만들고 있었습니다.

audit 쪽은 별개 원인입니다. `gold_audit_validation.py`가 `SELECT * FROM {table}`과 `add_batch_definition_whole_table`로 997,332행을 driver의 pandas에 전량 올립니다. 1 vCPU driver의 메모리 상한이 8 GB인데 그것이 부족했습니다.

### 정정 — 1단계에서 잘못 판단한 것 두 가지

**`maximumCapacity`를 늘리는 것은 비용을 늘리지 않습니다.** 1단계에서 "비용이 증가하므로 기각"했는데, `maximumCapacity`는 상한일 뿐 과금 대상이 아닙니다. 실제 과금은 job run이 띄운 워커의 사용량으로만 발생하고, 유휴 과금은 `initialCapacity`를 설정할 때만 생깁니다. 이 Application에는 `initialCapacity`가 없습니다.

**합계는 30 GB가 아니라 36 GB였습니다.** `emr_serverless.py` 주석이 driver의 `memoryOverhead=6g`를 빼먹었습니다. EMR Serverless는 worker 메모리를 `memory + memoryOverhead`로 잡습니다 — 경합 없는 job run의 실측 `GB-h / vCPU-h`가 7.20~7.22로 `36 ÷ 5`와 일치합니다.

### 결정

동시 제출을 Airflow pool `emr_serverless`(slot 1)로 **1건으로 고정**하고, job마다 필요한 자원이 10배까지 다르므로(`run_sensor_processing` 0.783 vCPU-h 대 `run_hourly_scoring` 0.073 vCPU-h) **자원 프로파일 3종**으로 나눴습니다. `maximumCapacity`는 가장 큰 프로파일 + 잔여 워커 1대분 여유로 **12 vCPU / 80 GB / 300 GB**로 올렸습니다.

`dynamicAllocation`의 min/max/initial은 상수로 두지 않고 `executor.instances`에서 **파생**시켰습니다. 2단계 사고가 이 세 값이 어긋나서 났으므로, 어긋나는 것 자체를 불가능하게 만든 것입니다.

전체 설계와 수동 변경 절차는 [2026-08-26 설계 문서](../superpowers/specs/2026-08-26-emr-serverless-capacity-design.md)에 있습니다.

### 기각한 대안

`vehicle_profile_id`(카디널리티 6)로 파티셔닝해 executor 6대에 하나씩 배분하기 — **기각.** 이 파이프라인이 실제로 셔플하는 키는 `event_id`와 `trip_id`이고 `vehicle_profile_id` 단독으로 파티셔닝하는 지점이 한 곳도 없어, 셔플이 줄지 않고 하나 늘어납니다. 파티션을 6개로 고정하면 AQE가 만드는 53~130개 대비 병렬도가 떨어지고 스큐를 흡수할 여지도 사라집니다.

---

## 재검토 조건

프로덕션 데이터 규모에서의 최적 worker 사이징은 별도 과제로 남겼었고, **3단계(#508)에서 다뤘습니다.**

아직 열려 있는 것은 셋입니다. `gold_audit_validation`의 전량 적재를 없애는 근본 수정 — 3단계의 driver 증량은 임시방편이라 테이블이 커지면 다시 죽습니다. executor를 4대에서 더 늘릴지 판단 — 직렬화 후 슬롯 점유율을 다시 재야 근거가 생깁니다. 그리고 Application을 IaC로 관리하기 — 지금은 생성도 변경도 수동입니다.

## 근거

- #372 (driver/executor 크기 고정), #374 (dynamic allocation)
- #368, #377 (같은 전환 과정에서 나온 인접 문제)
- #386 (executor exit 137 -> memoryOverhead), #443 (disk), #471 (executor 2대로)
- #508 (동시성 직렬화, job별 프로파일, maximumCapacity 재산정)
- `services/orchestration/dags/emr_serverless.py`
- [2026-08-26 EMR Serverless 용량과 job별 자원 배분 설계](../superpowers/specs/2026-08-26-emr-serverless-capacity-design.md)
