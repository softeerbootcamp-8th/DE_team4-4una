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

## 검증 방법

- `submit_batch_jobs_command`가 생성하는 `sparkSubmitParameters`에 8개 conf가 포함되는지 **자동화 테스트로 확인** (TDD로 진행)
- `standard_score_pipeline`의 job run 3건이 모두 success로 완료

## 결과

두 단계 모두 코드에 반영됐습니다. `emr_serverless.py`가 8개 값을 모듈 상수로 고정합니다. (추적 이슈 #374는 아직 열려 있으나 코드는 반영된 상태입니다.)

## 여기서 얻은 것

**관리형 서비스에서는 애플리케이션의 종료 코드와 플랫폼의 성공 판정이 갈릴 수 있습니다.** `exitCode 0`이고 데이터가 S3에 정상 커밋됐는데도 job이 FAILED인 상황은, 원인이 애플리케이션이 아니라 **플랫폼의 리소스 협상 과정**에 있었기 때문입니다. 관리형 런타임의 로그는 "내 코드가 뭘 했나"와 "플랫폼이 뭘 하려다 실패했나"를 나눠서 읽어야 합니다.

## 재검토 조건

프로덕션 데이터 규모에서의 최적 worker 사이징은 **별도 과제로 남겼습니다.** 데이터가 커져 executor 1개로 부족해지면 `maximumCapacity`와 함께 다시 계산해야 합니다 — 그때는 비용 증가를 감수할지가 함께 논의됩니다.

## 근거

- #372 (driver/executor 크기 고정), #374 (dynamic allocation)
- #368, #377 (같은 전환 과정에서 나온 인접 문제)
- `services/orchestration/dags/emr_serverless.py`
