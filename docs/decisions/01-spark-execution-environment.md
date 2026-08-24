# 01. Spark 배치는 EMR Serverless로 실행한다

> 결정이 한 번 도전받고 유지된 사례입니다.

← [의사결정 목록](README.md)

## 트리거

Airflow DAG를 설계하는 과정에서, Airflow 컨테이너가 Spark 배치를 **어디서** 실행할지에 대한 결정이 없다는 것이 드러났습니다. 공식 `apache/airflow` 이미지에는 PySpark가 포함되지 않고, "공식 이미지를 그대로 쓴다"는 결정은 이미 내려져 있었습니다.

## 관측된 사실

- 목표 실행 패턴 — **매시간 한 번 짧게 돌리고 나머지 시간은 쉰다.** 주기적이고 유휴 시간이 긴 워크로드.
- 시작 시점에 저장소에 EMR·Terraform 인프라가 전혀 없었다 (`terraform/envs/*`는 빈 폴더).

## 근본 원인

오케스트레이션과 연산의 실행 환경이 분리되지 않아 생긴 공백입니다. Airflow가 스케줄링만 담당한다면 연산을 위임할 대상이 필요하고, 그 대상이 정해지지 않은 상태였습니다.

## 선택지

| 대안 | 장점 | 단점 | 기각 이유 |
| --- | --- | --- | --- |
| 클래식 EMR (EC2) | 커스텀 AMI·부트스트랩 액션·SSH 등 세밀한 제어 | 매 실행마다 클러스터 부트스트랩 5~10분, 그 시간도 과금 | 배치 연산 시간이 부트스트랩 오버헤드와 비슷하거나 더 짧아, 짧은 주기에서 오버헤드 비율이 과도하다 |
| 상시 기동 EC2에서 Docker 실행 | 기존 `Dockerfile` 재사용, 구현이 가장 단순 | 상시 인프라 유지 비용, 오토스케일링·관리형 복구 없음 | 로컬 검증용으로는 유효하나, 운영 목표가 관리형 실행이다 |
| Airflow 이미지에 PySpark 설치 | 별도 실행 환경 연결이 불필요 | "공식 이미지 그대로" 결정과 충돌, Airflow/PySpark 의존성 버전 충돌 위험 | 오케스트레이션과 연산의 관심사 분리 위배 |
| **EMR Serverless** | 유휴 시 auto-stop, job 단위 과금, 전용 Airflow Operator 존재 | 콜드 스타트 수십 초~2분, 커스텀 네이티브 의존성·SSH 디버깅 불가 | **채택** |

## 결정

`EmrServerlessStartJobOperator`로, 미리 프로비저닝한 EMR Serverless Application에 각 단계를 job run으로 제출합니다. Application은 Terraform으로 관리하고 **사전 초기화 용량(pre-initialized capacity)은 두지 않습니다.**

## 최적화 대상과 포기한 것

유휴 비용과 운영 부담을 최소화하는 것을 택했습니다. 대가로 포기한 것:

- **콜드 스타트** 수십 초~2분 (사전 초기화 용량을 두면 없앨 수 있지만 유휴 비용이 다시 생긴다)
- **SSH 기반 디버깅**과 커스텀 bootstrap action
- `spark.jars.packages`로 Maven에서 JDBC 드라이버를 받아오던 방식 — S3 기반으로 전환이 필요했다

## 검증 방법

`standard_score_pipeline`의 EMR Serverless job run 3건이 모두 success로 완료되는지.

## 결과

이후 **EMR on EC2로 이관하자는 제안(#348)** 이 올라왔습니다. 검토 결과 **NOT_PLANNED로 철회**하고 EMR Serverless를 유지했습니다. 원 결정이 한 번 도전받고 살아남은 셈입니다.

다만 실제 운영에 붙이는 과정에서 용량 관련 문제가 연달아 드러났습니다 → [04. EMR Serverless 용량](04-emr-serverless-capacity.md)

## 재검토 조건

ADR-0001이 조건을 명시하고 있습니다.

- 커스텀 네이티브 라이브러리가 필요해질 때
- 상시 실행 스트리밍 워크로드를 같은 환경에 합치려 할 때 (EMR Serverless는 주기적 배치에 맞춰진 선택이다)

## 근거

- [ADR-0001](../adr/0001-batch-jobs-spark-execution-emr-serverless.md)
- #348 (이관 제안, NOT_PLANNED)
- #289 · #290 · #292 · #295 (EMR Serverless 전환 구현)
- `services/orchestration/dags/emr_serverless.py`
