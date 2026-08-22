---
status: accepted
date: 2026-08-17
supersedes:
superseded_by:
---

# 0001. batch-jobs Spark 배치잡 실행 환경으로 EMR Serverless 선택

## 배경

이슈 #152까지 완료된 batch-jobs의 4단계 배치 파이프라인(`cleanse-sensor-events` →
`build-hourly-segment-features` → `score-hourly-comfort` →
`load-segment-comfort-score`)을 Airflow(`services/orchestration`, #70에서
LocalExecutor로 부트스트랩됨)로 오케스트레이션하는 DAG를 설계하는 과정에서,
Airflow 컨테이너(공식 `apache/airflow` 이미지, pyspark 미포함)가 이 Spark
배치잡을 실제로 어디서·어떻게 실행시킬지에 대한 결정이 없었다는 게 드러났다.

목표 실행 패턴은 "매시간 한 번, 4단계 파이프라인을 짧게 돌리고 나머지 시간은
쉰다"는, 짧고 주기적이며 유휴 시간이 긴 워크로드다. 이 저장소에는 아직 EMR이나
Terraform 기반 AWS 인프라가 없다(`terraform/envs/*`는 리전별 빈 폴더뿐).

## 결정

batch-jobs의 Spark 배치잡은 **EMR Serverless**로 실행한다. Airflow는
`EmrServerlessStartJobOperator`(및 관련 센서)로, 미리 생성해 둔 EMR Serverless
Application에 각 단계를 job run으로 제출한다.

## 대안

| 대안 | 장점 | 단점 | 기각 이유 |
| --- | --- | --- | --- |
| 클래식 EMR(EC2, `EmrCreateJobFlowOperator`) | 커스텀 AMI·부트스트랩 액션·SSH 등 세밀한 제어 가능 | 매 실행마다 클러스터 부트스트랩(통상 5~10분)과 테어다운을 반복하고, 그 시간 동안도 EC2 요금이 그대로 나간다 | 4단계 배치잡 자체의 연산 시간이 부트스트랩 오버헤드와 비슷하거나 작아, 짧은 주기 워크로드에서 오버헤드 비율이 지나치게 커진다 |
| Docker 컨테이너를 로컬(또는 상시 기동된 EC2)에서 실행 | 이미 있는 `services/batch-jobs/Dockerfile`을 그대로 재사용할 수 있어 구현이 단순 | 상시 인프라(EC2/컨테이너)를 유지해야 하고, 오토스케일링이나 관리형 장애복구가 없다 | 로컬 개발 검증용으로는 여전히 유효하지만, 운영 목표가 관리형 실행이라 최종 실행 환경으로는 부적합 |
| Airflow 이미지 자체에 pyspark 설치(LocalExecutor 내부에서 직접 실행) | 별도 실행 환경 연결이 필요 없어 구조가 단순해 보임 | #70에서 "공식 이미지 그대로, 커스텀 빌드 안 함"으로 이미 확정한 결정과 충돌하고, Airflow/PySpark 의존성이 한 이미지에 섞여 버전 충돌 위험이 커진다 | 오케스트레이션과 연산의 관심사 분리 원칙에 위배 |

## 결과

- EMR Serverless Application은 Terraform으로 프로비저닝하고, 유휴 시
  auto-stop(과금 최소화)이 되도록 구성한다. 콜드 스타트(수십 초~1~2분)는
  감수한다 — 사전 초기화 용량(pre-initialized capacity)을 둘 수도 있지만,
  그러면 유휴 비용이 다시 생기므로 기본값은 "사전 초기화 없음"으로 한다.
- `comfort_score/gold_job.py`가 지금 `spark.jars.packages`로 Postgres
  JDBC 드라이버를 Maven에서 런타임에 받아오는 방식은, EMR Serverless의
  네트워크·의존성 모델에 맞게 JDBC jar를 S3에 미리 올려두고
  `spark.jars=s3://...`로 참조하는 방식으로 바꿔야 할 가능성이 높다.
  정확한 방식은 후속(EMR 연결) 이슈에서 검증한다.
- EMR Serverless는 커스텀 네이티브 의존성, SSH 기반 디버깅, 세밀한
  bootstrap action 등을 지원하지 않는다. 지금 파이프라인은 pyspark 표준
  기능과 JDBC 드라이버 정도만 쓰므로 당장 문제는 없을 것으로 보이지만,
  이후 이 가정이 깨지면(예: 커스텀 네이티브 라이브러리 필요) 이 ADR을
  재검토해야 한다.
- 이번 Airflow DAG 설계 이슈에서는 EMR Serverless로의 실제 연결(Application
  프로비저닝, IAM 역할, 네트워킹, job driver 스크립트)은 다루지 않는다 —
  DAG의 task 구조·의존성·스케줄링·멱등성만 확정하고, 각 task의 실행부는
  후속 이슈에서 `EmrServerlessStartJobOperator`로 채워질 자리로 남긴다.

## 영향 범위

- `services/orchestration` — DAG의 각 task 실행부가 이 결정을 따라 구현될
  예정 (후속 이슈).
- `terraform/envs/*` — EMR Serverless Application과 관련 IAM 역할·정책을
  이 결정에 따라 추가해야 한다 (후속 이슈).
- `services/batch-jobs/src/batch_jobs/comfort_score/gold_job.py` — Postgres
  JDBC 드라이버 로딩 방식을 S3 기반으로 바꿔야 할 가능성 (후속 이슈에서 검증).

## 참고

- 관련 이슈: #70 (Airflow LocalExecutor 부트스트랩), Airflow 파이프라인 DAG
  설계 이슈(이 ADR 작성 후 생성 예정)
- 관련 설계 문서: `docs/superpowers/specs/2026-08-14-airflow-bootstrap-design.md`
