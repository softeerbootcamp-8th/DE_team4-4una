# EMR Serverless 용량과 job별 자원 배분 설계 (#508)

## 배경

EMR Serverless Application `de4-batch-jobs`(`00g85ljahc0svj2p`) 하나를 두 DAG가
공유한다. `standard_score_pipeline`이 job run 3건(`run_sensor_processing`,
`run_hourly_scoring`, `run_standard_score`)을, `data_quality_audit`이 2건
(`audit_standard_segment_comfort_score`, `audit_current_segment_comfort_score`)을
제출한다.

`emr_serverless.py:130-148`은 이 5건 전부에 **같은** `sparkSubmitParameters`를
붙인다. driver 1 vCPU / 8 GB, executor 2 vCPU / 14 GB × 2대다. 그리고 동시 제출을
막는 장치가 어디에도 없다.

### 실측 — 2026-08-25 03:00~08:00 UTC, job run 40건과 driver 로그

**용량 경합.** `standard_score_pipeline` 06:00 run의 `run_sensor_processing`이
executor를 10분 12초 동안 받지 못했다.

```text
06:01:42 ExecutorContainerAllocator: Going to request 2 executors ... already provisioned: 0.
ApplicationMaxCapacityExceededException: Worker could not be allocated as the application
  has exceeded maximumCapacity settings: [memory: 48 GB, disk: 200 GB]
   ... 같은 예외 48회, 백오프 3초 → 58초
06:11:54 Registered executor ... with ID 33
06:11:54 Registered executor ... with ID 34
```

경합 상대는 직전 DAG run이 남긴 `validate_standard_score`(05:59:51~06:12:12)였고,
그것이 끝나는 순간 executor가 붙었다. 예외 메시지가 `cpu`를 지목하지 않는다 —
12 vCPU는 남아돌았고 **메모리와 disk가 병목**이었다.

과금 자원으로 executor 생존 시간을 역산하면 겹침과 정확히 맞는다. driver 1 vCPU가
job run 내내 살아 있다고 두고 나머지 vCPU-초를 executor 4코어로 나눈 값이다.

| job run | 실행 | executor 존재 | executor 부재 | 동시 실행 겹침 |
| --- | ---: | ---: | ---: | ---: |
| 03:01 `run_sensor_processing` | 949초 | 467초 | **482초** | 472초 (audit 3건) |
| 05:02 `run_sensor_processing` | 498초 | 240초 | 258초 | 없음 |
| 06:01 `run_sensor_processing` | 820초 | 198초 | **622초** | 642초 |
| 07:02 `run_sensor_processing` | 179초 | 164초 | 15초 | 없음 |

이 역산은 추정이 아니다. 같은 모델(driver 1 vCPU당 8 GB, executor 1 vCPU당 7 GB)로
메모리 GB-h를 예측하면 네 건 모두 소수 3자리까지 실측과 일치한다 — 예측
5.745 / 2.973 / 3.364 / 1.674, 실측 5.743 / 2.976 / 3.366 / 1.676.

부수적으로 이 일치는 `emr_serverless.py:73` 주석의 오기도 확인해 준다. 주석은 job run
합계를 `5 vCPU / 30 GB`로 적었지만 driver의 `memoryOverhead=6g`가 빠져 있다. 실제
합계는 **36 GB**이고, 경합 없는 job run의 실측 GB-h/vCPU-h가 7.20~7.22로
36 ÷ 5 = 7.2와 일치한다.

**audit driver의 exit 137.** `audit_standard_segment_comfort_score`가 08-25 03시에
두 번 연속 실패했다.

```text
ExitCode: 137. Last few exceptions: Worker has been killed as memory usage exceeded
  configured memory size, consider increasing memory size...
```

과금이 `0.16 vCPU-h / 575초` = 정확히 1 vCPU, `1.28 GB-h` → 8.0 GB/vCPU다. **executor는
한 대도 뜨지 않았고 죽은 것은 driver 컨테이너(1 vCPU / 8 GB)**다. 원인은
`gold_audit_validation.py:112`의 `SELECT * FROM {table}`과 `:215,230`의
`add_batch_definition_whole_table`이다 — Great Expectations가 997,332행을 driver의
pandas로 전량 적재한다. JVM 힙(2g) 밖 Python 영역이 `memoryOverhead=6g`를 넘겨
컨테이너가 SIGKILL된다.

**executor를 안 쓰는 job이 있다.** `GB/vCPU`가 8.00(= driver 단독)인 job run을 세면
`validate_standard_score` 7건 전원이다. 그중 하나는 22분 29초 동안 driver만 물고
있었다. 이 job run들은 #495(ADR-0012)가 이미 제거했으므로 이번 설계의 대상이 아니지만,
**"job마다 필요한 자원이 다르다"는 근거**로 남는다. `audit_*`도 같은 성질이다.

**동시 제출을 만드는 세 경로.**

1. `data_quality_audit.py:104-107`이 두 audit task를 의존관계 없이 병렬 제출한다.
   매일 03:00 UTC에 스스로 동시 2건을 만든다.
2. 그 시각 `standard_score_pipeline`도 시작한다(`0 * * * *`).
3. `standard_score_pipeline.py:218`에 `max_active_runs`가 없어 Airflow 기본값 16이다.
   DAG run이 1시간을 넘기면 다음 run이 겹친다 — 베이스라인에 1:09:46, 1:11:47 두 건이 있다.

## 결정 1 — 동시 job run을 1건으로 고정한다

Airflow pool `emr_serverless`(slot 1)를 만들고 `submit_batch_jobs_command`가 만드는
모든 task에 지정한다. `EmrServerlessStartJobOperator`는 `deferrable` 설정이 없어
기본값 `False`로 동작하므로(compose에 `default_deferrable` 없음) job run이 끝날
때까지 pool slot을 점유한다. 두 DAG에 걸친 직렬화가 pool 하나로 성립한다.

pool은 DB 객체라 코드로 선언할 수 없다. `infra/compose/airflow.yaml:104-106`의
`airflow-init` 엔트리포인트에서 `airflow db migrate` 뒤에 이어 만든다. 나머지 세
컨테이너가 `service_completed_successfully`로 이 서비스에 의존하므로
(`docs/deploy-orchestration.md:133-134`) DAG가 돌기 전에 반드시 존재하고, EC2 배포도
같은 compose 파일을 쓰므로 자동 전파된다.

`max_active_runs=1`과 audit task 직렬 의존은 pool이 있으면 중복 방어지만 그대로
넣는다 — pool은 "겹치면 대기"를 보장할 뿐이고, DAG run이 무한정 쌓이는 것과 audit
DAG가 자기 자신을 막는 구조는 별개 문제다.

## 결정 2 — job별 자원 프로파일 3종

`submit_batch_jobs_command`에 프로파일 인자를 추가한다. Application은 그대로 하나이고
바뀌는 것은 각 job run이 요청하는 워커 크기뿐이다. EMR Serverless는 job run마다 전용
driver·executor 세트를 새로 띄우므로(Application이 워커를 공유하지 않는다) 이는 기존
동작 안에서의 값 변경이다.

| 프로파일 | driver | executor | 합계 | 적용 |
| --- | --- | --- | --- | --- |
| `heavy` | 1 vCPU / 8 GB (`2g`+`6g`) / 20 GB | 2 vCPU / 14 GB (`8g`+`6g`) / 60 GB **× 4** | **9 vCPU / 64 GB / 260 GB** | `run_sensor_processing` |
| `default` | 1 vCPU / 8 GB / 20 GB | 2 vCPU / 14 GB / 60 GB × 2 | 5 vCPU / 36 GB / 140 GB | `run_hourly_scoring`, `run_standard_score` |
| `audit` | **2 vCPU / 16 GB (`4g`+`12g`)** / 20 GB | 2 vCPU / 14 GB / 60 GB × 1 | 4 vCPU / 30 GB / 80 GB | `audit_standard_*`, `audit_current_*` |

각 프로파일에서 `dynamicAllocation`의 `min`/`max`/`initial`을 반드시 그 프로파일의
`executor.instances`와 같게 둔다. 어긋나면 #372가 재발한다 — EMR Serverless는
dynamic allocation이 기본 활성이라 목표 executor 수가
`max(initialExecutors, minExecutors, executor.instances)`로 계산되고, 추가 요청이
발생하면 계산이 성공해도 job run이 FAILED로 판정된다.

**heavy의 driver는 줄이지 않는다.** `map_matching/candidates.py:109`가 road_segment
약 17만 건을 driver로 collect해 broadcast payload를 만든다. 이 driver는 실제로 Python
메모리를 쓴다.

**audit driver만 2 vCPU로 올린다.** 1 vCPU는 EMR Serverless의 허용 메모리 상한이
8 GB인데 그것이 지금 죽는 값이다. 그보다 크게 주려면 2 vCPU로 가야 한다. audit의
executor는 1대로 줄인다 — 실측상 `audit_current_segment_comfort_score`는 executor
존재 시간이 7초였다.

**cold start는 늘지 않는다.** 프로비저닝(`createdAt`→`startedAt`)은 워커 크기·종류나
Application 상태와 무관하게 job run당 84~91초로 일정하다(04:00 86초, 05:01 89초,
05:58 91초, 06:00 85초, 07:01 84초, 03:10 85초). job run 개수가 그대로 5건이므로
cold start 총량도 그대로다.

## 결정 3 — maximumCapacity를 12 vCPU / 80 GB / 300 GB로 올린다

동시 1건이 pool로 보장되므로 `maximumCapacity`는 **가장 큰 job run 하나 + 여유분**만
커버하면 된다. 가장 큰 것은 `heavy`(9 vCPU / 64 GB / 260 GB)다.

여유분은 "가장 큰 잔여 워커 1대"로 잡는다. 앞 job run의 워커 반납이 몇 초라도 늦으면
다음 job run의 executor가 `ApplicationMaxCapacityExceededException`을 맞고 백오프에
들어가기 때문이다 — 이번에 확인한 실패가 정확히 그 상황이다. 가장 큰 워커는 audit
driver(2 vCPU / 16 GB / 20 GB)다.

```text
vCPU    9 + 2  = 11  →  12
메모리  64 + 16 = 80  →  80 GB
disk   260 + 20 = 280 →  300 GB
```

64 GB는 `heavy` 합계와 정확히 같아 여유가 0이므로 채택하지 않는다.

**여유를 크게 잡는 데 비용이 들지 않는다.** 현재 Application에
`initialCapacity`가 없어(`get-application`으로 확인) 사전 확보 워커가 없고,
`maximumCapacity`는 상한일 뿐 과금 대상이 아니다. 실제 과금은 job run이 띄운 워커의
사용량으로만 발생한다. #372에서 "`maximumCapacity`를 늘리면 비용이 증가하므로 기각"한
판단은 이 점에서 부정확했다.

실제로 비용이 늘어나는 쪽은 `heavy`의 executor 2대 → 4대다. `run_sensor_processing`이
비례해서 빨라지지 않으면 vCPU-h가 증가한다. 배포 후 `pipeline-perf`로 측정한다.

## 결정 4 — audit DAG를 직렬화하고 스케줄을 옮긴다

두 audit task를 `audit_standard >> audit_current`로 연결한다. 병렬로 둘 이유가 없고,
그것 때문에 매일 03:00 UTC에 스스로 용량 초과 상태로 진입하고 있었다.

스케줄을 `0 3 * * *`에서 `40 8 * * *`(UTC)로 옮긴다. Bronze 입력량이 시각에 따라
단조 감소한다.

| 시각(UTC) | 뉴욕(EDT) | Bronze | `standard_score_pipeline` run 소요 |
| --- | --- | ---: | ---: |
| 03:00 | 23:00 | 1.7 GiB | 1:09:46 |
| 04:00 | 00:00 | 1.4 GiB | 9:43 |
| 05:00 | 01:00 | 862 MiB | 1:11:47 |
| 06:00 | 02:00 | 412 MiB | 45:29 |
| 07:00 | 03:00 | 257 MiB | 32:38 |

뉴욕 현지 새벽 3시에 최저이고, 현재 `0 3 * * *`은 뉴욕 밤 11시로 측정 구간 중 데이터가
가장 많은 시각이면서 hourly DAG와 정확히 동시 시작이다. `40 8 * * *`은 뉴욕 04:40으로,
08:00 hourly run이 끝난 뒤(07:00 run 실측 32:38) 09:00 run 전 틈에 들어간다.

두 가지를 알고 넘어간다. 08:00~09:00 UTC 구간은 `pipeline-perf` 실측이 없어 뉴욕
04~05시라는 근거로 추정한 값이다. 그리고 cron이 UTC 고정이라 겨울(EST)에는 뉴욕
03:40으로 밀리는데, 더 한산한 쪽이라 문제되지 않는다. 어느 쪽이든 pool이 있으면
시간대가 어긋나도 실패가 아니라 대기가 된다 — 스케줄은 최적화이고 pool이 안전장치다.

## 구현 표면

- `services/orchestration/dags/emr_serverless.py` — 프로파일 정의와
  `submit_batch_jobs_command`의 인자 추가, `:73` 합계 주석 수정, 모든 task에 pool 지정
- `services/orchestration/dags/standard_score_pipeline.py` — `max_active_runs=1`,
  `run_sensor_processing`에 `heavy` 지정
- `services/orchestration/dags/data_quality_audit.py` — task 직렬 의존, 스케줄 변경,
  두 task에 `audit` 지정
- `infra/compose/airflow.yaml` — `airflow-init`에서 pool 생성
- `services/orchestration/tests/test_emr_serverless_helper.py` 외 DAG 테스트 — 프로파일별
  conf와 pool 지정 검증
- `docs/decisions/04-emr-serverless-capacity.md` — 이번 실측으로 갱신
- `context/` — `services.md`의 EMR 관련 서술 확인

## maximumCapacity 수동 변경 절차

IaC가 없어 사람이 직접 실행한다. `UpdateApplication`은 Application이 `STOPPED`
(또는 `CREATED`) 상태여야 한다.

**실행 주체.** 로컬 IAM 사용자(`user/edu/codrae`)는 조직 SCP
(`arn:aws:organizations::652613583830:...:policy/p-ibyqe45g`)가 `emr-serverless:*`,
`s3:*`, `cloudwatch:*`를 명시적으로 거부해 아래 명령을 실행할 수 없다. 리전이나
서비스를 바꿔도 같고, 이 계정 IAM으로는 해제할 수 없다. **EC2 호스트
(`ec2-user@43.203.192.129`)에서 인스턴스 프로파일
`de4-serving-api-ec2-role`로 실행한다.**

```bash
APP=00g85ljahc0svj2p
REGION=ap-northeast-2

# 1) 두 DAG를 pause해 새 job run이 들어오지 않게 한다
docker exec compose-airflow-scheduler-1 airflow dags pause standard_score_pipeline
docker exec compose-airflow-scheduler-1 airflow dags pause data_quality_audit

# 2) 진행 중 job run이 없는지 확인한다 (빈 목록이어야 한다)
aws emr-serverless list-job-runs --application-id $APP --region $REGION \
  --states SUBMITTED PENDING SCHEDULED QUEUED RUNNING CANCELLING \
  --query 'jobRuns[].[id,name,state]' --output text

# 3) Application을 정지한다. state가 STOPPED가 될 때까지 기다린다
aws emr-serverless stop-application --application-id $APP --region $REGION
aws emr-serverless get-application --application-id $APP --region $REGION \
  --query 'application.state' --output text

# 4) 용량을 변경한다
aws emr-serverless update-application --application-id $APP --region $REGION \
  --maximum-capacity cpu=12vCPU,memory=80GB,disk=300GB

# 5) 반영을 확인한다
aws emr-serverless get-application --application-id $APP --region $REGION \
  --query 'application.maximumCapacity'

# 6) DAG를 재개한다. autoStartConfiguration이 enabled라 다음 job run 제출 시
#    Application이 자동으로 다시 뜬다 — 명시적 start는 필요 없다
docker exec compose-airflow-scheduler-1 airflow dags unpause standard_score_pipeline
docker exec compose-airflow-scheduler-1 airflow dags unpause data_quality_audit
```

2단계에서 job run이 남아 있으면 3단계가 `ValidationException`으로 실패한다. 끝날
때까지 기다리거나, 불필요한 실행이면 `cancel-job-run`으로 정리한 뒤 진행한다.
`stop-application --force`는 쓰지 않는다 — 다른 DAG의 job run까지 취소한다.

## 검증

1. `sparkSubmitParameters`가 프로파일별로 정확한 conf를 만드는지 자동화 테스트로 확인한다
   (TDD). 특히 각 프로파일에서 `dynamicAllocation`의 세 값이 `executor.instances`와
   같은지 확인한다 — #372 재발 방지선이다.
2. EMR 제출 task 전부가 `pool="emr_serverless"`를 갖는지 DAG 테스트로 확인한다.
3. 로컬 compose를 올려 `airflow pools list`에 `emr_serverless`(slots 1)가 보이는지
   확인한다.
4. 배포 후 driver 로그에 `ApplicationMaxCapacityExceededException`이 없는지 확인한다.
5. `data_quality_audit`이 연속 3회 success로 끝나는지 확인한다.
6. `pipeline-perf`를 재수집해 job run당 executor 부재 시간이 30초 이하인지 확인한다
   (경합 없는 07:00 run 실측 15초가 기준선이다).

## 기각한 대안

**executor를 6대로 늘리기.** `vehicle_profile_id`의 카디널리티가 6이라는 데서 나온
안이었다. 그런데 `run_sensor_processing`이 실제로 셔플하는 키는 `event_id`
(`cleansing/validate.py:125`)와 `trip_id`(`sensor_features/trip_window.py:10`,
`events.py:65`, `feature_pipeline.py:231-232`)이고, 최종 집계 키는
`(data_period_start, data_period_end, segment_id, vehicle_profile_id)`
(`sensor_features/aggregation.py:12-17`)다. `vehicle_profile_id` 단독으로 파티셔닝하는
지점이 한 곳도 없어, 앞단에서 그 키로 repartition해도 셔플이 줄지 않고 하나 늘어난다.
파티션을 6개로 고정하면 AQE가 만드는 53~130개 대비 병렬도가 떨어지고 스큐를 흡수할
여지도 사라진다(현재 task duration의 max/p50이 2.0 이상인 스테이지가 186개, 최대 29배).

**`cores=1`로 executor를 쪼개 현재 용량 안에서 6대 만들기.** `maximumCapacity`를 안
건드려도 된다는 장점이 있었지만, `maximumCapacity`가 과금 대상이 아니라는 사실이
확인되면서 그 장점이 사라졌다. JVM과 broadcast payload가 6벌로 늘어나는 대가만 남는다.

**`initialCapacity`로 워커를 미리 데워 cold start 없애기.** 사전 확보 워커는 유휴
상태에도 과금되고, 현재 파이프라인은 시간당 몇 분만 실행되므로 유휴 시간이 압도적이다.

**`maximumCapacity`를 24 vCPU로 올리기.** 메모리를 함께 올리지 않으면 못 쓴다. 우리
워커의 실측 비율이 7.2 GB/vCPU라 24 vCPU를 다 쓰려면 약 168 GB가 필요하다. 52 GB로는
executor 3대에서 이미 막힌다.

## 남긴 과제

- `gold_audit_validation`의 전량 적재를 SQL 집계나 샘플링으로 바꾸는 근본 수정. 이번
  driver 증량은 임시방편이고 테이블이 커지면 다시 죽는다.
- executor를 4대에서 더 늘릴지 판단. 슬롯 점유율 중앙값 52.6%는 측정 대상 job run
  상당수가 경합 상태여서 지금은 신뢰할 수 없다. 직렬화 후 재측정이 선행되어야 한다.
- `standard_score.postgres_merge`(4:09~5:43, psycopg2 단일 스레드)처럼 executor가
  살아 있는 채로 노는 driver 단독 구간의 처리.
- Application을 IaC로 관리하기. 지금은 수동 생성·수동 변경이라 이 문서의 절차에 의존한다.
