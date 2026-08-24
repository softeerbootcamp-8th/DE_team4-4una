# DAG 실행 결과 Slack 알림 + 담당자 레지스트리 설계 (#409)

## 배경

Airflow DAG 실행 결과(성공/실패)나 처리 건수를 확인하려면 매번 Web UI를 열어
로그를 봐야 한다 — 실패를 놓치기 쉽다. DAG/task 실패 시 누가 대응해야 하는지,
그 실패가 얼마나 심각한지 코드/문서 어디에도 명시돼 있지 않고, 실패 알림을
받아도 해당 DAG Run/Task Instance를 Web UI에서 직접 탐색해야 한다. Slack
연동은 이 저장소에 아직 전혀 없다(`services/orchestration/README.md`의
"범위 밖", ADR-0004도 at-rest 감시의 Slack 연동을 후속 이슈로 명시적으로
미뤄뒀다).

이 이슈는 담당자/심각도 레지스트리를 신설하고, 5개 대상 DAG(`standard_score_pipeline`,
`current_score_pipeline`, `zone_weather_pipeline`, `data_quality_audit`,
`bronze_compaction`)에 성공/실패 Slack 콜백을 배선한다(`hello_world` 제외).

관련 이슈: #409
관련 문서: `services/orchestration/README.md`,
`docs/adr/0004-data-quality-validation-with-great-expectations.md`

> 브레인스토밍 중 세 지점이 초기 제안에서 뒤집히거나 범위가 넓어졌다 — 왜
> 그랬는지 §2, §4, §6에 근거를 남긴다.

## 확정된 결정

### 1. 담당자 레지스트리 — `config/dag_owners.yaml`

사람 이름 → 식별자 매핑과 `dag_id`(+ 선택적 `task_id`/`task_group_id`) →
owner/severity 매핑을 분리해서 관리한다.

```yaml
users:
  alice:
    email: alice@example.com   # slack_id가 없으면 알림 시점에 users.lookupByEmail로 조회
  bob:
    slack_id: U0456GHIJKL      # 이미 알고 있으면 email 조회를 건너뛰고 바로 멘션

dags:
  standard_score_pipeline:
    owner: alice
    severity: critical
    tasks:
      sensor_processing.resolve_road_snapshot_date:
        owner: bob
        severity: high
  current_score_pipeline:
    owner: alice
    severity: high
  zone_weather_pipeline:
    owner: bob
    severity: medium
  data_quality_audit:
    owner: alice
    severity: medium
  bronze_compaction:
    owner: bob
    severity: low
```

- `users.<name>`은 `email`/`slack_id` 중 최소 하나를 가져야 한다(로드 시점에
  둘 다 없으면 에러). 둘 다 있으면 `slack_id`를 우선한다(API 호출 없이 바로
  멘션 가능).
- `severity`는 코드에 고정된 4단계 상수(critical🔴/high🟠/medium🟡/low⚪,
  이모지 포함)만 참조할 수 있다 — YAML 값이 이 집합 밖이면 로드 시점에 에러.
  임의로 단계를 늘리지 않는다(YAGNI).
- 조회 키는 `task_id`(TaskGroup이 붙은 전체 dotted id, 예:
  `sensor_processing.run_sensor_processing`) 또는 `task_group_id`. 폴백
  순서: **정확한 task_id → task_group_id → DAG 기본값**. DAG 기본값도 없으면
  로드 시점에 에러(모든 대상 DAG는 최소 owner/severity 기본값을 가져야 한다).
- 로더는 `services/orchestration/jobs/dag_owners.py`에 `jobs/weather_rules.py`와
  같은 패턴(dataclass + `load_...` 함수)으로 둔다:
  `load_dag_owners_registry(path=...) -> DagOwnersRegistry`,
  `registry.resolve_owner(dag_id, task_id=None, task_group_id=None) -> OwnerRef`,
  `registry.resolve_severity(...) -> Severity`.

### 2. Slack 연동 — Bot Token(`SlackHook`), Incoming Webhook 아님

**결론**: `apache-airflow-providers-slack`의 `SlackHook`(Bot Token 기반 Slack
Web API, `slack_sdk.WebClient` 래핑)을 쓴다. Connection type `slack`, conn_id
`slack_api_default`. `chat.postMessage`로 메시지를 보내고, 멘션 대상이
`email`로만 등록돼 있으면 `users.lookupByEmail`로 Slack user ID를 조회한다.

**뒤집힌 근거**: 이슈 원문은 Incoming Webhook을 전제했다. 그런데 브레인스토밍
중 "담당자를 이메일로도 등록하고 싶다"는 요구가 나왔고, 이메일→Slack 멘션
변환(`users.lookupByEmail`)은 Slack Web API 호출이 필요해 Incoming Webhook
(메시지 전송 전용, API 호출 권한 없음)으로는 불가능하다. 실무에서도 이런
이유로 알림 봇은 대부분 Bot Token 기반 Slack App 하나로 전송·조회를 함께
처리한다 — Webhook과 별도 API 자격증명 두 개를 관리하는 것보다 단순하다.

- `.env`: `AIRFLOW_CONN_SLACK_API_DEFAULT=slack://:xoxb-...@`(Airflow의
  URI 기반 env-var connection 규약). `.env.example`엔 플레이스홀더만.
- 필요 Slack App 스코프: `chat:write`, `users:read.email`.
- 전송 채널은 토큰에 안 묶이므로 별도 Variable로 지정:
  `AIRFLOW_VAR_SLACK_ALERT_CHANNEL`.
- email→ID 조회는 알림 발생 시점에 그때그때 호출한다 — 알림 빈도가 낮아
  캐싱은 불필요하고(YAGNI), Slack API rate limit에도 여유가 있다.
- `services/orchestration/pyproject.toml`에 `apache-airflow-providers-slack`
  추가(`SlackHook`도 같은 패키지), `uv lock` 갱신.

### 3. 공용 콜백 모듈 — `services/orchestration/dags/notifications.py`

- **`on_failure_callback`**(task 단위, 5개 DAG의 `default_args`에 배선):
  실패한 `task_id`/`dag_id`/실행 시각/예외 요약 + 레지스트리 조회 담당자
  멘션 + 심각도 라벨 + `context["task_instance"].log_url`(Task Instance로
  바로 이동) 포함. Airflow는 재시도가 남은 실패를 `UP_FOR_RETRY`로 보내고
  `on_failure_callback`은 재시도가 소진돼 최종 `FAILED`로 확정될 때만
  호출하므로, 재시도마다 스팸이 발송되지 않는다(별도 처리 불필요).
- **`on_success_callback`**(DAG 단위, DAG 선언에 배선): DagRun이 성공할 때
  1회, `context["dag_run"].get_absolute_url()` + §4에서 정한 처리 건수
  요약을 조합해 전송. DAG마다 "어느 task의 XCom을 요약에 쓸지"만 모듈 안의
  작은 매핑 테이블로 관리한다(아래 §4 표).
- 두 콜백 모두 최상위(모듈 import 시점)에서는 `yaml`/`slack_sdk`/레지스트리
  로딩을 하지 않는다 — 콜백 함수 본문 안에서 지연 수행한다. DAG 파일
  자체는 `airflow-webserver`도 파싱하므로, 그 프로세스에 이 신규
  의존성(pyyaml, apache-airflow-providers-slack)을 설치하지 않아도 DAG
  파싱이 깨지지 않게 하기 위함이다(`jobs.weather`를 함수 안에서만 import하는
  기존 관례와 동일).

### 4. DAG별 "처리 건수" — orchestration 전용 count 조회 task 추가

EMR Serverless로 제출되는 task(`standard_score_pipeline`의 3개 TaskGroup,
`data_quality_audit`)는 원격 Spark job이라 Airflow가 XCom으로 건수를 받을
방법이 없다(`EmrServerlessStartJobOperator`는 Job Run 상태만 폴링). "Airflow
task 로그에 건수를 찍고 나중에 긁어온다"는 방식은 자유 텍스트 파싱이 로그
포맷 변경에 취약하고 로그 보존 정책에 따라 사라질 수 있어 채택하지 않는다.
대신 아래처럼 **PythonOperator 반환값 → XCom**(Airflow의 표준 task-간
구조화된 값 전달 방식) 구조를 그대로 쓴다:

| DAG | 방식 | 요약 소스 task |
| --- | --- | --- |
| `current_score_pipeline` | 기존 `_run_current_score`가 이미 만드는 summary를 `print`뿐 아니라 `return`하도록 1줄 추가 | `run_current_score` |
| `zone_weather_pipeline` | 기존 `_collect_latest_zone_weather`의 summary를 `return`하도록 1줄 추가 | `run_weather_collection` |
| `bronze_compaction` | 기존 `_compact_zone_weather_snapshot`의 summary를 `return`하도록 1줄 추가 | `compact_zone_weather_snapshot` |
| `standard_score_pipeline` | `standard_score` TaskGroup 뒤에 신규 PythonOperator `report_processing_counts` 추가 — 이번 실행의 quarantine/feature 건수(S3 Parquet, `de4_core.ObjectStore`로 해당 파티션 read) + standard score 건수(Postgres `score_as_of` COUNT)를 한 번에 조회해 반환 | `report_processing_counts` |
| `data_quality_audit` | 두 audit task 뒤에 신규 PythonOperator `report_audit_counts` 추가 — `standard_segment_comfort_score`/`current_segment_comfort_score` 테이블 COUNT(\*)를 Postgres에서 직접 조회해 반환(감사 대상 건수) | `report_audit_counts` |

이슈 원문은 "각 DAG의 주요 task가 XCom으로 넘긴 값을 조합한다"고 썼지만,
EMR Job Run 자체는 XCom을 만들 수 없으므로 뒤에 조회 전용 task 하나를 붙이는
방식으로 단순화했다. 새 task들은 모두 `services/orchestration` 안에서만
구현되며(`psycopg2`/`pyarrow`/`de4_core.ObjectStore` 재사용), `services/batch-jobs`는
건드리지 않는다 — 서비스 경계를 지킨다.

`report_processing_counts`/`report_audit_counts`는 기본 `trigger_rule`(모든
상위 task 성공)로 두어, DAG 전체가 실패하면 이 task도 건너뛰어 실패 알림만
나가고 잘못된(부분적인) 처리 건수가 성공 요약으로 나가지 않게 한다.

### 5. 인프라 배선

- `config/`는 현재 어떤 Airflow 컨테이너에도 마운트돼 있지 않다.
  `infra/compose/airflow.yaml`의 `airflow-scheduler`·`airflow-dag-processor`에
  읽기 전용으로 마운트 추가.
- 두 컨테이너의 `_PIP_ADDITIONAL_REQUIREMENTS`에 `pyyaml`,
  `apache-airflow-providers-slack` 추가. DAG-level `on_success_callback`이
  실제로 scheduler/dag-processor 중 어느 프로세스에서 실행되는지는 로컬
  Airflow로 검증하며 확인하고, 불필요한 쪽의 추가 설치는 정리한다.
- `[webserver] base_url`(`AIRFLOW__WEBSERVER__BASE_URL`)을 `.env`/compose에
  추가 — DAG Run/Task Instance URL이 이 값을 기준으로 생성된다(로컬 기본값은
  `http://localhost:8080`).

### 6. EMR Serverless Job Run 로그 S3 영구 저장

**결론**: `dags/emr_serverless.py`의 `submit_batch_jobs_command`가 만드는
모든 EMR Serverless Job Run에 `monitoringConfiguration.s3MonitoringConfiguration`을
추가해, driver/executor 로그를 S3에 영구 저장한다. 실패 Slack 알림에는 로그
전문을 파싱해 넣지 않고, 해당 Job Run의 로그를 바로 볼 수 있는 링크만
포함한다.

**계기**: `standard_score_pipeline`/`data_quality_audit`이 EMR Serverless로
제출하는 task는 지금 `monitoringConfiguration`이 전혀 설정돼 있지 않아
Job Run의 Spark 로그가 EMR 콘솔에 잠깐 노출됐다가 사라질 뿐 어디에도
영구히 남지 않는다. 이 상태로는 §3의 `on_failure_callback`이 실패한 EMR
task에 대해 "Job Run이 실패했다"는 상태값 이상의 정보를 알림에 담을 방법이
없다 — 실무에서도 알림 메시지 자체엔 로그 전문을 넣지 않고(메시지가
길어짐) 영구 저장소에 남긴 뒤 그 위치로 가는 링크만 넣는 방식이 표준이라,
이 이슈 범위에 함께 포함하기로 했다.

**구현**:

- 새 S3 버킷 `de4-emr-serverless-logs`(사용자가 콘솔에서 사전 생성, 리전은
  기존 `de4-data-quality-docs`와 동일하게 `ap-northeast-2`)를 로그
  저장소로 쓴다. 버킷명은 `EMR_SERVERLESS_LOG_S3_URI` Airflow Variable로
  override 가능하게 하되(`GOLD_AUDIT_S3_BUCKET`과 동일한 패턴), 기본값을
  이 버킷의 `s3://de4-emr-serverless-logs/`로 둔다.
- `submit_batch_jobs_command`가 모든 Job Run에
  `"monitoringConfiguration": {"s3MonitoringConfiguration": {"logUri": <위 Variable>}}`을
  추가한다(태스크별로 경로를 나누지 않는다 — EMR Serverless가 이미
  `logUri/applications/<application-id>/jobs/<job-run-id>/...` 구조로
  Job Run별 하위 경로를 자동으로 나눠 쓴다).
- EMR execution role(IAM)에 이 버킷에 대한 `s3:PutObject` 권한을 미리
  부여해야 한다 — README "준비" 절에 다른 EMR 관련 IAM 요구사항과 같은
  형식으로 추가.
- `EmrServerlessStartJobOperator`는 실행한 Job Run의 `job_id`를 XCom(`return_value`)으로
  남긴다. `on_failure_callback`이 실패한 task가 EMR 기반 task(§4 표의
  `standard_score_pipeline`/`data_quality_audit` 소속)이면
  `ti.xcom_pull(task_ids=<실패한 task_id>)`로 job run id를 읽어, AWS 콘솔의
  EMR Serverless Job Run 상세 페이지 링크(`application_id` + `job_run_id`
  조합, `application_id`는 이미 알려진 Airflow Variable)를 알림에 덧붙인다.
  Job Run 생성 자체가 실패해 XCom이 없으면(제출 단계에서 바로 실패)
  이 링크 없이 Airflow `log_url`만 포함한다(fallback).
- Airflow 자체 task 로그(로컬 PythonOperator DAG들)의 원격 로깅
  (`AIRFLOW__LOGGING__REMOTE_LOGGING`) 설정은 이 결정과 별개다 — §제외
  범위 참고.

## 전체 데이터 흐름

```
task 실패(재시도 소진) ──▶ on_failure_callback(context)
                              │
                              ├─ dag_owners.resolve_owner(dag_id, task_id, task_group_id)
                              │     └─ slack_id 있으면 바로, 없으면 email → users.lookupByEmail
                              ├─ dag_owners.resolve_severity(...)
                              ├─ context["task_instance"].log_url
                              ├─ (EMR 기반 task면) xcom_pull(실패 task_id) → job_run_id
                              │     └─ 있으면 EMR Serverless 콘솔 링크도 함께 포함
                              └─ SlackHook.client.chat_postMessage(channel=..., text=...)

DagRun 성공 ──▶ on_success_callback(context)
                   ├─ dag_owners.resolve_owner/severity(dag_id)  # 정보성, 멘션 없음
                   ├─ context["dag_run"].get_absolute_url()
                   ├─ xcom_pull(task_ids=<요약 task>) 로 처리 건수 조합(§4 표)
                   └─ SlackHook.client.chat_postMessage(channel=..., text=...)
```

## 컴포넌트

### `config/`

- `dag_owners.yaml`(신규, §1).

### `services/orchestration`

- `jobs/dag_owners.py`(신규) — 레지스트리 로더 + 조회(dataclass 기반, §1).
- `dags/notifications.py`(신규) — `on_failure_callback`/`on_success_callback`
  구현(§3), Severity 상수(이모지 포함), DAG별 요약 task 매핑 테이블(§4).
- `dags/standard_score_pipeline.py` — `report_processing_counts` task 추가,
  `default_args`에 `on_failure_callback` 배선, DAG에 `on_success_callback`
  배선.
- `dags/current_score_pipeline.py` / `zone_weather_pipeline.py` /
  `bronze_compaction.py` / `data_quality_audit.py` — 각 summary 함수가
  `return`하도록 수정(`data_quality_audit`는 `report_audit_counts` 신규
  추가), 콜백 배선 동일하게 추가.
- `dags/emr_serverless.py` — `submit_batch_jobs_command`에
  `monitoringConfiguration.s3MonitoringConfiguration` 추가(§6).
- `pyproject.toml` — `apache-airflow-providers-slack` 추가.
- `README.md` — dag_owners.yaml 신규 항목 추가 절차, Slack Connection/
  `.env` 설정, `base_url` 설정 가이드, EMR Serverless 로그 버킷 IAM
  요구사항(§6) 추가. "범위 밖" 절에서 "Slack 실패 알림" 항목 제거.

### `infra/compose/airflow.yaml`, `.env.example`

- §5의 마운트/`_PIP_ADDITIONAL_REQUIREMENTS`/`base_url`/Connection
  플레이스홀더, §6의 `AIRFLOW_VAR_EMR_SERVERLESS_LOG_S3_URI` 플레이스홀더
  반영.

## 테스트 전략

- `tests/test_dag_owners.py`(신규) — 레지스트리 로드/조회 폴백(task →
  task_group → dag) 단위 테스트, `slack_id`/`email` 우선순위, 필수값 누락 시
  에러.
- `tests/test_notifications.py`(신규) — 콜백 함수를 가짜 Airflow context(dict)로
  단위 테스트, `SlackHook`은 mock. 멘션/심각도 라벨/URL이 메시지에
  포함되는지, email만 있을 때 `users_lookupByEmail`이 호출되는지 검증.
- 기존 `test_standard_score_pipeline_dag.py`/`test_current_score_pipeline_dag.py`/
  `test_zone_weather_pipeline_dag.py`/`test_data_quality_audit_dag.py`/
  `test_bronze_compaction_dag.py`에 신규 task/콜백 배선 반영(task 존재,
  `default_args`에 콜백 등록 확인).
- 기존 `test_emr_serverless_helper.py`에 `monitoringConfiguration.s3MonitoringConfiguration.logUri`가
  모든 Job Run에 설정되는지 검증하는 테스트 추가(§6).
- **로컬 Airflow 수동 확인**(완료 조건): 대상 DAG를 정상 실행해 처리 건수가
  Slack에 도착하는지, 특정 task를 의도적으로 실패시켜 담당자 멘션·심각도
  라벨·Task Instance URL(클릭 시 실제 실행 화면 이동)이 모두 포함된
  실패 알림이 오는지 확인. EMR 기반 task(standard_score_pipeline/
  data_quality_audit)를 의도적으로 실패시켜 S3에 로그가 실제로 쓰였는지,
  실패 알림의 EMR 콘솔 링크가 해당 Job Run으로 정상 이동하는지 확인.

## 제외 범위

- 실제 운영 Slack 워크스페이스/앱/채널 생성(사전에 사람이 준비) — Bot Token
  발급, `chat:write`/`users:read.email` 스코프 부여 포함.
- On-call 로테이션, 에스컬레이션 자동화.
- Slack 인터랙션(버튼 클릭 재실행 등) 연동.
- Grafana/Prometheus 등 다른 모니터링 채널과의 통합.
- email→Slack ID 조회 결과 캐싱(§2 — 현재 빈도에서는 불필요, 필요해지면
  후속 이슈).
- `services/batch-jobs`를 수정해 EMR Job Run이 직접 건수를 보고하게 하는
  대안(§4에서 기각) — 필요해지면 별도 이슈.
- Airflow 자체 task 로그(로컬 PythonOperator DAG들)의 원격 로깅
  (`AIRFLOW__LOGGING__REMOTE_LOGGING` + `remote_base_log_folder`) — EC2
  인스턴스가 살아있는 한 `log_url`이 문제없이 동작하고, EMR Job Run 로그
  (§6)와 달리 이 알림 기능 자체를 막지 않는 별개의 보존 정책 이슈다.
  필요해지면 별도 이슈.
- 실패 알림에 포함하는 S3/EMR 콘솔 링크를 넘어, 로그 내용 자체를 파싱해
  Slack 메시지에 에러 요약으로 임베드하는 기능(§6) — 링크 제공까지만
  이번 범위.
