# EMR Serverless 용량과 job별 자원 배분 구현 계획 (#508)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** EMR Serverless job run 동시 제출을 1건으로 직렬화하고, job별 자원 프로파일 3종을 도입하며, Application `maximumCapacity`를 12 vCPU / 80 GB / 300 GB로 올린다.

**Architecture:** `emr_serverless.py`에 frozen dataclass `SparkResourceProfile`을 두고 `submit_batch_jobs_command`가 프로파일을 받아 `sparkSubmitParameters`를 만든다. `dynamicAllocation`의 min/max/initial은 `executor_instances`에서 파생시켜 세 값이 구조적으로 어긋날 수 없게 한다(#372 재발 방지). 동시성은 Airflow pool `emr_serverless`(slot 1)로 두 DAG에 걸쳐 직렬화하고, pool은 `airflow-init` 컨테이너가 만든다.

**Tech Stack:** Python 3.12, uv, Airflow 3.3.1, `apache-airflow-providers-amazon`(`EmrServerlessStartJobOperator`), pytest, docker compose

**Spec:** [docs/superpowers/specs/2026-08-26-emr-serverless-capacity-design.md](../specs/2026-08-26-emr-serverless-capacity-design.md)

## Global Constraints

- 작업 대상은 `services/orchestration`과 `infra/compose`, `docs/`뿐이다. 다른 `services/*`와 `libs/de4-core`는 읽기만 한다(AGENTS.md).
- 각 프로파일에서 `spark.dynamicAllocation.minExecutors` / `maxExecutors` / `initialExecutors`는 반드시 그 프로파일의 `spark.executor.instances`와 같아야 한다. 어긋나면 EMR Serverless가 성공한 job run을 FAILED로 판정한다(#372).
- `sparkSubmitParameters`는 쉘을 거치지 않고 문자열 그대로 API에 전달된다. 값을 따옴표로 감싸면 그 문자가 값의 일부가 된다(#368). 기존 테스트의 `assert '"' not in params`를 유지한다.
- 모든 job run에 `PYSPARK_PYTHON=/usr/bin/python3.12` conf 4종이 유지되어야 한다(#360).
- 프로파일 합계는 `maximumCapacity` 12 vCPU / 80 GB / 300 GB 안에 들어가야 한다. worker 메모리는 `memory + memoryOverhead`이고, EMR Serverless가 과금·용량에 반영하는 값이다.
- 커밋 메시지는 `<type>: <title>` 형식, 제목은 영어 소문자로 시작한다(CONTRIBUTING.md 4절). Co-authored-by 푸터를 붙이지 않는다.
- 검증 명령: `uv run --all-packages ruff check .`, `uv run --all-packages pytest`.

## File Structure

| 파일 | 책임 |
| --- | --- |
| `services/orchestration/dags/emr_serverless.py` | `SparkResourceProfile` 정의, 프로파일 3종 상수, `submit_batch_jobs_command`의 프로파일·pool 적용 |
| `services/orchestration/dags/standard_score_pipeline.py` | `max_active_runs=1`, `run_sensor_processing`에 `heavy` 지정 |
| `services/orchestration/dags/data_quality_audit.py` | audit task 직렬 의존, 스케줄 `40 8 * * *`, 두 task에 `audit` 지정 |
| `infra/compose/airflow.yaml` | `airflow-init`에서 pool `emr_serverless` 생성 |
| `services/orchestration/tests/test_emr_serverless_helper.py` | 프로파일별 conf, 용량 적합성, pool 지정 검증 |
| `services/orchestration/tests/test_standard_score_pipeline_dag.py` | `max_active_runs`, 프로파일, pool 검증 |
| `services/orchestration/tests/test_data_quality_audit_dag.py` | 직렬 의존, 스케줄, 프로파일, pool 검증 |
| `docs/decisions/04-emr-serverless-capacity.md` | 이번 실측으로 갱신 |

## 배포 순서 — 반드시 지킬 것

`heavy` 프로파일 합계는 **64 GB**이고 현재 `maximumCapacity`는 **48 GB**다. **Task 1(수동 용량 상향)을 먼저 끝내지 않고 코드를 배포하면 `run_sensor_processing`이 매 실행 `ApplicationMaxCapacityExceededException`으로 executor를 못 받는다.** Task 1은 현재 코드와 하위 호환이므로(상한만 올라간다) 언제 해도 안전하다. 코드 배포보다 앞서기만 하면 된다.

---

### Task 1: Application maximumCapacity 상향 (수동, 코드 변경 없음)

**Files:** 없음 — AWS 콘솔/CLI 작업이다.

**Interfaces:**
- Consumes: 없음
- Produces: `maximumCapacity` = `{cpu: 12 vCPU, memory: 80 GB, disk: 300 GB}`. Task 2 이후의 모든 프로파일이 이 한도를 전제한다.

**실행 주체:** 로컬 IAM 사용자(`user/edu/codrae`)는 조직 SCP(`p-ibyqe45g`)가 `emr-serverless:*`를 명시적으로 거부해 아래 명령을 실행할 수 없다. **EC2 호스트에서 인스턴스 프로파일로 실행한다.**

```bash
ssh -i "<de4-serving-api-key.pem 경로>" ec2-user@43.203.192.129
```

키 파일 경로는 개발자 로컬마다 다르고 저장소에 두지 않으므로 이 문서에 적지 않는다.
아래 Task 7의 `<키 경로>`도 같다.

- [ ] **Step 1: 두 DAG를 pause한다**

```bash
docker exec compose-airflow-scheduler-1 airflow dags pause standard_score_pipeline
docker exec compose-airflow-scheduler-1 airflow dags pause data_quality_audit
```

- [ ] **Step 2: 진행 중인 job run이 없는지 확인한다**

```bash
APP=00g85ljahc0svj2p; REGION=ap-northeast-2
aws emr-serverless list-job-runs --application-id $APP --region $REGION \
  --states SUBMITTED PENDING SCHEDULED QUEUED RUNNING CANCELLING \
  --query 'jobRuns[].[id,name,state]' --output text
```

기대: 출력이 비어 있다. 남아 있으면 끝날 때까지 기다린다. 불필요한 실행이면 `aws emr-serverless cancel-job-run --application-id $APP --job-run-id <id> --region $REGION`으로 정리한다. `stop-application --force`는 쓰지 않는다 — 다른 DAG의 job run까지 취소한다.

- [ ] **Step 3: Application을 정지한다**

```bash
aws emr-serverless stop-application --application-id $APP --region $REGION
aws emr-serverless get-application --application-id $APP --region $REGION \
  --query 'application.state' --output text
```

기대: 두 번째 명령이 `STOPPED`를 출력할 때까지 반복한다. 보통 1분 안에 끝난다.

- [ ] **Step 4: 용량을 변경한다**

```bash
aws emr-serverless update-application --application-id $APP --region $REGION \
  --maximum-capacity cpu=12vCPU,memory=80GB,disk=300GB
```

`maximumCapacity` 구조의 키는 `cpu`, `memory`, `disk` 셋뿐이다(`--generate-cli-skeleton input`으로 확인). Application이 `STOPPED`가 아니면 `ValidationException`이 난다.

- [ ] **Step 5: 반영을 확인한다**

```bash
aws emr-serverless get-application --application-id $APP --region $REGION \
  --query 'application.maximumCapacity'
```

기대 출력:

```json
{ "cpu": "12 vCPU", "memory": "80 GB", "disk": "300 GB" }
```

- [ ] **Step 6: DAG를 재개한다**

```bash
docker exec compose-airflow-scheduler-1 airflow dags unpause standard_score_pipeline
docker exec compose-airflow-scheduler-1 airflow dags unpause data_quality_audit
```

`autoStartConfiguration`이 `enabled`라 다음 job run 제출 시 Application이 자동으로 다시 뜬다. 명시적 `start-application`은 필요 없다.

---

### Task 2: 자원 프로파일 3종 도입

**Files:**
- Modify: `services/orchestration/dags/emr_serverless.py:60-90` (자원 상수 블록), `:126-152` (`conf_flags` 구성)
- Test: `services/orchestration/tests/test_emr_serverless_helper.py`

**Interfaces:**
- Consumes: Task 1의 `maximumCapacity` 12 vCPU / 80 GB / 300 GB
- Produces:
  - `SparkResourceProfile` — frozen dataclass. 필드는 전부 `str`이다: `driver_cores`, `driver_memory`, `driver_memory_overhead`, `driver_disk`, `executor_cores`, `executor_memory`, `executor_memory_overhead`, `executor_disk`, `executor_instances`. 메서드 `conf_flags(self) -> list[str]`.
  - `RESOURCE_PROFILES: dict[str, SparkResourceProfile]` — 키는 `"default"`, `"heavy"`, `"audit"`.
  - `submit_batch_jobs_command(task_id, entry_point_arguments, *, driver_env=None, outlets=None, profile="default")` — Task 4·5가 `profile="heavy"` / `profile="audit"`로 호출한다.

- [ ] **Step 1: 프로파일별 conf와 용량 적합성 테스트를 쓴다 (실패해야 한다)**

`services/orchestration/tests/test_emr_serverless_helper.py` 끝에 추가한다.

```python
# --- #508: job별 자원 프로파일 ---


def _params(profile: str) -> str:
    operator = _build_operator(
        task_id="run_thing", entry_point_arguments=["cmd"], profile=profile
    )
    return operator.job_driver["sparkSubmit"]["sparkSubmitParameters"]


def _gigabytes(value: str) -> int:
    """'8g' / '20G' 같은 Spark 메모리·디스크 표기를 GB 정수로 바꾼다."""
    assert value[-1] in "gG", value
    return int(value[:-1])


def test_default_profile_keeps_the_sizes_that_were_already_deployed():
    params = _params("default")

    assert "spark.driver.cores=1" in params
    assert "spark.driver.memory=2g" in params
    assert "spark.driver.memoryOverhead=6g" in params
    assert "spark.emr-serverless.driver.disk=20G" in params
    assert "spark.executor.cores=2" in params
    assert "spark.executor.memory=8g" in params
    assert "spark.executor.memoryOverhead=6g" in params
    assert "spark.emr-serverless.executor.disk=60G" in params
    assert "spark.executor.instances=2" in params


def test_heavy_profile_only_raises_the_executor_count():
    # run_sensor_processing 전용이다. driver는 줄이지 않는다 —
    # map_matching/candidates.py:109가 road_segment 약 17만 건을 driver로 collect해
    # broadcast payload를 만들기 때문에 driver도 Python 메모리를 실제로 쓴다.
    params = _params("heavy")

    assert "spark.executor.instances=4" in params
    assert "spark.driver.cores=1" in params
    assert "spark.driver.memory=2g" in params
    assert "spark.executor.memory=8g" in params


def test_audit_profile_enlarges_the_driver_and_shrinks_the_executors():
    # audit job은 executor를 거의 쓰지 않고(실측: audit_current의 executor 존재
    # 시간 7초) driver가 exit 137로 죽는다 — Great Expectations가
    # gold_audit_validation.py:112의 `SELECT * FROM {table}`로 997,332행을 driver의
    # pandas에 전량 적재한다. 1 vCPU는 EMR Serverless 허용 메모리 상한이 8 GB라
    # 그보다 크게 주려면 2 vCPU로 가야 한다.
    params = _params("audit")

    assert "spark.driver.cores=2" in params
    assert "spark.driver.memory=4g" in params
    assert "spark.driver.memoryOverhead=12g" in params
    assert "spark.executor.instances=1" in params


def test_every_profile_pins_dynamic_allocation_to_its_executor_instances():
    # 세 값이 executor.instances와 어긋나면 EMR Serverless가 여분 executor를 계속
    # 요청하다 ApplicationMaxCapacityExceededException을 반복하고, 실제 계산이
    # 성공해도 job run을 FAILED로 판정한다(#372).
    from emr_serverless import RESOURCE_PROFILES

    for name, profile in RESOURCE_PROFILES.items():
        params = _params(name)
        instances = profile.executor_instances
        assert f"spark.dynamicAllocation.minExecutors={instances}" in params, name
        assert f"spark.dynamicAllocation.maxExecutors={instances}" in params, name
        assert f"spark.dynamicAllocation.initialExecutors={instances}" in params, name


def test_every_profile_fits_within_the_application_maximum_capacity():
    # Application maximumCapacity는 12 vCPU / 80 GB / 300 GB다(#508 Task 1).
    # EMR Serverless worker의 메모리는 memory + memoryOverhead이고 이 값이
    # 과금·용량에 반영된다 — 실측 GB-h/vCPU-h 7.20~7.22가 이를 확인해 준다.
    from emr_serverless import RESOURCE_PROFILES

    for name, profile in RESOURCE_PROFILES.items():
        instances = int(profile.executor_instances)
        vcpu = int(profile.driver_cores) + int(profile.executor_cores) * instances
        memory = (
            _gigabytes(profile.driver_memory)
            + _gigabytes(profile.driver_memory_overhead)
            + (
                _gigabytes(profile.executor_memory)
                + _gigabytes(profile.executor_memory_overhead)
            )
            * instances
        )
        disk = _gigabytes(profile.driver_disk) + _gigabytes(profile.executor_disk) * instances
        assert vcpu <= 12, (name, vcpu)
        assert memory <= 80, (name, memory)
        assert disk <= 300, (name, disk)


def test_unknown_profile_name_fails_loudly():
    import pytest

    with pytest.raises(KeyError):
        _build_operator(
            task_id="run_thing", entry_point_arguments=["cmd"], profile="nope"
        )
```

- [ ] **Step 2: 테스트가 실패하는지 확인한다**

```bash
uv run --all-packages pytest services/orchestration/tests/test_emr_serverless_helper.py -q
```

기대: `TypeError: submit_batch_jobs_command() got an unexpected keyword argument 'profile'`로 새 테스트가 전부 FAIL. 기존 15건은 PASS.

- [ ] **Step 3: `SparkResourceProfile`과 프로파일 3종을 정의한다**

`emr_serverless.py`의 `_SPARK_DRIVER_CORES`부터 `_SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS`까지(현재 `:60-90`)를 아래로 교체한다. `from dataclasses import dataclass, replace`를 파일 상단 import에 추가한다.

```python
@dataclass(frozen=True)
class SparkResourceProfile:
    """job run 하나가 요청할 driver/executor 크기 (#508).

    EMR Serverless는 Application이 워커를 공유하지 않고 job run마다 전용
    driver·executor 세트를 새로 띄운다. 그래서 job의 성격에 따라 크기를 다르게
    요청할 수 있고, job run 개수가 그대로면 cold start 총량도 그대로다
    (프로비저닝 실측 84~91초는 워커 크기·Application 상태와 무관하게 일정하다).

    worker 하나의 실제 메모리는 `memory + memoryOverhead`이고 이 값이 과금과
    maximumCapacity 계산에 들어간다 — 경합 없는 job run의 실측
    GB-h/vCPU-h 7.20~7.22가 이를 확인해 준다.
    """

    driver_cores: str
    driver_memory: str
    driver_memory_overhead: str
    driver_disk: str
    executor_cores: str
    executor_memory: str
    executor_memory_overhead: str
    executor_disk: str
    executor_instances: str

    def conf_flags(self) -> list[str]:
        return [
            f"--conf spark.driver.cores={self.driver_cores}",
            f"--conf spark.driver.memory={self.driver_memory}",
            f"--conf spark.driver.memoryOverhead={self.driver_memory_overhead}",
            f"--conf spark.emr-serverless.driver.disk={self.driver_disk}",
            f"--conf spark.executor.cores={self.executor_cores}",
            f"--conf spark.executor.memory={self.executor_memory}",
            f"--conf spark.executor.memoryOverhead={self.executor_memory_overhead}",
            f"--conf spark.emr-serverless.executor.disk={self.executor_disk}",
            f"--conf spark.executor.instances={self.executor_instances}",
            # EMR Serverless는 dynamic allocation이 기본 켜져 있어 실제 목표
            # executor 수가 max(initialExecutors, minExecutors, executor.instances)로
            # 계산된다. 셋을 instances에서 파생시켜 값이 어긋날 수 없게 한다(#372).
            f"--conf spark.dynamicAllocation.minExecutors={self.executor_instances}",
            f"--conf spark.dynamicAllocation.maxExecutors={self.executor_instances}",
            f"--conf spark.dynamicAllocation.initialExecutors={self.executor_instances}",
        ]


# 기본 프로파일 = 지금까지 모든 job run이 쓰던 값이다. 합계는 5 vCPU / 36 GB /
# 140 GB다 — 이전 주석이 30 GB로 적었던 것은 driver의 memoryOverhead 6g를 빼먹은
# 오기였다(#508에서 과금 실측으로 확인).
#
# executor의 memoryOverhead 6g는 Map Matching(find_segment_candidates의
# mapInPandas)이 파티션마다 Python worker에서 road_segment broadcast(약 17만 건)로
# STRtree를 만드는 데 필요하다 — 기본값(~800MB)으로는 executor가 exit 137로 죽었다(#386).
# executor disk 60G는 standard_score_pipeline 실행 중 /tmp가 꽉 차 실패한
# 이력 때문이다(#443).
_DEFAULT_PROFILE = SparkResourceProfile(
    driver_cores="1",
    driver_memory="2g",
    driver_memory_overhead="6g",
    driver_disk="20G",
    executor_cores="2",
    executor_memory="8g",
    executor_memory_overhead="6g",
    executor_disk="60G",
    executor_instances="2",
)

# run_sensor_processing 전용. 합계 9 vCPU / 64 GB / 260 GB.
# driver는 줄이지 않는다 — map_matching/candidates.py:109가 road_segment를
# driver로 collect해 broadcast payload를 만들기 때문에 driver도 Python 메모리를 쓴다.
_HEAVY_PROFILE = replace(_DEFAULT_PROFILE, executor_instances="4")

# audit_* 전용. 합계 4 vCPU / 30 GB / 80 GB.
# audit은 executor를 거의 쓰지 않고 driver가 exit 137로 죽는다 — Great Expectations가
# gold_audit_validation.py:112의 `SELECT * FROM {table}`로 테이블 전량을 driver의
# pandas에 올린다. 1 vCPU의 허용 메모리 상한이 8 GB(= 지금 죽는 값)라 2 vCPU로 올린다.
_AUDIT_PROFILE = replace(
    _DEFAULT_PROFILE,
    driver_cores="2",
    driver_memory="4g",
    driver_memory_overhead="12g",
    executor_instances="1",
)

RESOURCE_PROFILES: dict[str, SparkResourceProfile] = {
    "default": _DEFAULT_PROFILE,
    "heavy": _HEAVY_PROFILE,
    "audit": _AUDIT_PROFILE,
}
```

- [ ] **Step 4: `submit_batch_jobs_command`가 프로파일을 받게 한다**

시그니처에 `profile: str = "default"`를 추가하고, `conf_flags` 리스트에서 자원 관련 항목을 `RESOURCE_PROFILES[profile].conf_flags()`로 교체한다. PYSPARK_PYTHON conf 4종과 `driver_env` 전개는 그대로 둔다.

```python
def submit_batch_jobs_command(
    task_id: str,
    entry_point_arguments: list[str],
    *,
    driver_env: dict[str, str] | None = None,
    outlets: list[Any] | None = None,
    profile: str = "default",
) -> EmrServerlessStartJobOperator:
    ...
    conf_flags = [
        f"--conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON={_PYSPARK_PYTHON_PATH}",
        f"--conf spark.executorEnv.PYSPARK_PYTHON={_PYSPARK_PYTHON_PATH}",
        f"--conf spark.pyspark.python={_PYSPARK_PYTHON_PATH}",
        f"--conf spark.pyspark.driver.python={_PYSPARK_PYTHON_PATH}",
        *RESOURCE_PROFILES[profile].conf_flags(),
        *(
            f"--conf spark.emr-serverless.driverEnv.{key}={value}"
            for key, value in (driver_env or {}).items()
        ),
    ]
```

docstring에 `profile` 설명 한 문단을 덧붙인다.

- [ ] **Step 5: 테스트가 통과하는지 확인한다**

```bash
uv run --all-packages pytest services/orchestration/tests/test_emr_serverless_helper.py -q
```

기대: 21건 PASS. 기존 `test_driver_and_executor_sizes_fit_within_application_max_capacity`와 `test_dynamic_allocation_is_capped_to_match_executor_instances`도 계속 통과해야 한다 — 기본 프로파일이 기존 값과 같기 때문이다. 두 테스트의 주석에 남은 "5 vCPU/30GB"와 "12 vCPU / 48 GB"는 오기이므로 각각 "5 vCPU / 36 GB", "12 vCPU / 80 GB"로 고친다.

- [ ] **Step 6: 커밋한다**

```bash
git add services/orchestration/dags/emr_serverless.py \
        services/orchestration/tests/test_emr_serverless_helper.py
git commit -m "perf: add per-job Spark resource profiles for EMR Serverless (#508)"
```

---

### Task 3: Airflow pool로 job run 제출을 직렬화

**Files:**
- Modify: `services/orchestration/dags/emr_serverless.py` (`submit_batch_jobs_command`의 operator 생성)
- Modify: `infra/compose/airflow.yaml:104-106` (`airflow-init`)
- Test: `services/orchestration/tests/test_emr_serverless_helper.py`

**Interfaces:**
- Consumes: Task 2의 `submit_batch_jobs_command`
- Produces: 모듈 상수 `EMR_SERVERLESS_POOL = "emr_serverless"`. Task 4·5의 DAG 테스트가 이 이름을 import해 검증한다.

- [ ] **Step 1: pool 지정 테스트를 쓴다 (실패해야 한다)**

`test_emr_serverless_helper.py`에 추가한다.

```python
def test_job_run_submission_is_serialised_through_a_single_slot_pool():
    # 두 DAG(standard_score_pipeline, data_quality_audit)가 같은 Application을
    # 공유하는데, 겹치면 executor를 못 받고 ApplicationMaxCapacityExceededException을
    # 반복하다 job run이 FAILED가 된다(#508: 06:00 run이 10분 12초 동안 굶었다).
    # EmrServerlessStartJobOperator는 deferrable 설정이 없어 기본값 False로 동작하므로
    # job run이 끝날 때까지 pool slot을 점유한다 — pool 하나로 DAG 간 직렬화가 성립한다.
    from emr_serverless import EMR_SERVERLESS_POOL

    operator = _build_operator(task_id="run_thing", entry_point_arguments=["cmd"])

    assert EMR_SERVERLESS_POOL == "emr_serverless"
    assert operator.pool == EMR_SERVERLESS_POOL
    assert operator.deferrable is False
```

- [ ] **Step 2: 테스트가 실패하는지 확인한다**

```bash
uv run --all-packages pytest services/orchestration/tests/test_emr_serverless_helper.py -k serialised -q
```

기대: `ImportError: cannot import name 'EMR_SERVERLESS_POOL'`로 FAIL.

- [ ] **Step 3: 상수를 정의하고 operator에 pool을 지정한다**

`emr_serverless.py`의 `_PYSPARK_PYTHON_PATH` 근처에 상수를 두고, `EmrServerlessStartJobOperator(...)` 호출에 `pool=EMR_SERVERLESS_POOL`을 추가한다.

```python
# 두 DAG가 같은 Application을 공유하므로 job run 제출을 pool로 직렬화한다(#508).
# pool은 Airflow DB 객체라 코드로 선언할 수 없어 infra/compose/airflow.yaml의
# airflow-init이 만든다 — 나머지 컨테이너가 그 서비스에 의존하므로 DAG가 돌기 전에
# 반드시 존재한다.
EMR_SERVERLESS_POOL = "emr_serverless"
```

- [ ] **Step 4: `airflow-init`이 pool을 만들게 한다**

`infra/compose/airflow.yaml`의 `airflow-init` 블록을 교체한다.

```yaml
  airflow-init:
    <<: *airflow-common
    # db migrate 뒤에 EMR Serverless 제출 직렬화용 pool을 만든다(#508). 나머지 세
    # 컨테이너가 이 서비스에 service_completed_successfully로 의존하므로 DAG가
    # 돌기 전에 pool이 반드시 존재한다. `pools set`은 있으면 갱신하는 멱등 명령이다.
    entrypoint: ["/bin/bash", "-c"]
    command: >
      airflow db migrate &&
      airflow pools set emr_serverless 1
      'EMR Serverless job run submission is serialised to one at a time (#508)'
```

- [ ] **Step 5: 테스트가 통과하는지 확인한다**

```bash
uv run --all-packages pytest services/orchestration/tests/test_emr_serverless_helper.py -q
```

기대: 22건 PASS.

- [ ] **Step 6: 로컬 compose로 pool 생성을 확인한다**

```bash
docker compose -f infra/compose/airflow.yaml --env-file infra/compose/.env up airflow-init
docker compose -f infra/compose/airflow.yaml --env-file infra/compose/.env \
  run --rm airflow-init airflow pools list
```

기대: 목록에 `emr_serverless | 1 | EMR Serverless job run submission is serialised...`가 보인다. `.env`가 없으면 이 단계는 건너뛰고 Task 7의 EC2 배포 검증에서 확인한다.

- [ ] **Step 7: 커밋한다**

```bash
git add services/orchestration/dags/emr_serverless.py \
        services/orchestration/tests/test_emr_serverless_helper.py \
        infra/compose/airflow.yaml
git commit -m "perf: serialise EMR Serverless job runs through a single-slot pool (#508)"
```

---

### Task 4: standard_score_pipeline에 적용

**Files:**
- Modify: `services/orchestration/dags/standard_score_pipeline.py:218-234` (DAG 인자), `:250-270` (`run_sensor_processing`)
- Test: `services/orchestration/tests/test_standard_score_pipeline_dag.py`

**Interfaces:**
- Consumes: Task 2의 `profile` 인자, Task 3의 `EMR_SERVERLESS_POOL`
- Produces: 없음 (DAG 설정 변경)

- [ ] **Step 1: 테스트를 쓴다 (실패해야 한다)**

`test_standard_score_pipeline_dag.py` 끝에 추가한다. 이 파일에는 이미 `_load_dag_module()`(`:46`)이 있으므로 그대로 쓴다.

```python
# --- #508: 동시 제출 직렬화와 job별 자원 프로파일 ---


def test_only_one_dag_run_is_active_at_a_time():
    # 시간당 스케줄인데 DAG run이 1시간을 넘기면 다음 run이 겹쳐 job run 2건이
    # 동시에 뜬다 — 베이스라인에 1:09:46, 1:11:47 두 건이 있었다.
    module = _load_dag_module()

    assert module.dag.max_active_runs == 1


def test_every_emr_task_uses_the_serialising_pool():
    from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
    from emr_serverless import EMR_SERVERLESS_POOL

    module = _load_dag_module()

    emr_tasks = [
        task
        for task in module.dag.tasks
        if isinstance(task, EmrServerlessStartJobOperator)
    ]
    assert len(emr_tasks) == 3
    for task in emr_tasks:
        assert task.pool == EMR_SERVERLESS_POOL, task.task_id


def _spark_params(task) -> str:
    return task.job_driver["sparkSubmit"]["sparkSubmitParameters"]


def test_sensor_processing_uses_the_heavy_profile_and_the_others_default():
    # run_sensor_processing이 가장 무겁다(실측 0.783 vCPU-h, 2,953 tasks). 반면
    # run_hourly_scoring은 0.073 vCPU-h로 10배 차이가 난다.
    module = _load_dag_module()

    sensor = module.dag.get_task("sensor_processing.run_sensor_processing")
    hourly = module.dag.get_task("hourly_scoring.run_hourly_scoring")
    standard = module.dag.get_task("standard_score.run_standard_score")

    assert "spark.executor.instances=4" in _spark_params(sensor)
    assert "spark.executor.instances=2" in _spark_params(hourly)
    assert "spark.executor.instances=2" in _spark_params(standard)
```

- [ ] **Step 2: 테스트가 실패하는지 확인한다**

```bash
uv run --all-packages pytest services/orchestration/tests/test_standard_score_pipeline_dag.py -q
```

기대: `max_active_runs`는 16, `pool`은 `default_pool`, `instances=4`는 없어 세 건 FAIL.

- [ ] **Step 3: DAG를 수정한다**

`with DAG(...)` 인자에 추가한다.

```python
    catchup=False,
    # 이 DAG의 job run이 다음 시각 run과 겹치면 executor 확보 경합이 생긴다(#508).
    # pool이 이미 제출을 직렬화하지만, DAG run 자체가 쌓이는 것은 별개 문제다.
    max_active_runs=1,
```

`run_sensor_processing` 호출에 프로파일을 지정한다.

```python
        run_sensor_processing = submit_batch_jobs_command(
            task_id="run_sensor_processing",
            profile="heavy",
            entry_point_arguments=[
                ...
            ],
        )
```

- [ ] **Step 4: 테스트가 통과하는지 확인한다**

```bash
uv run --all-packages pytest services/orchestration/tests/test_standard_score_pipeline_dag.py -q
```

기대: 전부 PASS.

- [ ] **Step 5: 커밋한다**

```bash
git add services/orchestration/dags/standard_score_pipeline.py \
        services/orchestration/tests/test_standard_score_pipeline_dag.py
git commit -m "perf: cap standard_score_pipeline runs and size sensor processing (#508)"
```

---

### Task 5: data_quality_audit에 적용

**Files:**
- Modify: `services/orchestration/dags/data_quality_audit.py:72` (스케줄), `:85-107` (task와 의존)
- Test: `services/orchestration/tests/test_data_quality_audit_dag.py:48-54`, `:67-74` (기존 테스트 수정), 새 테스트 추가

**Interfaces:**
- Consumes: Task 2의 `profile` 인자, Task 3의 `EMR_SERVERLESS_POOL`
- Produces: 없음 (DAG 설정 변경)

- [ ] **Step 1: 기존 테스트 두 건을 새 동작에 맞게 고친다**

`test_dag_parses_with_expected_schedule`의 단언을 바꾼다.

```python
def test_dag_parses_with_expected_schedule():
    module = _load_dag_module()

    assert module.dag.dag_id == "data_quality_audit"
    # 뉴욕 04:40 EDT. Bronze 입력량이 03:00 UTC 1.7 GiB에서 07:00 UTC 257 MiB로
    # 단조 감소하고(뉴욕 23시 → 03시), 08:00 hourly run이 끝난 뒤 09:00 run 전
    # 틈에 들어간다(#508).
    assert module.dag.schedule == "40 8 * * *"
    assert module.dag.catchup is False
```

`test_audit_tasks_are_independent_of_each_other`를 직렬 의존 검증으로 교체한다. 이름도 바꾼다.

```python
def test_audit_tasks_run_one_after_another():
    # 병렬로 두면 이 DAG 혼자서 동시 job run 2건을 만들어 용량을 초과한다 —
    # 두 job의 합은 60 GB인데 executor 확보에는 그보다 더 필요하다(#508).
    module = _load_dag_module()

    standard_task = module.dag.get_task("audit_standard_segment_comfort_score")
    current_task = module.dag.get_task("audit_current_segment_comfort_score")

    assert standard_task.upstream_task_ids == set()
    assert current_task.upstream_task_ids == {"audit_standard_segment_comfort_score"}
```

`test_report_audit_counts_runs_after_both_audits`는 그대로 둔다 — `report_audit_counts`의 upstream은 여전히 두 task 모두다.

- [ ] **Step 2: 새 테스트를 추가한다**

```python
def test_audit_tasks_use_the_serialising_pool_and_the_audit_profile():
    # audit job은 executor를 거의 쓰지 않고 driver가 exit 137로 죽는다(08-25 03시
    # 2회 연속 실패). driver를 2 vCPU / 16 GB로 올리고 executor는 1대로 줄인다(#508).
    from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
    from emr_serverless import EMR_SERVERLESS_POOL

    module = _load_dag_module()

    emr_tasks = [
        task
        for task in module.dag.tasks
        if isinstance(task, EmrServerlessStartJobOperator)
    ]
    assert len(emr_tasks) == 2
    for task in emr_tasks:
        params = task.job_driver["sparkSubmit"]["sparkSubmitParameters"]
        assert task.pool == EMR_SERVERLESS_POOL, task.task_id
        assert "spark.driver.cores=2" in params, task.task_id
        assert "spark.driver.memory=4g" in params, task.task_id
        assert "spark.driver.memoryOverhead=12g" in params, task.task_id
        assert "spark.executor.instances=1" in params, task.task_id
```

- [ ] **Step 3: 테스트가 실패하는지 확인한다**

```bash
uv run --all-packages pytest services/orchestration/tests/test_data_quality_audit_dag.py -q
```

기대: 스케줄·의존·프로파일 세 건 FAIL.

- [ ] **Step 4: DAG를 수정한다**

스케줄을 바꾸고, 두 호출에 `profile="audit"`을 넣고, 의존을 직렬로 바꾼다.

```python
    schedule="40 8 * * *",
```

```python
    audit_standard_segment_comfort_score = submit_batch_jobs_command(
        task_id="audit_standard_segment_comfort_score",
        profile="audit",
        entry_point_arguments=[
            "audit-gold",
            "--table=standard_segment_comfort_score",
        ],
        driver_env=_audit_gold_driver_env(),
    )
    audit_current_segment_comfort_score = submit_batch_jobs_command(
        task_id="audit_current_segment_comfort_score",
        profile="audit",
        entry_point_arguments=[
            "audit-gold",
            "--table=current_segment_comfort_score",
        ],
        driver_env=_audit_gold_driver_env(),
    )
```

파일 끝의 의존 선언을 교체한다.

```python
    # 두 task를 병렬로 두면 이 DAG 혼자 동시 job run 2건을 만들어 용량을
    # 초과한다(#508). 직렬로 잇는다 — 감사 결과는 서로 독립이라 순서는 상관없다.
    (
        audit_standard_segment_comfort_score
        >> audit_current_segment_comfort_score
        >> report_audit_counts
    )
```

모듈 docstring의 "두 task는 서로 독립이라(의존관계 없음) 병렬로 실행된다" 서술과 `:83-84` 주석을 새 동작에 맞게 고친다.

- [ ] **Step 5: 테스트가 통과하는지 확인한다**

```bash
uv run --all-packages pytest services/orchestration/tests/test_data_quality_audit_dag.py -q
```

기대: 전부 PASS.

- [ ] **Step 6: 커밋한다**

```bash
git add services/orchestration/dags/data_quality_audit.py \
        services/orchestration/tests/test_data_quality_audit_dag.py
git commit -m "perf: serialise audit tasks and move the audit schedule off peak (#508)"
```

---

### Task 6: 의사결정 문서와 컨텍스트 갱신

**Files:**
- Modify: `docs/decisions/04-emr-serverless-capacity.md`
- Modify: `context/services.md` (EMR 관련 서술이 프로파일·pool과 어긋나면)
- Test: 없음 (문서)

**Interfaces:**
- Consumes: Task 1~5의 결과
- Produces: 없음

- [ ] **Step 1: 04번 의사결정 문서에 3단계를 추가한다**

기존 "최종 결정 — 8개 conf 고정" 표 아래에 절을 하나 더한다. 문서의 "재검토 조건"이 "프로덕션 데이터 규모에서의 최적 worker 사이징은 별도 과제로 남겼습니다"라고 적었는데 #508이 그 과제였으므로, 그 문단을 해결됨으로 갱신하고 #508과 설계 문서를 링크한다.

담을 내용은 셋이다. (1) `maximumCapacity`는 상한일 뿐 과금 대상이 아니므로 1단계에서 "비용 증가로 기각"한 판단이 부정확했다는 정정. (2) 합계가 30 GB가 아니라 36 GB였다는 정정과 그 근거(실측 GB-h/vCPU-h 7.20~7.22). (3) job마다 필요한 자원이 다르다는 것을 프로파일로 표현했다는 결정.

- [ ] **Step 2: 컨텍스트 문서를 확인한다**

```bash
grep -rn "EMR" context/ | grep -iv "^context/open-questions" | head -20
```

EMR Serverless의 동시성이나 자원 배분을 서술한 곳이 있으면 갱신한다. 없으면 넘어간다 — 컨텍스트 문서는 실행 가능한 계약을 복제하지 않는다(AGENTS.md).

- [ ] **Step 3: 전체 검증을 돌린다**

```bash
uv run --all-packages ruff check .
uv run --all-packages pytest
```

기대: ruff 통과, 테스트 전부 PASS. `services/batch-jobs`의 Spark 테스트에는 JDK 21이 필요하다 — `JAVA_HOME=/opt/homebrew/opt/openjdk@21 uv run --all-packages pytest`.

- [ ] **Step 4: 커밋한다**

```bash
git add docs/decisions/04-emr-serverless-capacity.md context/
git commit -m "docs: record the capacity and per-job sizing decision (#508)"
```

---

### Task 7: 배포와 실측 검증

**Files:** 없음 — 배포·관측 작업이다.

**Interfaces:**
- Consumes: Task 1~6 전부
- Produces: 완료 조건 3~5의 증거

- [ ] **Step 1: PR을 만들고 `develop`에 머지한다**

```bash
git push -u origin perf/508-size-emr-serverless-capacity-per-job
gh pr create --base develop --fill
```

머지되면 `deploy-orchestration.yml`이 EC2에 compose 스택을 배포한다. `airflow-init`이 다시 돌면서 pool을 만든다.

- [ ] **Step 2: pool이 만들어졌는지 확인한다**

```bash
ssh -i "<키 경로>" ec2-user@43.203.192.129 \
  "docker exec compose-airflow-scheduler-1 airflow pools list"
```

기대: `emr_serverless | 1 | ...` 행이 보인다. 없으면 `airflow-init` 로그를 확인한다 — pool이 없으면 EMR task가 전부 실패한다.

- [ ] **Step 3: hourly run 3회를 관측한다**

```bash
ssh -i "<키 경로>" ec2-user@43.203.192.129 '
APP=00g85ljahc0svj2p; REGION=ap-northeast-2
aws emr-serverless list-job-runs --application-id $APP --region $REGION --max-results 20 \
  --query "sort_by(jobRuns,&createdAt)[-12:].[createdAt,name,state]" --output text'
```

기대: 모든 job run이 `SUCCESS`이고, 같은 시각에 두 건이 겹치지 않는다.

- [ ] **Step 4: executor 부재 시간이 사라졌는지 확인한다**

각 `run_sensor_processing` job run에 대해 실행한다.

```bash
aws emr-serverless get-job-run --application-id $APP --job-run-id <id> --region $REGION \
  --query 'jobRun.{started:startedAt,ended:endedAt,vcpu:billedResourceUtilization.vCPUHour}'
```

`(vCPUHour × 3600 − 실행초) ÷ (executor 코어 수)`가 executor 존재 시간이다. `heavy` 프로파일은 executor 코어가 8개다. **기대: 실행초 − executor 존재 시간 ≤ 30초.** 경합 없던 07:00 run의 실측 15초가 기준선이다.

- [ ] **Step 5: driver 로그에 용량 예외가 없는지 확인한다**

```bash
BUCKET=s3://de4-observability-473551908409-ap-northeast-2-an/emr-serverless/logs
aws s3 cp "$BUCKET/applications/$APP/jobs/<id>/SPARK_DRIVER/stderr.gz" - | \
  gunzip | grep -c ApplicationMaxCapacityExceededException
```

기대: `0`.

- [ ] **Step 6: audit DAG가 연속 3회 성공하는지 확인한다**

배포 후 3일간 `40 8 * * *` 실행 결과를 본다. 급히 확인하려면 수동 트리거한다.

```bash
ssh -i "<키 경로>" ec2-user@43.203.192.129 \
  "docker exec compose-airflow-scheduler-1 airflow dags trigger data_quality_audit"
```

기대: 두 audit task 모두 success. `ExitCode: 137`이 다시 나오면 driver 메모리가 여전히 부족한 것이다 — 그 경우 `_AUDIT_PROFILE`의 `driver_memory_overhead`를 늘리기 전에 **설계 문서의 "남긴 과제" 첫 항목(전량 적재 제거)을 별도 이슈로 올린다.** 메모리를 계속 키우는 것은 해결이 아니다.

- [ ] **Step 7: pipeline-perf를 재수집해 베이스라인과 비교한다**

```bash
uv run --package pipeline-perf pipeline-perf collect --help
```

`tools/pipeline-perf/README.md`의 절차를 따라 수집하고 `render`로 리포트를 만든다. 확인할 값은 셋이다. job run당 executor 부재 시간(Step 4의 기준), 슬롯 점유율(이제 경합이 없으므로 신뢰할 수 있는 값이다 — executor 4대가 적정인지 판단할 근거가 된다), `run_sensor_processing`의 vCPU-h(executor 2대 → 4대로 늘린 비용 영향).

---

## 완료 후 남는 것

설계 문서의 "남긴 과제"를 이슈로 올린다. `gold_audit_validation`의 전량 적재 제거가 가장 급하다 — 이번 driver 증량은 임시방편이고 테이블이 커지면 다시 죽는다. 나머지 셋(executor 개수 재판단, `postgres_merge` 등 driver 단독 구간, Application IaC화)은 Task 7 Step 7의 측정 결과를 근거로 우선순위를 정한다.
