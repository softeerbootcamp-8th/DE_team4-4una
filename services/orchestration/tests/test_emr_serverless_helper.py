"""Tests for dags/emr_serverless.py's shared EMR Serverless submission helper (#292).

standard_score_pipeline과 data_quality_audit(#295)이 이 헬퍼로 batch-jobs
CLI 커맨드를 EmrServerlessStartJobOperator로 감싼다. Application ID·실행
역할 ARN·entry point는 Airflow Variable 템플릿으로 고정돼 있는지, entry
point arguments와 driver_env가 job_driver에 정확히 반영되는지 확인한다.
"""

from __future__ import annotations

import datetime
import sys
from pathlib import Path

from airflow.sdk import DAG

# 실제 Airflow는 dags_folder 전체를 sys.path에 등록하지만(airflow.dag_processing.dagbag),
# 이 테스트는 그 동작을 흉내내야 emr_serverless를 최상위 모듈로 임포트할 수 있다
# (test_current_score_pipeline_dag.py와 동일한 패턴).
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from emr_serverless import submit_batch_jobs_command


def _build_operator(**kwargs):
    with DAG(
        dag_id="test_emr_serverless_helper",
        start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
    ):
        return submit_batch_jobs_command(**kwargs)


def test_uses_airflow_variable_templates_for_application_id_and_execution_role():
    operator = _build_operator(task_id="run_thing", entry_point_arguments=["cmd"])

    assert operator.application_id == "{{ var.value.EMR_SERVERLESS_APPLICATION_ID }}"
    assert (
        operator.execution_role_arn
        == "{{ var.value.EMR_SERVERLESS_EXECUTION_ROLE_ARN }}"
    )


def test_entry_point_uses_the_configured_variable_template():
    operator = _build_operator(task_id="run_thing", entry_point_arguments=["cmd"])

    assert (
        operator.job_driver["sparkSubmit"]["entryPoint"]
        == "{{ var.value.BATCH_JOBS_EMR_ENTRY_POINT }}"
    )


def test_entry_point_arguments_are_passed_through_unchanged():
    args = ["cleanse-sensor-events", "--target-hour={{ data_interval_start.isoformat() }}"]

    operator = _build_operator(task_id="run_thing", entry_point_arguments=args)

    assert operator.job_driver["sparkSubmit"]["entryPointArguments"] == args


def test_without_driver_env_only_pyspark_python_confs_are_set():
    operator = _build_operator(task_id="run_thing", entry_point_arguments=["cmd"])

    params = operator.job_driver["sparkSubmit"]["sparkSubmitParameters"]
    # EMR Serverless는 sparkSubmitParameters를 쉘 없이 문자열 그대로 받으므로
    # 따옴표를 감싸면 그 문자가 값의 일부가 돼 버린다(#368) — 부분 문자열 검사가
    # 아니라 정확히 일치하는지, 따옴표가 섞이지 않았는지 함께 검증한다.
    assert "spark.emr-serverless.driverEnv.PYSPARK_PYTHON=/usr/bin/python3.12" in params
    assert "spark.executorEnv.PYSPARK_PYTHON=/usr/bin/python3.12" in params
    # driverEnv/executorEnv는 컨테이너 환경변수만 세팅할 뿐, entryPoint 스크립트를
    # 실제로 어떤 인터프리터로 실행할지는 EMR Serverless가 반영하지 않는 것으로
    # 확인됐다(#368 재발 조사) — Spark가 직접 읽는 spark.pyspark.python/
    # spark.pyspark.driver.python conf를 병행한다.
    assert "spark.pyspark.python=/usr/bin/python3.12" in params
    assert "spark.pyspark.driver.python=/usr/bin/python3.12" in params
    assert '"' not in params
    assert "POSTGRES_HOST" not in params


def test_driver_env_is_rendered_as_spark_submit_parameters():
    operator = _build_operator(
        task_id="run_thing",
        entry_point_arguments=["cmd"],
        driver_env={"POSTGRES_HOST": "db.example.com"},
    )

    params = operator.job_driver["sparkSubmit"]["sparkSubmitParameters"]
    assert "spark.emr-serverless.driverEnv.POSTGRES_HOST=db.example.com" in params
    assert '"' not in params


def test_pyspark_python_confs_are_always_set_regardless_of_driver_env():
    operator = _build_operator(
        task_id="run_thing",
        entry_point_arguments=["cmd"],
        driver_env={"POSTGRES_HOST": "db.example.com"},
    )

    params = operator.job_driver["sparkSubmit"]["sparkSubmitParameters"]
    assert "spark.emr-serverless.driverEnv.PYSPARK_PYTHON=/usr/bin/python3.12" in params
    assert "spark.executorEnv.PYSPARK_PYTHON=/usr/bin/python3.12" in params
    assert "spark.pyspark.python=/usr/bin/python3.12" in params
    assert "spark.pyspark.driver.python=/usr/bin/python3.12" in params
    assert '"' not in params


def test_driver_and_executor_sizes_fit_within_application_max_capacity():
    # Application의 maximumCapacity는 12 vCPU / 80 GB / 300 GB disk이다(#372,
    # #386, #471, #508에서 현재 값으로 상향). Spark 기본값(driver 4 vCPU/8G, executor
    # 2 vCPU/8G)은 driver 혼자 vCPU 예산을 전부 써버려 executor가 하나도 못 뜬다 —
    # 실제 Job Run에서 ApplicationMaxCapacityExceededException으로 재현됨.
    # driver를 작게 고정해 executor가 안정적으로 뜨도록 상한을 코드에 못박는다.
    # memoryOverhead=6g는 Map Matching mapInPandas의 Python worker(STRtree)
    # 메모리용이다(#386) — driver(1c/2g) + executor 2개(각 2c/8g+6g=14g) =
    # 5 vCPU/30GB로 현재 한도 안에 여유 있게 들어간다.
    operator = _build_operator(task_id="run_thing", entry_point_arguments=["cmd"])

    params = operator.job_driver["sparkSubmit"]["sparkSubmitParameters"]
    assert "spark.driver.cores=1" in params
    assert "spark.driver.memory=2g" in params
    assert "spark.executor.cores=2" in params
    assert "spark.executor.memory=8g" in params
    assert "spark.executor.memoryOverhead=6g" in params
    assert "spark.executor.instances=2" in params


def test_dynamic_allocation_is_capped_to_match_executor_instances():
    # spark.executor.instances만으로는 부족하다 — EMR Serverless는 dynamic
    # allocation이 기본 켜져 있어서 실제 목표 executor 수를
    # max(dynamicAllocation.initialExecutors, minExecutors, executor.instances)로
    # 계산한다. EMR 기본값이 executor.instances보다 커서 결국 여분 executor를
    # 계속 요청하다 ApplicationMaxCapacityExceededException을 반복적으로 만나고,
    # 그 이력만으로 EMR Serverless가 실제로는 성공한 Job Run을 FAILED로 판정한
    # 것이 실제 Job Run 재현으로 확인됐다(#372 재발 조사). min/max/initial을
    # 전부 executor.instances(#471에서 2로 상향)와 맞춰 애초에 추가 요청이
    # 발생하지 않게 한다. #508에서 이 파생을 SparkResourceProfile.conf_flags에
    # 넣어 세 값이 어긋나는 것 자체가 불가능해졌다.
    operator = _build_operator(task_id="run_thing", entry_point_arguments=["cmd"])

    params = operator.job_driver["sparkSubmit"]["sparkSubmitParameters"]
    assert "spark.dynamicAllocation.minExecutors=2" in params
    assert "spark.dynamicAllocation.maxExecutors=2" in params
    assert "spark.dynamicAllocation.initialExecutors=2" in params


def test_all_job_runs_persist_logs_to_s3_monitoring_configuration():
    # 기본값은 실재하는 관측 버킷을 가리켜야 한다 — 이전 기본값
    # s3://de4-emr-serverless-logs/는 실제로 만들어진 적이 없다(#409, EC2에서
    # NoSuchBucket 확인). 또 compose가 .env에 없는 변수를 빈 문자열로 채우므로
    # var.value.get의 default만으로는 부족해 `or`로 한 번 더 막는다.
    operator = _build_operator(task_id="run_thing", entry_point_arguments=["cmd"])

    log_uri = operator.configuration_overrides["monitoringConfiguration"][
        "s3MonitoringConfiguration"
    ]["logUri"]
    default = "s3://de4-observability-473551908409-ap-northeast-2-an/emr-serverless/logs/"
    assert log_uri == (
        f"{{{{ var.value.get('EMR_SERVERLESS_LOG_S3_URI', '{default}') or '{default}' }}}}"
    )


# --- #432: 파이프라인 종료 후 Application을 명시적으로 stop시키는 task들 ---


class _FakePaginator:
    def __init__(self, pages, recorder):
        self._pages = pages
        self._recorder = recorder

    def paginate(self, **kwargs):
        self._recorder.append(kwargs)
        return self._pages


class _FakeConn:
    def __init__(self, pages, recorder):
        self._pages = pages
        self._recorder = recorder

    def get_paginator(self, operation_name):
        assert operation_name == "list_job_runs"
        return _FakePaginator(self._pages, self._recorder)


def _patch_hook(monkeypatch, pages):
    """emr_serverless_has_no_running_jobs가 함수 안에서 지연 import하는
    EmrServerlessHook을, list_job_runs 페이지를 미리 정해둔 가짜로 바꾼다."""
    import airflow.providers.amazon.aws.hooks.emr as emr_hooks

    recorder: list[dict] = []

    class _FakeHook:
        JOB_INTERMEDIATE_STATES = emr_hooks.EmrServerlessHook.JOB_INTERMEDIATE_STATES

        def __init__(self, *args, **kwargs):
            self.conn = _FakeConn(pages, recorder)

    monkeypatch.setattr(emr_hooks, "EmrServerlessHook", _FakeHook)
    return recorder


def test_idle_check_is_true_when_no_job_run_is_in_flight(monkeypatch):
    from emr_serverless import emr_serverless_has_no_running_jobs

    _patch_hook(monkeypatch, pages=[{"jobRuns": []}])

    assert emr_serverless_has_no_running_jobs("00abc") is True


def test_idle_check_is_false_while_another_dags_job_run_is_in_flight(monkeypatch):
    # data_quality_audit(daily 03:00 UTC)이 같은 Application을 쓰므로 hourly
    # 파이프라인이 끝난 시점에도 남의 Job Run이 돌고 있을 수 있다 — 이때 stop을
    # 걸면 ValidationException으로 실패하므로 건너뛰어야 한다(#432).
    from emr_serverless import emr_serverless_has_no_running_jobs

    _patch_hook(monkeypatch, pages=[{"jobRuns": [{"id": "job-from-audit"}]}])

    assert emr_serverless_has_no_running_jobs("00abc") is False


def test_idle_check_only_counts_job_runs_in_non_terminal_states(monkeypatch):
    from emr_serverless import emr_serverless_has_no_running_jobs

    recorder = _patch_hook(monkeypatch, pages=[{"jobRuns": []}])

    emr_serverless_has_no_running_jobs("00abc")

    assert recorder[0]["applicationId"] == "00abc"
    assert set(recorder[0]["states"]) == {"PENDING", "RUNNING", "SCHEDULED", "SUBMITTED"}


def test_idle_check_task_resolves_the_application_id_from_the_shared_variable():
    from airflow.providers.standard.operators.python import ShortCircuitOperator
    from emr_serverless import check_emr_serverless_is_idle

    with DAG(
        dag_id="test_emr_serverless_helper_stop",
        start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
    ):
        operator = check_emr_serverless_is_idle(task_id="check_emr_serverless_idle")

    assert isinstance(operator, ShortCircuitOperator)
    # op_kwargs는 템플릿 필드라 실행 시점에 Variable이 렌더링된다.
    assert operator.op_kwargs == {
        "application_id": "{{ var.value.EMR_SERVERLESS_APPLICATION_ID }}"
    }
    assert "op_kwargs" in ShortCircuitOperator.template_fields


def test_stop_task_targets_the_shared_application_without_cancelling_other_jobs():
    from airflow.providers.amazon.aws.operators.emr import (
        EmrServerlessStopApplicationOperator,
    )
    from emr_serverless import stop_emr_serverless_application

    with DAG(
        dag_id="test_emr_serverless_helper_stop_op",
        start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
    ):
        operator = stop_emr_serverless_application(
            task_id="stop_emr_serverless_application"
        )

    assert isinstance(operator, EmrServerlessStopApplicationOperator)
    assert operator.application_id == "{{ var.value.EMR_SERVERLESS_APPLICATION_ID }}"
    # force_stop=True는 다른 DAG의 Job Run까지 취소해버린다 — 반드시 False여야 한다.
    assert operator.force_stop is False


def test_stop_task_waits_for_the_stopped_state_within_a_bounded_window():
    # provider 기본값(60초 × 25회 = 25분)은 hourly DAG가 붙들고 있기엔 너무 길다.
    from emr_serverless import stop_emr_serverless_application

    with DAG(
        dag_id="test_emr_serverless_helper_stop_waiter",
        start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
    ):
        operator = stop_emr_serverless_application(
            task_id="stop_emr_serverless_application"
        )

    assert operator.wait_for_completion is True
    assert operator.waiter_delay == 15
    assert operator.waiter_max_attempts == 20


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
    # Application maximumCapacity는 12 vCPU / 80 GB / 300 GB다(#508).
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
        disk = (
            _gigabytes(profile.driver_disk) + _gigabytes(profile.executor_disk) * instances
        )
        assert vcpu <= 12, (name, vcpu)
        assert memory <= 80, (name, memory)
        assert disk <= 300, (name, disk)


def test_unknown_profile_name_fails_loudly():
    import pytest

    with pytest.raises(KeyError):
        _build_operator(
            task_id="run_thing", entry_point_arguments=["cmd"], profile="nope"
        )
