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
    # Application의 maximumCapacity가 8 vCPU / 32 GB로 고정돼 있다(#372, #386).
    # Spark 기본값(driver 4 vCPU/8G, executor 2 vCPU/8G)은 driver 혼자 vCPU
    # 예산을 전부 써버려 executor가 하나도 못 뜬다 — 실제 Job Run에서
    # ApplicationMaxCapacityExceededException으로 재현됨. driver를 작게 고정해
    # executor 1개(기본 크기 유지)가 항상 같이 들어가도록 상한을 코드에 못박는다.
    # memoryOverhead=6g는 Map Matching mapInPandas의 Python worker(STRtree)
    # 메모리용이다(#386) — driver(1c/2g) + executor(2c/8g+6g=14g) = 3 vCPU/16GB로
    # 8 vCPU/32GB 한도 안에 여유 있게 들어간다.
    operator = _build_operator(task_id="run_thing", entry_point_arguments=["cmd"])

    params = operator.job_driver["sparkSubmit"]["sparkSubmitParameters"]
    assert "spark.driver.cores=1" in params
    assert "spark.driver.memory=2g" in params
    assert "spark.executor.cores=2" in params
    assert "spark.executor.memory=8g" in params
    assert "spark.executor.memoryOverhead=6g" in params
    assert "spark.executor.instances=1" in params


def test_dynamic_allocation_is_capped_to_match_executor_instances():
    # spark.executor.instances만으로는 부족하다 — EMR Serverless는 dynamic
    # allocation이 기본 켜져 있어서 실제 목표 executor 수를
    # max(dynamicAllocation.initialExecutors, minExecutors, executor.instances)로
    # 계산한다. EMR 기본값이 1보다 커서 결국 여분 executor를 계속 요청하다
    # ApplicationMaxCapacityExceededException을 반복적으로 만나고, 그 이력만으로
    # EMR Serverless가 실제로는 성공한 Job Run을 FAILED로 판정한 것이 실제 Job
    # Run 재현으로 확인됐다(#372 재발 조사). min/max/initial을 전부 1로 못박아
    # 애초에 추가 요청이 발생하지 않게 한다.
    operator = _build_operator(task_id="run_thing", entry_point_arguments=["cmd"])

    params = operator.job_driver["sparkSubmit"]["sparkSubmitParameters"]
    assert "spark.dynamicAllocation.minExecutors=1" in params
    assert "spark.dynamicAllocation.maxExecutors=1" in params
    assert "spark.dynamicAllocation.initialExecutors=1" in params
