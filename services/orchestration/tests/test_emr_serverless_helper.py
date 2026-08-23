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
    assert 'spark.emr-serverless.driverEnv.PYSPARK_PYTHON="/usr/bin/python3.12"' in params
    assert 'spark.executorEnv.PYSPARK_PYTHON="/usr/bin/python3.12"' in params
    assert "POSTGRES_HOST" not in params


def test_driver_env_is_rendered_as_spark_submit_parameters():
    operator = _build_operator(
        task_id="run_thing",
        entry_point_arguments=["cmd"],
        driver_env={"POSTGRES_HOST": "db.example.com"},
    )

    assert (
        'spark.emr-serverless.driverEnv.POSTGRES_HOST="db.example.com"'
        in operator.job_driver["sparkSubmit"]["sparkSubmitParameters"]
    )


def test_pyspark_python_confs_are_always_set_regardless_of_driver_env():
    operator = _build_operator(
        task_id="run_thing",
        entry_point_arguments=["cmd"],
        driver_env={"POSTGRES_HOST": "db.example.com"},
    )

    params = operator.job_driver["sparkSubmit"]["sparkSubmitParameters"]
    assert 'spark.emr-serverless.driverEnv.PYSPARK_PYTHON="/usr/bin/python3.12"' in params
    assert 'spark.executorEnv.PYSPARK_PYTHON="/usr/bin/python3.12"' in params
