"""Airflow LocalExecutor 부트스트랩(#70) 동작 확인용 최소 DAG.

BashOperator로 "hello world"를 출력하는 것 외에는 아무 일도 하지 않는다.
실제 파이프라인 DAG(Kafka -> Bronze, batch-jobs, gold-loader 스케줄링)는
후속 이슈에서 추가한다.
"""

from __future__ import annotations

import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="hello_world",
    description="Airflow LocalExecutor 부트스트랩 동작 확인용 최소 DAG",
    schedule=None,
    start_date=datetime.datetime(2026, 8, 14, tzinfo=datetime.UTC),
    catchup=False,
    tags=["bootstrap"],
) as dag:
    hello = BashOperator(
        task_id="hello",
        bash_command="echo hello world",
    )
