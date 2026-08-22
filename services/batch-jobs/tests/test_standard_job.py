"""Tests for batch_jobs/comfort_score/standard_job.py Spark session wiring (#290).

`_postgres_jdbc_spark_config()`가 실제 `SparkSession`을 만들지 않고도 EMR
Serverless(S3 jar)와 로컬 개발(Maven 자동 다운로드) 중 어떤 spark.jars* 설정을
고를지 검증할 수 있게 순수 함수로 분리했다.
"""

from __future__ import annotations

from batch_jobs.comfort_score.standard_job import (
    POSTGRES_JDBC_PACKAGE,
    _postgres_jdbc_spark_config,
)


def test_falls_back_to_maven_package_when_jar_uri_is_not_set():
    key, value = _postgres_jdbc_spark_config({})

    assert (key, value) == ("spark.jars.packages", POSTGRES_JDBC_PACKAGE)


def test_uses_s3_jar_uri_when_postgres_jdbc_jar_uri_is_set():
    key, value = _postgres_jdbc_spark_config(
        {"POSTGRES_JDBC_JAR_URI": "s3://de4-artifacts/jars/postgresql-42.7.4.jar"}
    )

    assert (key, value) == (
        "spark.jars",
        "s3://de4-artifacts/jars/postgresql-42.7.4.jar",
    )
