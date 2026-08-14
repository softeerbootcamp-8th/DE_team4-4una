"""Read Bronze sensor_event JSONL with an explicit schema, keeping unparseable rows."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from batch_jobs.schemas import BRONZE_SENSOR_EVENT_SCHEMA, CORRUPT_RECORD_COLUMN


def read_bronze_sensor_events(spark: SparkSession, path: str | Path) -> DataFrame:
    """Read one Bronze sensor_event JSONL path into a DataFrame.

    path는 파일, 디렉터리, glob 모두 가능하다.
    파싱에 실패한 행은 나머지 컬럼이 NULL이 되고 원본 문자열이
    CORRUPT_RECORD_COLUMN에 담긴다.
    """
    return (
        spark.read.schema(BRONZE_SENSOR_EVENT_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
        .json(str(path))
    )
