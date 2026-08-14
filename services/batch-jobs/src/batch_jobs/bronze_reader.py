"""Read Bronze sensor_event Parquet, keeping the original payload of every row."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from batch_jobs.schemas import (
    BRONZE_SENSOR_EVENT_SCHEMA,
    PARSE_FAILED_COLUMN,
    RAW_RECORD_COLUMN,
)


def read_bronze_sensor_events(spark: SparkSession, path: str | Path) -> DataFrame:
    """Read one Bronze sensor_event Parquet path into a DataFrame.

    stream-processor는 Kafka 레코드를 Parquet으로 적재하며 센서 필드는
    value 컬럼의 JSON 문자열에 담겨 있다. 그 JSON을 Bronze 스키마로 풀고,
    원본 문자열과 파싱 실패 여부를 함께 남긴다.
    """
    envelope = spark.read.parquet(str(path))
    parsed = F.from_json(F.col("value"), BRONZE_SENSOR_EVENT_SCHEMA)
    return envelope.select(
        *[parsed[field.name].alias(field.name) for field in BRONZE_SENSOR_EVENT_SCHEMA.fields],
        F.col("value").alias(RAW_RECORD_COLUMN),
        parsed.isNull().alias(PARSE_FAILED_COLUMN),
    )
