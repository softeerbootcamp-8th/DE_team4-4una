"""Read Bronze sensor_event Parquet, keeping the original payload of every row."""

from __future__ import annotations

from pathlib import Path

from batch_jobs.schemas import (
    BRONZE_SENSOR_EVENT_SCHEMA,
    PARSE_FAILED_COLUMN,
    RAW_RECORD_COLUMN,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# from_json이 파싱 실패를 표시하도록 Bronze 스키마에 corrupt record 컬럼을 덧붙인다.
CORRUPT_RECORD_COLUMN = "_corrupt_record"
_PARSE_SCHEMA = StructType(
    [
        *BRONZE_SENSOR_EVENT_SCHEMA.fields,
        StructField(CORRUPT_RECORD_COLUMN, StringType(), nullable=True),
    ]
)


def read_bronze_sensor_events(spark: SparkSession, path: str | Path) -> DataFrame:
    """Read one Bronze sensor_event Parquet path into a DataFrame.

    stream-processor는 Kafka 레코드를 Parquet으로 적재하며 센서 필드는
    value 컬럼의 JSON 문자열에 담겨 있다. 그 JSON을 Bronze 스키마로 풀고,
    원본 문자열과 파싱 실패 여부를 함께 남긴다.
    """
    envelope = spark.read.parquet(str(path))
    # 부분 파싱이 켜져 있으면 깨진 JSON도 앞부분 필드가 채워져 결과가 NULL이 아니다.
    # 실패 여부는 원본이 담기는 corrupt record 컬럼으로만 확실히 알 수 있다.
    parsed = F.from_json(
        F.col("value"), _PARSE_SCHEMA, {"columnNameOfCorruptRecord": CORRUPT_RECORD_COLUMN}
    )
    return envelope.select(
        *[parsed[field.name].alias(field.name) for field in BRONZE_SENSOR_EVENT_SCHEMA.fields],
        F.col("value").alias(RAW_RECORD_COLUMN),
        parsed[CORRUPT_RECORD_COLUMN].isNotNull().alias(PARSE_FAILED_COLUMN),
    )
