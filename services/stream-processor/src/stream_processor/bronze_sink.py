"""Prepare Kafka records and persist them to the local Bronze Parquet sink."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from stream_processor.config import StreamConfig
from stream_processor.schemas import SENSOR_EVENT_VALUE_SCHEMA


def add_ingestion_time_to_value(
    records: DataFrame,
    ingestion_time: Column | None = None,
) -> DataFrame:
    """Add the Spark-owned Bronze load time inside each valid JSON value."""
    # 적재 시각은 Producer가 아닌 Bronze sink 실행 시점에 생성한다.
    loaded_at = ingestion_time if ingestion_time is not None else F.current_timestamp()
    # value를 구조체로 변환해 센서 필드와 같은 레벨에 적재 시각을 추가한다.
    parsed = F.from_json("value", SENSOR_EVENT_VALUE_SCHEMA)
    enriched = F.struct(
        *(parsed[field.name].alias(field.name) for field in SENSOR_EVENT_VALUE_SCHEMA),
        loaded_at.alias("_ingested_at"),
    )
    enriched_json = F.to_json(enriched, options={"ignoreNullFields": "false"})

    # 파싱 불가능한 원문은 손상시키지 않고 후속 quarantine 처리를 위해 그대로 둔다.
    return records.withColumn(
        "value",
        F.when(F.try_parse_json("value").isNotNull(), enriched_json).otherwise(F.col("value")),
    )


def write_bronze_stream(records: DataFrame, config: StreamConfig) -> StreamingQuery:
    """Start the append-only local Parquet sink with a dedicated checkpoint."""
    bronze_records = add_ingestion_time_to_value(records)
    # checkpoint를 출력과 분리해 재시작 시 마지막 Kafka offset부터 이어간다.
    return (
        bronze_records.writeStream.format("parquet")
        .outputMode("append")
        .option("path", config.bronze_output_path)
        .option("checkpointLocation", config.bronze_checkpoint_location)
        .trigger(processingTime=f"{config.trigger_interval_seconds} seconds")
        .start()
    )
