"""Prepare Kafka records and persist them to a Bronze Parquet sink."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from stream_processor.config import StreamConfig
from stream_processor.schemas import SENSOR_EVENT_VALUE_SCHEMA

EVENT_DATE_PARTITION = "event_date"
EVENT_HOUR_PARTITION = "hour"


def prepare_bronze_records(
    records: DataFrame,
    ingestion_time: Column | None = None,
) -> DataFrame:
    """Add Bronze load metadata and UTC event-time partition columns."""
    loaded_at = ingestion_time if ingestion_time is not None else F.current_timestamp()
    parsed = F.from_json("value", SENSOR_EVENT_VALUE_SCHEMA)
    source_timestamp = F.col("timestamp") if "timestamp" in records.columns else loaded_at
    # 정상 이벤트는 센서 측정 시각으로 나누고, 파싱 불가 원문은 Kafka 시각으로 보존한다
    partition_time = F.coalesce(
        F.try_to_timestamp(parsed["event_time"]),
        source_timestamp,
        loaded_at,
    )
    enriched = F.struct(
        *(parsed[field.name].alias(field.name) for field in SENSOR_EVENT_VALUE_SCHEMA),
        loaded_at.alias("_ingested_at"),
    )
    enriched_json = F.to_json(enriched, options={"ignoreNullFields": "false"})

    return (
        records.withColumn(
            "value",
            F.when(F.try_parse_json("value").isNotNull(), enriched_json).otherwise(
                F.col("value")
            ),
        )
        .withColumn(EVENT_DATE_PARTITION, F.to_date(partition_time))
        .withColumn(EVENT_HOUR_PARTITION, F.date_format(partition_time, "HH"))
    )


def add_ingestion_time_to_value(
    records: DataFrame,
    ingestion_time: Column | None = None,
) -> DataFrame:
    """Add the Spark-owned Bronze load time inside each valid JSON value."""
    return prepare_bronze_records(records, ingestion_time).drop(
        EVENT_DATE_PARTITION, EVENT_HOUR_PARTITION
    )


def write_bronze_stream(records: DataFrame, config: StreamConfig) -> StreamingQuery:
    """Start the append-only partitioned Parquet sink with a checkpoint."""
    bronze_records = prepare_bronze_records(records)
    # Kafka partition마다 task가 하나씩 생겨 그대로 쓰면 배치당 파일이 partition 수만큼
    # 쏟아진다. trigger를 키워도 이걸 안 하면 파일 크기는 그대로라 쓰기 직전에 합친다.
    bronze_records = bronze_records.coalesce(config.bronze_output_partitions)
    # checkpoint를 출력과 분리해 재시작 시 마지막 Kafka offset부터 이어간다.
    return (
        bronze_records.writeStream.format("parquet")
        .outputMode("append")
        .option("path", config.bronze_output_path)
        .option("checkpointLocation", config.bronze_checkpoint_location)
        .partitionBy(EVENT_DATE_PARTITION, EVENT_HOUR_PARTITION)
        .trigger(processingTime=f"{config.trigger_interval_seconds} seconds")
        .start()
    )
