"""Read the `sensor-events` Kafka topic as a Structured Streaming DataFrame."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from stream_processor.config import StreamConfig


def select_sensor_columns(kafka_df: DataFrame) -> DataFrame:
    """Decode Kafka's raw key/value bytes and keep the metadata columns downstream needs."""
    return kafka_df.select(
        # Kafka 소스의 key/value는 원래 binary라서 UTF-8 문자열로 캐스팅해야 콘솔에서 읽힌다.
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp"),
    )


def read_kafka_stream(spark: SparkSession, config: StreamConfig) -> DataFrame:
    """Subscribe to `config.topic` and return the decoded sensor-event stream."""
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.bootstrap_servers)
        .option("subscribe", config.topic)
        # 체크포인트가 이미 있으면 Spark가 이 옵션을 무시하고 체크포인트 기준으로 재개한다.
        .option("startingOffsets", config.starting_offsets)
        .load()
    )
    return select_sensor_columns(kafka_df)
