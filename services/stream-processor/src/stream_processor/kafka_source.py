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


def kafka_read_options(config: StreamConfig) -> dict[str, str]:
    """Build the Kafka source options.

    read_kafka_stream은 실제 broker가 있어야 돌아가서 테스트가 어렵다. 옵션 구성만
    떼어 두면 어떤 옵션이 걸리는지는 broker 없이 검증할 수 있다.
    """
    options = {
        "kafka.bootstrap.servers": config.bootstrap_servers,
        "subscribe": config.topic,
        # 체크포인트가 이미 있으면 Spark가 이 옵션을 무시하고 체크포인트 기준으로 재개한다.
        "startingOffsets": config.starting_offsets,
    }
    if config.min_offsets_per_trigger > 0:
        # 전체 partition에 쌓인 offset 합계 기준이다. 이만큼 모여야 배치가 돈다.
        options["minOffsetsPerTrigger"] = str(config.min_offsets_per_trigger)
        # 위 조건만 두면 한산할 때 배치가 아예 안 돈다. 둘은 항상 같이 걸어야 한다.
        options["maxTriggerDelay"] = config.max_trigger_delay
    if config.max_offsets_per_trigger > 0:
        # 하한과 달리 단독으로 건다 -- 정상 배치는 minOffsetsPerTrigger가 정하고,
        # 이 값은 복구 경로에서 한 배치가 무제한으로 커지는 것만 막는다(#482).
        options["maxOffsetsPerTrigger"] = str(config.max_offsets_per_trigger)
    return options


def read_kafka_stream(spark: SparkSession, config: StreamConfig) -> DataFrame:
    """Subscribe to `config.topic` and return the decoded sensor-event stream."""
    kafka_df = (
        spark.readStream.format("kafka").options(**kafka_read_options(config)).load()
    )
    return select_sensor_columns(kafka_df)
