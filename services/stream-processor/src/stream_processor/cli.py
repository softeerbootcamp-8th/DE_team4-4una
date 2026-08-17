"""Entry point for the Kafka-to-local-Bronze streaming job."""

from __future__ import annotations

import logging
import signal

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from stream_processor.bronze_sink import write_bronze_stream
from stream_processor.config import StreamConfig
from stream_processor.kafka_source import read_kafka_stream
from stream_processor.progress import ProgressLogger

# Kafka 커넥터는 pip 패키지가 아니라 Maven jar라서, 최초 실행 시 이 좌표로 내려받는다.
# pyspark 버전과 정확히 맞아야 해서 하드코딩 대신 설치된 버전을 그대로 사용한다.
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("stream-processor")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def install_shutdown_handler(query: StreamingQuery) -> None:
    """SIGINT/SIGTERM 수신 시 query.stop()을 먼저 호출해, awaitTermination()이
    KeyboardInterrupt 등 예외로 인한 스택트레이스 없이 정상 종료되도록 한다."""

    def _handle_signal(signum: int, frame: object) -> None:
        query.stop()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)


def main() -> None:
    # basicConfig 없이는 logging의 INFO 레벨이 조용히 버려져서 ProgressLogger가 아무것도
    # 출력하지 않는다 (핸들러가 없으면 root logger 기본 레벨은 WARNING). 실행 로그로 남도록 설정한다.
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

    config = StreamConfig.from_env()
    spark = build_spark_session()
    spark.streams.addListener(ProgressLogger())

    stream_df = read_kafka_stream(spark, config)
    query = write_bronze_stream(stream_df, config)
    install_shutdown_handler(query)
    query.awaitTermination()
