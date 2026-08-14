"""Entry point: subscribe to `sensor-events` and print records to the console."""

from __future__ import annotations

import logging
import sys

import pyspark
from pyspark.sql import SparkSession

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
        .getOrCreate()
    )


def main() -> None:
    # basicConfig 없이는 logging의 INFO 레벨이 조용히 버려져서 ProgressLogger가 아무것도
    # 출력하지 않는다 (핸들러가 없으면 root logger 기본 레벨은 WARNING). 실행 로그로 남도록 설정한다.
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

    config = StreamConfig.from_env()
    spark = build_spark_session()
    spark.streams.addListener(ProgressLogger())

    stream_df = read_kafka_stream(spark, config)
    # 완료 조건 검증용: 실제 출력 스키마를 stdout에 남겨 PR에 그대로 첨부할 수 있게 한다.
    # stdout이 파일로 리다이렉트되면 완전 버퍼링되어, 종료 시그널을 받으면 이 출력이
    # flush되지 못하고 사라질 수 있어 즉시 flush한다.
    stream_df.printSchema()
    sys.stdout.flush()

    query = (
        stream_df.writeStream.format("console")
        .option("truncate", "false")
        .option("checkpointLocation", config.checkpoint_location)
        .trigger(processingTime=f"{config.trigger_interval_seconds} seconds")
        .outputMode("append")
        .start()
    )
    query.awaitTermination()
