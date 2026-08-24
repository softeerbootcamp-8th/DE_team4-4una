"""Entry point for the Kafka-to-Bronze streaming job."""

from __future__ import annotations

import logging
import os
import re
import signal
import time
from pathlib import Path
from threading import Event

import pyspark
from prometheus_client import start_http_server
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from stream_processor.bronze_sink import write_bronze_stream
from stream_processor.config import StreamConfig
from stream_processor.kafka_source import read_kafka_stream
from stream_processor.metrics import StreamMetrics
from stream_processor.progress import ProgressLogger

# Kafka 커넥터는 pip 패키지가 아니라 Maven jar라서, 최초 실행 시 이 좌표로 내려받는다.
# pyspark 버전과 정확히 맞아야 해서 하드코딩 대신 설치된 버전을 그대로 사용한다.
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"
_HADOOP_CLIENT_JAR = re.compile(r"hadoop-client-api-(?P<version>[0-9.]+)\.jar$")

# Prometheus metrics 노출 포트. Serving API의 9101과 겹치지 않게 잡았다.
DEFAULT_METRICS_PORT = 9103


def bundled_hadoop_version() -> str:
    """Return the Hadoop version bundled with the installed PySpark wheel."""
    jar_dir = Path(pyspark.__file__).resolve().parent / "jars"
    versions = {
        match.group("version")
        for jar in jar_dir.glob("hadoop-client-api-*.jar")
        if (match := _HADOOP_CLIENT_JAR.fullmatch(jar.name)) is not None
    }
    if len(versions) != 1:
        raise RuntimeError(f"expected one bundled Hadoop version, found {sorted(versions)}")
    return versions.pop()


# PySpark에 포함되지 않은 S3A 구현을 번들 Hadoop과 같은 버전으로 내려받는다
HADOOP_AWS_PACKAGE = f"org.apache.hadoop:hadoop-aws:{bundled_hadoop_version()}"
SPARK_PACKAGES = f"{KAFKA_PACKAGE},{HADOOP_AWS_PACKAGE}"


def build_spark_session() -> SparkSession:
    # spark.sql.session.timeZone은 SQL 표현식/포맷팅에만 적용된다 -- StreamingQueryListener가
    # observedMetrics로 받는 Timestamp는 Py4J가 JVM 기본 타임존으로 변환해 돌려주므로
    # (실제로 이 호스트가 UTC가 아니면 값이 그만큼 어긋난다 -- 로컬에서 9시간 어긋남을
    # 직접 확인함, #426 후속), JVM을 띄우기 전에 프로세스 TZ 자체를 UTC로 못박는다.
    os.environ["TZ"] = "UTC"
    time.tzset()
    builder = (
        SparkSession.builder.appName("stream-processor")
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.sql.session.timeZone", "UTC")
    )
    # spark-submit/클러스터에서는 master를 주입하지 않고, 단독 Docker 실행에서만 지정한다
    if master := os.getenv("STREAM_SPARK_MASTER"):
        builder = builder.master(master)
    return builder.getOrCreate()


def install_shutdown_handler() -> Event:
    """Return an event set when the process receives SIGINT or SIGTERM."""
    shutdown_requested = Event()

    def _handle_signal(signum: int, frame: object) -> None:
        # signal handler 안에서 Py4J를 재호출하면 진행 중인 JVM 응답과 충돌할 수 있다
        shutdown_requested.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    return shutdown_requested


def await_shutdown(query: StreamingQuery, shutdown_requested: Event) -> None:
    """Wait for query completion or stop it safely after a process signal."""
    while not shutdown_requested.is_set():
        if query.awaitTermination(1):
            return
    query.stop()


def main() -> None:
    # basicConfig 없이는 logging의 INFO 레벨이 조용히 버려져서 ProgressLogger가 아무것도
    # 출력하지 않는다 (핸들러가 없으면 root logger 기본 레벨은 WARNING). 실행 로그로 남도록 설정한다.
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

    config = StreamConfig.from_env()

    metrics = StreamMetrics()
    metrics_port = int(os.getenv("STREAM_METRICS_PORT", str(DEFAULT_METRICS_PORT)))
    start_http_server(metrics_port, registry=metrics.registry)

    spark = build_spark_session()
    spark.streams.addListener(ProgressLogger(metrics=metrics))

    stream_df = read_kafka_stream(spark, config)
    query = write_bronze_stream(stream_df, config)
    shutdown_requested = install_shutdown_handler()
    await_shutdown(query, shutdown_requested)
