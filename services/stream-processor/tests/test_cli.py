import signal
from threading import Event
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from stream_processor import cli
from stream_processor.cli import (
    HADOOP_AWS_PACKAGE,
    KAFKA_PACKAGE,
    SPARK_PACKAGES,
    await_shutdown,
    bundled_hadoop_version,
    install_shutdown_handler,
)
from stream_processor.config import StreamConfig


def test_spark_packages_include_matching_kafka_and_s3a_connectors() -> None:
    assert SPARK_PACKAGES == (
        f"{KAFKA_PACKAGE},org.apache.hadoop:hadoop-aws:{bundled_hadoop_version()}"
    )
    assert HADOOP_AWS_PACKAGE in SPARK_PACKAGES


@pytest.fixture(autouse=True)
def _restore_signal_handlers():
    # 테스트가 등록한 핸들러가 이후 테스트나 pytest 자체의 SIGINT 처리에 새어나가지 않도록
    # 원래 핸들러를 저장해두고 테스트가 끝나면 되돌린다.
    original_handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    yield
    for sig, handler in original_handlers.items():
        signal.signal(sig, handler)


@pytest.mark.parametrize("triggered_signal", [signal.SIGINT, signal.SIGTERM])
def test_install_shutdown_handler_records_shutdown_request(triggered_signal) -> None:
    shutdown_requested = install_shutdown_handler()
    handler = signal.getsignal(triggered_signal)
    handler(triggered_signal, None)

    assert shutdown_requested.is_set()


def test_await_shutdown_stops_query_outside_signal_handler() -> None:
    query = Mock()
    shutdown_requested = Event()
    shutdown_requested.set()

    await_shutdown(query, shutdown_requested)

    query.stop.assert_called_once_with()
    query.awaitTermination.assert_not_called()


def test_build_spark_session_applies_the_configured_driver_memory(monkeypatch) -> None:
    # Spark 기본 driver heap은 1 GiB이고 local[*]에서는 driver가 곧 executor라
    # micro-batch 전체가 그 heap에서 파싱된다(#482). 세션을 실제로 띄우지 않고
    # builder에 conf가 실리는지만 확인한다.
    recorded: dict[str, str] = {}

    class FakeBuilder:
        appName = None

        def config(self, key: str, value: str) -> "FakeBuilder":
            recorded[key] = value
            return self

        def master(self, _value: str) -> "FakeBuilder":
            return self

        def getOrCreate(self) -> str:
            return "session"

    builder = FakeBuilder()
    monkeypatch.setattr(
        cli, "SparkSession", SimpleNamespace(builder=SimpleNamespace(appName=lambda _n: builder))
    )
    monkeypatch.delenv("STREAM_SPARK_MASTER", raising=False)

    cli.build_spark_session(StreamConfig.from_env({"STREAM_DRIVER_MEMORY": "6g"}))

    assert recorded["spark.driver.memory"] == "6g"
    assert recorded["spark.sql.session.timeZone"] == "UTC"
