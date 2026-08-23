"""Prometheus metrics derived from Spark Structured Streaming query progress (#358)."""

from __future__ import annotations

from datetime import UTC, datetime

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

QUERY_RUNNING_METRIC_NAME = "stream_processor_query_running"
INPUT_ROWS_TOTAL_METRIC_NAME = "stream_processor_input_rows_total"
INPUT_ROWS_PER_SECOND_METRIC_NAME = "stream_processor_input_rows_per_second"
PROCESSED_ROWS_PER_SECOND_METRIC_NAME = "stream_processor_processed_rows_per_second"
BATCH_DURATION_METRIC_NAME = "stream_processor_batch_duration_seconds"
LAST_PROGRESS_TIMESTAMP_METRIC_NAME = "stream_processor_last_progress_timestamp_seconds"
QUERY_FAILURES_TOTAL_METRIC_NAME = "stream_processor_query_failures_total"

# progress.durationMs는 addBatch/queryPlanning/walCommit 등 여러 세부 항목을 담고 있는데,
# 마이크로배치 전체 소요 시간은 이 키 하나로 대표된다.
_TOTAL_DURATION_KEY = "triggerExecution"
_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


class StreamMetrics:
    """`ProgressLogger`가 받는 이벤트를 Prometheus metric으로 바꿔 쌓아 둔다.

    `registry` 생략 시 인스턴스마다 새 `CollectorRegistry`를 쓴다 — global registry를
    공유하면 반복 생성하는 테스트가 "Duplicated timeseries" 오류로 깨진다.
    """

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry if registry is not None else CollectorRegistry()
        self.query_running = Gauge(
            QUERY_RUNNING_METRIC_NAME,
            "1 if the streaming query is currently running, 0 otherwise",
            registry=self.registry,
        )
        self.input_rows_total = Counter(
            INPUT_ROWS_TOTAL_METRIC_NAME,
            "Cumulative number of input rows consumed across all micro-batches",
            registry=self.registry,
        )
        self.input_rows_per_second = Gauge(
            INPUT_ROWS_PER_SECOND_METRIC_NAME,
            "Input rows per second reported for the most recent micro-batch",
            registry=self.registry,
        )
        self.processed_rows_per_second = Gauge(
            PROCESSED_ROWS_PER_SECOND_METRIC_NAME,
            "Processed rows per second reported for the most recent micro-batch",
            registry=self.registry,
        )
        self.batch_duration_seconds = Histogram(
            BATCH_DURATION_METRIC_NAME,
            "Micro-batch total duration in seconds",
            registry=self.registry,
        )
        self.last_progress_timestamp_seconds = Gauge(
            LAST_PROGRESS_TIMESTAMP_METRIC_NAME,
            "Unix timestamp of the most recently reported micro-batch progress",
            registry=self.registry,
        )
        self.query_failures_total = Counter(
            QUERY_FAILURES_TOTAL_METRIC_NAME,
            "Number of times the streaming query terminated with an exception",
            registry=self.registry,
        )

    def observe_started(self) -> None:
        self.query_running.set(1)

    def observe_progress(self, progress) -> None:
        self.input_rows_total.inc(progress.numInputRows)
        self.input_rows_per_second.set(progress.inputRowsPerSecond)
        self.processed_rows_per_second.set(progress.processedRowsPerSecond)

        duration_ms = progress.durationMs.get(_TOTAL_DURATION_KEY)
        if duration_ms is not None:
            self.batch_duration_seconds.observe(duration_ms / 1000)

        self.last_progress_timestamp_seconds.set(_parse_timestamp(progress.timestamp))

    def observe_terminated(self, exception: str | None) -> None:
        self.query_running.set(0)
        if exception is not None:
            self.query_failures_total.inc()


def _parse_timestamp(timestamp: str) -> float:
    """Spark progress timestamp(예: `2016-01-15T20:12:00.000Z`)를 Unix epoch 초로 바꾼다."""
    return (
        datetime.strptime(timestamp, _TIMESTAMP_FORMAT).replace(tzinfo=UTC).timestamp()
    )
