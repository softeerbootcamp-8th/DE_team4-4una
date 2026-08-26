"""Prometheus metrics derived from Spark Structured Streaming query progress (#358)."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

from stream_processor.bronze_sink import (
    BATCH_STATS_OBSERVATION_NAME,
    MAX_EVENT_TIME_FIELD,
)

logger = logging.getLogger(__name__)

QUERY_RUNNING_METRIC_NAME = "stream_processor_query_running"
INPUT_ROWS_TOTAL_METRIC_NAME = "stream_processor_input_rows_total"
INPUT_ROWS_PER_SECOND_METRIC_NAME = "stream_processor_input_rows_per_second"
PROCESSED_ROWS_PER_SECOND_METRIC_NAME = "stream_processor_processed_rows_per_second"
BATCH_DURATION_METRIC_NAME = "stream_processor_batch_duration_seconds"
LAST_PROGRESS_TIMESTAMP_METRIC_NAME = "stream_processor_last_progress_timestamp_seconds"
QUERY_FAILURES_TOTAL_METRIC_NAME = "stream_processor_query_failures_total"
EVENT_TIME_LAG_METRIC_NAME = "stream_processor_event_time_lag_seconds"
KAFKA_OFFSET_LAG_METRIC_NAME = "stream_processor_kafka_offset_lag"
KAFKA_END_OFFSET_SUM_METRIC_NAME = "stream_processor_kafka_end_offset_sum"

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
        self.event_time_lag_seconds = Gauge(
            EVENT_TIME_LAG_METRIC_NAME,
            "Seconds between now and the latest sensor event_time processed in the "
            "most recent micro-batch",
            registry=self.registry,
        )
        self.kafka_offset_lag = Gauge(
            KAFKA_OFFSET_LAG_METRIC_NAME,
            "Sum across partitions of (latestOffset - endOffset) for the subscribed "
            "Kafka topic, from the most recent micro-batch",
            registry=self.registry,
        )
        self.kafka_end_offset_sum = Gauge(
            KAFKA_END_OFFSET_SUM_METRIC_NAME,
            "Sum of Kafka end offsets committed by the most recent micro-batch",
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

        event_time_lag = _event_time_lag_seconds(progress)
        if event_time_lag is not None:
            self.event_time_lag_seconds.set(event_time_lag)

        offset_stats = _kafka_offset_stats(progress)
        if offset_stats is not None:
            offset_lag, end_offset_sum = offset_stats
            self.kafka_offset_lag.set(offset_lag)
            self.kafka_end_offset_sum.set(end_offset_sum)

    def observe_terminated(self, exception: str | None) -> None:
        self.query_running.set(0)
        if exception is not None:
            self.query_failures_total.inc()


def _parse_timestamp(timestamp: str) -> float:
    """Spark progress timestamp(예: `2016-01-15T20:12:00.000Z`)를 Unix epoch 초로 바꾼다."""
    return (
        datetime.strptime(timestamp, _TIMESTAMP_FORMAT).replace(tzinfo=UTC).timestamp()
    )


def _event_time_lag_seconds(progress) -> float | None:
    """이번 micro-batch에서 처리한 가장 최신 event_time과 지금 사이 초.

    bronze_sink.write_bronze_stream()의 observe()가 매 배치 progress.observedMetrics에
    실어 보낸다 — 별도 집계 로직 없이 Spark가 이미 제공하는 값이다. 빈 배치는
    max_event_time이 None이라(#426 후속, 로컬에서 직접 확인) lag를 계산할 수 없는데,
    이건 장애가 아니라 그냥 이번 배치에 새 행이 없었다는 뜻이라 이전 값을 그대로 둔다.

    Timestamp는 naive datetime으로 온다 -- Py4J가 JVM 기본 타임존으로 변환해서 주기
    때문에(spark.sql.session.timeZone과 무관, cli.py가 프로세스 TZ 자체를 UTC로
    맞춰둔 이유) UTC로 간주해도 안전하다.
    """
    observed_metrics = getattr(progress, "observedMetrics", None)
    if not observed_metrics:
        return None
    row = observed_metrics.get(BATCH_STATS_OBSERVATION_NAME)
    if row is None:
        return None
    max_event_time = row[MAX_EVENT_TIME_FIELD]
    if max_event_time is None:
        return None
    if max_event_time.tzinfo is None:
        max_event_time = max_event_time.replace(tzinfo=UTC)
    return (datetime.now(UTC) - max_event_time).total_seconds()


def _kafka_offset_lag(progress) -> int | None:
    """subscribe 중인 Kafka topic의 (latestOffset - endOffset) 합.

    Kafka에는 이미 있지만 이번 micro-batch까지 아직 못 읽은 메시지 수다.
    이 파이프라인은 topic 하나만 subscribe하므로(kafka_source.py) sources[0]만 본다.
    startOffset/endOffset/latestOffset은 Kafka source에서 partition별 offset을 담은
    JSON 문자열이다(#426 후속, 로컬에서 직접 확인) — 파싱 실패나 값 없음(첫 배치 등)은
    형식이 다른 source이거나 아직 값이 없다는 뜻이라 조용히 None을 돌려 gauge를
    갱신하지 않는다(직전 값 유지, 예외로 전체 metric 갱신을 막지 않는다).
    """
    stats = _kafka_offset_stats(progress)
    return stats[0] if stats is not None else None


def _kafka_offset_stats(progress) -> tuple[int, int] | None:
    """Return current-batch lag and the sum of offsets committed by Spark.

    end offset 합계를 별도로 노출하면 Spark progress가 멈춘 동안에도 Kafka Exporter의
    현재 offset과 비교해 backlog 증가를 계산할 수 있다
    """
    sources = getattr(progress, "sources", None)
    if not sources:
        return None
    source = sources[0]
    try:
        end_offsets = json.loads(source.endOffset)
        latest_offsets = json.loads(source.latestOffset)
    except (TypeError, ValueError) as exc:
        logger.debug("kafka offset lag를 계산할 수 없음: %s", exc)
        return None
    if not isinstance(end_offsets, dict) or not isinstance(latest_offsets, dict):
        return None
    lag = 0
    end_offset_sum = 0
    for topic, partitions in latest_offsets.items():
        end_partitions = end_offsets.get(topic, {})
        for partition, latest in partitions.items():
            end = end_partitions.get(partition, 0)
            lag += latest - end
            end_offset_sum += end
    return lag, end_offset_sum
