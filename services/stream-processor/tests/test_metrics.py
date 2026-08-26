from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from prometheus_client import CollectorRegistry
from stream_processor.bronze_sink import (
    BATCH_STATS_OBSERVATION_NAME,
    MAX_EVENT_TIME_FIELD,
)
from stream_processor.metrics import (
    BATCH_DURATION_METRIC_NAME,
    EVENT_TIME_LAG_METRIC_NAME,
    INPUT_ROWS_PER_SECOND_METRIC_NAME,
    INPUT_ROWS_TOTAL_METRIC_NAME,
    KAFKA_END_OFFSET_SUM_METRIC_NAME,
    KAFKA_OFFSET_LAG_METRIC_NAME,
    LAST_PROGRESS_TIMESTAMP_METRIC_NAME,
    PROCESSED_ROWS_PER_SECOND_METRIC_NAME,
    QUERY_FAILURES_TOTAL_METRIC_NAME,
    QUERY_RUNNING_METRIC_NAME,
    StreamMetrics,
)


def _progress(**overrides) -> SimpleNamespace:
    defaults = {
        "batchId": 3,
        "numInputRows": 120,
        "inputRowsPerSecond": 30.0,
        "processedRowsPerSecond": 24.5,
        "durationMs": {"triggerExecution": 4500, "queryPlanning": 12},
        "timestamp": "2016-01-15T20:12:00.000Z",
        # 실제 progress 객체를 흉내낸다 -- 둘 다 비어 있는 게 기본(#426 후속).
        "observedMetrics": {},
        "sources": [],
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_observe_started_sets_query_running_to_one() -> None:
    metrics = StreamMetrics(CollectorRegistry())
    metrics.observe_started()

    assert metrics.registry.get_sample_value(QUERY_RUNNING_METRIC_NAME) == 1.0


def test_observe_progress_updates_throughput_gauges_and_counter() -> None:
    metrics = StreamMetrics(CollectorRegistry())

    metrics.observe_progress(_progress())
    metrics.observe_progress(_progress(numInputRows=80))

    assert metrics.registry.get_sample_value(INPUT_ROWS_TOTAL_METRIC_NAME) == 200.0
    assert metrics.registry.get_sample_value(INPUT_ROWS_PER_SECOND_METRIC_NAME) == 30.0
    assert metrics.registry.get_sample_value(PROCESSED_ROWS_PER_SECOND_METRIC_NAME) == 24.5


def test_observe_progress_records_batch_duration_in_seconds() -> None:
    metrics = StreamMetrics(CollectorRegistry())

    metrics.observe_progress(_progress(durationMs={"triggerExecution": 2500}))

    count = metrics.registry.get_sample_value(f"{BATCH_DURATION_METRIC_NAME}_count")
    total = metrics.registry.get_sample_value(f"{BATCH_DURATION_METRIC_NAME}_sum")
    assert count == 1.0
    assert total == 2.5


def test_observe_progress_skips_batch_duration_when_trigger_execution_is_missing() -> None:
    metrics = StreamMetrics(CollectorRegistry())

    metrics.observe_progress(_progress(durationMs={"queryPlanning": 12}))

    count = metrics.registry.get_sample_value(f"{BATCH_DURATION_METRIC_NAME}_count")
    assert count == 0.0


def test_observe_progress_converts_timestamp_to_unix_epoch_seconds() -> None:
    metrics = StreamMetrics(CollectorRegistry())

    metrics.observe_progress(_progress(timestamp="2016-01-15T20:12:00.000Z"))

    assert (
        metrics.registry.get_sample_value(LAST_PROGRESS_TIMESTAMP_METRIC_NAME)
        == 1452888720.0
    )


def test_observe_terminated_without_exception_only_clears_running_flag() -> None:
    metrics = StreamMetrics(CollectorRegistry())
    metrics.observe_started()

    metrics.observe_terminated(None)

    assert metrics.registry.get_sample_value(QUERY_RUNNING_METRIC_NAME) == 0.0
    assert metrics.registry.get_sample_value(QUERY_FAILURES_TOTAL_METRIC_NAME) == 0.0


def test_observe_terminated_with_exception_increments_failure_counter() -> None:
    metrics = StreamMetrics(CollectorRegistry())
    metrics.observe_started()

    metrics.observe_terminated("boom")

    assert metrics.registry.get_sample_value(QUERY_RUNNING_METRIC_NAME) == 0.0
    assert metrics.registry.get_sample_value(QUERY_FAILURES_TOTAL_METRIC_NAME) == 1.0


def test_default_registry_is_not_shared_across_instances() -> None:
    # RequestMetrics와 같은 이유: 인스턴스 전용 registry가 기본값이 아니면 여러 테스트가
    # global registry에 같은 이름의 metric을 등록하려다 예외가 난다.
    StreamMetrics()
    StreamMetrics()


def _source(end_offset: str, latest_offset: str) -> SimpleNamespace:
    return SimpleNamespace(endOffset=end_offset, latestOffset=latest_offset)


class TestEventTimeLag:
    def test_computes_seconds_since_the_latest_observed_event_time(self) -> None:
        metrics = StreamMetrics(CollectorRegistry())
        max_event_time = datetime.now(UTC).replace(tzinfo=None) - timedelta(seconds=42)

        metrics.observe_progress(
            _progress(
                observedMetrics={
                    BATCH_STATS_OBSERVATION_NAME: {MAX_EVENT_TIME_FIELD: max_event_time}
                }
            )
        )

        lag = metrics.registry.get_sample_value(EVENT_TIME_LAG_METRIC_NAME)
        assert lag is not None
        assert 40 <= lag <= 44

    def test_empty_batch_keeps_the_previous_value(self) -> None:
        # observe()는 빈 배치에서 max_event_time=None을 돌려준다 -- 장애가 아니라
        # 이번 배치에 새 행이 없었다는 뜻이라 갱신하지 않고 이전 값을 그대로 둔다(#426 후속).
        # Gauge는 생성 시 기본값이 0.0이라 "None이면 그대로 둔다"는 이전 값이 있어야
        # 검증된다 -- 먼저 실제 값을 한 번 세팅한다.
        metrics = StreamMetrics(CollectorRegistry())
        max_event_time = datetime.now(UTC).replace(tzinfo=None) - timedelta(seconds=42)
        metrics.observe_progress(
            _progress(
                observedMetrics={
                    BATCH_STATS_OBSERVATION_NAME: {MAX_EVENT_TIME_FIELD: max_event_time}
                }
            )
        )
        first_value = metrics.registry.get_sample_value(EVENT_TIME_LAG_METRIC_NAME)

        metrics.observe_progress(
            _progress(
                observedMetrics={
                    BATCH_STATS_OBSERVATION_NAME: {MAX_EVENT_TIME_FIELD: None}
                }
            )
        )

        assert metrics.registry.get_sample_value(EVENT_TIME_LAG_METRIC_NAME) == first_value

    def test_missing_observed_metrics_does_not_raise(self) -> None:
        metrics = StreamMetrics(CollectorRegistry())

        metrics.observe_progress(_progress(observedMetrics={}))

        # Gauge 기본값(0.0)에서 바뀌지 않는다 -- 예외 없이 조용히 건너뛴 것으로 충분하다.
        assert metrics.registry.get_sample_value(EVENT_TIME_LAG_METRIC_NAME) == 0.0


class TestKafkaOffsetLag:
    def test_sums_the_lag_across_partitions(self) -> None:
        metrics = StreamMetrics(CollectorRegistry())
        end_offset = '{"sensor-events":{"0":100,"1":200}}'
        latest_offset = '{"sensor-events":{"0":150,"1":205}}'

        metrics.observe_progress(
            _progress(sources=[_source(end_offset, latest_offset)])
        )

        # (150-100) + (205-200) = 55
        assert metrics.registry.get_sample_value(KAFKA_OFFSET_LAG_METRIC_NAME) == 55.0
        assert (
            metrics.registry.get_sample_value(KAFKA_END_OFFSET_SUM_METRIC_NAME) == 300.0
        )

    def test_no_sources_does_not_raise(self) -> None:
        metrics = StreamMetrics(CollectorRegistry())

        metrics.observe_progress(_progress(sources=[]))

        assert metrics.registry.get_sample_value(KAFKA_OFFSET_LAG_METRIC_NAME) == 0.0
        assert metrics.registry.get_sample_value(KAFKA_END_OFFSET_SUM_METRIC_NAME) == 0.0

    def test_unparseable_offsets_keep_the_previous_value_without_raising(self) -> None:
        # 실제로 file source에서 관측됨: latestOffset이 문자열 "None"으로 온다
        # (JSON이 아니다) -- 여기서 죽으면 나머지 metric까지 다 못 올라간다(#426 후속).
        metrics = StreamMetrics(CollectorRegistry())
        metrics.observe_progress(
            _progress(
                sources=[_source('{"sensor-events":{"0":100}}', '{"sensor-events":{"0":150}}')]
            )
        )
        first_value = metrics.registry.get_sample_value(KAFKA_OFFSET_LAG_METRIC_NAME)
        first_end_offset_sum = metrics.registry.get_sample_value(
            KAFKA_END_OFFSET_SUM_METRIC_NAME
        )

        metrics.observe_progress(
            _progress(sources=[_source('{"sensor-events":{"0":100}}', "None")])
        )

        assert metrics.registry.get_sample_value(KAFKA_OFFSET_LAG_METRIC_NAME) == first_value
        assert (
            metrics.registry.get_sample_value(KAFKA_END_OFFSET_SUM_METRIC_NAME)
            == first_end_offset_sum
        )
        # 나머지 metric은 정상적으로 계속 갱신되어야 한다.
        assert metrics.registry.get_sample_value(INPUT_ROWS_TOTAL_METRIC_NAME) == 240.0
