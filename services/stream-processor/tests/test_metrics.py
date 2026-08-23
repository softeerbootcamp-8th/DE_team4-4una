from types import SimpleNamespace

from prometheus_client import CollectorRegistry
from stream_processor.metrics import (
    BATCH_DURATION_METRIC_NAME,
    INPUT_ROWS_PER_SECOND_METRIC_NAME,
    INPUT_ROWS_TOTAL_METRIC_NAME,
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
