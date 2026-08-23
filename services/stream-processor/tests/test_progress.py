import logging
from types import SimpleNamespace
from unittest.mock import Mock

from stream_processor.progress import ProgressLogger


def _progress_event(**overrides) -> SimpleNamespace:
    defaults = {
        "batchId": 3,
        "numInputRows": 120,
        "inputRowsPerSecond": 30.0,
        "processedRowsPerSecond": 24.5,
        "durationMs": {"triggerExecution": 4500},
        "timestamp": "2016-01-15T20:12:00.000Z",
    }
    defaults.update(overrides)
    return SimpleNamespace(progress=SimpleNamespace(**defaults))


def test_on_query_progress_logs_input_rows_and_rate(caplog) -> None:
    listener = ProgressLogger(metrics=Mock())
    event = _progress_event(batchId=3, numInputRows=120, processedRowsPerSecond=24.5)

    with caplog.at_level(logging.INFO):
        listener.onQueryProgress(event)

    assert "batchId=3" in caplog.text
    assert "numInputRows=120" in caplog.text
    assert "processedRowsPerSecond=24.5" in caplog.text


def test_on_query_progress_forwards_progress_to_metrics() -> None:
    metrics = Mock()
    listener = ProgressLogger(metrics=metrics)
    event = _progress_event()

    listener.onQueryProgress(event)

    metrics.observe_progress.assert_called_once_with(event.progress)


def test_on_query_started_marks_query_running() -> None:
    metrics = Mock()
    listener = ProgressLogger(metrics=metrics)

    listener.onQueryStarted(SimpleNamespace(id="query-1"))

    metrics.observe_started.assert_called_once_with()


def test_on_query_terminated_forwards_exception_to_metrics() -> None:
    metrics = Mock()
    listener = ProgressLogger(metrics=metrics)

    listener.onQueryTerminated(SimpleNamespace(id="query-1", exception="boom"))

    metrics.observe_terminated.assert_called_once_with("boom")
