import logging
from types import SimpleNamespace

from stream_processor.progress import ProgressLogger


def test_on_query_progress_logs_input_rows_and_rate(caplog) -> None:
    listener = ProgressLogger()
    # 실제 QueryProgressEvent를 흉내낸 최소 stub: 리스너가 읽는 속성만 갖춘다.
    event = SimpleNamespace(
        progress=SimpleNamespace(batchId=3, numInputRows=120, processedRowsPerSecond=24.5)
    )

    with caplog.at_level(logging.INFO):
        listener.onQueryProgress(event)

    assert "batchId=3" in caplog.text
    assert "numInputRows=120" in caplog.text
    assert "processedRowsPerSecond=24.5" in caplog.text
