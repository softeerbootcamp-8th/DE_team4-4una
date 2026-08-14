"""Log per-batch throughput metrics for the running streaming query."""

from __future__ import annotations

import logging

from pyspark.sql.streaming.listener import (
    QueryProgressEvent,
    QueryStartedEvent,
    QueryTerminatedEvent,
    StreamingQueryListener,
)

logger = logging.getLogger(__name__)


class ProgressLogger(StreamingQueryListener):
    """Logs numInputRows/processedRowsPerSecond via `spark.streams.addListener(...)`.

    log4j 레벨 설정과 무관하게 항상 로그가 남도록 log4j 대신 파이썬 표준 logging을 쓴다.
    """

    def onQueryStarted(self, event: QueryStartedEvent) -> None:
        logger.info("stream query started id=%s", event.id)

    def onQueryProgress(self, event: QueryProgressEvent) -> None:
        # 매 마이크로배치가 끝날 때마다 Spark가 호출해준다.
        progress = event.progress
        logger.info(
            "stream progress batchId=%s numInputRows=%s processedRowsPerSecond=%s",
            progress.batchId,
            progress.numInputRows,
            progress.processedRowsPerSecond,
        )

    def onQueryTerminated(self, event: QueryTerminatedEvent) -> None:
        logger.info("stream query terminated id=%s", event.id)
