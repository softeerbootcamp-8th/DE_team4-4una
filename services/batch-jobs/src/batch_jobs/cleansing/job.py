"""Entry point for the Bronze-to-Silver sensor event cleansing job."""

from __future__ import annotations

import logging
import time
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession

from batch_jobs.cleansing.config import CleansingJobConfig
from batch_jobs.cleansing.reader import read_bronze_sensor_events
from batch_jobs.cleansing.rules import load_cleansing_config
from batch_jobs.cleansing.sink import (
    to_processed_sensor_events,
    write_processed_sensor_events,
    write_quarantined_events,
)
from batch_jobs.cleansing.validate import cleanse_sensor_events

logger = logging.getLogger(__name__)


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("sensor-event-cleansing")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def run_cleansing_job(
    spark: SparkSession,
    config: CleansingJobConfig,
    run_id: str,
    processed_at: datetime,
) -> None:
    """Read Bronze, cleanse it, and store the passed and quarantined rows."""
    started = time.monotonic()
    logger.info("cleansing started run_id=%s", run_id)
    logger.info("  input=%s", config.bronze_input_path)
    logger.info("  processed=%s", config.processed_output_path)
    logger.info("  quarantine=%s", config.quarantine_output_path)

    bronze = read_bronze_sensor_events(spark, config.bronze_input_path)
    result = cleanse_sensor_events(bronze, load_cleansing_config(), run_id, processed_at)
    processed = to_processed_sensor_events(result.passed, run_id, processed_at)

    write_processed_sensor_events(
        processed, config.processed_output_path, config.processed_partition_column
    )
    write_quarantined_events(
        result.quarantined, config.quarantine_output_path, config.quarantine_partition_column
    )

    _log_summary(bronze, result.quarantined)
    logger.info("cleansing finished run_id=%s elapsed=%.1fs", run_id, time.monotonic() - started)


def _log_summary(bronze: DataFrame, quarantined: DataFrame) -> None:
    reason_counts = {
        row["reject_reason"]: row["count"]
        for row in quarantined.groupBy("reject_reason").count().collect()
    }
    input_count = bronze.count()
    quarantined_count = sum(reason_counts.values())

    logger.info(
        "input=%d passed=%d quarantined=%d",
        input_count,
        input_count - quarantined_count,
        quarantined_count,
    )
    for reason, count in sorted(reason_counts.items()):
        logger.info("  %s=%d", reason, count)
