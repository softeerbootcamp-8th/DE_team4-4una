"""Entry point for the Bronze-to-Silver sensor event cleansing job."""

from __future__ import annotations

import logging
import time
from datetime import datetime

from pyspark.sql import SparkSession

from batch_jobs.cleansing.config import CleansingJobConfig
from batch_jobs.cleansing.reader import (
    filter_bronze_sensor_events_for_hour,
    read_bronze_sensor_events,
)
from batch_jobs.cleansing.rules import load_cleansing_config
from batch_jobs.cleansing.sink import to_processed_sensor_events
from batch_jobs.cleansing.hourly_storage import write_hourly_cleansing_results
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
    target_hour: datetime,
    processed_at: datetime,
) -> None:
    """Cleanse and replace one UTC hour of Silver and quarantine rows."""
    started = time.monotonic()
    logger.info("cleansing started run_id=%s target_hour=%s", run_id, target_hour.isoformat())
    logger.info("  input=%s", config.bronze_input_path)
    logger.info("  processed=%s", config.processed_output_path)
    logger.info("  quarantine=%s", config.quarantine_output_path)

    bronze = filter_bronze_sensor_events_for_hour(
        read_bronze_sensor_events(spark, config.bronze_input_path), target_hour
    )
    result = cleanse_sensor_events(bronze, load_cleansing_config(), run_id, processed_at)
    processed = to_processed_sensor_events(result.passed, run_id, processed_at)

    write_result = write_hourly_cleansing_results(
        spark,
        processed,
        result.quarantined,
        config.processed_output_path,
        config.quarantine_output_path,
        target_hour,
        run_id,
    )

    _log_summary(
        write_result.processed_count,
        write_result.quarantined_count,
        target_hour,
    )
    logger.info("cleansing finished run_id=%s elapsed=%.1fs", run_id, time.monotonic() - started)


def _log_summary(processed_count: int, quarantined_count: int, target_hour: datetime) -> None:
    logger.info(
        "target_hour=%s input=%d passed=%d quarantined=%d",
        target_hour.isoformat(),
        processed_count + quarantined_count,
        processed_count,
        quarantined_count,
    )
