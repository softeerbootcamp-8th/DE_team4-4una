"""Entry point for the Bronze-to-Silver sensor event cleansing job."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import SparkSession

from cleansing.config import CleansingJobConfig
from cleansing.reader import read_bronze_sensor_events
from cleansing.rules import load_cleansing_config
from cleansing.sink import (
    to_processed_sensor_events,
    write_processed_sensor_events,
    write_quarantined_events,
)
from cleansing.validate import cleanse_sensor_events


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
    bronze = read_bronze_sensor_events(spark, config.bronze_input_path)
    result = cleanse_sensor_events(bronze, load_cleansing_config(), run_id, processed_at)
    processed = to_processed_sensor_events(result.passed, run_id, processed_at)

    write_processed_sensor_events(
        processed, config.processed_output_path, config.processed_partition_column
    )
    write_quarantined_events(
        result.quarantined, config.quarantine_output_path, config.quarantine_partition_column
    )
