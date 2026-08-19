"""Entry point that passes cleansed Bronze events directly to feature processing."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession

from batch_jobs.cleansing.config import CleansingJobConfig
from batch_jobs.cleansing.hourly_storage import write_hourly_quarantine
from batch_jobs.cleansing.reader import (
    filter_bronze_sensor_events_for_hour,
    read_bronze_sensor_events,
)
from batch_jobs.cleansing.rules import CleansingConfig, load_cleansing_config
from batch_jobs.cleansing.sink import to_processed_sensor_events
from batch_jobs.cleansing.validate import cleanse_sensor_events
from batch_jobs.hourly_segment_feature_job import (
    HourlySegmentFeatureJobConfig,
    HourlySegmentFeatureJobSummary,
    feature_input_window,
    run_hourly_segment_feature_job,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CleansedSensorEvents:
    """Typed passed rows and quarantined rows from one cleansing invocation."""

    processed: DataFrame
    quarantined: DataFrame


@dataclass(frozen=True, slots=True)
class CleansingJobSummary:
    processed_count: int
    quarantined_count: int
    quarantine_output_path: str
    feature_summary: HourlySegmentFeatureJobSummary


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("hourly-sensor-processing")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def run_cleansing_job(
    spark: SparkSession,
    cleansing_config: CleansingJobConfig,
    feature_config: HourlySegmentFeatureJobConfig,
    run_id: str,
    target_hour: datetime,
    road_snapshot_date: date,
    feature_version: str,
    processed_at: datetime,
) -> CleansingJobSummary:
    """Run T1 and pass its in-memory result to T2 in the same Spark session."""
    started = time.monotonic()
    logger.info(
        "hourly sensor processing started run_id=%s target_hour=%s",
        run_id,
        target_hour.isoformat(),
    )
    logger.info("  bronze=%s", cleansing_config.bronze_input_path)
    logger.info("  quarantine=%s", cleansing_config.quarantine_output_path)
    logger.info("  features=%s", feature_config.output_path)

    bronze = read_bronze_sensor_events(spark, cleansing_config.bronze_input_path)
    rules = load_cleansing_config(cleansing_config.rules_config_path)
    window_start, window_end = feature_input_window(feature_config, target_hour)
    processed_frames: list[DataFrame] = []
    cached_bronze_frames: list[DataFrame] = []
    target_processed: DataFrame | None = None
    target_quarantined: DataFrame | None = None

    for hour in _overlapping_hours(window_start, window_end):
        hourly_bronze = filter_bronze_sensor_events_for_hour(bronze, hour)
        cleansed = cleanse_bronze_sensor_events(
            hourly_bronze,
            rules,
            run_id,
            processed_at,
        )
        cached_bronze_frames.append(hourly_bronze)
        processed_frames.append(cleansed.processed)
        if hour == target_hour:
            target_processed = cleansed.processed
            target_quarantined = cleansed.quarantined

    if target_processed is None or target_quarantined is None:
        raise ValueError("feature input window does not contain target_hour")

    processed_window = _union_frames(processed_frames).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    target_quarantined = target_quarantined.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        processed_count = target_processed.count()
        quarantine_write = write_hourly_quarantine(
            spark,
            target_quarantined,
            cleansing_config.quarantine_output_path,
            target_hour,
            run_id,
        )
        feature_summary = run_hourly_segment_feature_job(
            spark,
            processed_window,
            feature_config,
            target_hour,
            road_snapshot_date,
            feature_version,
            run_id,
            processed_at,
        )
    finally:
        target_quarantined.unpersist()
        processed_window.unpersist()
        for hourly_bronze in cached_bronze_frames:
            hourly_bronze.unpersist()

    _log_summary(
        processed_count,
        quarantine_write.row_count,
        feature_summary.result_count,
        target_hour,
    )
    logger.info(
        "hourly sensor processing finished run_id=%s elapsed=%.1fs",
        run_id,
        time.monotonic() - started,
    )
    return CleansingJobSummary(
        processed_count=processed_count,
        quarantined_count=quarantine_write.row_count,
        quarantine_output_path=quarantine_write.output_path,
        feature_summary=feature_summary,
    )


def cleanse_bronze_sensor_events(
    bronze: DataFrame,
    rules: CleansingConfig,
    run_id: str,
    processed_at: datetime,
) -> CleansedSensorEvents:
    """Apply T1 rules and prepare the typed DataFrame consumed by T2."""
    result = cleanse_sensor_events(bronze, rules, run_id, processed_at)
    return CleansedSensorEvents(
        processed=to_processed_sensor_events(result.passed, run_id, processed_at),
        quarantined=result.quarantined,
    )


def _overlapping_hours(window_start: datetime, window_end: datetime) -> tuple[datetime, ...]:
    hour = window_start.replace(minute=0, second=0, microsecond=0)
    hours: list[datetime] = []
    while hour < window_end:
        hours.append(hour)
        hour += timedelta(hours=1)
    return tuple(hours)


def _union_frames(frames: list[DataFrame]) -> DataFrame:
    if not frames:
        raise ValueError("at least one cleansed DataFrame is required")
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result


def _log_summary(
    processed_count: int,
    quarantined_count: int,
    feature_count: int,
    target_hour: datetime,
) -> None:
    logger.info(
        "target_hour=%s input=%d passed=%d quarantined=%d features=%d",
        target_hour.isoformat(),
        processed_count + quarantined_count,
        processed_count,
        quarantined_count,
        feature_count,
    )
