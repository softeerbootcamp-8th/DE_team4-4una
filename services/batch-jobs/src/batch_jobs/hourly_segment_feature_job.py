"""Spark batch entry point for Silver-to-Silver2 hourly segment feature building."""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from batch_jobs.hourly_segment_feature_storage import (
    HourlySegmentFeatureWriteResult,
    write_hourly_segment_features,
)
from batch_jobs.map_matching.candidates import (
    ROAD_SEGMENT_COLUMNS,
    find_segment_candidates,
)
from batch_jobs.map_matching.config import (
    DEFAULT_MAP_MATCHING_CONFIG_PATH,
    load_map_matching_config,
)
from batch_jobs.map_matching.scoring import score_segment_candidates
from batch_jobs.map_matching.selection import select_best_segment
from batch_jobs.schemas import PROCESSED_SENSOR_EVENT_SCHEMA
from batch_jobs.sensor_features.aggregation import build_hourly_segment_features
from batch_jobs.sensor_features.config import (
    DEFAULT_EVENT_FEATURE_CONFIG_PATH,
    DEFAULT_STEERING_FEATURE_CONFIG_PATH,
    load_event_feature_config,
    load_steering_feature_config,
)
from batch_jobs.sensor_features.events import (
    add_hard_acceleration_event,
    add_hard_braking_event,
    add_sharp_steering_event,
)
from batch_jobs.sensor_features.steering import add_steering_rate, add_steering_reversal

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class HourlySegmentFeatureJobConfig:
    processed_sensor_event_path: str
    road_segment_path: str
    output_path: str
    event_feature_config_path: Path
    steering_feature_config_path: Path
    map_matching_config_path: Path

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> HourlySegmentFeatureJobConfig:
        source = env if env is not None else os.environ
        return cls(
            processed_sensor_event_path=source.get(
                "HOURLY_SEGMENT_FEATURE_INPUT_PATH",
                "data/local-lake/silver/processed_sensor_event",
            ),
            road_segment_path=source.get(
                "HOURLY_SEGMENT_FEATURE_ROAD_SEGMENT_PATH", "data/processed/road_segment"
            ),
            output_path=source.get(
                "HOURLY_SEGMENT_FEATURE_OUTPUT_PATH",
                "data/local-lake/silver/hourly_segment_features",
            ),
            event_feature_config_path=Path(
                source.get("HOURLY_SEGMENT_FEATURE_EVENT_CONFIG_PATH")
                or DEFAULT_EVENT_FEATURE_CONFIG_PATH
            ),
            steering_feature_config_path=Path(
                source.get("HOURLY_SEGMENT_FEATURE_STEERING_CONFIG_PATH")
                or DEFAULT_STEERING_FEATURE_CONFIG_PATH
            ),
            map_matching_config_path=Path(
                source.get("HOURLY_SEGMENT_FEATURE_MAP_MATCHING_CONFIG_PATH")
                or DEFAULT_MAP_MATCHING_CONFIG_PATH
            ),
        )


@dataclass(frozen=True, slots=True)
class HourlySegmentFeatureJobSummary:
    result_count: int
    output_path: str
    target_hour: datetime
    run_id: str


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("hourly-segment-feature-building")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


# Map Matching -> Steering/Event Feature -> Hourly 집계 순으로 연결해 한 시간을 처리하고 저장한다
def run_hourly_segment_feature_job(
    spark: SparkSession,
    config: HourlySegmentFeatureJobConfig,
    target_hour: datetime,
    road_snapshot_date: date,
    feature_version: str,
    run_id: str,
    processed_at: datetime,
) -> HourlySegmentFeatureJobSummary:
    _validate_job_arguments(target_hour, feature_version, run_id, processed_at)

    event_config = load_event_feature_config(config.event_feature_config_path)
    steering_config = load_steering_feature_config(config.steering_feature_config_path)
    matching_config = load_map_matching_config(config.map_matching_config_path)

    target_hour_end = target_hour + timedelta(hours=1)
    lookback = timedelta(
        seconds=max(event_config.max_gap_seconds.value, steering_config.max_gap_seconds.value)
    )
    lookahead = timedelta(seconds=event_config.lookahead_seconds.value)
    window_start = target_hour - lookback
    window_end = target_hour_end + lookahead

    sensor_df = (
        spark.read.schema(PROCESSED_SENSOR_EVENT_SCHEMA)
        .parquet(config.processed_sensor_event_path)
        .filter(F.col("event_date").between(window_start.date(), window_end.date()))
        .filter((F.col("event_time") >= window_start) & (F.col("event_time") < window_end))
    )

    road_segment_path = (
        f"{config.road_segment_path.rstrip('/')}/"
        f"snapshot_date={road_snapshot_date.isoformat()}/data.parquet"
    )
    road_segment_df = spark.read.parquet(road_segment_path).select(*ROAD_SEGMENT_COLUMNS)

    search_radius_m = matching_config.candidate_search_radius_m.value
    candidates = find_segment_candidates(sensor_df, road_segment_df, search_radius_m)
    scored = score_segment_candidates(
        candidates,
        sensor_df,
        search_radius_m,
        matching_config.distance_weight.value,
        matching_config.heading_weight.value,
    )
    selected = select_best_segment(scored)

    matched_df = sensor_df.join(
        selected.select("event_id", "segment_id", "road_snapshot_date"), on="event_id", how="left"
    )

    steering_df = add_steering_reversal(
        add_steering_rate(matched_df, steering_config.max_gap_seconds.value),
        steering_config.steering_rate_deadband_deg_per_sec.value,
    )
    event_df = add_hard_acceleration_event(
        steering_df,
        event_config.hard_accel_threshold_mps2.value,
        event_config.min_event_duration_seconds.value,
        event_config.max_gap_seconds.value,
    )
    event_df = add_hard_braking_event(
        event_df,
        event_config.hard_brake_threshold_mps2.value,
        event_config.min_event_duration_seconds.value,
        event_config.max_gap_seconds.value,
    )
    event_df = add_sharp_steering_event(
        event_df,
        event_config.sharp_steer_threshold_deg_per_sec.value,
        event_config.sharp_steer_min_duration_seconds.value,
        event_config.max_gap_seconds.value,
    )

    # 이벤트 탐지가 끝난 뒤에야 대상 시간만 남긴다. 재계산 방지를 위해 persist한다.
    target_df = event_df.filter(
        (F.col("event_time") >= target_hour) & (F.col("event_time") < target_hour_end)
    ).persist(StorageLevel.MEMORY_AND_DISK)

    try:
        result = build_hourly_segment_features(
            target_df, feature_version=feature_version, run_id=run_id, processed_at=processed_at
        ).persist(StorageLevel.MEMORY_AND_DISK)
        try:
            write_result = write_hourly_segment_features(
                spark, result, config.output_path, target_hour, run_id
            )
            _log_summary(run_id, target_hour, sensor_df, target_df, write_result)
            return HourlySegmentFeatureJobSummary(
                result_count=write_result.row_count,
                output_path=write_result.output_path,
                target_hour=target_hour,
                run_id=run_id,
            )
        finally:
            result.unpersist()
    finally:
        target_df.unpersist()


def _validate_job_arguments(
    target_hour: datetime, feature_version: str, run_id: str, processed_at: datetime
) -> None:
    if target_hour.utcoffset() != timedelta(0):
        raise ValueError("target_hour must be UTC timezone-aware")
    if (target_hour.minute, target_hour.second, target_hour.microsecond) != (0, 0, 0):
        raise ValueError("target_hour must be truncated to the hour")
    if not feature_version.strip():
        raise ValueError("feature_version must not be blank")
    if not run_id.strip():
        raise ValueError("run_id must not be blank")
    if processed_at.utcoffset() != timedelta(0):
        raise ValueError("processed_at must be UTC timezone-aware")


def _log_summary(
    run_id: str,
    target_hour: datetime,
    sensor_df: DataFrame,
    target_df: DataFrame,
    write_result: HourlySegmentFeatureWriteResult,
) -> None:
    started = time.monotonic()
    sensor_count = sensor_df.count()
    # count 2개를 액션 1번으로 합친다.
    counts = target_df.agg(
        F.count(F.lit(1)).alias("target_count"),
        F.sum(F.when(F.col("segment_id").isNull(), 1).otherwise(0)).alias("unmatched_count"),
    ).first()
    logger.info(
        "hourly segment feature job finished run_id=%s target_hour=%s "
        "read=%d target=%d unmatched=%d result=%d output_path=%s elapsed=%.1fs",
        run_id,
        target_hour.isoformat(),
        sensor_count,
        counts["target_count"],
        counts["unmatched_count"],
        write_result.row_count,
        write_result.output_path,
        time.monotonic() - started,
    )
