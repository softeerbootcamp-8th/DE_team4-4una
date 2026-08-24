"""Build hourly segment features from an in-memory cleansed sensor DataFrame."""

from __future__ import annotations

import logging
import os
import time
import urllib.parse
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from batch_jobs.cleansing.hourly_storage import write_hourly_quarantine
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
from batch_jobs.schemas import RAW_RECORD_COLUMN
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
    road_segment_path: str
    output_path: str
    event_feature_config_path: Path
    steering_feature_config_path: Path
    map_matching_config_path: Path

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> HourlySegmentFeatureJobConfig:
        source = env if env is not None else os.environ
        return cls(
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
    accepted_count: int
    map_matching_quarantined_count: int
    quarantined_count: int
    quarantine_output_path: str
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
    sensor_df: DataFrame,
    config: HourlySegmentFeatureJobConfig,
    target_hour: datetime,
    road_snapshot_date: date,
    feature_version: str,
    run_id: str,
    processed_at: datetime,
    *,
    cleansing_quarantine: DataFrame,
    quarantine_output_path: str,
) -> HourlySegmentFeatureJobSummary:
    _validate_job_arguments(target_hour, feature_version, run_id, processed_at)

    event_config = load_event_feature_config(config.event_feature_config_path)
    steering_config = load_steering_feature_config(config.steering_feature_config_path)
    matching_config = load_map_matching_config(config.map_matching_config_path)

    window_start, window_end = feature_input_window(target_hour)
    target_hour_end = target_hour + timedelta(hours=1)
    sensor_df = sensor_df.filter(
        (F.col("event_time") >= window_start) & (F.col("event_time") < window_end)
    )

    # config.road_segment_path는 Manifest의 road_segment Artifact URI 그대로다 — 경로를 추가로 조립하지 않는다.
    road_segment_df = spark.read.parquet(
        _decode_local_file_uri(config.road_segment_path)
    ).select(*ROAD_SEGMENT_COLUMNS)
    _require_matching_road_snapshot_date(road_segment_df, road_snapshot_date)

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

    matched_df = sensor_df.join(selected, on="event_id", how="left")

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

    # 입력이 이미 대상 시간뿐이라 이 필터는 안전장치다. 재계산 방지를 위해 persist한다.
    target_df = event_df.filter(
        (F.col("event_time") >= target_hour) & (F.col("event_time") < target_hour_end)
    ).persist(StorageLevel.MEMORY_AND_DISK)
    # persist는 lazy라 여기서 즉시 materialize하지 않으면, Map Matching을 포함한
    # 전체 상류 lineage가 뒤쪽의 첫 action(validate_hourly_segment_features 내부)에서야
    # 실행돼 실패 스택트레이스가 엉뚱하게 aggregation.py를 가리킨다(#386). count()로
    # 여기서 먼저 materialize해 실패 지점을 Map Matching 단계로 정확히 드러낸다.
    target_count = target_df.count()

    try:
        map_matching_quarantine = _build_map_matching_quarantine(
            target_df, run_id, processed_at
        ).persist(StorageLevel.MEMORY_AND_DISK)
        try:
            map_matching_quarantined_count = map_matching_quarantine.count()
            combined_quarantine = cleansing_quarantine.unionByName(
                map_matching_quarantine
            )
            quarantine_write = write_hourly_quarantine(
                spark,
                combined_quarantine,
                quarantine_output_path,
                target_hour,
                run_id,
            )
        finally:
            map_matching_quarantine.unpersist()

        accepted_count = target_count - map_matching_quarantined_count
        result = build_hourly_segment_features(
            target_df, feature_version=feature_version, run_id=run_id, processed_at=processed_at
        ).persist(StorageLevel.MEMORY_AND_DISK)
        try:
            write_result = write_hourly_segment_features(
                spark, result, config.output_path, target_hour, run_id
            )
            _log_summary(
                run_id,
                target_hour,
                sensor_df,
                target_count,
                accepted_count,
                map_matching_quarantined_count,
                quarantine_write.row_count,
                write_result,
            )
            return HourlySegmentFeatureJobSummary(
                result_count=write_result.row_count,
                accepted_count=accepted_count,
                map_matching_quarantined_count=map_matching_quarantined_count,
                quarantined_count=quarantine_write.row_count,
                quarantine_output_path=quarantine_write.output_path,
                output_path=write_result.output_path,
                target_hour=target_hour,
                run_id=run_id,
            )
        finally:
            result.unpersist()
    finally:
        target_df.unpersist()


def _build_map_matching_quarantine(
    matched: DataFrame,
    run_id: str,
    rejected_at: datetime,
) -> DataFrame:
    """맵매칭 실패 행을 공통 sensor_event_quarantine 형태로 변환한다."""
    reject_detail = F.to_json(
        F.struct(
            F.lit("distance+heading").alias("match_method"),
            F.col("map_match_status"),
            F.col("road_snapshot_date"),
            F.col("map_match_distance_m"),
            F.col("map_match_heading_diff_deg"),
            F.col("map_match_score"),
            F.col("candidate_count"),
        ),
        options={"ignoreNullFields": "false"},
    )
    return matched.filter(F.col("segment_id").isNull()).select(
        "event_id",
        "trip_id",
        F.to_date("event_time").alias("event_date"),
        F.lit("MAP_MATCH_FAILED").alias("reject_reason"),
        reject_detail.alias("reject_detail"),
        F.col(RAW_RECORD_COLUMN).alias("raw_record"),
        F.lit(run_id).alias("_run_id"),
        F.lit(rejected_at).alias("_rejected_at"),
        F.lit(rejected_at.date()).alias("rejected_date"),
    )


def feature_input_window(target_hour: datetime) -> tuple[datetime, datetime]:
    """Return the event-time interval T2 reads for one target hour."""
    # 대상 시간만 읽는다. 앞뒤로 여유 구간을 두면 입력 구간이 이웃 시간까지 걸치고,
    # 그 시간마다 Bronze를 통째로 다시 읽게 된다. 뒤쪽 여유는 애초에 얻을 수도 없다 —
    # 13시 잡은 14시에 도는데 그 시점엔 14시 데이터가 아직 없다.
    # 시간 경계에 걸친 episode는 길이가 짧게 잡혀 min_event_duration_seconds에서 걸러진다.
    return target_hour, target_hour + timedelta(hours=1)


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


def _decode_local_file_uri(path: str) -> str:
    # de4_core.join_uri()가 로컬 file:// URI에서 '='를 %3D로 인코딩하는데 Spark는 이를 못 읽어서 디코딩한다.
    if path.startswith("file://"):
        return urllib.parse.unquote(path)
    return path


def _require_matching_road_snapshot_date(
    road_segment_df: DataFrame, road_snapshot_date: date
) -> None:
    # 엉뚱한 snapshot의 road_segment를 넘겨받았을 때 값비싼 Map Matching 전에 바로 실패하게 한다.
    mismatched = road_segment_df.filter(F.col("snapshot_date") != F.lit(road_snapshot_date))
    if mismatched.limit(1).count():
        raise ValueError(
            f"road_segment_path contains snapshot_date values other than "
            f"{road_snapshot_date.isoformat()}"
        )


def _log_summary(
    run_id: str,
    target_hour: datetime,
    sensor_df: DataFrame,
    target_count: int,
    accepted_count: int,
    map_matching_quarantined_count: int,
    quarantined_count: int,
    write_result: HourlySegmentFeatureWriteResult,
) -> None:
    started = time.monotonic()
    sensor_count = sensor_df.count()
    logger.info(
        "hourly segment feature job finished run_id=%s target_hour=%s "
        "read=%d target=%d accepted=%d map_match_quarantined=%d "
        "quarantined=%d result=%d output_path=%s elapsed=%.1fs",
        run_id,
        target_hour.isoformat(),
        sensor_count,
        target_count,
        accepted_count,
        map_matching_quarantined_count,
        quarantined_count,
        write_result.row_count,
        write_result.output_path,
        time.monotonic() - started,
    )
