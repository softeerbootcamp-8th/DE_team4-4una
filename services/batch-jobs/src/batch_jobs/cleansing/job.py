"""Entry point that passes cleansed Bronze events directly to feature processing."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from de4_core import perf_phase
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession

from batch_jobs.cleansing.config import CleansingJobConfig
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
from batch_jobs.schemas import RAW_RECORD_COLUMN

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CleansedSensorEvents:
    """Typed passed rows and quarantined rows from one cleansing invocation."""

    processed: DataFrame
    quarantined: DataFrame
    # dedup Window 캐시(validate.CleansingResult.cache_to_release)가 있으면 그대로 전달한다.
    dedup_cache: DataFrame | None = None


@dataclass(frozen=True, slots=True)
class CleansingJobSummary:
    input_count: int
    processed_count: int
    accepted_count: int
    cleansing_quarantined_count: int
    map_matching_quarantined_count: int
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

    rules = load_cleansing_config(cleansing_config.rules_config_path)
    window_start, window_end = feature_input_window(target_hour)
    processed_frames: list[DataFrame] = []
    # 여기서 캐시해 넘긴 프레임(hourly bronze, dedup Window 결과)을 모아 뒀다가
    # 아래 finally에서 한 번에 unpersist한다.
    cached_frames: list[DataFrame] = []
    target_processed: DataFrame | None = None
    target_quarantined: DataFrame | None = None
    target_bronze: DataFrame | None = None

    # T1 cleanse와 T2 feature가 한 Spark 세션에서 이어 돌기 때문에(#205, ADR-0006)
    # Job Run 총시간만으로는 둘을 못 가른다. 둘 다 phase로 재서 뺄셈 없이 읽는다(#527) —
    # 예전에는 T2만 재고 T1은 CLI 경계의 sensor_processing.job에서 빼서 얻었다(#461).
    # 두 phase 모두 이미 있던 count() 경계에 붙으므로 액션을 새로 강제하지 않는다.
    with perf_phase(logger, "sensor_processing.cleanse") as cleanse_fields:
        for hour in _overlapping_hours(window_start, window_end):
            bronze_hour = read_bronze_sensor_events(
                spark, cleansing_config.bronze_input_path, hour
            )
            hourly_bronze = filter_bronze_sensor_events_for_hour(bronze_hour, hour)
            cleansed = cleanse_bronze_sensor_events(
                hourly_bronze,
                rules,
                run_id,
                processed_at,
            )
            cached_frames.append(hourly_bronze)
            if cleansed.dedup_cache is not None:
                cached_frames.append(cleansed.dedup_cache)
            processed_frames.append(cleansed.processed)
            if hour == target_hour:
                target_processed = cleansed.processed
                target_quarantined = cleansed.quarantined
                target_bronze = hourly_bronze

        if (
            target_processed is None
            or target_quarantined is None
            or target_bronze is None
        ):
            raise ValueError("feature input window does not contain target_hour")

        processed_window = _union_frames(processed_frames).persist(
            StorageLevel.MEMORY_AND_DISK
        )
        # persist는 lazy라 여기서 즉시 materialize하지 않으면, 바로 아래 첫 action이
        # processed_window가 아닌 다른 DataFrame을 건드려서 실제 계산이
        # run_hourly_segment_feature_job 안의 target_df.count()(#386)까지 조용히
        # 미뤄진다(#389). count()로 여기서 먼저 materialize하고 그 값을 재사용한다.
        processed_window_count = processed_window.count()
        target_quarantined = target_quarantined.persist(StorageLevel.MEMORY_AND_DISK)
        # feature_input_window()가 항상 target_hour 한 시간만 돌려주므로(_overlapping_hours가
        # 원소 1개짜리 리스트를 만들고 _union_frames가 그 원소를 그대로 돌려줌)
        # processed_window는 target_processed와 같은 객체라 위에서 이미 잰 값을 그대로
        # 쓴다. 창이 넓어져 여러 시간이 섞이면(len(processed_frames) > 1) target_processed만
        # 별도로 다시 센다.
        processed_count = (
            processed_window_count
            if len(processed_frames) == 1
            else target_processed.count()
        )
        cleansing_quarantined_count = target_quarantined.count()
        cleanse_fields["rows"] = processed_count + cleansing_quarantined_count
    try:
        with perf_phase(logger, "sensor_processing.features"):
            feature_summary = run_hourly_segment_feature_job(
                spark,
                processed_window,
                feature_config,
                target_hour,
                road_snapshot_date,
                feature_version,
                run_id,
                processed_at,
                cleansing_quarantine=target_quarantined,
                cleansing_quarantined_count=cleansing_quarantined_count,
                # 맵매칭 실패 격리가 원문을 붙일 때만 쓰는 조회용 두 컬럼.
                # 이미 캐시된 대상 시간 Bronze에서 뽑으므로 다시 읽지 않는다.
                raw_record_source=target_bronze.select("event_id", RAW_RECORD_COLUMN),
                quarantine_output_path=cleansing_config.quarantine_output_path,
            )
    finally:
        target_quarantined.unpersist()
        processed_window.unpersist()
        for cached_frame in cached_frames:
            cached_frame.unpersist()

    _log_summary(
        target_hour=target_hour,
        window_start=window_start,
        window_end=window_end,
        bronze_input_path=cleansing_config.bronze_input_path,
        # 격리 쓰기가 run_hourly_segment_feature_job 안으로 옮겨가면서(#438) 실제로
        # 쓰인 경로는 feature_summary에만 있다 — 설정 root가 아니라 이 값을 남긴다.
        quarantine_output_path=feature_summary.quarantine_output_path,
        feature_output_path=feature_summary.output_path,
        processed_count=processed_count,
        accepted_count=feature_summary.accepted_count,
        cleansing_quarantined_count=cleansing_quarantined_count,
        map_matching_quarantined_count=feature_summary.map_matching_quarantined_count,
        quarantined_count=feature_summary.quarantined_count,
        feature_count=feature_summary.result_count,
    )
    logger.info(
        "hourly sensor processing finished run_id=%s elapsed=%.1fs",
        run_id,
        time.monotonic() - started,
    )
    return CleansingJobSummary(
        input_count=processed_count + cleansing_quarantined_count,
        processed_count=processed_count,
        accepted_count=feature_summary.accepted_count,
        cleansing_quarantined_count=cleansing_quarantined_count,
        map_matching_quarantined_count=feature_summary.map_matching_quarantined_count,
        quarantined_count=feature_summary.quarantined_count,
        quarantine_output_path=feature_summary.quarantine_output_path,
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
        dedup_cache=result.cache_to_release,
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
    *,
    target_hour: datetime,
    window_start: datetime,
    window_end: datetime,
    bronze_input_path: str,
    quarantine_output_path: str,
    feature_output_path: str,
    processed_count: int,
    accepted_count: int,
    cleansing_quarantined_count: int,
    map_matching_quarantined_count: int,
    quarantined_count: int,
    feature_count: int,
) -> None:
    """무엇을 어느 구간에서 읽어 어디에 썼는지 요약 한 줄에 다 담는다(#406).

    시작 로그에도 경로가 있지만 그건 설정된 root라, 실제로 쓰인 시간별 경로
    (quarantine_output_path/feature_output_path)와 feature 입력 윈도우는 여기서만
    확인된다. 요약 한 줄만 보고도 추적이 되도록 경로를 다시 남긴다.
    """
    logger.info(
        "target_hour=%s feature_input_window=[%s, %s) bronze=%s quarantine=%s "
        "features=%s input=%d cleansed=%d accepted=%d "
        "cleansing_quarantined=%d map_match_quarantined=%d quarantined=%d "
        "features_written=%d",
        target_hour.isoformat(),
        window_start.isoformat(),
        window_end.isoformat(),
        bronze_input_path,
        quarantine_output_path,
        feature_output_path,
        processed_count + cleansing_quarantined_count,
        processed_count,
        accepted_count,
        cleansing_quarantined_count,
        map_matching_quarantined_count,
        quarantined_count,
        feature_count,
    )
