"""Spark batch entry point for Silver2-to-Silver3 hourly comfort scoring."""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession

from batch_jobs.comfort_scoring_config import (
    DEFAULT_HOURLY_SCORING_CONFIG_PATH,
    load_hourly_scoring_config,
)
from batch_jobs.hourly_comfort import calculate_hourly_comfort_scores
from batch_jobs.schemas import (
    HOURLY_COMFORT_SCORE_SCHEMA,
    HOURLY_SEGMENT_FEATURE_SCHEMA,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class HourlyComfortJobConfig:
    feature_input_path: str
    score_output_path: str
    rejected_output_path: str
    scoring_config_path: Path

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> HourlyComfortJobConfig:
        source = env if env is not None else os.environ
        return cls(
            feature_input_path=source.get(
                "HOURLY_COMFORT_INPUT_PATH",
            )
            or "data/local-lake/silver/hourly_segment_features",
            score_output_path=source.get(
                "HOURLY_COMFORT_OUTPUT_PATH",
            )
            or "data/local-lake/silver/hourly_comfort_score",
            rejected_output_path=source.get(
                "HOURLY_COMFORT_REJECTED_OUTPUT_PATH",
            )
            or "data/local-lake/quarantine/hourly_comfort_score",
            scoring_config_path=Path(
                source.get("HOURLY_COMFORT_SCORING_CONFIG_PATH")
                or DEFAULT_HOURLY_SCORING_CONFIG_PATH
            ),
        )


@dataclass(frozen=True, slots=True)
class HourlyComfortJobSummary:
    scored_count: int
    rejected_count: int


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("hourly-comfort-scoring")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def run_hourly_comfort_job(
    spark: SparkSession,
    config: HourlyComfortJobConfig,
    run_id: str,
    processed_at: datetime,
) -> HourlyComfortJobSummary:
    """Read Silver2, calculate directional scores, and overwrite one output slice."""
    _validate_job_config(config)
    features = spark.read.schema(HOURLY_SEGMENT_FEATURE_SCHEMA).parquet(
        config.feature_input_path
    )
    scoring_config = load_hourly_scoring_config(config.scoring_config_path)
    result = calculate_hourly_comfort_scores(
        features, run_id, processed_at, scoring_config
    )
    scored = _select_declared_score_schema(result.scored).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    rejected = result.rejected.persist(StorageLevel.MEMORY_AND_DISK)

    try:
        summary = HourlyComfortJobSummary(scored.count(), rejected.count())
        # 같은 시간 범위를 재실행해도 행이 누적되지 않도록 해당 출력 경로를 교체한다
        scored.write.mode("overwrite").parquet(config.score_output_path)
        rejected.write.mode("overwrite").parquet(config.rejected_output_path)
    finally:
        scored.unpersist()
        rejected.unpersist()

    _log_summary(config, run_id=run_id, processed_at=processed_at, summary=summary)
    return summary


def _log_summary(
    config: HourlyComfortJobConfig,
    *,
    run_id: str,
    processed_at: datetime,
    summary: HourlyComfortJobSummary,
) -> None:
    """이번 실행이 어느 경로를, 어느 시각 기준으로 처리했는지 한 줄로 남긴다(#406).

    이 job은 target_hour 인자를 따로 받지 않는다 — 대상 시간대는 Airflow가
    실행마다 템플릿으로 갈아끼우는 feature_input_path에 들어 있으므로, 경로를
    남기면 어느 시간대였는지 함께 추적된다. S3 경로는 자격증명이 아니라 로그에
    남겨도 안전하다.
    """
    logger.info(
        "hourly comfort scoring finished run_id=%s processed_at=%s "
        "input=%s score_output=%s rejected_output=%s scored=%d rejected=%d",
        run_id,
        processed_at.isoformat(),
        config.feature_input_path,
        config.score_output_path,
        config.rejected_output_path,
        summary.scored_count,
        summary.rejected_count,
    )


def _select_declared_score_schema(scored: DataFrame) -> DataFrame:
    return scored.select(
        *[
            scored[field.name].cast(field.dataType).alias(field.name)
            for field in HOURLY_COMFORT_SCORE_SCHEMA
        ]
    )


def _validate_job_config(config: HourlyComfortJobConfig) -> None:
    paths = {
        config.feature_input_path,
        config.score_output_path,
        config.rejected_output_path,
    }
    if len(paths) != 3 or any(not path for path in paths):
        raise ValueError("input, score, and rejected paths must be non-empty and distinct")
