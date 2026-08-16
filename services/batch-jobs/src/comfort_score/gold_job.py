"""Spark batch entry point for Gold segment_comfort_score loading (#129)."""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from comfort_score.config import (
    DEFAULT_COMFORT_SCORE_CONFIG_PATH,
    load_comfort_score_config,
)
from comfort_score.formula import compute_segment_comfort_scores
from comfort_score.gold_writer import write_segment_comfort_scores
from comfort_score.loader import (
    DEFAULT_WINDOW_HOURS,
    load_hourly_comfort_score_for_gold,
)

logger = logging.getLogger(__name__)

# 운영 배포 이미지에는 미리 구워 넣는 걸 후속 과제로 남긴다 — 로컬 개발에서는
# Spark가 Maven에서 자동으로 받는다. 버전은 정확히 고정한다.
POSTGRES_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.4"


@dataclass(frozen=True, slots=True)
class SegmentComfortScoreJobConfig:
    data_lake_uri: str
    window_hours: int
    comfort_score_config_path: Path
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> SegmentComfortScoreJobConfig:
        source = env if env is not None else os.environ
        return cls(
            data_lake_uri=source.get("SEGMENT_COMFORT_SCORE_DATA_LAKE_URI")
            or "data/local-lake",
            window_hours=int(
                source.get("SEGMENT_COMFORT_SCORE_WINDOW_HOURS") or DEFAULT_WINDOW_HOURS
            ),
            comfort_score_config_path=Path(
                source.get("SEGMENT_COMFORT_SCORE_CONFIG_PATH")
                or DEFAULT_COMFORT_SCORE_CONFIG_PATH
            ),
            postgres_host=_require(source, "POSTGRES_HOST"),
            postgres_port=int(_require(source, "POSTGRES_PORT")),
            postgres_db=_require(source, "POSTGRES_DB"),
            postgres_user=_require(source, "POSTGRES_USER"),
            postgres_password=_require(source, "POSTGRES_PASSWORD"),
        )


def _require(source: Mapping[str, str], key: str) -> str:
    value = source.get(key)
    if not value:
        raise ValueError(f"{key} must be set")
    return value


@dataclass(frozen=True, slots=True)
class SegmentComfortScoreJobSummary:
    scored_count: int
    merged_count: int
    inserted_count: int
    updated_count: int


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("segment-comfort-score-gold-load")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", POSTGRES_JDBC_PACKAGE)
        .getOrCreate()
    )


def run_segment_comfort_score_job(
    spark: SparkSession,
    config: SegmentComfortScoreJobConfig,
    as_of: datetime,
    connection,
) -> SegmentComfortScoreJobSummary:
    """168h 윈도우를 읽어 Gold 집계 후 PostgreSQL에 UPSERT한다.

    connection: 대상 Postgres에 대한 DB-API 커넥션. 0행이면 이 함수는 이
    connection을 전혀 건드리지 않고 반환한다(락도 잡지 않음).
    """
    _validate_as_of(as_of)
    hourly_df = load_hourly_comfort_score_for_gold(
        spark, config.data_lake_uri, as_of, config.window_hours
    )
    scoring_config = load_comfort_score_config(config.comfort_score_config_path)
    scored = _attach_calculated_at(
        compute_segment_comfort_scores(hourly_df, scoring_config), as_of
    ).persist(StorageLevel.MEMORY_AND_DISK)

    try:
        scored_count = scored.count()
        if scored_count == 0:
            logger.warning(
                "segment comfort score gold job produced 0 rows; skipping merge"
            )
            return SegmentComfortScoreJobSummary(0, 0, 0, 0)

        write_summary = write_segment_comfort_scores(
            scored, config.jdbc_url, config.postgres_user, config.postgres_password,
            connection,
        )
        summary = SegmentComfortScoreJobSummary(
            scored_count=scored_count,
            merged_count=write_summary.staging_count,
            inserted_count=write_summary.inserted_count,
            updated_count=write_summary.updated_count,
        )
        logger.info(
            "segment comfort score gold job finished scored=%d inserted=%d updated=%d",
            summary.scored_count,
            summary.inserted_count,
            summary.updated_count,
        )
        return summary
    finally:
        scored.unpersist()


def _attach_calculated_at(df: DataFrame, as_of: datetime) -> DataFrame:
    # F.current_timestamp()가 아니라 as_of 리터럴을 드라이버에서 한 번만
    # 계산해 고정한다 — 태스크마다 다른 값이 나오면 같은 실행 안에서도
    # calculated_at이 흔들려 stale 판정/재실행 비교가 어려워진다.
    return df.withColumn("calculated_at", F.lit(as_of))


def _validate_as_of(as_of: datetime) -> None:
    if as_of.utcoffset() is None:
        raise ValueError("as_of must be timezone-aware")
