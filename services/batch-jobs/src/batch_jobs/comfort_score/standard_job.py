"""Spark batch entry point for standard_segment_comfort_score loading (#198).

context/comfort-score.md "Migration order" 2단계. 함께 있던 구 segment_comfort_score
경로(gold_job/gold_writer)는 7단계에서 제거했다(#227).
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from batch_jobs.comfort_score.config import (
    DEFAULT_COMFORT_SCORE_CONFIG_PATH,
    load_comfort_score_config,
)
from batch_jobs.comfort_score.formula import compute_standard_comfort_scores
from batch_jobs.comfort_score.loader import (
    DEFAULT_WINDOW_HOURS,
    load_hourly_comfort_score_for_gold,
)
from batch_jobs.comfort_score.standard_writer import (
    EXPECTED_STAGING_COLUMNS,
    write_standard_comfort_scores,
)
from batch_jobs.comfort_score.universe import load_universe

logger = logging.getLogger(__name__)

# 로컬 개발에서는 Spark가 Maven에서 자동으로 받는다. 버전은 정확히 고정한다.
POSTGRES_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.4"

# EMR Serverless는 job 실행마다 Maven Central까지 나가는 네트워크 경로가 없거나
# 불안정할 수 있다(ADR-0001). POSTGRES_JDBC_JAR_URI가 설정되면 미리 S3에 올려둔
# jar를 spark.jars로 직접 참조하고, 없으면(로컬 개발) 기존처럼 Maven에서 받는다.
POSTGRES_JDBC_JAR_URI_ENV = "POSTGRES_JDBC_JAR_URI"


@dataclass(frozen=True, slots=True)
class StandardComfortScoreJobConfig:
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
    def from_env(cls, env: Mapping[str, str] | None = None) -> StandardComfortScoreJobConfig:
        source = env if env is not None else os.environ
        return cls(
            data_lake_uri=source.get("STANDARD_COMFORT_SCORE_DATA_LAKE_URI")
            or "data/local-lake",
            window_hours=int(
                source.get("STANDARD_COMFORT_SCORE_WINDOW_HOURS")
                or DEFAULT_WINDOW_HOURS
            ),
            comfort_score_config_path=Path(
                source.get("STANDARD_COMFORT_SCORE_CONFIG_PATH")
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
class StandardComfortScoreJobSummary:
    scored_count: int
    merged_count: int
    inserted_count: int
    updated_count: int


def _postgres_jdbc_spark_config(
    env: Mapping[str, str] | None = None,
) -> tuple[str, str]:
    """Postgres JDBC 드라이버를 어떤 spark.jars* 설정으로 로드할지 고른다."""
    source = env if env is not None else os.environ
    jar_uri = source.get(POSTGRES_JDBC_JAR_URI_ENV)
    if jar_uri:
        return "spark.jars", jar_uri
    return "spark.jars.packages", POSTGRES_JDBC_PACKAGE


def build_spark_session() -> SparkSession:
    jdbc_config_key, jdbc_config_value = _postgres_jdbc_spark_config()
    return (
        SparkSession.builder.appName("standard-segment-comfort-score-load")
        .config("spark.sql.session.timeZone", "UTC")
        .config(jdbc_config_key, jdbc_config_value)
        .getOrCreate()
    )


def run_standard_comfort_score_job(
    spark: SparkSession,
    config: StandardComfortScoreJobConfig,
    as_of: datetime,
    connection,
) -> StandardComfortScoreJobSummary:
    """168h 윈도우를 읽어 standard 점수를 산출한 뒤 PostgreSQL에 UPSERT한다.

    `as_of`가 그대로 `score_as_of`가 된다 — 데이터에서 유도하지 않는 실행 식별자다.
    universe가 비어 있지 않은 한 산출 행 수는 입력 데이터가 아니라 도로망 크기로
    정해지므로, 0행은 정상 상황이 아니라 설정 오류에 가깝다.
    """
    _validate_as_of(as_of)
    hourly_df = load_hourly_comfort_score_for_gold(
        spark, config.data_lake_uri, as_of, config.window_hours
    )
    universe_df = load_universe(spark, config.data_lake_uri, connection)
    scoring_config = load_comfort_score_config(config.comfort_score_config_path)

    scored = _select_staging_columns(
        _attach_score_as_of(
            _fill_missing_periods(
                _attach_calculated_at(
                    compute_standard_comfort_scores(
                        hourly_df, scoring_config, universe_df
                    ),
                    as_of,
                ),
                as_of,
                config.window_hours,
            ),
            as_of,
        )
    ).persist(StorageLevel.MEMORY_AND_DISK)

    try:
        scored_count = scored.count()
        if scored_count == 0:
            raise RuntimeError(
                "standard comfort score job produced 0 rows — the universe resolved "
                "to no (segment_id, vehicle_profile_id) combination, or the window "
                "had no qualifying hour at all so no population mean could be formed"
            )

        write_summary = write_standard_comfort_scores(
            scored, config.jdbc_url, config.postgres_user, config.postgres_password,
            connection,
        )
        summary = StandardComfortScoreJobSummary(
            scored_count=scored_count,
            merged_count=write_summary.staging_count,
            inserted_count=write_summary.inserted_count,
            updated_count=write_summary.updated_count,
        )
        logger.info(
            "standard comfort score job finished scored=%d inserted=%d updated=%d",
            summary.scored_count,
            summary.inserted_count,
            summary.updated_count,
        )
        return summary
    finally:
        scored.unpersist()


def _select_staging_columns(df: DataFrame) -> DataFrame:
    """compute_standard_comfort_scores()가 남기는 qualifying_hours/observed_score/
    population_mean 같은 진단용 컬럼을 걸러, staging 테이블 컬럼과 정확히 맞춘다.
    안 그러면 JDBC write가 컬럼 불일치로 실패한다."""
    return df.select(*EXPECTED_STAGING_COLUMNS)


def _fill_missing_periods(df: DataFrame, as_of: datetime, window_hours: int) -> DataFrame:
    """qualifying_hours=0인 행은 formula.py에서 MIN/MAX로 롤업할 시간이 없어
    data_period_start/data_period_end가 NULL로 나온다. 확정 스키마에서 두 컬럼은
    NOT NULL이므로, 이 실행이 커버하려던 배치 윈도우 [as_of - window_hours, as_of)로
    채운다 — loader.py._filter_window_hours와 동일한 경계 정의를 쓴다."""
    window_start = as_of - timedelta(hours=window_hours)
    return df.withColumn(
        "data_period_start", F.coalesce(F.col("data_period_start"), F.lit(window_start))
    ).withColumn(
        "data_period_end", F.coalesce(F.col("data_period_end"), F.lit(as_of))
    )


def _attach_score_as_of(df: DataFrame, as_of: datetime) -> DataFrame:
    return df.withColumn("score_as_of", F.lit(as_of))


def _attach_calculated_at(df: DataFrame, as_of: datetime) -> DataFrame:
    # F.current_timestamp()가 아니라 as_of 리터럴을 드라이버에서 한 번만
    # 계산해 고정한다 — 태스크마다 다른 값이 나오면 같은 실행 안에서도
    # calculated_at이 흔들려 stale 판정/재실행 비교가 어려워진다.
    return df.withColumn("calculated_at", F.lit(as_of))


def _validate_as_of(as_of: datetime) -> None:
    if as_of.utcoffset() is None:
        raise ValueError("as_of must be timezone-aware")
