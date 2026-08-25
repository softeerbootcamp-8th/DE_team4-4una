"""Spark batch entry point for Silver2-to-Silver3 hourly comfort scoring."""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession

from batch_jobs.comfort_scoring_config import (
    DEFAULT_HOURLY_SCORING_CONFIG_PATH,
    load_hourly_scoring_config,
)
from batch_jobs.hourly_comfort import build_hourly_scoring_plan
from batch_jobs.hourly_comfort_storage import write_hourly_comfort_partition
from batch_jobs.hourly_segment_feature_storage import (
    hour_output_path as feature_hour_path,
)
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
    target_hour: datetime,
) -> HourlyComfortJobSummary:
    """Score one Silver2 hour partition and replace the matching Silver3 partition."""
    _validate_job_config(config)
    features = spark.read.schema(HOURLY_SEGMENT_FEATURE_SCHEMA).parquet(
        feature_hour_path(config.feature_input_path, target_hour)
    )
    scoring_config = load_hourly_scoring_config(config.scoring_config_path)
    plan = build_hourly_scoring_plan(features, run_id, processed_at, scoring_config)
    # scored와 rejected는 같은 채점 결과에서 갈라지는 두 갈래다. 나눈 뒤에 각각 캐시하면
    # 공통 lineage(rate -> speed scale -> 방향별 점수)를 두 번 계산하므로, 분기 전에
    # 필요한 컬럼만 남긴 공통 결과를 한 번만 캐시한다.
    classified = plan.classified.persist(StorageLevel.MEMORY_AND_DISK)

    try:
        # 이 Action 하나가 캐시를 채우면서 입력 검증과 두 출력의 행 수를 함께 구한다.
        # 이후 쓰기는 캐시에서 필터만 하므로 점수 계산이 다시 일어나지 않는다.
        counts = plan.audit(classified)
        # 재실행해도 행이 누적되지 않도록 해당 시간 파티션만 교체한다(ADR-0011).
        score_result = write_hourly_comfort_partition(
            spark,
            plan.scored(classified),
            config.score_output_path,
            target_hour,
            run_id,
            HOURLY_COMFORT_SCORE_SCHEMA,
            expected_count=counts.scored_count,
        )
        # rejected에는 선언된 스키마 상수가 없다(hourly_comfort.py가 즉석에서 만든다).
        # writer가 read-back에 쓸 스키마가 필요해 자기 것을 그대로 넘긴다.
        # 격리 대상이 하나도 없는 것이 정상이므로 빈 결과를 허용한다 — 점수 출력과 달리
        # 0행이 이상 신호가 아니다.
        rejected = plan.rejected(classified)
        rejected_result = write_hourly_comfort_partition(
            spark,
            rejected,
            config.rejected_output_path,
            target_hour,
            run_id,
            rejected.schema,
            allow_empty=True,
            expected_count=counts.rejected_count,
        )
        summary = HourlyComfortJobSummary(
            score_result.row_count, rejected_result.row_count
        )
    finally:
        classified.unpersist()

    _log_summary(
        config,
        run_id=run_id,
        processed_at=processed_at,
        target_hour=target_hour,
        summary=summary,
    )
    return summary


def _log_summary(
    config: HourlyComfortJobConfig,
    *,
    run_id: str,
    processed_at: datetime,
    target_hour: datetime,
    summary: HourlyComfortJobSummary,
) -> None:
    """이번 실행이 어느 시간대를, 어느 경로에서 처리했는지 한 줄로 남긴다(#406, #469).

    이전에는 target_hour 인자가 없어 "대상 시간대는 Airflow가 템플릿으로 갈아끼우는
    feature_input_path에 들어 있다"고 설명했지만 사실이 아니었다 — DAG는 시간 템플릿이
    없는 고정 Variable을 넘겼다. 이제 target_hour를 직접 받아 그대로 남긴다. S3 경로는
    자격증명이 아니라 로그에 남겨도 안전하다.
    """
    logger.info(
        "hourly comfort scoring finished run_id=%s target_hour=%s processed_at=%s "
        "input=%s score_output=%s rejected_output=%s scored=%d rejected=%d",
        run_id,
        target_hour.isoformat(),
        processed_at.isoformat(),
        config.feature_input_path,
        config.score_output_path,
        config.rejected_output_path,
        summary.scored_count,
        summary.rejected_count,
    )


def _validate_job_config(config: HourlyComfortJobConfig) -> None:
    paths = {
        config.feature_input_path,
        config.score_output_path,
        config.rejected_output_path,
    }
    if len(paths) != 3 or any(not path for path in paths):
        raise ValueError("input, score, and rejected paths must be non-empty and distinct")
