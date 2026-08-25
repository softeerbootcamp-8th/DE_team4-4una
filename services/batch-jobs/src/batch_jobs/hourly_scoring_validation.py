"""`hourly_scoring` TaskGroup 산출물을 Great Expectations로 검증한다 (#249, ADR-0004).

`hourly_comfort_score`의 방향별 점수 범위(0~100)와 `scoring_version` 형식(SemVer)을
GX Suite로 검증한다. 스키마/필수값 같은 하드 인바리언트는
`HOURLY_COMFORT_SCORE_SCHEMA`(nullable=False 필드)가 쓰기 시점에 이미 강제하므로
여기서 다시 다루지 않는다(ADR-0004: 하드 인바리언트는 GX로 옮기지 않는다).

`run_hourly_scoring`이 target_hour 파티션 하나만 쓰므로(#469), 검증도 그 파티션만
대상으로 한다 — `validate_sensor_processing`과 같은 방식이다.

이전에 있던 zero-sample 비율 검증은 제거했다. `hourly_comfort.py:72`의 `eligible`
조건이 `sample_count > 0`인 행만 출력에 넣어 비율의 분자가 항상 0이었고, 구조적으로
실패할 수 없는 검증이었다.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import great_expectations as gx
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
)
from pyspark.sql import DataFrame, SparkSession

from batch_jobs.hourly_comfort_storage import hour_output_path
from batch_jobs.resources import RESOURCE_DIR

DEFAULT_SCORE_RANGES_SUITE_PATH = RESOURCE_DIR / "expectations" / "hourly_comfort_score_suite.json"


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("validate-hourly-scoring")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


@dataclass(frozen=True, slots=True)
class HourlyScoringValidationConfig:
    """`run_hourly_scoring`가 쓴 것과 같은 경로를 가리켜야 하므로 같은 env var를 재사용한다."""

    score_output_path: str
    score_ranges_suite_path: Path

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> HourlyScoringValidationConfig:
        source = env if env is not None else os.environ
        return cls(
            score_output_path=source.get(
                "HOURLY_COMFORT_OUTPUT_PATH",
                "data/local-lake/silver/hourly_comfort_score",
            ),
            score_ranges_suite_path=Path(
                source.get("HOURLY_SCORING_SCORE_RANGES_SUITE_PATH")
                or DEFAULT_SCORE_RANGES_SUITE_PATH
            ),
        )


@dataclass(frozen=True, slots=True)
class HourlyScoringValidationSummary:
    target_hour: datetime
    row_count: int
    score_ranges_success: bool

    @property
    def success(self) -> bool:
        return self.score_ranges_success


class HourlyScoringValidationFailed(Exception):
    """검증 실패 시 발생시켜 Airflow task를 hard fail시킨다(ADR-0004)."""


def load_expectation_suite(path: Path) -> gx.ExpectationSuite:
    payload = json.loads(Path(path).read_text())
    return gx.ExpectationSuite(**payload)


def validate_dataframe(
    df: DataFrame, suite: gx.ExpectationSuite, asset_name: str
) -> ExpectationSuiteValidationResult:
    """이미 활성화된 Spark 세션을 GX가 그대로 재사용한다(`force_reuse_spark_context` 기본값, ADR-0004)."""
    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_spark(name=f"{asset_name}_datasource")
    asset = datasource.add_dataframe_asset(name=asset_name)
    batch_definition = asset.add_batch_definition_whole_dataframe(f"{asset_name}_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    return batch.validate(suite)


def read_hourly_comfort_score_partition(
    spark: SparkSession, score_output_path: str, target_hour: datetime
) -> DataFrame | None:
    path = hour_output_path(score_output_path, target_hour)
    if not _path_exists(spark, path):
        return None
    return spark.read.parquet(path)


def _path_exists(spark: SparkSession, path: str) -> bool:
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    filesystem = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return bool(filesystem.exists(hadoop_path))


def run_hourly_scoring_validation(
    spark: SparkSession,
    config: HourlyScoringValidationConfig,
    target_hour: datetime,
) -> HourlyScoringValidationSummary:
    """`hourly_comfort_score`의 target_hour 파티션만 검증한다(in-flight, 전체 이력 아님)."""
    scores_df = read_hourly_comfort_score_partition(
        spark, config.score_output_path, target_hour
    )
    if scores_df is None:
        raise HourlyScoringValidationFailed(
            f"no hourly_comfort_score partition found for "
            f"target_hour={target_hour.isoformat()} under {config.score_output_path}"
        )

    row_count = scores_df.count()
    if row_count == 0:
        # 파티션 writer가 빈 결과를 막지만(#469), 다른 경로로 빈 파티션이 생겼을 때도
        # 검증이 조용히 통과하지 않도록 자체적으로 확인한다.
        raise HourlyScoringValidationFailed(
            f"hourly_comfort_score partition for target_hour={target_hour.isoformat()} "
            "has zero rows"
        )

    ranges_suite = load_expectation_suite(config.score_ranges_suite_path)
    ranges_result = validate_dataframe(scores_df, ranges_suite, "hourly_comfort_score")

    summary = HourlyScoringValidationSummary(
        target_hour=target_hour,
        row_count=row_count,
        score_ranges_success=ranges_result.success,
    )
    if not summary.success:
        raise HourlyScoringValidationFailed(
            f"hourly_scoring validation failed for {config.score_output_path}: {summary}"
        )
    return summary
