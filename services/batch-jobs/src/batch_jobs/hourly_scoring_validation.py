"""`hourly_scoring` TaskGroup 산출물을 Great Expectations로 검증한다 (#249, ADR-0004).

`hourly_comfort_score`의 방향별 점수 범위(0~100), `scoring_version` 형식(SemVer),
zero-sample 비율을 GX Suite로 검증한다. 스키마/필수값 같은 하드 인바리언트는
`HOURLY_COMFORT_SCORE_SCHEMA`(nullable=False 필드)가 쓰기 시점에 이미 강제하므로
여기서 다시 다루지 않는다(ADR-0004: 하드 인바리언트는 GX로 옮기지 않는다).

`run_hourly_scoring`은 `hourly_segment_features` 전체 이력을 읽어
`hourly_comfort_score` 전체를 매 실행마다 overwrite하는 풀 리컴퓨트 방식이라
(sensor_processing과 달리 hour 파티션이 없다), 검증도 target_hour 파티션이 아니라
현재 output 전체를 대상으로 한다.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import great_expectations as gx
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from batch_jobs.resources import RESOURCE_DIR

DEFAULT_SCORE_RANGES_SUITE_PATH = RESOURCE_DIR / "expectations" / "hourly_comfort_score_suite.json"
DEFAULT_ZERO_SAMPLE_RATE_SUITE_PATH = (
    RESOURCE_DIR / "expectations" / "hourly_comfort_score_zero_sample_rate_suite.json"
)


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
    zero_sample_rate_suite_path: Path

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
            zero_sample_rate_suite_path=Path(
                source.get("HOURLY_SCORING_ZERO_SAMPLE_RATE_SUITE_PATH")
                or DEFAULT_ZERO_SAMPLE_RATE_SUITE_PATH
            ),
        )


@dataclass(frozen=True, slots=True)
class HourlyScoringValidationSummary:
    row_count: int
    zero_sample_count: int
    zero_sample_rate: float
    score_ranges_success: bool
    zero_sample_rate_success: bool

    @property
    def success(self) -> bool:
        return self.score_ranges_success and self.zero_sample_rate_success


class HourlyScoringValidationFailed(Exception):
    """검증 실패 시 발생시켜 Airflow task를 hard fail시킨다(ADR-0004)."""


def compute_zero_sample_rate(zero_sample_count: int, total_count: int) -> float:
    """sample_count가 0인 행 / 전체 행. 아무것도 없으면 0."""
    if total_count == 0:
        return 0.0
    return zero_sample_count / total_count


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


def read_hourly_comfort_score(spark: SparkSession, score_output_path: str) -> DataFrame | None:
    if not _path_exists(spark, score_output_path):
        return None
    return spark.read.parquet(score_output_path)


def _path_exists(spark: SparkSession, path: str) -> bool:
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    filesystem = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return bool(filesystem.exists(hadoop_path))


def run_hourly_scoring_validation(
    spark: SparkSession,
    config: HourlyScoringValidationConfig,
) -> HourlyScoringValidationSummary:
    """`hourly_comfort_score` 전체(풀 리컴퓨트 결과)를 검증한다(in-flight)."""
    scores_df = read_hourly_comfort_score(spark, config.score_output_path)
    if scores_df is None:
        raise HourlyScoringValidationFailed(
            f"no hourly_comfort_score output found at {config.score_output_path}"
        )

    row_count = scores_df.count()
    if row_count == 0:
        # row_count=0이면 compute_zero_sample_rate가 0.0(=정상 범위)을 반환해 zero_sample_rate
        # 검증이 vacuously 통과한다(standard_score_validation.py와 동일한 가드, 리뷰 반영: #252).
        raise HourlyScoringValidationFailed(
            f"hourly_comfort_score output at {config.score_output_path} has zero rows"
        )
    zero_sample_count = scores_df.filter(F.col("sample_count") == 0).count()
    zero_sample_rate = compute_zero_sample_rate(zero_sample_count, row_count)

    ranges_suite = load_expectation_suite(config.score_ranges_suite_path)
    ranges_result = validate_dataframe(scores_df, ranges_suite, "hourly_comfort_score")

    rate_df = spark.createDataFrame([(zero_sample_rate,)], ["zero_sample_rate"])
    rate_suite = load_expectation_suite(config.zero_sample_rate_suite_path)
    rate_result = validate_dataframe(rate_df, rate_suite, "zero_sample_rate")

    summary = HourlyScoringValidationSummary(
        row_count=row_count,
        zero_sample_count=zero_sample_count,
        zero_sample_rate=zero_sample_rate,
        score_ranges_success=ranges_result.success,
        zero_sample_rate_success=rate_result.success,
    )
    if not summary.success:
        raise HourlyScoringValidationFailed(
            f"hourly_scoring validation failed for {config.score_output_path}: {summary}"
        )
    return summary
