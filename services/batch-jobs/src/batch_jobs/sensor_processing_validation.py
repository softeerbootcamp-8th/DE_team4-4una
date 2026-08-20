"""`sensor_processing` TaskGroup 산출물을 Great Expectations로 검증한다 (#220, ADR-0004).

`hourly_segment_features`의 물리량 컬럼 범위(음수 불가)와 격리(quarantine) 비율을
GX Suite로 검증한다. 스키마/필수값/PK 중복 같은 하드 인바리언트는
`sensor_features.aggregation.validate_hourly_segment_features`가 쓰기 시점에
이미 강제하므로 여기서 다시 다루지 않는다(ADR-0004: 하드 인바리언트는 GX로
옮기지 않는다).
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
from pyspark.sql import functions as F

from batch_jobs.cleansing.hourly_storage import quarantine_hour_path
from batch_jobs.hourly_segment_feature_storage import hour_output_path
from batch_jobs.resources import RESOURCE_DIR

DEFAULT_FEATURE_RANGES_SUITE_PATH = RESOURCE_DIR / "expectations" / "hourly_segment_features_suite.json"
DEFAULT_QUARANTINE_RATE_SUITE_PATH = (
    RESOURCE_DIR / "expectations" / "sensor_processing_quarantine_rate_suite.json"
)


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("validate-sensor-processing")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


@dataclass(frozen=True, slots=True)
class SensorProcessingValidationConfig:
    """`run_sensor_processing`가 쓴 것과 같은 경로를 가리켜야 하므로 같은 env var를 재사용한다."""

    feature_output_path: str
    quarantine_output_path: str
    feature_ranges_suite_path: Path
    quarantine_rate_suite_path: Path

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> SensorProcessingValidationConfig:
        source = env if env is not None else os.environ
        return cls(
            feature_output_path=source.get(
                "HOURLY_SEGMENT_FEATURE_OUTPUT_PATH",
                "data/local-lake/silver/hourly_segment_features",
            ),
            quarantine_output_path=source.get(
                "CLEANSING_QUARANTINE_OUTPUT_PATH",
                "data/local-lake/silver/sensor_event_quarantine",
            ),
            feature_ranges_suite_path=Path(
                source.get("SENSOR_PROCESSING_FEATURE_RANGES_SUITE_PATH")
                or DEFAULT_FEATURE_RANGES_SUITE_PATH
            ),
            quarantine_rate_suite_path=Path(
                source.get("SENSOR_PROCESSING_QUARANTINE_RATE_SUITE_PATH")
                or DEFAULT_QUARANTINE_RATE_SUITE_PATH
            ),
        )


@dataclass(frozen=True, slots=True)
class SensorProcessingValidationSummary:
    target_hour: datetime
    feature_row_count: int
    accepted_sample_count: int
    quarantine_row_count: int
    quarantine_rate: float
    feature_ranges_success: bool
    quarantine_rate_success: bool

    @property
    def success(self) -> bool:
        return self.feature_ranges_success and self.quarantine_rate_success


class SensorProcessingValidationFailed(Exception):
    """검증 실패 시 발생시켜 Airflow task를 hard fail시킨다(ADR-0004)."""


def compute_quarantine_rate(quarantined_count: int, accepted_count: int) -> float:
    """격리된 건수 / (격리 + 정상 처리) 건수. 아무것도 처리하지 않았으면 0."""
    total = quarantined_count + accepted_count
    if total == 0:
        return 0.0
    return quarantined_count / total


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


def read_hourly_segment_features_partition(
    spark: SparkSession, output_path: str, target_hour: datetime
) -> DataFrame | None:
    path = hour_output_path(output_path, target_hour)
    if not _path_exists(spark, path):
        return None
    return spark.read.parquet(path)


def read_quarantine_partition(
    spark: SparkSession, quarantine_output_path: str, target_hour: datetime
) -> DataFrame | None:
    path = quarantine_hour_path(quarantine_output_path, target_hour)
    if not _path_exists(spark, path):
        return None
    return spark.read.parquet(path)


def _path_exists(spark: SparkSession, path: str) -> bool:
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    filesystem = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return bool(filesystem.exists(hadoop_path))


def run_sensor_processing_validation(
    spark: SparkSession,
    config: SensorProcessingValidationConfig,
    target_hour: datetime,
) -> SensorProcessingValidationSummary:
    """`hourly_segment_features`/quarantine의 target_hour 파티션만 검증한다(in-flight, 전체 이력 아님)."""
    features_df = read_hourly_segment_features_partition(
        spark, config.feature_output_path, target_hour
    )
    if features_df is None:
        raise SensorProcessingValidationFailed(
            f"no hourly_segment_features partition found for target_hour={target_hour.isoformat()}"
        )
    quarantine_df = read_quarantine_partition(spark, config.quarantine_output_path, target_hour)

    feature_row_count = features_df.count()
    accepted_sample_count = features_df.agg(F.sum("sample_count")).first()[0] or 0
    quarantine_row_count = quarantine_df.count() if quarantine_df is not None else 0
    quarantine_rate = compute_quarantine_rate(quarantine_row_count, accepted_sample_count)

    ranges_suite = load_expectation_suite(config.feature_ranges_suite_path)
    ranges_result = validate_dataframe(features_df, ranges_suite, "hourly_segment_features")

    rate_df = spark.createDataFrame([(quarantine_rate,)], ["quarantine_rate"])
    rate_suite = load_expectation_suite(config.quarantine_rate_suite_path)
    rate_result = validate_dataframe(rate_df, rate_suite, "quarantine_rate")

    summary = SensorProcessingValidationSummary(
        target_hour=target_hour,
        feature_row_count=feature_row_count,
        accepted_sample_count=accepted_sample_count,
        quarantine_row_count=quarantine_row_count,
        quarantine_rate=quarantine_rate,
        feature_ranges_success=ranges_result.success,
        quarantine_rate_success=rate_result.success,
    )
    if not summary.success:
        raise SensorProcessingValidationFailed(
            f"sensor_processing validation failed for target_hour={target_hour.isoformat()}: {summary}"
        )
    return summary
