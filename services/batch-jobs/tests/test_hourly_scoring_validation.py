"""Tests for batch_jobs/hourly_scoring_validation.py (#249)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from batch_jobs.hourly_scoring_validation import (
    DEFAULT_SCORE_RANGES_SUITE_PATH,
    DEFAULT_ZERO_SAMPLE_RATE_SUITE_PATH,
    HourlyScoringValidationConfig,
    HourlyScoringValidationFailed,
    compute_zero_sample_rate,
    load_expectation_suite,
    run_hourly_scoring_validation,
)
from batch_jobs.schemas import HOURLY_COMFORT_SCORE_SCHEMA
from pyspark.sql import SparkSession

PROCESSED_AT = datetime(2026, 8, 16, 10, 0, 0, tzinfo=UTC)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("batch-jobs-tests")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestHourlyScoringValidationConfig:
    def test_from_env_uses_same_default_as_the_hourly_scoring_job(self) -> None:
        config = HourlyScoringValidationConfig.from_env({})

        assert config.score_output_path == "data/local-lake/silver/hourly_comfort_score"
        assert config.score_ranges_suite_path == DEFAULT_SCORE_RANGES_SUITE_PATH
        assert config.zero_sample_rate_suite_path == DEFAULT_ZERO_SAMPLE_RATE_SUITE_PATH

    def test_from_env_reads_overrides(self) -> None:
        config = HourlyScoringValidationConfig.from_env(
            {
                "HOURLY_COMFORT_OUTPUT_PATH": "custom/hourly_comfort_score",
                "HOURLY_SCORING_SCORE_RANGES_SUITE_PATH": "custom/ranges.json",
                "HOURLY_SCORING_ZERO_SAMPLE_RATE_SUITE_PATH": "custom/rate.json",
            }
        )

        assert config.score_output_path == "custom/hourly_comfort_score"
        assert config.score_ranges_suite_path == Path("custom/ranges.json")
        assert config.zero_sample_rate_suite_path == Path("custom/rate.json")


class TestComputeZeroSampleRate:
    def test_zero_when_nothing_has_zero_samples(self) -> None:
        assert compute_zero_sample_rate(zero_sample_count=0, total_count=100) == 0.0

    def test_ratio_of_zero_sample_rows_over_total(self) -> None:
        assert compute_zero_sample_rate(zero_sample_count=5, total_count=100) == pytest.approx(0.05)

    def test_zero_when_total_is_zero(self) -> None:
        assert compute_zero_sample_rate(zero_sample_count=0, total_count=0) == 0.0


class TestLoadExpectationSuite:
    def test_loads_the_committed_score_ranges_suite(self) -> None:
        suite = load_expectation_suite(DEFAULT_SCORE_RANGES_SUITE_PATH)

        assert suite.name == "hourly_comfort_score_suite"
        assert len(suite.expectations) == 4

    def test_loads_the_committed_zero_sample_rate_suite(self) -> None:
        suite = load_expectation_suite(DEFAULT_ZERO_SAMPLE_RATE_SUITE_PATH)

        assert suite.name == "hourly_comfort_score_zero_sample_rate_suite"
        assert len(suite.expectations) == 1


class TestRunHourlyScoringValidation:
    """`write.mode("overwrite")`로 쓴 `hourly_comfort_score` 전체를 매번 다시 검증한다(풀 리컴퓨트)."""

    def score_row(self, **overrides: object) -> dict[str, object]:
        row = {
            "segment_id": "S1",
            "vehicle_profile_id": 1,
            "data_period_start": PROCESSED_AT,
            "data_period_end": PROCESSED_AT.replace(hour=PROCESSED_AT.hour + 1),
            "road_snapshot_date": PROCESSED_AT.date(),
            "vertical_score": 50.0,
            "longitudinal_score": 50.0,
            "lateral_score": 50.0,
            "scoring_version": "1.0.0",
            "sample_count": 10,
            "trip_count": 2,
            "_run_id": "run-1",
            "_processed_at": PROCESSED_AT,
        }
        row.update(overrides)
        return row

    def score_rows_df(self, spark, rows: list[dict[str, object]]):
        ordered = [
            tuple(row[field.name] for field in HOURLY_COMFORT_SCORE_SCHEMA.fields) for row in rows
        ]
        return spark.createDataFrame(ordered, HOURLY_COMFORT_SCORE_SCHEMA)

    def config(self, tmp_path: Path) -> HourlyScoringValidationConfig:
        return HourlyScoringValidationConfig(
            score_output_path=str(tmp_path / "hourly_comfort_score"),
            score_ranges_suite_path=DEFAULT_SCORE_RANGES_SUITE_PATH,
            zero_sample_rate_suite_path=DEFAULT_ZERO_SAMPLE_RATE_SUITE_PATH,
        )

    def test_succeeds_when_scores_and_zero_sample_rate_are_healthy(self, spark, tmp_path) -> None:
        config = self.config(tmp_path)
        rows = [self.score_row() for _ in range(19)] + [self.score_row(sample_count=0)]
        self.score_rows_df(spark, rows).write.mode("overwrite").parquet(config.score_output_path)

        summary = run_hourly_scoring_validation(spark, config)

        assert summary.success
        assert summary.row_count == 20
        assert summary.zero_sample_count == 1
        assert summary.zero_sample_rate == pytest.approx(0.05)

    def test_raises_when_a_score_is_out_of_range(self, spark, tmp_path) -> None:
        config = self.config(tmp_path)
        rows = [self.score_row(vertical_score=150.0)]
        self.score_rows_df(spark, rows).write.mode("overwrite").parquet(config.score_output_path)

        with pytest.raises(HourlyScoringValidationFailed):
            run_hourly_scoring_validation(spark, config)

    def test_raises_when_scoring_version_is_not_semver(self, spark, tmp_path) -> None:
        config = self.config(tmp_path)
        rows = [self.score_row(scoring_version="v1")]
        self.score_rows_df(spark, rows).write.mode("overwrite").parquet(config.score_output_path)

        with pytest.raises(HourlyScoringValidationFailed):
            run_hourly_scoring_validation(spark, config)

    def test_raises_when_zero_sample_rate_exceeds_threshold(self, spark, tmp_path) -> None:
        config = self.config(tmp_path)
        # 4/19 ≈ 0.21, suite의 max_value(0.05)를 넘는다.
        rows = [self.score_row() for _ in range(15)] + [
            self.score_row(sample_count=0) for _ in range(4)
        ]
        self.score_rows_df(spark, rows).write.mode("overwrite").parquet(config.score_output_path)

        with pytest.raises(HourlyScoringValidationFailed):
            run_hourly_scoring_validation(spark, config)

    def test_raises_when_no_output_path_exists(self, spark, tmp_path) -> None:
        config = self.config(tmp_path)

        with pytest.raises(HourlyScoringValidationFailed):
            run_hourly_scoring_validation(spark, config)

    def test_raises_when_output_has_zero_rows(self, spark, tmp_path) -> None:
        # 경로는 존재하지만 row가 0개면 zero_sample_rate가 0.0(정상 범위)으로 계산되어
        # 검증이 vacuously 통과할 수 있었던 케이스(#252 리뷰).
        config = self.config(tmp_path)
        self.score_rows_df(spark, []).write.mode("overwrite").parquet(config.score_output_path)

        with pytest.raises(HourlyScoringValidationFailed):
            run_hourly_scoring_validation(spark, config)
