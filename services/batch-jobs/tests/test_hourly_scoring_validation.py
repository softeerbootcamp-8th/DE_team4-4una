"""Tests for batch_jobs/hourly_scoring_validation.py (#249, #469)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from batch_jobs.hourly_comfort_storage import hour_output_path
from batch_jobs.hourly_scoring_validation import (
    DEFAULT_SCORE_RANGES_SUITE_PATH,
    HourlyScoringValidationConfig,
    HourlyScoringValidationFailed,
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

    def test_from_env_reads_overrides(self) -> None:
        config = HourlyScoringValidationConfig.from_env(
            {
                "HOURLY_COMFORT_OUTPUT_PATH": "custom/hourly_comfort_score",
                "HOURLY_SCORING_SCORE_RANGES_SUITE_PATH": "custom/ranges.json",
            }
        )

        assert config.score_output_path == "custom/hourly_comfort_score"
        assert config.score_ranges_suite_path == Path("custom/ranges.json")


class TestLoadExpectationSuite:
    def test_loads_the_committed_score_ranges_suite(self) -> None:
        suite = load_expectation_suite(DEFAULT_SCORE_RANGES_SUITE_PATH)

        assert suite.name == "hourly_comfort_score_suite"
        assert len(suite.expectations) == 4


class TestRunHourlyScoringValidation:
    """`run_hourly_scoring`이 방금 쓴 target_hour 파티션만 검증한다(#469)."""

    TARGET_HOUR = PROCESSED_AT

    def score_row(self, **overrides: object) -> dict[str, object]:
        period_start = overrides.pop("period_start", self.TARGET_HOUR)
        row = {
            "segment_id": "S1",
            "vehicle_profile_id": 1,
            "data_period_start": period_start,
            "data_period_end": period_start + timedelta(hours=1),
            "road_snapshot_date": period_start.date(),
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

    def write_partition(self, spark, config, target_hour, rows) -> None:
        self.score_rows_df(spark, rows).write.mode("overwrite").parquet(
            hour_output_path(config.score_output_path, target_hour)
        )

    def config(self, tmp_path: Path) -> HourlyScoringValidationConfig:
        return HourlyScoringValidationConfig(
            score_output_path=str(tmp_path / "hourly_comfort_score"),
            score_ranges_suite_path=DEFAULT_SCORE_RANGES_SUITE_PATH,
        )

    def test_succeeds_when_scores_are_in_range(self, spark, tmp_path) -> None:
        config = self.config(tmp_path)
        self.write_partition(
            spark, config, self.TARGET_HOUR, [self.score_row() for _ in range(20)]
        )

        summary = run_hourly_scoring_validation(spark, config, self.TARGET_HOUR)

        assert summary.success
        assert summary.target_hour == self.TARGET_HOUR
        assert summary.row_count == 20

    def test_ignores_other_hours(self, spark, tmp_path) -> None:
        """다른 시간대의 잘못된 값에 영향받지 않는다 — 전체를 읽던 시절과의 차이다."""
        config = self.config(tmp_path)
        other_hour = self.TARGET_HOUR + timedelta(hours=1)
        self.write_partition(spark, config, self.TARGET_HOUR, [self.score_row()])
        self.write_partition(
            spark,
            config,
            other_hour,
            [self.score_row(period_start=other_hour, vertical_score=150.0)],
        )

        summary = run_hourly_scoring_validation(spark, config, self.TARGET_HOUR)

        assert summary.success
        assert summary.row_count == 1

    def test_raises_when_a_score_is_out_of_range(self, spark, tmp_path) -> None:
        config = self.config(tmp_path)
        self.write_partition(
            spark, config, self.TARGET_HOUR, [self.score_row(vertical_score=150.0)]
        )

        with pytest.raises(HourlyScoringValidationFailed):
            run_hourly_scoring_validation(spark, config, self.TARGET_HOUR)

    def test_raises_when_scoring_version_is_not_semver(self, spark, tmp_path) -> None:
        config = self.config(tmp_path)
        self.write_partition(
            spark, config, self.TARGET_HOUR, [self.score_row(scoring_version="v1")]
        )

        with pytest.raises(HourlyScoringValidationFailed):
            run_hourly_scoring_validation(spark, config, self.TARGET_HOUR)

    def test_raises_when_the_target_hour_partition_is_missing(self, spark, tmp_path) -> None:
        config = self.config(tmp_path)
        # 다른 시간대만 있는 상태 — 루트는 존재하지만 대상 파티션은 없다.
        self.write_partition(
            spark,
            config,
            self.TARGET_HOUR + timedelta(hours=1),
            [self.score_row(period_start=self.TARGET_HOUR + timedelta(hours=1))],
        )

        with pytest.raises(HourlyScoringValidationFailed, match="no hourly_comfort_score"):
            run_hourly_scoring_validation(spark, config, self.TARGET_HOUR)

    def test_raises_when_the_partition_has_zero_rows(self, spark, tmp_path) -> None:
        # 파티션 writer가 빈 결과를 막지만(#469), 검증도 자체적으로 확인한다.
        config = self.config(tmp_path)
        self.write_partition(spark, config, self.TARGET_HOUR, [])

        with pytest.raises(HourlyScoringValidationFailed, match="zero rows"):
            run_hourly_scoring_validation(spark, config, self.TARGET_HOUR)
