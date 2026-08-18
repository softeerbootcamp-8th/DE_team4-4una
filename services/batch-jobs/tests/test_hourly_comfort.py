from datetime import UTC, date, datetime, timedelta

import pytest
from batch_jobs.comfort_scoring_config import (
    DEFAULT_HOURLY_SCORING_CONFIG,
    DEFAULT_HOURLY_SCORING_CONFIG_PATH,
)
from batch_jobs.hourly_comfort import calculate_hourly_comfort_scores
from batch_jobs.hourly_comfort_job import HourlyComfortJobConfig, run_hourly_comfort_job
from batch_jobs.schemas import (
    HOURLY_COMFORT_SCORE_SCHEMA,
    HOURLY_SEGMENT_FEATURE_SCHEMA,
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("hourly-comfort-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestCalculateHourlyComfortScores:
    PERIOD_START = datetime(2026, 8, 1, 10, 0, tzinfo=UTC)
    PROCESSED_AT = datetime(2026, 8, 15, 3, 0, tzinfo=UTC)

    def feature_row(self, segment_id: str = "100001", **overrides: object) -> dict[str, object]:
        row = {
            "segment_id": segment_id,
            "vehicle_profile_id": 1,
            "data_period_start": self.PERIOD_START,
            "data_period_end": self.PERIOD_START + timedelta(hours=1),
            "road_snapshot_date": date(2026, 8, 1),
            "avg_speed_mps": 7.0,
            "rms_accel_x": 0.0,
            "rms_accel_y": 0.0,
            "rms_accel_z": 0.0,
            "p95_abs_accel_x": 0.0,
            "p95_abs_accel_y": 0.0,
            "p95_abs_accel_z": 0.0,
            "rms_jerk_x": 0.0,
            "rms_jerk_y": 0.0,
            "rms_jerk_z": 0.0,
            "p95_abs_jerk_x": 0.0,
            "p95_abs_jerk_y": 0.0,
            "p95_abs_jerk_z": 0.0,
            "hard_brake_count": 0,
            "hard_accel_count": 0,
            "sharp_steer_count": 0,
            "steer_reversal_count": 0,
            "rms_steering_rate": 0.0,
            "rms_steering_vibration": 0.0,
            "sample_count": 36_000,
            "trip_count": 10,
            "feature_version": "hourly-features-v1",
            "_processed_at": self.PROCESSED_AT,
            "_run_id": "silver2-run",
        }
        return row | overrides

    def uncomfortable_row(self, segment_id: str, output_column: str) -> dict[str, object]:
        row = self.feature_row(segment_id)
        rules = {
            rule.output_column: rule for rule in DEFAULT_HOURLY_SCORING_CONFIG.components
        }
        normalizers = dict(DEFAULT_HOURLY_SCORING_CONFIG.normalizers)
        count_columns = {
            "hard_brake_rate": "hard_brake_count",
            "hard_accel_rate": "hard_accel_count",
            "sharp_steer_rate": "sharp_steer_count",
            "steer_reversal_rate": "steer_reversal_count",
        }
        for name, _ in rules[output_column].weights:
            target = count_columns.get(name, name)
            value = normalizers[name].uncomfortable
            row[target] = int(value * row["trip_count"]) if name in count_columns else value
        return row

    def score(self, spark, *rows: dict[str, object]):
        features = spark.createDataFrame(list(rows), HOURLY_SEGMENT_FEATURE_SCHEMA)
        return calculate_hourly_comfort_scores(features, "silver3-run", self.PROCESSED_AT)

    def test_produces_the_declared_schema_and_metadata(self, spark):
        result = self.score(spark, self.feature_row())

        assert result.scored.collect() == self.score(spark, self.feature_row()).scored.collect()
        assert result.scored.columns == [
            field.name for field in HOURLY_COMFORT_SCORE_SCHEMA
        ]
        assert [field.dataType for field in result.scored.schema] == [
            field.dataType for field in HOURLY_COMFORT_SCORE_SCHEMA
        ]
        row = result.scored.first()
        assert all(row[column] is not None for column in result.scored.columns)
        assert row["scoring_version"] == "1.0.0"
        assert row["_run_id"] == "silver3-run"
        assert (row["sample_count"], row["trip_count"]) == (36_000, 10)

    def test_each_discomfort_group_only_lowers_its_directional_score(self, spark):
        rows = self.score(
            spark,
            self.feature_row("smooth"),
            self.uncomfortable_row("rough", "vertical_score"),
            self.uncomfortable_row("stop-and-go", "longitudinal_score"),
            self.uncomfortable_row("turning", "lateral_score"),
            self.feature_row("ten-trips", hard_brake_count=10, trip_count=10),
            self.feature_row("twenty-trips", hard_brake_count=20, trip_count=20),
        ).scored
        by_segment = {row.segment_id: row for row in rows.collect()}
        smooth = by_segment["smooth"]

        assert by_segment["rough"].vertical_score < smooth.vertical_score
        assert by_segment["rough"].longitudinal_score == smooth.longitudinal_score
        assert by_segment["stop-and-go"].longitudinal_score < smooth.longitudinal_score
        assert by_segment["turning"].lateral_score < smooth.lateral_score
        assert (
            by_segment["ten-trips"].longitudinal_score
            == by_segment["twenty-trips"].longitudinal_score
        )
        assert all(
            0 <= value <= 100
            for row in by_segment.values()
            for value in (row.vertical_score, row.longitudinal_score, row.lateral_score)
        )

    def test_missing_features_follow_the_configured_weight_policy(self, spark):
        accepted = self.score(spark, self.feature_row(rms_steering_vibration=None))
        rejected = self.score(spark, self.feature_row(rms_accel_z=None, p95_abs_accel_z=None))

        assert accepted.scored.count() == 1
        assert accepted.rejected.count() == 0
        assert rejected.scored.count() == 0
        assert rejected.rejected.first().reject_reason == "INSUFFICIENT_SCORING_FEATURES"

    def test_rejects_an_unsupported_feature_version(self, spark):
        with pytest.raises(ValueError, match="unsupported feature versions"):
            self.score(spark, self.feature_row(feature_version="hourly-features-v2"))

    def test_rejects_duplicate_primary_keys(self, spark):
        with pytest.raises(ValueError, match="duplicate primary keys"):
            self.score(spark, self.feature_row(), self.feature_row())


class TestRunHourlyComfortJob:
    PERIOD_START = datetime(2026, 8, 15, 10, 0, tzinfo=UTC)
    PROCESSED_AT = datetime(2026, 8, 15, 11, 5, tzinfo=UTC)

    def feature_row(self, segment_id: str, sample_count: int = 36_000) -> dict[str, object]:
        return {
            "segment_id": segment_id,
            "vehicle_profile_id": 1,
            "data_period_start": self.PERIOD_START,
            "data_period_end": self.PERIOD_START + timedelta(hours=1),
            "road_snapshot_date": date(2026, 8, 1),
            "avg_speed_mps": 7.0,
            "rms_accel_x": 0.2,
            "rms_accel_y": 0.2,
            "rms_accel_z": 0.2,
            "p95_abs_accel_x": 0.5,
            "p95_abs_accel_y": 0.5,
            "p95_abs_accel_z": 0.5,
            "rms_jerk_x": 0.6,
            "rms_jerk_y": 0.6,
            "rms_jerk_z": 0.6,
            "p95_abs_jerk_x": 1.6,
            "p95_abs_jerk_y": 1.6,
            "p95_abs_jerk_z": 2.1,
            "hard_brake_count": 1,
            "hard_accel_count": 1,
            "sharp_steer_count": 1,
            "steer_reversal_count": 1,
            "rms_steering_rate": 3.0,
            "rms_steering_vibration": 0.1,
            "sample_count": sample_count,
            "trip_count": 10,
            "feature_version": "hourly-features-v1",
            "_processed_at": self.PROCESSED_AT,
            "_run_id": "silver2-run",
        }

    def test_reads_scores_and_idempotently_writes_parquet(self, spark, tmp_path):
        input_path = tmp_path / "features"
        score_path = tmp_path / "scores"
        rejected_path = tmp_path / "rejected"
        spark.createDataFrame(
            [self.feature_row("accepted"), self.feature_row("rejected", sample_count=0)],
            HOURLY_SEGMENT_FEATURE_SCHEMA,
        ).write.parquet(str(input_path))
        config = HourlyComfortJobConfig(
            str(input_path),
            str(score_path),
            str(rejected_path),
            DEFAULT_HOURLY_SCORING_CONFIG_PATH,
        )

        first_summary = run_hourly_comfort_job(
            spark, config, "silver3-run", self.PROCESSED_AT
        )
        first_rows = spark.read.parquet(str(score_path)).collect()
        second_summary = run_hourly_comfort_job(
            spark, config, "silver3-run", self.PROCESSED_AT
        )
        scores = spark.read.parquet(str(score_path))

        assert first_summary == second_summary
        assert (first_summary.scored_count, first_summary.rejected_count) == (1, 1)
        assert scores.collect() == first_rows
        assert scores.columns == [field.name for field in HOURLY_COMFORT_SCORE_SCHEMA]
        assert [field.dataType for field in scores.schema] == [
            field.dataType for field in HOURLY_COMFORT_SCORE_SCHEMA
        ]
        row = scores.first()
        assert row["scoring_version"] == "1.0.0"
        assert row["_run_id"] == "silver3-run"
        stored_epoch = scores.select(F.unix_timestamp("_processed_at")).first()[0]
        assert stored_epoch == int(self.PROCESSED_AT.timestamp())
        assert spark.read.parquet(str(rejected_path)).first()["segment_id"] == "rejected"

    def test_job_config_supports_environment_overrides(self, tmp_path):
        config_path = tmp_path / "scoring.yaml"
        config = HourlyComfortJobConfig.from_env(
            {
                "HOURLY_COMFORT_INPUT_PATH": "input",
                "HOURLY_COMFORT_OUTPUT_PATH": "output",
                "HOURLY_COMFORT_REJECTED_OUTPUT_PATH": "rejected",
                "HOURLY_COMFORT_SCORING_CONFIG_PATH": str(config_path),
            }
        )

        assert config == HourlyComfortJobConfig(
            "input", "output", "rejected", config_path
        )
