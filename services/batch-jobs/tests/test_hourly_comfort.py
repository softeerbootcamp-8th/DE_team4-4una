from datetime import UTC, date, datetime, timedelta

import pytest
from batch_jobs.comfort_scoring_config import (
    DEFAULT_HOURLY_SCORING_CONFIG,
    DEFAULT_HOURLY_SCORING_CONFIG_PATH,
)
from batch_jobs.hourly_comfort import calculate_hourly_comfort_scores
from batch_jobs.hourly_comfort_job import HourlyComfortJobConfig, run_hourly_comfort_job
from batch_jobs.hourly_comfort_storage import hour_output_path as score_hour_path
from batch_jobs.hourly_segment_feature_storage import (
    hour_output_path as feature_hour_path,
)
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

    def feature_row(
        self,
        segment_id: str,
        sample_count: int = 36_000,
        period_start: datetime | None = None,
    ) -> dict[str, object]:
        # period_start를 바꿀 수 있어야 여러 시간 파티션을 만드는 테스트를 쓸 수 있다(#469).
        period_start = period_start or self.PERIOD_START
        return {
            "segment_id": segment_id,
            "vehicle_profile_id": 1,
            "data_period_start": period_start,
            "data_period_end": period_start + timedelta(hours=1),
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

    def write_feature_partition(self, spark, feature_root, period_start, rows):
        """Silver2의 해당 시간 파티션에 feature 행을 쓴다."""
        spark.createDataFrame(rows, HOURLY_SEGMENT_FEATURE_SCHEMA).write.parquet(
            feature_hour_path(str(feature_root), period_start)
        )

    def test_reads_scores_and_idempotently_writes_parquet(self, spark, tmp_path):
        input_path = tmp_path / "features"
        score_path = tmp_path / "scores"
        rejected_path = tmp_path / "rejected"
        self.write_feature_partition(
            spark,
            input_path,
            self.PERIOD_START,
            [self.feature_row("accepted"), self.feature_row("rejected", sample_count=0)],
        )
        config = HourlyComfortJobConfig(
            str(input_path),
            str(score_path),
            str(rejected_path),
            DEFAULT_HOURLY_SCORING_CONFIG_PATH,
        )
        score_partition = score_hour_path(str(score_path), self.PERIOD_START)

        first_summary = run_hourly_comfort_job(
            spark, config, "silver3-run", self.PROCESSED_AT, self.PERIOD_START
        )
        first_rows = spark.read.parquet(score_partition).collect()
        second_summary = run_hourly_comfort_job(
            spark, config, "silver3-run", self.PROCESSED_AT, self.PERIOD_START
        )
        scores = spark.read.parquet(score_partition)

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
        rejected_partition = score_hour_path(str(rejected_path), self.PERIOD_START)
        assert spark.read.parquet(rejected_partition).first()["segment_id"] == "rejected"

    def test_reads_only_the_target_hour_partition_of_silver2(self, spark, tmp_path):
        """Silver2 루트가 아니라 target_hour 파티션만 읽는다 (#469)."""
        input_path = tmp_path / "features"
        other_hour = self.PERIOD_START + timedelta(hours=1)
        self.write_feature_partition(
            spark, input_path, self.PERIOD_START, [self.feature_row("in-scope")]
        )
        self.write_feature_partition(
            spark,
            input_path,
            other_hour,
            [self.feature_row("out-of-scope", period_start=other_hour)],
        )
        config = HourlyComfortJobConfig(
            str(input_path),
            str(tmp_path / "scores"),
            str(tmp_path / "rejected"),
            DEFAULT_HOURLY_SCORING_CONFIG_PATH,
        )

        summary = run_hourly_comfort_job(
            spark, config, "silver3-run", self.PROCESSED_AT, self.PERIOD_START
        )

        # 전체를 읽었다면 2행이 나온다.
        assert summary.scored_count == 1
        scored = spark.read.parquet(
            score_hour_path(str(tmp_path / "scores"), self.PERIOD_START)
        )
        assert [row["segment_id"] for row in scored.collect()] == ["in-scope"]

    def test_writes_only_the_target_hour_partition_of_silver3(self, spark, tmp_path):
        """두 번째 실행이 첫 번째 시간대의 파티션을 덮어쓰지 않는다 (#469)."""
        input_path = tmp_path / "features"
        score_path = tmp_path / "scores"
        second_hour = self.PERIOD_START + timedelta(hours=1)
        self.write_feature_partition(
            spark, input_path, self.PERIOD_START, [self.feature_row("first")]
        )
        self.write_feature_partition(
            spark,
            input_path,
            second_hour,
            [self.feature_row("second", period_start=second_hour)],
        )
        config = HourlyComfortJobConfig(
            str(input_path),
            str(score_path),
            str(tmp_path / "rejected"),
            DEFAULT_HOURLY_SCORING_CONFIG_PATH,
        )

        run_hourly_comfort_job(
            spark, config, "run-1", self.PROCESSED_AT, self.PERIOD_START
        )
        run_hourly_comfort_job(spark, config, "run-2", self.PROCESSED_AT, second_hour)

        first = spark.read.parquet(score_hour_path(str(score_path), self.PERIOD_START))
        second = spark.read.parquet(score_hour_path(str(score_path), second_hour))
        assert [row["segment_id"] for row in first.collect()] == ["first"]
        assert [row["segment_id"] for row in second.collect()] == ["second"]

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
