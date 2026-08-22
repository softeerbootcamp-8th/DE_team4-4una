"""Tests for batch_jobs/sensor_processing_validation.py (#220)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from batch_jobs.cleansing.hourly_storage import write_hourly_quarantine
from batch_jobs.hourly_segment_feature_storage import write_hourly_segment_features
from batch_jobs.schemas import (
    HOURLY_SEGMENT_FEATURE_SCHEMA,
    SENSOR_EVENT_QUARANTINE_SCHEMA,
)
from batch_jobs.sensor_processing_validation import (
    DEFAULT_FEATURE_RANGES_SUITE_PATH,
    DEFAULT_QUARANTINE_RATE_SUITE_PATH,
    SensorProcessingValidationConfig,
    SensorProcessingValidationFailed,
    compute_quarantine_rate,
    load_expectation_suite,
    run_sensor_processing_validation,
)
from pyspark.sql import SparkSession

TARGET_HOUR = datetime(2026, 8, 16, 10, 0, 0, tzinfo=UTC)


@pytest.fixture(scope="session")
def spark():
    # 세션 전체에서 재사용: SparkSession 기동에 몇 초가 걸린다(다른 테스트 파일과 동일 컨벤션).
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


class TestSensorProcessingValidationConfig:
    def test_from_env_uses_same_defaults_as_the_sensor_processing_job(self) -> None:
        config = SensorProcessingValidationConfig.from_env({})

        assert config.feature_output_path == "data/local-lake/silver/hourly_segment_features"
        assert config.quarantine_output_path == "data/local-lake/silver/sensor_event_quarantine"
        assert config.feature_ranges_suite_path == DEFAULT_FEATURE_RANGES_SUITE_PATH
        assert config.quarantine_rate_suite_path == DEFAULT_QUARANTINE_RATE_SUITE_PATH

    def test_from_env_reads_overrides(self) -> None:
        config = SensorProcessingValidationConfig.from_env(
            {
                "HOURLY_SEGMENT_FEATURE_OUTPUT_PATH": "custom/features",
                "CLEANSING_QUARANTINE_OUTPUT_PATH": "custom/quarantine",
                "SENSOR_PROCESSING_FEATURE_RANGES_SUITE_PATH": "custom/ranges.json",
                "SENSOR_PROCESSING_QUARANTINE_RATE_SUITE_PATH": "custom/rate.json",
            }
        )

        assert config.feature_output_path == "custom/features"
        assert config.quarantine_output_path == "custom/quarantine"
        assert config.feature_ranges_suite_path == Path("custom/ranges.json")
        assert config.quarantine_rate_suite_path == Path("custom/rate.json")


class TestComputeQuarantineRate:
    def test_zero_when_nothing_processed(self) -> None:
        assert compute_quarantine_rate(quarantined_count=0, accepted_count=0) == 0.0

    def test_ratio_of_quarantined_over_total(self) -> None:
        assert compute_quarantine_rate(quarantined_count=5, accepted_count=95) == pytest.approx(0.05)

    def test_all_quarantined_is_one(self) -> None:
        assert compute_quarantine_rate(quarantined_count=10, accepted_count=0) == 1.0


class TestLoadExpectationSuite:
    def test_loads_the_committed_feature_ranges_suite(self) -> None:
        suite = load_expectation_suite(DEFAULT_FEATURE_RANGES_SUITE_PATH)

        assert suite.name == "hourly_segment_features_suite"
        assert len(suite.expectations) == 15

    def test_loads_the_committed_quarantine_rate_suite(self) -> None:
        suite = load_expectation_suite(DEFAULT_QUARANTINE_RATE_SUITE_PATH)

        assert suite.name == "sensor_processing_quarantine_rate_suite"
        assert len(suite.expectations) == 1


class TestRunSensorProcessingValidation:
    """`write_hourly_segment_features`/`write_hourly_quarantine`로 실제 파티션을 쓴 뒤 검증한다."""

    def feature_row(self, **overrides: object) -> dict[str, object]:
        row = {
            "segment_id": "S1",
            "vehicle_profile_id": 1,
            "data_period_start": TARGET_HOUR,
            "data_period_end": TARGET_HOUR.replace(hour=TARGET_HOUR.hour + 1),
            "road_snapshot_date": date(2026, 8, 13),
            "avg_speed_mps": 10.0,
            "rms_accel_x": 1.0,
            "rms_accel_y": 1.0,
            "rms_accel_z": 1.0,
            "p95_abs_accel_x": 1.0,
            "p95_abs_accel_y": 1.0,
            "p95_abs_accel_z": 1.0,
            "rms_jerk_x": 1.0,
            "rms_jerk_y": 1.0,
            "rms_jerk_z": 1.0,
            "p95_abs_jerk_x": 1.0,
            "p95_abs_jerk_y": 1.0,
            "p95_abs_jerk_z": 1.0,
            "hard_brake_count": 0,
            "hard_accel_count": 0,
            "sharp_steer_count": 0,
            "steer_reversal_count": 0,
            "rms_steering_rate": 1.0,
            "rms_steering_vibration": 1.0,
            "sample_count": 10,
            "trip_count": 2,
            "feature_version": "v1",
            "_processed_at": TARGET_HOUR,
            "_run_id": "run-1",
        }
        row.update(overrides)
        return row

    def feature_rows_df(self, spark, rows: list[dict[str, object]]):
        ordered = [
            tuple(row[field.name] for field in HOURLY_SEGMENT_FEATURE_SCHEMA.fields) for row in rows
        ]
        return spark.createDataFrame(ordered, HOURLY_SEGMENT_FEATURE_SCHEMA)

    def quarantine_row(self, **overrides: object) -> dict[str, object]:
        row = {
            "event_id": "e1",
            "trip_id": "t1",
            "event_date": TARGET_HOUR.date(),
            "reject_reason": "OUT_OF_RANGE",
            "reject_detail": "steering_angle=90.0",
            "raw_record": "{}",
            "_run_id": "run-1",
            "_rejected_at": TARGET_HOUR,
            "rejected_date": TARGET_HOUR.date(),
        }
        row.update(overrides)
        return row

    def quarantine_rows_df(self, spark, rows: list[dict[str, object]]):
        ordered = [
            tuple(row[field.name] for field in SENSOR_EVENT_QUARANTINE_SCHEMA.fields) for row in rows
        ]
        return spark.createDataFrame(ordered, SENSOR_EVENT_QUARANTINE_SCHEMA)

    def config(self, tmp_path: Path) -> SensorProcessingValidationConfig:
        return SensorProcessingValidationConfig(
            feature_output_path=str(tmp_path / "hourly_segment_features"),
            quarantine_output_path=str(tmp_path / "sensor_event_quarantine"),
            feature_ranges_suite_path=DEFAULT_FEATURE_RANGES_SUITE_PATH,
            quarantine_rate_suite_path=DEFAULT_QUARANTINE_RATE_SUITE_PATH,
        )

    def test_succeeds_when_ranges_and_quarantine_rate_are_healthy(self, spark, tmp_path) -> None:
        config = self.config(tmp_path)
        features_df = self.feature_rows_df(spark, [self.feature_row(sample_count=95)])
        write_hourly_segment_features(spark, features_df, config.feature_output_path, TARGET_HOUR, "run-1")
        quarantine_df = self.quarantine_rows_df(spark, [self.quarantine_row() for _ in range(5)])
        write_hourly_quarantine(spark, quarantine_df, config.quarantine_output_path, TARGET_HOUR, "run-1")

        summary = run_sensor_processing_validation(spark, config, TARGET_HOUR)

        assert summary.success
        assert summary.feature_row_count == 1
        assert summary.accepted_sample_count == 95
        assert summary.quarantine_row_count == 5
        assert summary.quarantine_rate == pytest.approx(0.05)

    def test_raises_when_a_magnitude_column_is_negative(self, spark, tmp_path) -> None:
        config = self.config(tmp_path)
        features_df = self.feature_rows_df(spark, [self.feature_row(rms_accel_x=-1.0)])
        write_hourly_segment_features(spark, features_df, config.feature_output_path, TARGET_HOUR, "run-1")

        with pytest.raises(SensorProcessingValidationFailed):
            run_sensor_processing_validation(spark, config, TARGET_HOUR)

    def test_raises_when_quarantine_rate_exceeds_threshold(self, spark, tmp_path) -> None:
        config = self.config(tmp_path)
        features_df = self.feature_rows_df(spark, [self.feature_row(sample_count=10)])
        write_hourly_segment_features(spark, features_df, config.feature_output_path, TARGET_HOUR, "run-1")
        # rate = 5 / (5 + 10) ≈ 0.33, suite의 max_value(0.05)를 넘는다.
        quarantine_df = self.quarantine_rows_df(spark, [self.quarantine_row() for _ in range(5)])
        write_hourly_quarantine(spark, quarantine_df, config.quarantine_output_path, TARGET_HOUR, "run-1")

        with pytest.raises(SensorProcessingValidationFailed):
            run_sensor_processing_validation(spark, config, TARGET_HOUR)

    def test_raises_when_no_feature_partition_exists(self, spark, tmp_path) -> None:
        config = self.config(tmp_path)

        with pytest.raises(SensorProcessingValidationFailed):
            run_sensor_processing_validation(spark, config, TARGET_HOUR)

    def test_succeeds_when_quarantine_partition_is_absent(self, spark, tmp_path) -> None:
        # write_hourly_quarantine는 격리 행이 0건이면 파티션 자체를 만들지 않는다 — 정상 케이스로 처리돼야 한다.
        config = self.config(tmp_path)
        features_df = self.feature_rows_df(spark, [self.feature_row(sample_count=10)])
        write_hourly_segment_features(spark, features_df, config.feature_output_path, TARGET_HOUR, "run-1")

        summary = run_sensor_processing_validation(spark, config, TARGET_HOUR)

        assert summary.success
        assert summary.quarantine_row_count == 0
