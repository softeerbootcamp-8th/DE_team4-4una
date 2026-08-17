from datetime import UTC, date, datetime, timedelta

import pytest
from batch_jobs.comfort_scoring_config import DEFAULT_HOURLY_SCORING_CONFIG_PATH
from batch_jobs.hourly_comfort_job import (
    HourlyComfortJobConfig,
    run_hourly_comfort_job,
)
from batch_jobs.schemas import (
    HOURLY_COMFORT_SCORE_SCHEMA,
    HOURLY_SEGMENT_FEATURE_SCHEMA,
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PERIOD_START = datetime(2026, 8, 15, 10, 0, tzinfo=UTC)
PROCESSED_AT = datetime(2026, 8, 15, 11, 5, tzinfo=UTC)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("hourly-comfort-job-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def feature_row(segment_id: str, sample_count: int = 36_000) -> dict[str, object]:
    return {
        "segment_id": segment_id,
        "vehicle_profile_id": 1,
        "data_period_start": PERIOD_START,
        "data_period_end": PERIOD_START + timedelta(hours=1),
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
        "_processed_at": PROCESSED_AT,
        "_run_id": "silver2-run",
    }


def test_reads_scores_and_idempotently_writes_parquet(spark, tmp_path):
    input_path = tmp_path / "features"
    score_path = tmp_path / "scores"
    rejected_path = tmp_path / "rejected"
    spark.createDataFrame(
        [feature_row("accepted"), feature_row("rejected", sample_count=0)],
        HOURLY_SEGMENT_FEATURE_SCHEMA,
    ).write.parquet(str(input_path))
    config = HourlyComfortJobConfig(
        str(input_path),
        str(score_path),
        str(rejected_path),
        DEFAULT_HOURLY_SCORING_CONFIG_PATH,
    )

    first_summary = run_hourly_comfort_job(
        spark, config, "silver3-run", PROCESSED_AT
    )
    first_rows = spark.read.parquet(str(score_path)).collect()
    second_summary = run_hourly_comfort_job(
        spark, config, "silver3-run", PROCESSED_AT
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
    assert stored_epoch == int(PROCESSED_AT.timestamp())
    assert spark.read.parquet(str(rejected_path)).first()["segment_id"] == "rejected"


def test_job_config_supports_environment_overrides(tmp_path):
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
