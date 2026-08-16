"""Tests for comfort_score/gold_job.py (#129)."""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime

import pytest
from batch_jobs.schemas import HOURLY_COMFORT_SCORE_SCHEMA
from comfort_score.config import DEFAULT_COMFORT_SCORE_CONFIG_PATH
from comfort_score.gold_job import (
    SegmentComfortScoreJobConfig,
    SegmentComfortScoreJobSummary,
    _attach_calculated_at,
    _validate_as_of,
    run_segment_comfort_score_job,
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("gold-job-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def make_config(tmp_path, data_lake_uri: str) -> SegmentComfortScoreJobConfig:
    return SegmentComfortScoreJobConfig(
        data_lake_uri=data_lake_uri,
        window_hours=168,
        comfort_score_config_path=DEFAULT_COMFORT_SCORE_CONFIG_PATH,
        postgres_host="unused",
        postgres_port=5432,
        postgres_db="unused",
        postgres_user="unused",
        postgres_password="unused",
    )


def test_validate_as_of_raises_on_naive_datetime():
    with pytest.raises(ValueError, match="timezone-aware"):
        _validate_as_of(datetime(2026, 8, 16, 0, 0))  # noqa: DTZ001


def test_validate_as_of_accepts_aware_datetime():
    _validate_as_of(datetime(2026, 8, 16, 0, 0, tzinfo=UTC))  # must not raise


def test_attach_calculated_at_uses_the_same_as_of_literal_for_every_row(spark):
    as_of = datetime(2026, 8, 16, 3, 0, tzinfo=UTC)
    df = spark.createDataFrame(
        [("seg-1", 1), ("seg-2", 2)], "segment_id string, vehicle_profile_id int"
    )

    result = _attach_calculated_at(df, as_of)

    epochs = [row[0] for row in result.select(F.unix_timestamp("calculated_at")).collect()]
    assert epochs == [int(as_of.timestamp())] * 2


def test_returns_zero_merged_count_and_skips_write_when_window_has_no_rows(
    spark, tmp_path
):
    input_path = tmp_path / "silver" / "hourly_comfort_score"
    spark.createDataFrame([], HOURLY_COMFORT_SCORE_SCHEMA).write.parquet(str(input_path))
    config = make_config(tmp_path, str(tmp_path))
    as_of = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)

    summary = run_segment_comfort_score_job(spark, config, as_of, connection=None)

    assert summary == SegmentComfortScoreJobSummary(0, 0, 0, 0)
