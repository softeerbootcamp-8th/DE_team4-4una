"""Tests for comfort_score/gold_job.py (#129)."""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime

import pytest
from batch_jobs.comfort_score.config import DEFAULT_COMFORT_SCORE_CONFIG_PATH
from batch_jobs.comfort_score.gold_job import (
    SegmentComfortScoreJobConfig,
    SegmentComfortScoreJobSummary,
    _attach_calculated_at,
    _fill_missing_periods,
    _select_staging_columns,
    _validate_as_of,
    run_segment_comfort_score_job,
)
from batch_jobs.comfort_score.gold_writer import EXPECTED_STAGING_COLUMNS
from batch_jobs.schemas import HOURLY_COMFORT_SCORE_SCHEMA
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


def test_select_staging_columns_drops_diagnostic_columns_not_in_the_staging_table(spark):
    # formula.py의 출력에는 qualifying_hours/observed_score/population_mean처럼
    # staging 테이블에 없는 진단용 컬럼이 섞여 있다 (#152) — 그대로 write하면
    # JDBC write가 컬럼 불일치로 실패한다.
    df = spark.createDataFrame(
        [
            (
                "seg-1", 1,
                datetime(2026, 8, 15, 12, tzinfo=UTC), datetime(2026, 8, 15, 13, tzinfo=UTC),
                80.0, 0.9, 100, 5, 78.0, 82.0, "1.0.0", datetime(2026, 8, 16, tzinfo=UTC),
            )
        ],
        "segment_id string, vehicle_profile_id int, data_period_start timestamp, "
        "data_period_end timestamp, comfort_score double, "
        "confidence_score double, sample_count long, qualifying_hours long, "
        "observed_score double, population_mean double, score_version string, "
        "calculated_at timestamp",
    )

    result = _select_staging_columns(df)

    assert result.columns == list(EXPECTED_STAGING_COLUMNS)


def test_fill_missing_periods_leaves_existing_bounds_untouched(spark):
    as_of = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
    start = datetime(2026, 8, 15, 3, 0, tzinfo=UTC)
    end = datetime(2026, 8, 15, 4, 0, tzinfo=UTC)
    df = spark.createDataFrame(
        [("seg-1", 1, start, end)],
        "segment_id string, vehicle_profile_id int, data_period_start timestamp, "
        "data_period_end timestamp",
    )

    result = _fill_missing_periods(df, as_of, window_hours=168)

    row = result.select(
        F.unix_timestamp("data_period_start"), F.unix_timestamp("data_period_end")
    ).collect()[0]
    assert row[0] == int(start.timestamp())
    assert row[1] == int(end.timestamp())


def test_fill_missing_periods_uses_the_batch_window_bounds_when_null(spark):
    # qualifying_hours=0인 행은 formula.py에서 MIN/MAX로 롤업할 시간이 없어
    # NULL로 나온다 (#163) — 이 행이 실제로 커버하려던 배치 윈도우
    # [as_of - window_hours, as_of)로 채운다.
    as_of = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
    df = spark.createDataFrame(
        [("seg-z", 1, None, None)],
        "segment_id string, vehicle_profile_id int, data_period_start timestamp, "
        "data_period_end timestamp",
    )

    result = _fill_missing_periods(df, as_of, window_hours=168)

    row = result.select(
        F.unix_timestamp("data_period_start"), F.unix_timestamp("data_period_end")
    ).collect()[0]
    window_start = datetime(2026, 8, 9, 0, 0, tzinfo=UTC)
    assert row[0] == int(window_start.timestamp())
    assert row[1] == int(as_of.timestamp())


def test_returns_zero_merged_count_and_skips_write_when_window_has_no_rows(
    spark, tmp_path
):
    input_path = tmp_path / "silver" / "hourly_comfort_score"
    spark.createDataFrame([], HOURLY_COMFORT_SCORE_SCHEMA).write.parquet(str(input_path))
    config = make_config(tmp_path, str(tmp_path))
    as_of = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)

    summary = run_segment_comfort_score_job(spark, config, as_of, connection=None)

    assert summary == SegmentComfortScoreJobSummary(0, 0, 0, 0)
