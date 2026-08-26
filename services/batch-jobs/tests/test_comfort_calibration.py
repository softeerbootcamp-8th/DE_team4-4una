from datetime import UTC, date, datetime, timedelta

import pytest
from batch_jobs.comfort_calibration import (
    ALWAYS_INCLUDED_COLUMNS,
    PERCENTILES,
    build_feature_distributions,
    filter_representative_period,
    scoring_feature_columns,
    with_rate_columns,
)
from batch_jobs.comfort_scoring_config import DEFAULT_HOURLY_SCORING_CONFIG
from batch_jobs.schemas import HOURLY_SEGMENT_FEATURE_SCHEMA
from pyspark.sql import SparkSession

PERIOD_START = datetime(2026, 8, 1, 10, 0, tzinfo=UTC)
PROCESSED_AT = datetime(2026, 8, 15, 3, 0, tzinfo=UTC)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("comfort-calibration-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def feature_row(segment_id: str = "100001", **overrides: object) -> dict[str, object]:
    row = {
        "segment_id": segment_id,
        "vehicle_profile_id": 1,
        "data_period_start": PERIOD_START,
        "data_period_end": PERIOD_START + timedelta(hours=1),
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
        "_processed_at": PROCESSED_AT,
        "_run_id": "silver2-run",
    }
    return row | overrides


def build_df(spark, *rows: dict[str, object]):
    return spark.createDataFrame(list(rows), HOURLY_SEGMENT_FEATURE_SCHEMA)


def test_scoring_feature_columns_matches_config_normalizers():
    # anchor 목록이 바뀌어도 분석 대상이 따로 안 고쳐도 같이 바뀌어야 한다(드리프트 방지).
    expected = {name for name, _ in DEFAULT_HOURLY_SCORING_CONFIG.normalizers}
    assert set(scoring_feature_columns(DEFAULT_HOURLY_SCORING_CONFIG)) == expected


def test_with_rate_columns_derives_count_over_trip_count(spark):
    df = build_df(spark, feature_row(hard_brake_count=3, trip_count=10))

    result = with_rate_columns(df, ("hard_brake_rate",))

    assert result.first()["hard_brake_rate"] == pytest.approx(0.3)


def test_with_rate_columns_is_null_when_trip_count_is_zero(spark):
    df = build_df(spark, feature_row(hard_brake_count=0, trip_count=0))

    result = with_rate_columns(df, ("hard_brake_rate",))

    assert result.first()["hard_brake_rate"] is None


def test_with_rate_columns_ignores_features_not_requested(spark):
    df = build_df(spark, feature_row())

    result = with_rate_columns(df, ("rms_accel_x",))

    assert "hard_brake_rate" not in result.columns


def test_build_feature_distributions_computes_count_mean_min_max(spark):
    df = build_df(
        spark,
        *[feature_row(f"seg-{i}", rms_accel_x=float(i)) for i in range(1, 11)],
    )

    distributions = build_feature_distributions(df, ("rms_accel_x",))

    dist = distributions["rms_accel_x"]
    assert dist.count == 10
    assert dist.mean == pytest.approx(5.5)
    assert dist.minimum == pytest.approx(1.0)
    assert dist.maximum == pytest.approx(10.0)
    assert len(dist.percentiles) == len(PERCENTILES)


def test_build_feature_distributions_includes_avg_speed_mps_even_if_unrequested(spark):
    df = build_df(spark, feature_row(avg_speed_mps=12.0))

    distributions = build_feature_distributions(df, ())

    assert set(ALWAYS_INCLUDED_COLUMNS) <= set(distributions)
    assert distributions["avg_speed_mps"].mean == pytest.approx(12.0)


def test_build_feature_distributions_covers_rate_features(spark):
    df = build_df(spark, feature_row(hard_brake_count=5, trip_count=10))

    distributions = build_feature_distributions(df, ("hard_brake_rate",))

    assert distributions["hard_brake_rate"].mean == pytest.approx(0.5)


def test_filter_representative_period_keeps_only_the_requested_window(spark):
    df = build_df(
        spark,
        feature_row("before", data_period_start=PERIOD_START - timedelta(hours=1)),
        feature_row("inside", data_period_start=PERIOD_START),
        feature_row("after", data_period_start=PERIOD_START + timedelta(hours=1)),
    )

    result = filter_representative_period(
        df, start=PERIOD_START, end=PERIOD_START + timedelta(hours=1)
    )

    assert [row["segment_id"] for row in result.collect()] == ["inside"]


def test_filter_representative_period_without_bounds_is_a_no_op(spark):
    df = build_df(spark, feature_row("a"), feature_row("b"))

    result = filter_representative_period(df, start=None, end=None)

    assert result.count() == 2
