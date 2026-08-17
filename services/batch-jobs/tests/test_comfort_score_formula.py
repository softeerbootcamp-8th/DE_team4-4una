"""Tests for comfort_score/formula.py (#127).

context/comfort-score.md의 Step 1~5, vehicle-agnostic 버전을 검증한다. 가중치를
YAML 기본값(0.5/0.3/0.2)과 다르게 설정해 실제로 config에서 읽는지(하드코딩이 아닌지)를
자연스럽게 함께 검증한다.
"""

from __future__ import annotations

import os
import time
from datetime import datetime

import pytest
from batch_jobs.comfort_score.config import ComfortScoreConfig
from batch_jobs.comfort_score.formula import compute_segment_comfort_scores
from batch_jobs.sensor_features.config import ProvisionalThreshold
from pyspark.sql import SparkSession

# test_comfort_score_loader.py와 동일한 이유로 TZ를 고정한다: naive datetime을
# 실행 머신 로컬 타임존이 아니라 UTC로 일관되게 다루기 위함.
os.environ["TZ"] = "UTC"
time.tzset()

# 기본 YAML 값(0.5/0.3/0.2, T_min=5, k=10)과 일부러 다르게 둬서, 결과가 이 값들을
# 실제로 반영하는지(하드코딩된 상수가 아닌지) 자연스럽게 검증한다.
TEST_CONFIG = ComfortScoreConfig(
    vertical_weight=ProvisionalThreshold(value=0.6, provisional=True),
    longitudinal_weight=ProvisionalThreshold(value=0.3, provisional=True),
    lateral_weight=ProvisionalThreshold(value=0.1, provisional=True),
    min_traffic_threshold=ProvisionalThreshold(value=5.0, provisional=True),
    shrinkage_k=ProvisionalThreshold(value=4.0, provisional=True),
)

HOURLY_SCHEMA = (
    "segment_id string, vehicle_profile_id int, data_period_start timestamp, "
    "vertical_score double, longitudinal_score double, lateral_score double, "
    "trip_count long, sample_count long"
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("comfort-score-formula-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def hour(
    segment_id: str = "seg-1",
    vehicle_profile_id: int = 1,
    data_period_start: datetime = datetime(2026, 8, 1, 0, 0, 0),  # noqa: DTZ001
    vertical_score: float = 0.0,
    longitudinal_score: float = 0.0,
    lateral_score: float = 0.0,
    trip_count: int = 10,
    sample_count: int = 0,
) -> tuple:
    return (
        segment_id,
        vehicle_profile_id,
        data_period_start,
        vertical_score,
        longitudinal_score,
        lateral_score,
        trip_count,
        sample_count,
    )


def hourly_df(spark, *rows: tuple):
    return spark.createDataFrame(list(rows), HOURLY_SCHEMA)


def per_vehicle_rows(result) -> dict:
    return {
        (row.segment_id, row.vehicle_profile_id): row
        for row in result.collect()
        if row.vehicle_profile_id != 0
    }


def test_combines_directional_scores_with_configured_weights_for_one_qualifying_hour(spark):
    df = hourly_df(
        spark,
        hour(vertical_score=100.0, longitudinal_score=0.0, lateral_score=0.0, trip_count=10),
    )

    result = compute_segment_comfort_scores(df, TEST_CONFIG)

    row = per_vehicle_rows(result)[("seg-1", 1)]
    # c_h = 0.6*100 + 0.3*0 + 0.1*0 = 60; 세그먼트가 하나뿐이라 mu_p도 60이라 shrink 후에도 60 그대로.
    assert row.comfort_score == pytest.approx(60.0)
    assert row.confidence_score == pytest.approx(1 / 5)  # N=1, k=4 -> 1/(1+4)
    assert row.score_version == "1.0.0"


def test_hours_below_min_traffic_threshold_are_excluded(spark):
    df = hourly_df(
        spark,
        hour(trip_count=2, vertical_score=0.0, sample_count=999),  # T_min=5 미달 -> 제외
        hour(trip_count=10, vertical_score=100.0, sample_count=50),  # 포함
    )

    result = compute_segment_comfort_scores(df, TEST_CONFIG)

    row = per_vehicle_rows(result)[("seg-1", 1)]
    assert row.qualifying_hours == 1
    assert row.sample_count == 50  # 제외된 시간의 999는 합산되지 않는다
    assert row.comfort_score == pytest.approx(60.0)


def test_shrinks_toward_the_population_mean_across_segments(spark):
    df = hourly_df(
        spark,
        hour(segment_id="seg-x", vertical_score=100.0, trip_count=10),  # c_h=60
        hour(segment_id="seg-y", vertical_score=0.0, trip_count=10),  # c_h=0
    )

    result = compute_segment_comfort_scores(df, TEST_CONFIG)
    rows = per_vehicle_rows(result)

    # mu_p = (60+0)/2 = 30. ComfortScore = (N*c_obs + k*mu_p)/(N+k), N=1, k=4.
    assert rows[("seg-x", 1)].comfort_score == pytest.approx((1 * 60 + 4 * 30) / 5)
    assert rows[("seg-y", 1)].comfort_score == pytest.approx((1 * 0 + 4 * 30) / 5)


def test_a_pair_with_hours_that_never_qualify_falls_back_to_the_population_mean(spark):
    df = hourly_df(
        spark,
        hour(segment_id="seg-x", vertical_score=100.0, trip_count=10),  # c_h=60, qualifies
        hour(segment_id="seg-y", vertical_score=0.0, trip_count=10),  # c_h=0, qualifies
        # seg-z: 기록(원본 행)은 있지만 T_min(5) 미달이라 qualifying hour가 0개가 된다.
        hour(segment_id="seg-z", vertical_score=999.0, trip_count=2, sample_count=999),
    )

    result = compute_segment_comfort_scores(df, TEST_CONFIG)
    rows = per_vehicle_rows(result)

    # seg-z는 mu_p(=30, seg-x/seg-y에서만 계산됨)로 그대로 대체되고 confidence는 0이어야 한다.
    z_row = rows[("seg-z", 1)]
    assert z_row.qualifying_hours == 0
    assert z_row.sample_count == 0
    assert z_row.comfort_score == pytest.approx(30.0)
    assert z_row.confidence_score == pytest.approx(0.0)


def vehicle_agnostic_row(result, segment_id: str):
    matches = [
        row
        for row in result.collect()
        if row.segment_id == segment_id and row.vehicle_profile_id == 0
    ]
    assert len(matches) == 1, f"expected exactly one vehicle-agnostic row for {segment_id}"
    return matches[0]


def test_vehicle_agnostic_row_pools_profiles_in_the_same_hour_weighted_by_traffic(spark):
    same_hour = datetime(2026, 8, 1, 3, 0, 0)  # noqa: DTZ001
    df = hourly_df(
        spark,
        hour(
            vehicle_profile_id=1,
            data_period_start=same_hour,
            vertical_score=100.0,
            trip_count=10,
            sample_count=5,
        ),  # c_h=60
        hour(
            vehicle_profile_id=2,
            data_period_start=same_hour,
            vertical_score=0.0,
            trip_count=30,
            sample_count=15,
        ),  # c_h=0
    )

    result = compute_segment_comfort_scores(df, TEST_CONFIG)

    # 이 윈도우엔 이 세그먼트의 이 한 시간뿐이라, pooled c_h가 곧 c_obs이자 mu(전체 population)다.
    # c_h,s = (10*60 + 30*0) / (10+30) = 15
    row = vehicle_agnostic_row(result, "seg-1")
    assert row.comfort_score == pytest.approx(15.0)
    assert row.sample_count == 20  # 5 + 15, 두 프로필의 sample_count 합
    assert row.qualifying_hours == 1

    # 차량별 행(profile 1, 2)도 vehicle-agnostic 행과 함께 그대로 남아 있어야 한다.
    by_key = per_vehicle_rows(result)
    assert {1, 2} <= {vehicle_profile_id for (_, vehicle_profile_id) in by_key}


def test_vehicle_agnostic_row_shrinks_toward_the_global_population_mean(spark):
    df = hourly_df(
        spark,
        # c_h=60
        hour(segment_id="seg-p", vehicle_profile_id=1, vertical_score=100.0, trip_count=10),
        # c_h=0
        hour(segment_id="seg-q", vehicle_profile_id=1, vertical_score=0.0, trip_count=10),
    )

    result = compute_segment_comfort_scores(df, TEST_CONFIG)
    expected_p = (1 * 60 + 4 * 30) / 5  # 전역 mu = (60+0)/2 = 30. 프로필이 하나뿐이라 pooling은 no-op.
    expected_q = (1 * 0 + 4 * 30) / 5

    assert vehicle_agnostic_row(result, "seg-p").comfort_score == pytest.approx(expected_p)
    assert vehicle_agnostic_row(result, "seg-q").comfort_score == pytest.approx(expected_q)


def test_raises_a_clear_error_when_a_required_column_is_missing(spark):
    incomplete = spark.createDataFrame(
        [("seg-1", 1)], "segment_id string, vehicle_profile_id int"
    )

    with pytest.raises(ValueError, match="vertical_score"):
        compute_segment_comfort_scores(incomplete, TEST_CONFIG)


def test_raises_when_input_uses_the_reserved_vehicle_agnostic_sentinel_id(spark):
    df = hourly_df(spark, hour(vehicle_profile_id=0))

    with pytest.raises(ValueError, match="reserved"):
        compute_segment_comfort_scores(df, TEST_CONFIG)


def test_a_profile_with_no_qualifying_hour_anywhere_is_omitted_not_null(spark):
    df = hourly_df(
        spark,
        hour(segment_id="seg-x", vehicle_profile_id=1, vertical_score=100.0, trip_count=10),
        # profile 2는 이 윈도우 전체에서 유일한 행이 T_min 미달이라, 어디에도 qualifying hour가
        # 없다 -> mu_2 자체가 정의되지 않는다.
        hour(segment_id="seg-y", vehicle_profile_id=2, trip_count=2),
    )

    result = compute_segment_comfort_scores(df, TEST_CONFIG)
    rows = per_vehicle_rows(result)

    assert ("seg-x", 1) in rows  # profile 1은 정상적으로 산출됨
    assert ("seg-y", 2) not in rows  # mu_2가 없어 계산 불가 -> NULL 대신 행 자체가 없어야 한다


def test_a_window_with_no_qualifying_hour_at_all_yields_no_rows(spark):
    df = hourly_df(spark, hour(trip_count=2))  # 유일한 행이 T_min 미달 -> qualifying hour 0개

    result = compute_segment_comfort_scores(df, TEST_CONFIG)

    assert result.count() == 0
