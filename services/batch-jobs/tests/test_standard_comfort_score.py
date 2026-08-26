"""Tests for directional standard comfort scores (#198).

방향별 점수 산출과 universe materialization을 다룬다. 구 segment_comfort_score
경로는 #227에서 제거했다.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta

import pytest
from batch_jobs.comfort_score.config import ComfortScoreConfig
from batch_jobs.comfort_score.formula import (
    VEHICLE_AGNOSTIC_VEHICLE_PROFILE_ID,
    Universe,
    compute_standard_comfort_scores,
)
from batch_jobs.comfort_score.universe import resolve_segment_artifact_uri
from batch_jobs.sensor_features.config import ProvisionalThreshold
from pyspark.sql import SparkSession

# 기본 YAML 값(0.5/0.3/0.2, T_min=5, k=10)과 일부러 다르게 둬서, 결과가 이 값들을
# 실제로 반영하는지(하드코딩된 상수가 아닌지) 자연스럽게 검증한다.
TEST_CONFIG = ComfortScoreConfig(
    vertical_weight=ProvisionalThreshold(value=0.6, provisional=True),
    longitudinal_weight=ProvisionalThreshold(value=0.3, provisional=True),
    lateral_weight=ProvisionalThreshold(value=0.1, provisional=True),
    evidence_saturation_trip_count=ProvisionalThreshold(value=5.0, provisional=True),
    shrinkage_k=ProvisionalThreshold(value=4.0, provisional=True),
)


def evidence_weight(trip_count: float, config: ComfortScoreConfig = TEST_CONFIG) -> float:
    return min(1.0, trip_count / config.evidence_saturation_trip_count.value)

HOURLY_SCHEMA = (
    "segment_id string, vehicle_profile_id int, data_period_start timestamp, "
    "data_period_end timestamp, vertical_score double, longitudinal_score double, "
    "lateral_score double, trip_count long, sample_count long"
)



@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("batch-jobs-standard-tests")
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
    data_period_end: datetime | None = None,
    vertical_score: float = 0.0,
    longitudinal_score: float = 0.0,
    lateral_score: float = 0.0,
    trip_count: int = 10,
    sample_count: int = 0,
) -> tuple:
    if data_period_end is None:
        data_period_end = data_period_start + timedelta(hours=1)
    return (
        segment_id,
        vehicle_profile_id,
        data_period_start,
        data_period_end,
        vertical_score,
        longitudinal_score,
        lateral_score,
        trip_count,
        sample_count,
    )


def hourly_df(spark, *rows: tuple):
    return spark.createDataFrame(list(rows), HOURLY_SCHEMA)


def universe_df(spark, *pairs: tuple):
    """(segment, profile) 쌍 목록을 Universe로 바꾼다.

    Universe는 두 축을 따로 들고 있지만, 테스트는 조합을 나열하는 쪽이 읽기 쉬워서
    여기서 두 축으로 쪼갠다. 순서는 나열 순서를 유지한다.
    """
    segment_ids = list(dict.fromkeys(segment_id for segment_id, _ in pairs))
    profile_ids = tuple(dict.fromkeys(profile_id for _, profile_id in pairs))
    return Universe(
        segments=spark.createDataFrame(
            [(segment_id,) for segment_id in segment_ids], "segment_id string"
        ),
        profile_ids=profile_ids,
    )


def rows_by_key(result) -> dict:
    return {(row.segment_id, row.vehicle_profile_id): row for row in result.collect()}


def weighted_sum(row, config: ComfortScoreConfig = TEST_CONFIG) -> float:
    return (
        config.vertical_weight.value * row.vertical_score
        + config.longitudinal_weight.value * row.longitudinal_score
        + config.lateral_weight.value * row.lateral_score
    )


class TestDirectionalScores:
    def test_directional_scores_are_reported_per_direction(self, spark):
        df = hourly_df(
            spark,
            hour(vertical_score=100.0, longitudinal_score=50.0, lateral_score=10.0),
        )

        result = compute_standard_comfort_scores(
            df, TEST_CONFIG, universe_df(spark, ("seg-1", 1))
        )

        row = rows_by_key(result)[("seg-1", 1)]
        # 세그먼트가 하나뿐이라 방향별 mu도 관측값과 같아 shrink 후에도 그대로다.
        assert row.vertical_score == pytest.approx(100.0)
        assert row.longitudinal_score == pytest.approx(50.0)
        assert row.lateral_score == pytest.approx(10.0)

    def test_comfort_score_equals_the_weighted_sum_of_directional_scores(self, spark):
        # 방향별로 다른 값을 가진 여러 세그먼트 — shrinkage가 실제로 걸리는 상황에서
        # 선형성이 유지되는지 본다.
        df = hourly_df(
            spark,
            hour(segment_id="seg-x", vertical_score=90.0, longitudinal_score=20.0,
                 lateral_score=70.0),
            hour(segment_id="seg-y", vertical_score=10.0, longitudinal_score=80.0,
                 lateral_score=30.0),
            hour(segment_id="seg-z", vertical_score=55.0, longitudinal_score=45.0,
                 lateral_score=5.0),
        )

        result = compute_standard_comfort_scores(
            df,
            TEST_CONFIG,
            universe_df(spark, ("seg-x", 1), ("seg-y", 1), ("seg-z", 1)),
        )

        for row in result.collect():
            assert row.comfort_score == pytest.approx(weighted_sum(row), abs=1e-5)

    def test_comfort_score_follows_the_documented_steps(self, spark):
        """comfort-score.md Step 1~5를 손으로 계산한 값과 일치한다(evidence weighting, #566).

        구 segment_comfort_score 경로와 값을 비교하던 테스트였다(#227에서 그 경로를
        제거). 공식에서 직접 계산한 기대값으로 바꿔, 산출식이 바뀌면 여기서 깨지도록
        회귀 보증을 유지한다.
        """
        rows = (
            hour(segment_id="seg-x", vertical_score=90.0, longitudinal_score=20.0,
                 lateral_score=70.0, sample_count=10),
            hour(segment_id="seg-y", vertical_score=10.0, longitudinal_score=80.0,
                 lateral_score=30.0, sample_count=20),
            # trip_count=1 < saturation(5) — hard cutoff 없이 evidence=0.2로 일부 인정된다.
            hour(segment_id="seg-y", vertical_score=99.0, trip_count=1, sample_count=99),
        )

        standard = rows_by_key(
            compute_standard_comfort_scores(
                hourly_df(spark, *rows),
                TEST_CONFIG,
                universe_df(spark, ("seg-x", 1), ("seg-y", 1)),
            )
        )

        # Step 1: 방향 점수를 가중 결합
        c_x = 0.6 * 90.0 + 0.3 * 20.0 + 0.1 * 70.0
        c_y1 = 0.6 * 10.0 + 0.3 * 80.0 + 0.1 * 30.0
        c_y2 = 0.6 * 99.0 + 0.3 * 0.0 + 0.1 * 0.0
        # Step 2: evidence weight
        e_x, e_y1, e_y2 = evidence_weight(10), evidence_weight(10), evidence_weight(1)
        # Step 3: evidence-weighted 관측 평균
        n_x, n_y = e_x, e_y1 + e_y2
        c_x_obs = c_x
        c_y_obs = (e_y1 * c_y1 + e_y2 * c_y2) / n_y
        # Step 4: mu_p는 프로필 1의 evidence 전체를 pool한 가중 평균
        mu = (e_x * c_x + e_y1 * c_y1 + e_y2 * c_y2) / (e_x + e_y1 + e_y2)
        k = 4.0
        for key, c_obs, n in (("seg-x", c_x_obs, n_x), ("seg-y", c_y_obs, n_y)):
            row = standard[(key, 1)]
            assert row.comfort_score == pytest.approx((n * c_obs + k * mu) / (n + k), abs=1e-5)
            # Step 5: Confidence = N_eff / (N_eff + k)
            assert row.confidence_score == pytest.approx(n / (n + k), abs=1e-5)

        # trip_count=1인 시간도 이제 evidence로 인정되므로 sample_count(99)가 합산된다(#566).
        assert standard[("seg-y", 1)].sample_count == 20 + 99

    def test_vehicle_agnostic_row_also_carries_directional_scores(self, spark):
        df = hourly_df(
            spark,
            hour(vehicle_profile_id=1, vertical_score=100.0, trip_count=10),
            hour(vehicle_profile_id=2, vertical_score=0.0, trip_count=30),
        )

        result = compute_standard_comfort_scores(
            df, TEST_CONFIG, universe_df(spark, ("seg-1", 1), ("seg-1", 2))
        )

        row = rows_by_key(result)[("seg-1", VEHICLE_AGNOSTIC_VEHICLE_PROFILE_ID)]
        # 트래픽 가중 평균: (10*100 + 30*0)/40 = 25. 세그먼트가 하나라 mu도 25.
        assert row.vertical_score == pytest.approx(25.0)
        assert row.comfort_score == pytest.approx(weighted_sum(row), abs=1e-5)


class TestUniverseMaterialization:
    def test_every_universe_combination_gets_a_row(self, spark):
        df = hourly_df(spark, hour(segment_id="seg-x", vehicle_profile_id=1,
                                   vertical_score=100.0))

        result = compute_standard_comfort_scores(
            df,
            TEST_CONFIG,
            universe_df(spark, ("seg-x", 1), ("seg-x", 2), ("seg-y", 1), ("seg-y", 2)),
        )

        rows = rows_by_key(result)
        for segment_id in ("seg-x", "seg-y"):
            for vehicle_profile_id in (1, 2):
                assert (segment_id, vehicle_profile_id) in rows
            # vehicle-agnostic sentinel 행도 segment마다 하나씩 나온다.
            assert (segment_id, VEHICLE_AGNOSTIC_VEHICLE_PROFILE_ID) in rows

    def test_a_never_observed_combination_falls_back_with_zero_confidence(self, spark):
        df = hourly_df(
            spark,
            hour(segment_id="seg-x", vehicle_profile_id=1, vertical_score=100.0),
            hour(segment_id="seg-y", vehicle_profile_id=1, vertical_score=0.0),
        )

        result = compute_standard_comfort_scores(
            df, TEST_CONFIG, universe_df(spark, ("seg-x", 1), ("seg-y", 1), ("seg-z", 1))
        )

        row = rows_by_key(result)[("seg-z", 1)]
        # N_eff=0 -> Step 4가 mu_p로 수렴한다. mu_p의 vertical = (100+0)/2 = 50.
        assert row.confidence_score == pytest.approx(0.0)
        assert row.vertical_score == pytest.approx(50.0)
        assert row.evidence_hours == 0
        # 롤업할 qualifying hour가 없어 경계는 NULL로 나오고, 채움은 job의 책임이다.
        assert row.data_period_start is None
        assert row.data_period_end is None

    def test_a_profile_with_no_qualifying_hour_anywhere_uses_the_global_mean(self, spark):
        df = hourly_df(
            spark,
            hour(segment_id="seg-x", vehicle_profile_id=1, vertical_score=100.0,
                 trip_count=10),
            # 프로필 2는 이 윈도우 전체에서 trip_count=0(evidence 없음)이라 mu_2가
            # 정의되지 않는다 — trip_count=2처럼 낮아도 0이 아니면 evidence weight
            # 0.4로 일부 인정되므로(#566) mu_2가 정의되려면 정확히 0이어야 한다.
            hour(segment_id="seg-y", vehicle_profile_id=2, vertical_score=0.0,
                 trip_count=0),
        )

        result = compute_standard_comfort_scores(
            df, TEST_CONFIG, universe_df(spark, ("seg-x", 1), ("seg-y", 2))
        )

        rows = rows_by_key(result)
        # 기존 Gold 경로에서는 행 자체가 생기지 않던 조합이다 (전역 mu로 대체).
        assert ("seg-y", 2) in rows
        assert rows[("seg-y", 2)].vertical_score == pytest.approx(100.0)
        assert rows[("seg-y", 2)].comfort_score is not None

    def test_universe_rejects_the_reserved_sentinel_profile(self, spark):
        df = hourly_df(spark, hour())

        with pytest.raises(ValueError, match="vehicle_profile_id=0"):
            compute_standard_comfort_scores(
                df, TEST_CONFIG, universe_df(spark, ("seg-1", 0))
            )

    def test_universe_requires_the_segment_id_column(self, spark):
        df = hourly_df(spark, hour())
        broken = Universe(
            segments=spark.createDataFrame([("seg-1",)], "other_id string"),
            profile_ids=(1,),
        )

        with pytest.raises(ValueError, match="segment_id"):
            compute_standard_comfort_scores(df, TEST_CONFIG, broken)

    def test_universe_requires_at_least_one_profile(self, spark):
        df = hourly_df(spark, hour())
        empty = Universe(
            segments=spark.createDataFrame([("seg-1",)], "segment_id string"),
            profile_ids=(),
        )

        with pytest.raises(ValueError, match="no vehicle_profile_id"):
            compute_standard_comfort_scores(df, TEST_CONFIG, empty)


class TestUniverseResolution:
    """활성 environment pointer -> manifest -> artifact URI 해석 (#198)."""

    @staticmethod
    def write_environment(tmp_path, artifacts: list[dict]) -> str:
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps({"artifacts": artifacts}))
        pointer_dir = tmp_path / "prepared" / "simulation_environment"
        pointer_dir.mkdir(parents=True)
        (pointer_dir / "active.json").write_text(
            json.dumps({"manifest_uri": manifest_path.as_uri()})
        )
        return str(tmp_path)

    def test_resolves_the_enriched_segment_reference_artifact(self, tmp_path):
        road_environment_uri = self.write_environment(
            tmp_path,
            [
                {"role": "road_segment", "uri": "file:///other.parquet"},
                {"role": "enriched_segment_reference", "uri": "file:///wanted.parquet"},
            ],
        )

        assert resolve_segment_artifact_uri(road_environment_uri) == "file:///wanted.parquet"

    def test_raises_when_the_manifest_has_no_segment_artifact(self, tmp_path):
        road_environment_uri = self.write_environment(
            tmp_path, [{"role": "road_segment", "uri": "file:///other.parquet"}]
        )

        with pytest.raises(ValueError, match="enriched_segment_reference"):
            resolve_segment_artifact_uri(road_environment_uri)
