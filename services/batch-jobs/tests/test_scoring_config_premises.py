"""점수 범위 보장이 기대는 설정 전제를 로드 시점에 강제하는지 확인한다 (#495, ADR-0012).

hourly는 `100*(1-weighted_penalty/valid_weight)`, standard는
`(N*c_obs + k*mu)/(N+k)` 볼록결합이라 0~100이 공식 구조에서 따라 나온다. 그 구조는
가중치가 비음수이고 방향 가중치의 합이 1이며 shrinkage_k가 비음수라는 전제 위에
서 있는데, 지금까지 그 전제를 검사하는 코드가 없었다.
"""

from __future__ import annotations

import pytest
from batch_jobs.comfort_score.config import (
    ComfortScoreConfig,
    load_comfort_score_config,
)
from batch_jobs.comfort_scoring_config import (
    ComponentRule,
    HourlyScoringConfig,
    NormalizationRange,
    SpeedBand,
    load_hourly_scoring_config,
)
from batch_jobs.sensor_features.config import ProvisionalThreshold

_COMFORT_DEFAULTS = {
    "vertical_weight": 0.5,
    "longitudinal_weight": 0.3,
    "lateral_weight": 0.2,
    "evidence_saturation_trip_count": 5.0,
    "shrinkage_k": 10.0,
}


def _comfort_config(**overrides: float) -> ComfortScoreConfig:
    values = {**_COMFORT_DEFAULTS, **overrides}
    return ComfortScoreConfig(
        **{
            name: ProvisionalThreshold(value=value, provisional=True)
            for name, value in values.items()
        }
    )


def _hourly_config_with_components(
    components: tuple[ComponentRule, ...],
) -> HourlyScoringConfig:
    return HourlyScoringConfig(
        scoring_version="1.0.0",
        compatible_feature_versions=frozenset({"v1"}),
        minimum_valid_weight=0.5,
        speed_bands=(SpeedBand(upper_mps=None, anchor_scale=1.0),),
        normalizers=(
            ("a", NormalizationRange(comfortable=0.0, uncomfortable=1.0)),
            ("b", NormalizationRange(comfortable=0.0, uncomfortable=1.0)),
        ),
        components=components,
    )


def _hourly_config(weights: tuple[tuple[str, float], ...]) -> HourlyScoringConfig:
    return _hourly_config_with_components(
        (ComponentRule(output_column="vertical_score", weights=weights),)
    )


class TestComfortScoreConfigPremises:
    def test_accepts_weights_that_sum_to_one(self) -> None:
        assert _comfort_config().vertical_weight.value == 0.5

    def test_rejects_a_negative_direction_weight(self) -> None:
        with pytest.raises(ValueError, match="must not be negative"):
            _comfort_config(vertical_weight=-0.1, longitudinal_weight=0.9)

    def test_rejects_direction_weights_that_do_not_sum_to_one(self) -> None:
        # 합이 1.5면 comfort_score가 150까지 나올 수 있다. 방향 점수는 각각 범위
        # 안이라 GX는 통과하고, Postgres MERGE의 CHECK 제약에서야 죽는다.
        with pytest.raises(ValueError, match="must sum to 1"):
            _comfort_config(vertical_weight=1.0)

    def test_rejects_a_negative_shrinkage_k(self) -> None:
        with pytest.raises(ValueError, match="shrinkage_k"):
            _comfort_config(shrinkage_k=-1.0)

    def test_rejects_a_zero_shrinkage_k(self) -> None:
        # evidence_hours=0인 universe row(#566)에서 confidence/shrinkage 분모가
        # (N_eff + k) = (0 + k)가 되므로, k=0이면 그 행에서 0/0이 된다.
        with pytest.raises(ValueError, match="shrinkage_k"):
            _comfort_config(shrinkage_k=0.0)

    def test_rejects_a_non_positive_evidence_saturation_trip_count(self) -> None:
        with pytest.raises(ValueError, match="evidence_saturation_trip_count"):
            _comfort_config(evidence_saturation_trip_count=0.0)

    def test_the_shipped_config_satisfies_the_premises(self) -> None:
        assert load_comfort_score_config() is not None


class TestHourlyScoringConfigPremises:
    def test_accepts_non_negative_weights(self) -> None:
        assert _hourly_config((("a", 0.5), ("b", 0.5))) is not None

    def test_rejects_a_negative_component_weight(self) -> None:
        # 합은 1이라 기존 검사는 통과한다. penalty가 (1.0, 0.0)이면 점수는 150이 된다.
        with pytest.raises(ValueError, match="must not be negative"):
            _hourly_config((("a", -0.5), ("b", 1.5)))

    def test_rejects_an_empty_component_set(self) -> None:
        # component별 검증은 루프 안에 있어서, 비어 있으면 아무것도 검증하지 않고
        # 통과한다. 그 설정으로는 방향별 점수를 하나도 만들 수 없다.
        with pytest.raises(ValueError, match="components must be configured"):
            _hourly_config_with_components(())

    def test_the_shipped_config_satisfies_the_premises(self) -> None:
        assert load_hourly_scoring_config() is not None
