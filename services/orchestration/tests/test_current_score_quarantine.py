# jobs/current_score_quarantine.py 테스트 (#251).

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from jobs import current_score_quarantine
from jobs.current_score_quarantine import (
    DEFAULT_SUITE_PATH,
    CurrentScoreCircuitBreakerTripped,
    check_circuit_breaker,
    compute_identity_diff,
    insert_quarantined_rows,
    load_expectation_suite,
    split_batch,
)
from jobs.weather_rules import (
    LOW_VISIBILITY,
    format_impact_signature,
    load_weather_rule_config,
)

RULE_CONFIG = load_weather_rule_config()
CALCULATED_AT = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)


def _weighted_sum(vertical: float, longitudinal: float, lateral: float) -> float:
    return (
        RULE_CONFIG.vertical_weight.value * vertical
        + RULE_CONFIG.longitudinal_weight.value * longitudinal
        + RULE_CONFIG.lateral_weight.value * lateral
    )


class TestComputeIdentityDiff:
    def test_zero_diff_when_comfort_score_matches_weighted_sum(self):
        row = {
            "vertical_score": 80.0,
            "longitudinal_score": 70.0,
            "lateral_score": 60.0,
            "comfort_score": _weighted_sum(80.0, 70.0, 60.0),
            "weather_impact_signature": None,
        }

        assert compute_identity_diff(row, RULE_CONFIG) == pytest.approx(0.0, abs=1e-9)

    def test_nonzero_diff_when_comfort_score_does_not_match(self):
        row = {
            "vertical_score": 80.0,
            "longitudinal_score": 70.0,
            "lateral_score": 60.0,
            "comfort_score": 0.0,
            "weather_impact_signature": None,
        }

        assert compute_identity_diff(row, RULE_CONFIG) > 1.0

    def test_skips_check_when_low_visibility_is_active(self):
        signature = format_impact_signature(frozenset({LOW_VISIBILITY}))
        row = {
            "vertical_score": 80.0,
            "longitudinal_score": 70.0,
            "lateral_score": 60.0,
            "comfort_score": 0.0,
            "weather_impact_signature": signature,
        }

        assert compute_identity_diff(row, RULE_CONFIG) == 0.0


class TestCheckCircuitBreaker:
    def test_does_nothing_when_no_rows_were_processed(self):
        check_circuit_breaker(upserted_count=0, quarantined_count=0)

    def test_does_nothing_when_quarantine_rate_is_within_threshold(self):
        check_circuit_breaker(upserted_count=8, quarantined_count=2)  # 20% <= 25%

    def test_trips_when_all_rows_are_quarantined(self):
        with pytest.raises(CurrentScoreCircuitBreakerTripped, match="quarantined"):
            check_circuit_breaker(upserted_count=0, quarantined_count=5)

    def test_trips_when_quarantine_rate_exceeds_threshold(self):
        with pytest.raises(CurrentScoreCircuitBreakerTripped, match="quarantine_rate"):
            check_circuit_breaker(upserted_count=7, quarantined_count=3)  # 30% > 25%
