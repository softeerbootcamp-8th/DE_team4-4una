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


def _valid_row(segment_id: str) -> dict:
    return {
        "segment_id": segment_id,
        "vehicle_profile_id": 1,
        "location_id": 76,
        "standard_score_as_of": CALCULATED_AT,
        "weather_time": None,
        "data_period_start": None,
        "vertical_score": 80.0,
        "longitudinal_score": 70.0,
        "lateral_score": 60.0,
        "comfort_score": _weighted_sum(80.0, 70.0, 60.0),
        "sample_count": 900,
        "confidence_score": 0.9,
        "standard_score_version": "1.0.0",
        "weather_rule_version": None,
        "weather_impact_signature": None,
        "calculated_at": CALCULATED_AT,
    }


class TestSplitBatch:
    def test_all_normal_rows_stay_normal(self):
        suite = load_expectation_suite(DEFAULT_SUITE_PATH)
        rows = [_valid_row("1"), _valid_row("2")]

        split = split_batch(rows, RULE_CONFIG, suite)

        assert [row["segment_id"] for row in split.normal_rows] == ["1", "2"]
        assert split.quarantined_records == []

    def test_out_of_range_row_is_quarantined_and_normal_row_kept(self):
        suite = load_expectation_suite(DEFAULT_SUITE_PATH)
        bad_row = _valid_row("2")
        bad_row["comfort_score"] = 150.0
        rows = [_valid_row("1"), bad_row]

        split = split_batch(rows, RULE_CONFIG, suite)

        assert [row["segment_id"] for row in split.normal_rows] == ["1"]
        assert len(split.quarantined_records) == 1
        record = split.quarantined_records[0]
        assert record["segment_id"] == "2"
        assert record["vehicle_profile_id"] == 1
        assert record["calculated_at"] == CALCULATED_AT
        assert "comfort_score" in record["reject_reason"]
        assert record["raw_row"]["segment_id"] == "2"

    def test_empty_batch_returns_empty_split(self):
        suite = load_expectation_suite(DEFAULT_SUITE_PATH)

        split = split_batch([], RULE_CONFIG, suite)

        assert split.normal_rows == []
        assert split.quarantined_records == []


class FakeQuarantineCursor:
    def __init__(self):
        self.calls: list[tuple] = []

    def execute(self, sql, parameters=None):
        self.calls.append((sql, parameters))


class TestInsertQuarantinedRows:
    def test_does_nothing_for_empty_records(self, monkeypatch):
        called = []
        monkeypatch.setattr(
            current_score_quarantine, "execute_values", lambda *a, **k: called.append(a)
        )

        insert_quarantined_rows(FakeQuarantineCursor(), [])

        assert called == []

    def test_inserts_records_via_execute_values(self, monkeypatch):
        captured = {}

        def fake_execute_values(cursor, sql, argslist):
            captured["sql"] = sql
            captured["argslist"] = argslist

        monkeypatch.setattr(current_score_quarantine, "execute_values", fake_execute_values)
        records = [
            {
                "segment_id": "2",
                "vehicle_profile_id": 1,
                "calculated_at": CALCULATED_AT,
                "reject_reason": "comfort_score",
                "reject_detail": [{"expectation_type": "expect_column_values_to_be_between"}],
                "raw_row": _valid_row("2"),
            }
        ]

        insert_quarantined_rows(FakeQuarantineCursor(), records)

        assert "current_segment_comfort_score_quarantine" in captured["sql"]
        (row,) = captured["argslist"]
        assert row[0] == "2"
        assert row[1] == 1
        assert row[2] == CALCULATED_AT
        assert row[3] == "comfort_score"
        assert len(row) == 6
