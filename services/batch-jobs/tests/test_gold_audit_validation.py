"""Tests for batch_jobs/gold_audit_validation.py (#253, ADR-0004 at-rest audit)."""

from __future__ import annotations

import json
from pathlib import Path

RESOURCE_DIR = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "batch_jobs"
    / "resources"
    / "expectations"
)


class TestAuditSuiteFiles:
    def test_standard_range_suite_has_four_score_range_expectations(self) -> None:
        payload = json.loads(
            (RESOURCE_DIR / "standard_segment_comfort_score_audit_range_suite.json").read_text()
        )

        assert payload["name"] == "standard_segment_comfort_score_audit_range_suite"
        assert len(payload["expectations"]) == 4
        columns = {e["kwargs"]["column"] for e in payload["expectations"]}
        assert columns == {
            "comfort_score",
            "vertical_score",
            "longitudinal_score",
            "lateral_score",
        }
        for expectation in payload["expectations"]:
            assert expectation["type"] == "expect_column_values_to_be_between"
            assert expectation["kwargs"]["min_value"] == 0.0
            assert expectation["kwargs"]["max_value"] == 100.0

    def test_current_range_suite_has_four_score_range_expectations(self) -> None:
        payload = json.loads(
            (RESOURCE_DIR / "current_segment_comfort_score_audit_range_suite.json").read_text()
        )

        assert payload["name"] == "current_segment_comfort_score_audit_range_suite"
        assert len(payload["expectations"]) == 4

    def test_standard_summary_suite_checks_freshness_and_orphan_count(self) -> None:
        payload = json.loads(
            (RESOURCE_DIR / "standard_segment_comfort_score_audit_summary_suite.json").read_text()
        )

        assert payload["name"] == "standard_segment_comfort_score_audit_summary_suite"
        assert len(payload["expectations"]) == 2
        by_column = {e["kwargs"]["column"]: e for e in payload["expectations"]}
        assert by_column["age_seconds"]["kwargs"]["min_value"] == 0
        assert by_column["age_seconds"]["kwargs"]["max_value"] == 10800
        assert by_column["orphan_vehicle_profile_count"]["kwargs"]["min_value"] == 0
        assert by_column["orphan_vehicle_profile_count"]["kwargs"]["max_value"] == 0

    def test_current_summary_suite_checks_freshness_and_orphan_count(self) -> None:
        payload = json.loads(
            (RESOURCE_DIR / "current_segment_comfort_score_audit_summary_suite.json").read_text()
        )

        assert payload["name"] == "current_segment_comfort_score_audit_summary_suite"
        assert len(payload["expectations"]) == 2
