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


from batch_jobs.gold_audit_validation import (
    DEFAULT_RANGE_SUITE_PATHS,
    DEFAULT_SUMMARY_SUITE_PATHS,
    TABLES,
    GoldAuditValidationConfig,
    _validate_table,
)


class TestValidateTable:
    def test_accepts_known_tables(self) -> None:
        for table in TABLES:
            _validate_table(table)  # must not raise

    def test_rejects_unknown_table(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="standard_segment_comfort_score"):
            _validate_table("segment_comfort_score; DROP TABLE vehicle_profile")


class TestGoldAuditValidationConfig:
    def test_from_env_reads_postgres_and_s3_vars(self) -> None:
        config = GoldAuditValidationConfig.from_env(
            {
                "POSTGRES_HOST": "db.local",
                "POSTGRES_PORT": "5433",
                "POSTGRES_DB": "de4",
                "POSTGRES_USER": "app",
                "POSTGRES_PASSWORD": "secret",
                "GOLD_AUDIT_S3_BUCKET": "custom-bucket",
            }
        )

        assert config.postgres_host == "db.local"
        assert config.postgres_port == 5433
        assert config.postgres_db == "de4"
        assert config.postgres_user == "app"
        assert config.postgres_password == "secret"
        assert config.s3_bucket == "custom-bucket"
        assert config.range_suite_paths == DEFAULT_RANGE_SUITE_PATHS
        assert config.summary_suite_paths == DEFAULT_SUMMARY_SUITE_PATHS

    def test_from_env_defaults_s3_bucket(self) -> None:
        config = GoldAuditValidationConfig.from_env(
            {
                "POSTGRES_HOST": "db.local",
                "POSTGRES_PORT": "5433",
                "POSTGRES_DB": "de4",
                "POSTGRES_USER": "app",
                "POSTGRES_PASSWORD": "secret",
            }
        )

        assert config.s3_bucket == "de4-data-quality-docs"

    def test_from_env_requires_postgres_vars(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="POSTGRES_HOST"):
            GoldAuditValidationConfig.from_env({})

    def test_connection_string_uses_sqlalchemy_postgres_dialect(self) -> None:
        config = GoldAuditValidationConfig(
            postgres_host="db.local",
            postgres_port=5433,
            postgres_db="de4",
            postgres_user="app",
            postgres_password="secret",
            s3_bucket="de4-data-quality-docs",
            range_suite_paths=DEFAULT_RANGE_SUITE_PATHS,
            summary_suite_paths=DEFAULT_SUMMARY_SUITE_PATHS,
        )

        assert config.connection_string == "postgresql+psycopg2://app:secret@db.local:5433/de4"
