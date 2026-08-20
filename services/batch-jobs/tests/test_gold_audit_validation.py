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


from batch_jobs.gold_audit_validation import build_range_query, build_summary_query


class TestBuildRangeQuery:
    def test_selects_the_full_table(self) -> None:
        query = build_range_query("standard_segment_comfort_score")

        assert query == "SELECT * FROM standard_segment_comfort_score"

    def test_rejects_unknown_table(self) -> None:
        import pytest

        with pytest.raises(ValueError):
            build_range_query("not_a_real_table")


class TestBuildSummaryQuery:
    def test_standard_table_uses_score_as_of_for_freshness(self) -> None:
        query = build_summary_query("standard_segment_comfort_score")

        assert "MAX(score_as_of)" in query
        assert "age_seconds" in query
        assert "orphan_vehicle_profile_count" in query
        assert "LEFT JOIN vehicle_profile vp" in query
        assert "FROM standard_segment_comfort_score" in query

    def test_current_table_uses_calculated_at_for_freshness(self) -> None:
        query = build_summary_query("current_segment_comfort_score")

        assert "MAX(calculated_at)" in query
        assert "FROM current_segment_comfort_score" in query

    def test_rejects_unknown_table(self) -> None:
        import pytest

        with pytest.raises(ValueError):
            build_summary_query("not_a_real_table")


from batch_jobs.gold_audit_validation import (
    count_rows,
    load_expectation_suite,
    upload_data_docs_to_s3,
)


class _FakeCursor:
    def __init__(self, row: tuple) -> None:
        self._row = row

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None

    def execute(self, sql: str) -> None:
        self.executed_sql = sql

    def fetchone(self) -> tuple:
        return self._row


class _FakeConnection:
    def __init__(self, row: tuple) -> None:
        self._row = row
        self.cursor_used: _FakeCursor | None = None

    def cursor(self) -> _FakeCursor:
        self.cursor_used = _FakeCursor(self._row)
        return self.cursor_used


class TestCountRows:
    def test_returns_the_count_from_the_query(self) -> None:
        connection = _FakeConnection((42,))

        assert count_rows(connection, "standard_segment_comfort_score") == 42
        assert "COUNT(*)" in connection.cursor_used.executed_sql
        assert "standard_segment_comfort_score" in connection.cursor_used.executed_sql

    def test_rejects_unknown_table(self) -> None:
        import pytest

        with pytest.raises(ValueError):
            count_rows(_FakeConnection((0,)), "not_a_real_table")


class TestLoadExpectationSuite:
    def test_loads_the_committed_range_suite(self) -> None:
        from batch_jobs.gold_audit_validation import DEFAULT_RANGE_SUITE_PATHS

        suite = load_expectation_suite(
            DEFAULT_RANGE_SUITE_PATHS["standard_segment_comfort_score"]
        )

        assert suite.name == "standard_segment_comfort_score_audit_range_suite"
        assert len(suite.expectations) == 4


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def put_object(self, **kwargs: object) -> None:
        self.objects[(str(kwargs["Bucket"]), str(kwargs["Key"]))] = kwargs["Body"]  # type: ignore[assignment]


class TestUploadDataDocsToS3:
    def test_uploads_every_file_with_relative_path_as_key(self, tmp_path: Path) -> None:
        (tmp_path / "index.html").write_text("<html></html>")
        nested = tmp_path / "expectations"
        nested.mkdir()
        (nested / "suite.html").write_text("<html>suite</html>")
        client = FakeS3Client()

        uploaded = upload_data_docs_to_s3(
            tmp_path, "de4-data-quality-docs", "data-quality-audit/gold/standard_segment_comfort_score", client
        )

        assert uploaded == 2
        assert client.objects[
            ("de4-data-quality-docs", "data-quality-audit/gold/standard_segment_comfort_score/index.html")
        ] == b"<html></html>"
        assert client.objects[
            (
                "de4-data-quality-docs",
                "data-quality-audit/gold/standard_segment_comfort_score/expectations/suite.html",
            )
        ] == b"<html>suite</html>"
