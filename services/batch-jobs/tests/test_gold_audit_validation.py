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


import os
from datetime import UTC, datetime, timedelta

import psycopg2
import pytest
from batch_jobs.gold_audit_validation import (
    GoldAuditSummary,
    GoldAuditValidationFailed,
    run_gold_audit,
)

RUN_INTEGRATION = os.environ.get("RUN_INTEGRATION") == "1"


def _pg_connect():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def _config() -> GoldAuditValidationConfig:
    from batch_jobs.gold_audit_validation import GoldAuditValidationConfig

    return GoldAuditValidationConfig(
        postgres_host=os.environ["POSTGRES_HOST"],
        postgres_port=int(os.environ["POSTGRES_PORT"]),
        postgres_db=os.environ["POSTGRES_DB"],
        postgres_user=os.environ["POSTGRES_USER"],
        postgres_password=os.environ["POSTGRES_PASSWORD"],
        s3_bucket="de4-data-quality-docs",
        range_suite_paths=DEFAULT_RANGE_SUITE_PATHS,
        summary_suite_paths=DEFAULT_SUMMARY_SUITE_PATHS,
    )


def _standard_row(**overrides: object) -> dict[str, object]:
    now = datetime.now(UTC)
    row = {
        "segment_id": "seg-1",
        "vehicle_profile_id": 0,
        "score_as_of": now,
        "data_period_start": now - timedelta(hours=168),
        "data_period_end": now,
        "vertical_score": 50.0,
        "longitudinal_score": 50.0,
        "lateral_score": 50.0,
        "comfort_score": 50.0,
        "sample_count": 10,
        "confidence_score": 0.5,
        "score_version": "1.0.0",
        "calculated_at": now,
    }
    row.update(overrides)
    return row


def _insert(connection, table: str, rows: list[dict[str, object]]) -> None:
    columns = list(rows[0].keys())
    placeholders = ", ".join(["%s"] * len(columns))
    with connection.cursor() as cursor:
        for row in rows:
            cursor.execute(
                f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
                [row[column] for column in columns],
            )
    connection.commit()


@pytest.mark.skipif(
    not RUN_INTEGRATION, reason="set RUN_INTEGRATION=1 to run against a real Postgres"
)
class TestRunGoldAuditAgainstStandardSegmentComfortScore:
    TABLE = "standard_segment_comfort_score"

    @pytest.fixture(autouse=True)
    def _clean_table(self):
        connection = _pg_connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"DELETE FROM {self.TABLE}")
            connection.commit()
        finally:
            connection.close()
        yield

    def test_succeeds_with_fresh_in_range_data(self) -> None:
        connection = _pg_connect()
        try:
            _insert(connection, self.TABLE, [_standard_row()])
            s3_client = FakeS3Client()

            summary = run_gold_audit(_config(), connection, self.TABLE, s3_client)

            assert summary == GoldAuditSummary(table=self.TABLE, row_count=1, success=True)
            assert len(s3_client.objects) > 0
            assert any(
                key.startswith(f"data-quality-audit/gold/{self.TABLE}/")
                for _, key in s3_client.objects
            )
        finally:
            connection.close()

    def test_fails_on_out_of_range_comfort_score(self) -> None:
        # comfort_score 자체는 DB CHECK 제약(0002/0006/0008 마이그레이션)이 있어
        # INSERT 단계에서 걸린다. GX range suite가 실제로 걸러내는 걸 보이려면
        # DB 제약이 없는 다른 범위 컬럼(vertical_score)으로 위반을 재현해야 한다.
        connection = _pg_connect()
        try:
            _insert(connection, self.TABLE, [_standard_row(vertical_score=150.0)])
            s3_client = FakeS3Client()

            with pytest.raises(GoldAuditValidationFailed):
                run_gold_audit(_config(), connection, self.TABLE, s3_client)

            # Data Docs는 실패해도 업로드돼 있어야 한다(soft fail, spec §6).
            assert len(s3_client.objects) > 0
        finally:
            connection.close()

    def test_fails_on_stale_score_as_of(self) -> None:
        connection = _pg_connect()
        try:
            stale_time = datetime.now(UTC) - timedelta(hours=10)
            _insert(
                connection,
                self.TABLE,
                [_standard_row(score_as_of=stale_time, data_period_end=stale_time)],
            )
            s3_client = FakeS3Client()

            with pytest.raises(GoldAuditValidationFailed):
                run_gold_audit(_config(), connection, self.TABLE, s3_client)
        finally:
            connection.close()

    def test_fails_when_table_is_empty(self) -> None:
        connection = _pg_connect()
        try:
            with pytest.raises(GoldAuditValidationFailed, match="no rows"):
                run_gold_audit(_config(), connection, self.TABLE, FakeS3Client())
        finally:
            connection.close()


@pytest.mark.skipif(
    not RUN_INTEGRATION, reason="set RUN_INTEGRATION=1 to run against a real Postgres"
)
class TestRunGoldAuditAgainstCurrentSegmentComfortScore:
    TABLE = "current_segment_comfort_score"

    @pytest.fixture(autouse=True)
    def _clean_table(self):
        connection = _pg_connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"DELETE FROM {self.TABLE}")
            connection.commit()
        finally:
            connection.close()
        yield

    def _row(self, **overrides: object) -> dict[str, object]:
        now = datetime.now(UTC)
        row = {
            "segment_id": "seg-1",
            "vehicle_profile_id": 0,
            "location_id": 1,
            "standard_score_as_of": now,
            "weather_time": None,
            "data_period_start": now - timedelta(hours=168),
            "vertical_score": 50.0,
            "longitudinal_score": 50.0,
            "lateral_score": 50.0,
            "comfort_score": 50.0,
            "sample_count": 10,
            "confidence_score": 0.5,
            "standard_score_version": "1.0.0",
            "weather_rule_version": None,
            "calculated_at": now,
        }
        row.update(overrides)
        return row

    def test_succeeds_with_fresh_in_range_data(self) -> None:
        connection = _pg_connect()
        try:
            standard_as_of = datetime.now(UTC)
            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM standard_segment_comfort_score")
                cursor.execute(
                    "INSERT INTO standard_segment_comfort_score "
                    "(segment_id, vehicle_profile_id, score_as_of, data_period_start, "
                    "data_period_end, vertical_score, longitudinal_score, lateral_score, "
                    "comfort_score, sample_count, confidence_score, score_version, calculated_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        "seg-1", 0, standard_as_of, standard_as_of, standard_as_of,
                        50.0, 50.0, 50.0, 50.0, 10, 0.5, "1.0.0", standard_as_of,
                    ),
                )
            connection.commit()
            _insert(
                connection,
                self.TABLE,
                [self._row(standard_score_as_of=standard_as_of)],
            )
            s3_client = FakeS3Client()

            summary = run_gold_audit(_config(), connection, self.TABLE, s3_client)

            assert summary == GoldAuditSummary(table=self.TABLE, row_count=1, success=True)
        finally:
            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM current_segment_comfort_score")
                cursor.execute("DELETE FROM standard_segment_comfort_score")
            connection.commit()
            connection.close()

    def test_fails_when_table_is_empty(self) -> None:
        connection = _pg_connect()
        try:
            with pytest.raises(GoldAuditValidationFailed, match="no rows"):
                run_gold_audit(_config(), connection, self.TABLE, FakeS3Client())
        finally:
            connection.close()
