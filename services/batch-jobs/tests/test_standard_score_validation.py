"""Tests for batch_jobs/standard_score_validation.py (#249).

`standard_segment_comfort_score`(Postgres)는 SqlAlchemyExecutionEngine으로 직접
조회한다(ADR-0004). DB에 실제로 붙는 부분은 `test_current_score_signature_migration.py`와
같은 RUN_INTEGRATION=1 게이트로 로컬 Postgres에서만 돈다 — 기본 `pytest`에서는
건너뛴다.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

import psycopg2
import pytest
from batch_jobs.migrate import MigrationConfig, run_migrations
from batch_jobs.standard_score_validation import (
    DEFAULT_SUITE_PATH,
    StandardScoreValidationConfig,
    StandardScoreValidationFailed,
    build_scope_query,
    count_scope_rows,
    load_expectation_suite,
    run_standard_score_validation,
)

RUN_INTEGRATION = os.environ.get("RUN_INTEGRATION") == "1"

AS_OF = datetime(2026, 8, 19, 0, 0, 0, tzinfo=UTC)


class TestStandardScoreValidationConfig:
    def test_from_env_reads_the_same_postgres_vars_as_the_load_job(self) -> None:
        config = StandardScoreValidationConfig.from_env(
            {
                "POSTGRES_HOST": "db.local",
                "POSTGRES_PORT": "5433",
                "POSTGRES_DB": "de4",
                "POSTGRES_USER": "app",
                "POSTGRES_PASSWORD": "secret",
            }
        )

        assert config.postgres_host == "db.local"
        assert config.postgres_port == 5433
        assert config.postgres_db == "de4"
        assert config.postgres_user == "app"
        assert config.postgres_password == "secret"
        assert config.suite_path == DEFAULT_SUITE_PATH

    def test_from_env_requires_postgres_vars(self) -> None:
        with pytest.raises(ValueError, match="POSTGRES_HOST"):
            StandardScoreValidationConfig.from_env({})

    def test_from_env_reads_suite_path_override(self) -> None:
        config = StandardScoreValidationConfig.from_env(
            {
                "POSTGRES_HOST": "db.local",
                "POSTGRES_PORT": "5433",
                "POSTGRES_DB": "de4",
                "POSTGRES_USER": "app",
                "POSTGRES_PASSWORD": "secret",
                "STANDARD_SCORE_SUITE_PATH": "custom/suite.json",
            }
        )

        assert config.suite_path == Path("custom/suite.json")

    def test_connection_string_uses_sqlalchemy_postgres_dialect(self) -> None:
        config = StandardScoreValidationConfig(
            postgres_host="db.local",
            postgres_port=5433,
            postgres_db="de4",
            postgres_user="app",
            postgres_password="secret",
            suite_path=DEFAULT_SUITE_PATH,
        )

        assert config.connection_string == "postgresql+psycopg2://app:secret@db.local:5433/de4"


class TestBuildScopeQuery:
    def test_scopes_to_the_score_as_of_literal(self) -> None:
        query = build_scope_query(AS_OF)

        assert "standard_segment_comfort_score" in query
        assert "score_as_of" in query
        assert "2026-08-19T00:00:00+00:00" in query

    def test_rejects_naive_as_of(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            build_scope_query(datetime(2026, 8, 19, 0, 0, 0))  # noqa: DTZ001


class TestLoadExpectationSuite:
    def test_loads_the_committed_suite(self) -> None:
        suite = load_expectation_suite(DEFAULT_SUITE_PATH)

        assert suite.name == "standard_segment_comfort_score_suite"
        assert len(suite.expectations) == 5


def _connect():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def _row(**overrides: object) -> dict[str, object]:
    row = {
        "segment_id": "seg-1",
        "vehicle_profile_id": 1,
        "score_as_of": AS_OF,
        "data_period_start": AS_OF,
        "data_period_end": AS_OF,
        "vertical_score": 50.0,
        "longitudinal_score": 50.0,
        "lateral_score": 50.0,
        "comfort_score": 50.0,
        "sample_count": 10,
        "confidence_score": 0.5,
        "score_version": "1.0.0",
        "calculated_at": AS_OF,
    }
    row.update(overrides)
    return row


def _insert(connection, rows: list[dict[str, object]]) -> None:
    columns = list(rows[0].keys())
    placeholders = ", ".join(["%s"] * len(columns))
    with connection.cursor() as cursor:
        for row in rows:
            cursor.execute(
                f"INSERT INTO standard_segment_comfort_score ({', '.join(columns)}) "
                f"VALUES ({placeholders})",
                [row[column] for column in columns],
            )
    connection.commit()


@pytest.mark.skipif(
    not RUN_INTEGRATION, reason="set RUN_INTEGRATION=1 to run against a real Postgres"
)
class TestRunStandardScoreValidation:
    @pytest.fixture(autouse=True)
    def _clean_table(self):
        connection = _connect()
        try:
            run_migrations(MigrationConfig.from_env().migrations_dir, connection)
            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM standard_segment_comfort_score")
            connection.commit()
        finally:
            connection.close()
        yield

    def config(self) -> StandardScoreValidationConfig:
        return StandardScoreValidationConfig.from_env()

    def test_succeeds_when_scores_are_in_range(self) -> None:
        connection = _connect()
        try:
            _insert(connection, [_row()])

            summary = run_standard_score_validation(self.config(), AS_OF, connection)

            assert summary.success
            assert summary.row_count == 1
        finally:
            connection.close()

    def test_raises_when_comfort_score_is_out_of_range(self) -> None:
        connection = _connect()
        try:
            _insert(connection, [_row(comfort_score=150.0)])

            with pytest.raises(StandardScoreValidationFailed):
                run_standard_score_validation(self.config(), AS_OF, connection)
        finally:
            connection.close()

    def test_raises_when_no_rows_match_the_as_of(self) -> None:
        connection = _connect()
        try:
            with pytest.raises(StandardScoreValidationFailed):
                run_standard_score_validation(self.config(), AS_OF, connection)
        finally:
            connection.close()

    def test_count_scope_rows_only_counts_the_matching_as_of(self) -> None:
        connection = _connect()
        try:
            other_as_of = datetime(2026, 8, 18, 0, 0, 0, tzinfo=UTC)
            _insert(connection, [_row(), _row(score_as_of=other_as_of)])

            assert count_scope_rows(connection, AS_OF) == 1
        finally:
            connection.close()
