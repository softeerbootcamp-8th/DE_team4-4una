# migration 0009(#216) 통합 테스트: current_segment_comfort_score에 weather_impact_signature 추가.

from __future__ import annotations

import os

import psycopg2
import pytest
from batch_jobs.migrate import MigrationConfig, run_migrations

RUN_INTEGRATION = os.environ.get("RUN_INTEGRATION") == "1"

MIGRATION_FILENAME = "0009_current_score_impact_signature.sql"
TABLE = "current_segment_comfort_score"


def _connect():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


@pytest.mark.skipif(
    not RUN_INTEGRATION, reason="set RUN_INTEGRATION=1 to run against a real Postgres"
)
class TestCurrentScoreSignatureMigration:
    def test_migration_applies_once_and_skips_on_rerun(self):
        connection = _connect()
        try:
            result = run_migrations(MigrationConfig.from_env().migrations_dir, connection)

            assert MIGRATION_FILENAME not in result.applied
            assert MIGRATION_FILENAME in result.skipped
        finally:
            connection.close()

    def test_signature_column_is_nullable_text(self):
        connection = _connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT data_type, is_nullable FROM information_schema.columns "
                    "WHERE table_name = %s AND column_name = 'weather_impact_signature'",
                    (TABLE,),
                )
                row = cursor.fetchone()

            assert row == ("text", "YES")
        finally:
            connection.close()

    def test_signature_must_be_null_exactly_when_weather_time_is_null(self):
        connection = _connect()
        try:
            with connection.cursor() as cursor:
                # weather_time만 NULL로 두고 서명을 채우면 CHECK가 막아야 한다.
                cursor.execute(
                    "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                    "WHERE conname = %s",
                    (f"{TABLE}_impact_signature_check",),
                )
                (definition,) = cursor.fetchone()

            assert "weather_time IS NULL" in definition
            assert "weather_impact_signature IS NULL" in definition
        finally:
            connection.close()

    def test_location_id_is_indexed_for_the_zone_scoped_path(self):
        connection = _connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT indexdef FROM pg_indexes "
                    "WHERE tablename = %s AND indexname = %s",
                    (TABLE, f"{TABLE}_location_id_idx"),
                )
                row = cursor.fetchone()

            assert row is not None
            assert "location_id" in row[0]
        finally:
            connection.close()
