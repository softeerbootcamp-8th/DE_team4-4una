# migration 0007(#209) 통합 테스트: 시계열 zone_weather_snapshot → 존당 최신 1행 latest_zone_weather.

from __future__ import annotations

import os
from datetime import UTC, datetime

import psycopg2
import pytest
from batch_jobs.migrate import MigrationConfig, run_migrations

RUN_INTEGRATION = os.environ.get("RUN_INTEGRATION") == "1"

MIGRATION_FILENAME = "0007_latest_zone_weather.sql"


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
class TestChangeWeatherStorageMigration:
    def test_migration_applies_once_and_skips_on_rerun(self):
        connection = _connect()
        try:
            result = run_migrations(MigrationConfig.from_env().migrations_dir, connection)
            assert MIGRATION_FILENAME not in result.applied
            assert MIGRATION_FILENAME in result.skipped
        finally:
            connection.close()

    def test_zone_weather_snapshot_no_longer_exists(self):
        connection = _connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT to_regclass('zone_weather_snapshot')")
                (result,) = cursor.fetchone()
            assert result is None
        finally:
            connection.close()

    def test_latest_zone_weather_primary_key_is_location_id_only(self):
        connection = _connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT a.attname FROM pg_index i "
                    "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
                    "WHERE i.indrelid = 'latest_zone_weather'::regclass AND i.indisprimary"
                )
                columns = {row[0] for row in cursor.fetchall()}
            assert columns == {"location_id"}
        finally:
            connection.close()

    def test_current_segment_comfort_score_has_no_weather_fk(self):
        connection = _connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT confrelid::regclass::text FROM pg_constraint "
                    "WHERE conrelid = 'current_segment_comfort_score'::regclass AND contype = 'f'"
                )
                referenced_tables = {row[0] for row in cursor.fetchall()}
            assert referenced_tables == {"standard_segment_comfort_score"}
        finally:
            connection.close()

    def test_dedup_keeps_only_the_latest_weather_time_per_zone(self):
        # 임시 스키마에 0006까지 재생해 옛 다중행 상태를 만들고 0007로 정리되는지 확인(public 스키마는 안 건드림).
        migrations_dir = MigrationConfig.from_env().migrations_dir
        pre_209_sql = [
            path.read_text()
            for path in sorted(migrations_dir.glob("*.sql"))
            if path.name < MIGRATION_FILENAME
        ]
        migration_209_sql = (migrations_dir / MIGRATION_FILENAME).read_text()

        connection = _connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS test_209_dedup CASCADE")
                cursor.execute("CREATE SCHEMA test_209_dedup")
                cursor.execute("SET search_path TO test_209_dedup")
                for sql in pre_209_sql:
                    cursor.execute(sql)
                cursor.execute(
                    "INSERT INTO zone_weather_snapshot "
                    "(location_id, weather_time, latitude, longitude, weather_state, "
                    " impact_signature, fetched_at) VALUES "
                    "(181, %(t1)s, 40.7, -73.9, 'dry', 'sig-1', now()), "
                    "(181, %(t2)s, 40.7, -73.9, 'dry', 'sig-2', now()), "
                    "(181, %(t3)s, 40.7, -73.9, 'rain', 'sig-3', now())",
                    {
                        "t1": datetime(2026, 8, 19, 10, 0, tzinfo=UTC),
                        "t2": datetime(2026, 8, 19, 10, 15, tzinfo=UTC),
                        "t3": datetime(2026, 8, 19, 10, 30, tzinfo=UTC),
                    },
                )
                cursor.execute(migration_209_sql)
                cursor.execute(
                    "SELECT weather_time, weather_state FROM latest_zone_weather "
                    "WHERE location_id = 181"
                )
                rows = cursor.fetchall()
            connection.rollback()
            assert rows == [(datetime(2026, 8, 19, 10, 30, tzinfo=UTC), "rain")]
        finally:
            with connection.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS test_209_dedup CASCADE")
            connection.commit()
            connection.close()
