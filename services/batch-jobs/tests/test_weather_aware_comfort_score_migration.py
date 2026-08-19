"""Integration tests for migration 0006 (#196): standard/weather/current
comfort score tables.

RUN_INTEGRATION 미설정 시 skip(로컬 편의). RUN_INTEGRATION=1인데 Postgres
접속이 실패하면 skip이 아니라 fail한다 (test_segment_comfort_score.py와
동일한 정책).
"""

from __future__ import annotations

import os
from datetime import UTC, datetime

import psycopg2
import pytest
from batch_jobs.migrate import MigrationConfig, run_migrations

RUN_INTEGRATION = os.environ.get("RUN_INTEGRATION") == "1"

MIGRATION_FILENAME = "0006_create_weather_aware_comfort_score_tables.sql"

EXISTING_SEGMENT_COMFORT_SCORE_COLUMNS = {
    "segment_id",
    "vehicle_profile_id",
    "comfort_score",
    "confidence_score",
    "sample_count",
    "score_version",
    "calculated_at",
    "data_period_start",
    "data_period_end",
}


def _connect():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def _insert(connection, table: str, row: dict) -> None:
    columns = ", ".join(row)
    placeholders = ", ".join(["%s"] * len(row))
    with connection.cursor() as cursor:
        cursor.execute(
            f"INSERT INTO {table} ({columns}) VALUES ({placeholders})",
            tuple(row.values()),
        )
    connection.commit()


def standard_row(**overrides) -> dict:
    row = {
        "segment_id": "seg-1",
        "vehicle_profile_id": 1,
        "score_as_of": datetime(2026, 8, 19, 10, 0, tzinfo=UTC),
        "data_period_start": datetime(2026, 8, 12, 10, 0, tzinfo=UTC),
        "data_period_end": datetime(2026, 8, 19, 10, 0, tzinfo=UTC),
        "vertical_score": 80.0,
        "longitudinal_score": 80.0,
        "lateral_score": 80.0,
        "comfort_score": 80.0,
        "sample_count": 100,
        "confidence_score": 0.8,
        "score_version": "1.0.0",
        "calculated_at": datetime(2026, 8, 19, 10, 5, tzinfo=UTC),
    }
    row.update(overrides)
    return row


def weather_row(**overrides) -> dict:
    row = {
        "location_id": 181,
        "weather_time": datetime(2026, 8, 19, 10, 0, tzinfo=UTC),
        "latitude": 40.75,
        "longitude": -73.94,
        "temperature_2m_c": 20.0,
        "precipitation_mm": 0.0,
        "rain_mm": 0.0,
        "snowfall_cm": 0.0,
        "visibility_m": 10000.0,
        "wind_speed_10m_mps": 3.0,
        "wind_gusts_10m_mps": 5.0,
        "weather_code": 0,
        "weather_state": "dry",
        "impact_signature": "dry-v1",
        "fetched_at": datetime(2026, 8, 19, 10, 1, tzinfo=UTC),
    }
    row.update(overrides)
    return row


def current_row(**overrides) -> dict:
    row = {
        "segment_id": "seg-1",
        "vehicle_profile_id": 1,
        "location_id": 181,
        "standard_score_as_of": datetime(2026, 8, 19, 10, 0, tzinfo=UTC),
        "weather_time": datetime(2026, 8, 19, 10, 0, tzinfo=UTC),
        "data_period_start": datetime(2026, 8, 12, 10, 0, tzinfo=UTC),
        "vertical_score": 80.0,
        "longitudinal_score": 80.0,
        "lateral_score": 80.0,
        "comfort_score": 80.0,
        "sample_count": 100,
        "confidence_score": 0.8,
        "standard_score_version": "1.0.0",
        "weather_rule_version": "1.0.0",
        "calculated_at": datetime(2026, 8, 19, 10, 5, tzinfo=UTC),
    }
    row.update(overrides)
    return row


@pytest.mark.skipif(
    not RUN_INTEGRATION, reason="set RUN_INTEGRATION=1 to run against a real Postgres"
)
class TestWeatherAwareComfortScoreMigration:
    @staticmethod
    @pytest.fixture(scope="class", autouse=True)
    def migrated():
        connection = _connect()
        try:
            run_migrations(MigrationConfig.from_env().migrations_dir, connection)
            with connection.cursor() as cursor:
                # 0005가 이미 심어 두지만, 다른 테스트 클래스가 재정의했을 수도
                # 있으니 FK 대상이 확실히 존재하도록 독립적으로 보장한다.
                cursor.execute(
                    "INSERT INTO vehicle_profile "
                    "(vehicle_profile_id, profile_name, body_type, size_class, "
                    " vertical_response_factor, longitudinal_response_factor, "
                    " lateral_response_factor, damping_factor, steering_vibration_factor, "
                    " is_active, created_at, updated_at) "
                    "VALUES (1, 'test_profile', 'sedan', 'compact', "
                    " 1.0, 1.0, 1.0, 1.0, 1.0, TRUE, now(), now()) "
                    "ON CONFLICT (vehicle_profile_id) DO NOTHING"
                )
            connection.commit()
        finally:
            connection.close()

    @staticmethod
    def _truncate() -> None:
        connection = _connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "TRUNCATE standard_segment_comfort_score, "
                    "zone_weather_snapshot, current_segment_comfort_score"
                )
            connection.commit()
        finally:
            connection.close()

    @staticmethod
    @pytest.fixture(autouse=True)
    def clean_tables():
        # 앞뒤로 모두 비운다 — 뒤도 비우지 않으면 여기서 심은
        # vehicle_profile_id=1 참조 행이 남아, 다른 테스트 파일이
        # "DELETE FROM vehicle_profile WHERE vehicle_profile_id != 0"로
        # 정리하려 할 때 FK 위반으로 실패한다.
        TestWeatherAwareComfortScoreMigration._truncate()
        yield
        TestWeatherAwareComfortScoreMigration._truncate()

    def test_migration_applies_once_and_skips_on_rerun(self):
        connection = _connect()
        try:
            result = run_migrations(MigrationConfig.from_env().migrations_dir, connection)
            assert MIGRATION_FILENAME not in result.applied
            assert MIGRATION_FILENAME in result.skipped
        finally:
            connection.close()

    def test_existing_segment_comfort_score_table_is_untouched(self):
        connection = _connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'segment_comfort_score'"
                )
                columns = {row[0] for row in cursor.fetchall()}
            assert columns == EXISTING_SEGMENT_COMFORT_SCORE_COLUMNS
        finally:
            connection.close()

    def test_standard_rejects_duplicate_primary_key(self):
        connection = _connect()
        try:
            _insert(connection, "standard_segment_comfort_score", standard_row())
            with pytest.raises(psycopg2.errors.UniqueViolation):
                _insert(connection, "standard_segment_comfort_score", standard_row())
        finally:
            connection.rollback()
            connection.close()

    def test_standard_rejects_a_vehicle_profile_that_does_not_exist(self):
        connection = _connect()
        try:
            with pytest.raises(psycopg2.errors.ForeignKeyViolation):
                _insert(
                    connection,
                    "standard_segment_comfort_score",
                    standard_row(vehicle_profile_id=999),
                )
        finally:
            connection.rollback()
            connection.close()

    @pytest.mark.parametrize(
        "overrides",
        [
            {"data_period_start": None},
            {"data_period_end": None},
            {"data_period_start": None, "data_period_end": None},
        ],
    )
    def test_standard_rejects_a_null_data_period(self, overrides):
        """0007이 두 컬럼을 NOT NULL로 바꿨다 (#198).

        N=0이어도 배치 윈도우로 채워 넣으므로 NULL이 올라올 일이 없고, 그래서 0006의
        "둘 다 NULL이거나 둘 다 값" 짝 CHECK도 함께 사라졌다 — 반쪽이든 양쪽이든
        NotNullViolation으로 막힌다.
        """
        connection = _connect()
        try:
            with pytest.raises(psycopg2.errors.NotNullViolation):
                _insert(
                    connection,
                    "standard_segment_comfort_score",
                    standard_row(**overrides),
                )
        finally:
            connection.rollback()
            connection.close()

    def test_standard_accepts_a_zero_confidence_row_with_a_filled_window(self):
        """N=0 행은 confidence_score=0으로 식별되고, 기간은 배치 윈도우로 채워진다."""
        connection = _connect()
        try:
            _insert(
                connection,
                "standard_segment_comfort_score",
                standard_row(confidence_score=0.0),
            )
        finally:
            connection.rollback()
            connection.close()

    def test_standard_no_longer_has_a_qualifying_hour_count_column(self):
        connection = _connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'standard_segment_comfort_score'"
                )
                columns = {row[0] for row in cursor.fetchall()}
            assert "qualifying_hour_count" not in columns
        finally:
            connection.close()

    def test_weather_rejects_duplicate_primary_key(self):
        connection = _connect()
        try:
            _insert(connection, "zone_weather_snapshot", weather_row())
            with pytest.raises(psycopg2.errors.UniqueViolation):
                _insert(connection, "zone_weather_snapshot", weather_row())
        finally:
            connection.rollback()
            connection.close()

    def test_current_rejects_a_half_null_weather_pair(self):
        connection = _connect()
        try:
            _insert(connection, "standard_segment_comfort_score", standard_row())
            with pytest.raises(psycopg2.errors.CheckViolation):
                _insert(
                    connection,
                    "current_segment_comfort_score",
                    current_row(weather_time=None),
                )
        finally:
            connection.rollback()
            connection.close()

    def test_current_allows_both_weather_columns_null_before_first_fetch(self):
        connection = _connect()
        try:
            _insert(connection, "standard_segment_comfort_score", standard_row())
            _insert(
                connection,
                "current_segment_comfort_score",
                current_row(weather_time=None, weather_rule_version=None),
            )
        finally:
            connection.rollback()
            connection.close()

    def test_current_rejects_a_standard_snapshot_that_does_not_exist(self):
        connection = _connect()
        try:
            with pytest.raises(psycopg2.errors.ForeignKeyViolation):
                _insert(connection, "current_segment_comfort_score", current_row())
        finally:
            connection.rollback()
            connection.close()

    def test_current_rejects_a_weather_observation_that_does_not_exist(self):
        connection = _connect()
        try:
            _insert(connection, "standard_segment_comfort_score", standard_row())
            with pytest.raises(psycopg2.errors.ForeignKeyViolation):
                _insert(connection, "current_segment_comfort_score", current_row())
        finally:
            connection.rollback()
            connection.close()

    def test_current_succeeds_when_standard_and_weather_both_exist(self):
        connection = _connect()
        try:
            _insert(connection, "standard_segment_comfort_score", standard_row())
            _insert(connection, "zone_weather_snapshot", weather_row())
            _insert(connection, "current_segment_comfort_score", current_row())
        finally:
            connection.rollback()
            connection.close()
