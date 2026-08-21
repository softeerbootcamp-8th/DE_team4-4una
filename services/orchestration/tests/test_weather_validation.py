# jobs/weather_validation.py 테스트 (#250, ADR-0004 예외).
#
# latest_zone_weather는 GX가 아니라 인라인 Python/SQL로 검증한다 — 이유는
# weather_validation.py 모듈 docstring 참고. DB에 실제로 붙는 부분은
# test_weather_job.py와 같은 RUN_INTEGRATION=1 게이트로 로컬 Postgres에서만
# 돈다 — 기본 `pytest`에서는 건너뛴다.

from __future__ import annotations

import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Self

import psycopg2
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from jobs.weather_validation import (
    TABLE,
    WeatherValidationFailed,
    fetch_scope_rows,
    find_row_violations,
    run_weather_collection_validation,
)

RUN_INTEGRATION = os.environ.get("RUN_INTEGRATION") == "1"

TARGET_TIME = datetime(2026, 8, 19, 10, 15, tzinfo=UTC)


def _row(**overrides: object) -> dict[str, object]:
    row = {
        "location_id": 140,
        "temperature_2m_c": 20.0,
        "precipitation_mm": 0.0,
        "rain_mm": 0.0,
        "snowfall_cm": 0.0,
        "visibility_m": 10000.0,
        "wind_speed_10m_mps": 3.0,
        "wind_gusts_10m_mps": 5.0,
        "weather_code": 0,
        "weather_state": "dry",
        "impact_signature": "1.0.0|clear",
        "freshness_seconds": 30.0,
    }
    row.update(overrides)
    return row


class TestFindRowViolations:
    def test_valid_row_has_no_violations(self) -> None:
        assert find_row_violations(_row()) == []

    def test_temperature_out_of_range_is_a_violation(self) -> None:
        violations = find_row_violations(_row(temperature_2m_c=999.0))

        assert len(violations) == 1
        assert "temperature_2m_c" in violations[0]
        assert "140" in violations[0]

    def test_temperature_none_is_not_a_violation(self) -> None:
        assert find_row_violations(_row(temperature_2m_c=None)) == []

    @pytest.mark.parametrize(
        "column",
        [
            "precipitation_mm",
            "rain_mm",
            "snowfall_cm",
            "visibility_m",
            "wind_speed_10m_mps",
            "wind_gusts_10m_mps",
        ],
    )
    def test_negative_physical_magnitude_is_a_violation(self, column: str) -> None:
        violations = find_row_violations(_row(**{column: -1.0}))

        assert len(violations) == 1
        assert column in violations[0]

    def test_weather_code_out_of_range_is_a_violation(self) -> None:
        violations = find_row_violations(_row(weather_code=100))

        assert len(violations) == 1
        assert "weather_code" in violations[0]

    def test_weather_state_not_in_allowed_set_is_a_violation(self) -> None:
        violations = find_row_violations(_row(weather_state="tornado"))

        assert len(violations) == 1
        assert "weather_state" in violations[0]

    @pytest.mark.parametrize(
        "signature",
        ["not-a-signature", "1.0.0|unknown", "1.0|clear", "1.0.0|rain,rain"],
    )
    def test_malformed_impact_signature_is_a_violation(self, signature: str) -> None:
        violations = find_row_violations(_row(impact_signature=signature))

        assert len(violations) == 1
        assert "impact_signature" in violations[0]

    def test_sorted_multi_condition_impact_signature_is_valid(self) -> None:
        assert find_row_violations(_row(impact_signature="1.0.0|ice,rain,snow")) == []

    def test_freshness_out_of_range_is_a_violation(self) -> None:
        violations = find_row_violations(_row(freshness_seconds=3600.0))

        assert len(violations) == 1
        assert "freshness" in violations[0]

    def test_multiple_violations_are_all_reported(self) -> None:
        violations = find_row_violations(_row(temperature_2m_c=999.0, weather_code=100))

        assert len(violations) == 2


class _FakeCursor:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows
        self.executed_sql: str | None = None
        self.executed_params: object = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None

    def execute(self, sql: str, params: object = None) -> None:
        self.executed_sql = sql
        self.executed_params = params

    def fetchall(self) -> list[tuple]:
        return self._rows

    @property
    def description(self):
        return [(column,) for column in _SCOPE_QUERY_COLUMNS]


_SCOPE_QUERY_COLUMNS = (
    "location_id",
    "temperature_2m_c",
    "precipitation_mm",
    "rain_mm",
    "snowfall_cm",
    "visibility_m",
    "wind_speed_10m_mps",
    "wind_gusts_10m_mps",
    "weather_code",
    "weather_state",
    "impact_signature",
    "freshness_seconds",
)


class _FakeConnection:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows
        self.cursor_used: _FakeCursor | None = None

    def cursor(self) -> _FakeCursor:
        self.cursor_used = _FakeCursor(self._rows)
        return self.cursor_used


class TestFetchScopeRows:
    def test_scopes_by_weather_time_and_returns_dict_rows(self) -> None:
        connection = _FakeConnection([(140, 20.0, 0.0, 0.0, 0.0, 10000.0, 3.0, 5.0, 0, "dry", "1.0.0|clear", 30.0)])

        rows = fetch_scope_rows(connection, TARGET_TIME)

        assert rows == [_row()]
        assert TABLE in connection.cursor_used.executed_sql
        assert "weather_time" in connection.cursor_used.executed_sql
        assert connection.cursor_used.executed_params == (TARGET_TIME,)

    def test_rejects_naive_target_time(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            fetch_scope_rows(_FakeConnection([]), datetime(2026, 8, 19, 10, 15))  # noqa: DTZ001


class TestRunWeatherCollectionValidation:
    def test_raises_when_no_rows_were_upserted(self) -> None:
        connection = _FakeConnection([])

        with pytest.raises(WeatherValidationFailed, match="no .* rows"):
            run_weather_collection_validation(connection, TARGET_TIME)

    def test_raises_when_a_row_violates_a_rule(self) -> None:
        connection = _FakeConnection(
            [(140, 999.0, 0.0, 0.0, 0.0, 10000.0, 3.0, 5.0, 0, "dry", "1.0.0|clear", 30.0)]
        )

        with pytest.raises(WeatherValidationFailed, match="temperature_2m_c"):
            run_weather_collection_validation(connection, TARGET_TIME)

    def test_succeeds_when_every_row_is_valid(self) -> None:
        connection = _FakeConnection(
            [
                (140, 20.0, 0.0, 0.0, 0.0, 10000.0, 3.0, 5.0, 0, "dry", "1.0.0|clear", 30.0),
                (141, 5.0, 2.0, 2.0, 0.0, 8000.0, 4.0, 6.0, 61, "rain", "1.0.0|rain", 45.0),
            ]
        )

        summary = run_weather_collection_validation(connection, TARGET_TIME)

        assert summary.success
        assert summary.row_count == 2


@pytest.mark.skipif(
    not RUN_INTEGRATION, reason="set RUN_INTEGRATION=1 to run against a real Postgres"
)
class TestRunWeatherCollectionValidationIntegration:
    # `make migrate`가 먼저 적용돼 있어야 한다(latest_zone_weather 테이블).

    def _connect(self):
        return psycopg2.connect(
            host=os.environ["POSTGRES_HOST"],
            port=int(os.environ["POSTGRES_PORT"]),
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        )

    @pytest.fixture(autouse=True)
    def _clean_table(self):
        connection = self._connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"DELETE FROM {TABLE}")
            connection.commit()
        finally:
            connection.close()
        yield

    def _insert(self, connection, **overrides: object) -> None:
        row = {
            "location_id": 140,
            "weather_time": TARGET_TIME,
            "latitude": 40.7,
            "longitude": -73.9,
            "temperature_2m_c": 20.0,
            "precipitation_mm": 0.0,
            "rain_mm": 0.0,
            "snowfall_cm": 0.0,
            "visibility_m": 10000.0,
            "wind_speed_10m_mps": 3.0,
            "wind_gusts_10m_mps": 5.0,
            "weather_code": 0,
            "weather_state": "dry",
            "impact_signature": "1.0.0|clear",
            "fetched_at": TARGET_TIME,
        }
        row.update(overrides)
        columns = list(row.keys())
        placeholders = ", ".join(["%s"] * len(columns))
        with connection.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO {TABLE} ({', '.join(columns)}) VALUES ({placeholders})",
                [row[column] for column in columns],
            )
        connection.commit()

    def test_succeeds_against_a_real_row(self) -> None:
        connection = self._connect()
        try:
            self._insert(connection)

            summary = run_weather_collection_validation(connection, TARGET_TIME)

            assert summary.success
            assert summary.row_count == 1
        finally:
            connection.close()

    def test_raises_when_the_real_row_violates_a_rule(self) -> None:
        connection = self._connect()
        try:
            self._insert(connection, temperature_2m_c=999.0)

            with pytest.raises(WeatherValidationFailed):
                run_weather_collection_validation(connection, TARGET_TIME)
        finally:
            connection.close()
