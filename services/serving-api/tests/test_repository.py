"""Tests for serving_api/repository.py (#160, #226).

실제 PostgreSQL 없이 SQL과 파라미터, 행 매핑, 폴백 분기만 검증한다. 쿼리가 실제로
도는지는 통합 테스트가 확인한다.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Self

from serving_api.repository import (
    CURRENT_BATCH_SQL,
    CURRENT_SINGLE_SQL,
    CURRENT_TABLE,
    STANDARD_BATCH_SQL,
    STANDARD_TABLE,
    fetch_many,
    fetch_one,
)

PERIOD_START = datetime(2026, 8, 10, tzinfo=UTC)
AS_OF = datetime(2026, 8, 17, tzinfo=UTC)


def current_row(segment_id: str, comfort_score: float = 82.5) -> tuple[object, ...]:
    """ROW_FIELDS 순서의 current 행 — 날씨가 반영된 경우."""
    return (
        segment_id, 0, comfort_score, 90.0, 70.0, 80.0, 0.94, 1200,
        PERIOD_START, AS_OF, "1.0.0", AS_OF, "1.0.0", AS_OF,
    )


def standard_row(segment_id: str, comfort_score: float = 96.3) -> tuple[object, ...]:
    """같은 순서의 standard 행 — 날씨 컬럼이 NULL로 채워져 온다."""
    return (
        segment_id, 0, comfort_score, 96.0, 97.0, 96.0, 0.17, 900,
        PERIOD_START, AS_OF, "1.0.0", None, None, AS_OF,
    )


class FakeCursor:
    """조회 대상 테이블에 따라 다른 행을 돌려준다 — 폴백 분기를 검증하기 위한 것."""

    def __init__(self, current: list[tuple], standard: list[tuple]) -> None:
        self._current = current
        self._standard = standard
        self._rows: list[tuple] = []
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *arguments: object) -> bool:
        return False

    def execute(self, sql: str, parameters: tuple[object, ...]) -> None:
        self.executed.append((sql, parameters))
        if CURRENT_TABLE in sql:
            self._rows = list(self._current)
        elif STANDARD_TABLE in sql:
            requested = set(parameters[1])
            self._rows = [row for row in self._standard if row[0] in requested]
        else:
            self._rows = []

    def fetchone(self) -> tuple[object, ...] | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[tuple[object, ...]]:
        return list(self._rows)


class FakeConnection:
    def __init__(self, current: list[tuple] = (), standard: list[tuple] = ()) -> None:
        self.opened_cursor = FakeCursor(list(current), list(standard))

    def cursor(self) -> FakeCursor:
        return self.opened_cursor


def queried_tables(connection: FakeConnection) -> list[str]:
    return [
        CURRENT_TABLE if CURRENT_TABLE in sql else STANDARD_TABLE
        for sql, _ in connection.opened_cursor.executed
    ]


class TestFetchOne:
    def test_returns_the_current_row_when_it_exists(self) -> None:
        connection = FakeConnection(current=[current_row("0032900")])

        score = fetch_one(connection, "0032900", 0)

        assert score is not None
        assert score.source == "current"
        assert score.comfort_score == 82.5
        assert score.weather_time == AS_OF
        assert score.weather_rule_version == "1.0.0"
        # 날씨가 있으면 standard를 볼 이유가 없다
        assert queried_tables(connection) == [CURRENT_TABLE]

    def test_falls_back_to_standard_when_current_is_missing(self) -> None:
        connection = FakeConnection(current=[], standard=[standard_row("0032900")])

        score = fetch_one(connection, "0032900", 0)

        assert score is not None
        assert score.source == "standard"
        assert score.comfort_score == 96.3
        # 폴백 응답은 날씨를 반영하지 않았음을 null로 알린다
        assert score.weather_time is None
        assert score.weather_rule_version is None
        assert queried_tables(connection) == [CURRENT_TABLE, STANDARD_TABLE]

    def test_returns_none_when_neither_table_has_the_row(self) -> None:
        connection = FakeConnection()

        assert fetch_one(connection, "0032900", 0) is None

    def test_passes_the_lookup_keys_as_parameters(self) -> None:
        connection = FakeConnection(current=[current_row("0032900")])

        fetch_one(connection, "0032900", 0)
        sql, parameters = connection.opened_cursor.executed[0]

        assert sql == CURRENT_SINGLE_SQL
        assert parameters == ("0032900", 0)


class TestFetchMany:
    def test_returns_current_rows_without_a_second_query(self) -> None:
        connection = FakeConnection(current=[current_row("1"), current_row("2")])

        scores = fetch_many(connection, 0, ["1", "2"])

        assert [score.segment_id for score in scores] == ["1", "2"]
        assert {score.source for score in scores} == {"current"}
        assert queried_tables(connection) == [CURRENT_TABLE]

    def test_falls_back_only_for_the_missing_segments(self) -> None:
        connection = FakeConnection(
            current=[current_row("1")],
            standard=[standard_row("2"), standard_row("3")],
        )

        scores = fetch_many(connection, 0, ["1", "2"])
        by_segment = {score.segment_id: score.source for score in scores}

        assert by_segment == {"1": "current", "2": "standard"}
        # 두 번째 쿼리에는 빠진 구간만 넘어간다 — 3은 요청에 없었으므로 조회되지 않는다
        _, parameters = connection.opened_cursor.executed[1]
        assert parameters == (0, ["2"])

    def test_omits_a_segment_that_neither_table_has(self) -> None:
        connection = FakeConnection(current=[current_row("1")])

        scores = fetch_many(connection, 0, ["1", "missing"])

        assert [score.segment_id for score in scores] == ["1"]

    def test_no_segment_ids_skips_the_database(self) -> None:
        connection = FakeConnection(current=[current_row("1")])

        assert fetch_many(connection, 0, []) == []
        assert connection.opened_cursor.executed == []

    def test_batch_sql_uses_array_parameters(self) -> None:
        connection = FakeConnection(current=[current_row("1")])

        fetch_many(connection, 0, ["1"])
        sql, parameters = connection.opened_cursor.executed[0]

        assert sql == CURRENT_BATCH_SQL
        assert parameters == (0, ["1"])


def test_standard_query_takes_the_latest_run_per_segment() -> None:
    # standard는 실행마다 행이 쌓이므로 최신 하나만 골라야 한다.
    assert "DISTINCT ON (segment_id, vehicle_profile_id)" in STANDARD_BATCH_SQL
    assert "ORDER BY segment_id, vehicle_profile_id, score_as_of DESC" in STANDARD_BATCH_SQL
