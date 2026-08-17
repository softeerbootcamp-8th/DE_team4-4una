"""Tests for serving_api/repository.py (#160).

실제 PostgreSQL 없이 SQL과 파라미터, 행 매핑만 검증한다. 쿼리가 실제로 도는지는
통합 테스트가 확인한다.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Self

from serving_api.repository import BATCH_SQL, COLUMNS, SINGLE_SQL, fetch_many, fetch_one
from serving_api.schemas import ComfortScore, ComfortScoreKey

# COLUMNS와 같은 순서의 한 행.
ROW = (
    "0032900",
    0,
    date(2026, 8, 10),
    date(2026, 8, 16),
    82.5,
    1200,
    0.94,
    "1.0.0",
    datetime(2026, 8, 17, tzinfo=UTC),
)


class FakeCursor:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._rows = rows
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *arguments: object) -> bool:
        return False

    def execute(self, sql: str, parameters: tuple[object, ...]) -> None:
        self.executed.append((sql, parameters))

    def fetchone(self) -> tuple[object, ...] | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[tuple[object, ...]]:
        return list(self._rows)


class FakeConnection:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.opened_cursor = FakeCursor(rows)

    def cursor(self) -> FakeCursor:
        return self.opened_cursor


def test_fetch_one_maps_row_to_model() -> None:
    connection = FakeConnection([ROW])

    score = fetch_one(connection, "0032900", 0)

    assert score is not None
    assert score.segment_id == "0032900"
    assert score.vehicle_profile_id == 0
    assert score.data_period_start == date(2026, 8, 10)
    assert score.data_period_end == date(2026, 8, 16)
    assert score.comfort_score == 82.5
    assert score.sample_count == 1200
    assert score.confidence_score == 0.94
    assert score.score_version == "1.0.0"
    assert connection.opened_cursor.executed == [(SINGLE_SQL, ("0032900", 0))]


def test_fetch_one_returns_none_when_no_row_matches() -> None:
    assert fetch_one(FakeConnection([]), "0032900", 0) is None


def test_fetch_many_passes_one_array_per_key_column() -> None:
    connection = FakeConnection([ROW])
    keys = [
        ComfortScoreKey(segment_id="0032900", vehicle_profile_id=0),
        ComfortScoreKey(segment_id="0032901", vehicle_profile_id=1),
    ]

    scores = fetch_many(connection, keys)

    assert [score.segment_id for score in scores] == ["0032900"]
    assert connection.opened_cursor.executed == [
        (BATCH_SQL, (["0032900", "0032901"], [0, 1]))
    ]


def test_fetch_many_skips_the_query_when_no_keys_are_given() -> None:
    connection = FakeConnection([ROW])

    assert fetch_many(connection, []) == []
    assert connection.opened_cursor.executed == []


def test_columns_cover_every_response_field() -> None:
    # SELECT 목록과 응답 모델이 어긋나면 매핑이 조용히 깨진다.
    assert set(COLUMNS) == set(ComfortScore.model_fields)
