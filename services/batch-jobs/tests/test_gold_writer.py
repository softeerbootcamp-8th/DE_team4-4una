"""Tests for comfort_score/gold_writer.py (#129)."""

from __future__ import annotations

import pytest
from batch_jobs.comfort_score.gold_writer import (
    _MERGE_SQL,
    EXPECTED_STAGING_COLUMNS,
    _acquire_lock,
    _merge,
    _validate_no_duplicates_or_nan,
    _validate_staging_table_shape,
)


class FakeCursor:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self._queued: list[object] = []

    def queue(self, result) -> None:
        """다음 execute() 이후 fetchone()이 반환할 값을 예약한다."""
        self._queued.append(result)

    def execute(self, sql: str, params: tuple = ()) -> None:
        self.executed.append(sql.strip())
        self._current = self._queued.pop(0) if self._queued else None

    def fetchone(self):
        return self._current

    def fetchall(self):
        return self._current or []


def test_acquire_lock_raises_when_already_held():
    cursor = FakeCursor()
    cursor.queue((False,))

    with pytest.raises(RuntimeError, match="lock"):
        _acquire_lock(cursor)


def test_acquire_lock_succeeds_when_available():
    cursor = FakeCursor()
    cursor.queue((True,))

    _acquire_lock(cursor)  # must not raise


def test_validate_staging_table_shape_raises_when_table_missing():
    cursor = FakeCursor()
    cursor.queue([])

    with pytest.raises(RuntimeError, match="make migrate"):
        _validate_staging_table_shape(cursor)


def test_validate_staging_table_shape_raises_on_type_mismatch():
    cursor = FakeCursor()
    wrong_columns = dict(EXPECTED_STAGING_COLUMNS)
    wrong_columns["sample_count"] = "integer"  # 실제는 bigint여야 함
    cursor.queue(list(wrong_columns.items()))

    with pytest.raises(RuntimeError, match="sample_count"):
        _validate_staging_table_shape(cursor)


def test_validate_staging_table_shape_passes_when_columns_match():
    cursor = FakeCursor()
    cursor.queue(list(EXPECTED_STAGING_COLUMNS.items()))

    _validate_staging_table_shape(cursor)  # must not raise


def test_validate_no_duplicates_raises_on_duplicate_keys():
    cursor = FakeCursor()
    cursor.queue((3, 2))  # 3 rows, 2 distinct keys -> 1 duplicate

    with pytest.raises(ValueError, match="duplicate"):
        _validate_no_duplicates_or_nan(cursor)


def test_validate_no_duplicates_raises_on_nan_or_infinity_scores():
    cursor = FakeCursor()
    cursor.queue((2, 2))  # no duplicates
    cursor.queue((1,))  # 1 row with NaN/Infinity

    with pytest.raises(ValueError, match="NaN"):
        _validate_no_duplicates_or_nan(cursor)


def test_validate_no_duplicates_passes_when_clean():
    cursor = FakeCursor()
    cursor.queue((2, 2))
    cursor.queue((0,))

    _validate_no_duplicates_or_nan(cursor)  # must not raise


def test_merge_returns_inserted_and_updated_counts():
    cursor = FakeCursor()
    cursor.queue((7, 3))

    inserted, updated = _merge(cursor)

    assert (inserted, updated) == (7, 3)
    assert "ON CONFLICT" in cursor.executed[-1]


def test_expected_staging_columns_include_data_period_bounds():
    assert EXPECTED_STAGING_COLUMNS["data_period_start"] == "timestamp with time zone"
    assert EXPECTED_STAGING_COLUMNS["data_period_end"] == "timestamp with time zone"


def test_merge_sql_carries_data_period_bounds_through_insert_and_update():
    # PK(segment_id, vehicle_profile_id)는 그대로 두되, 기간 컬럼은 매 rerun마다
    # 갱신돼야 한다 — 그렇지 않으면 창이 옮겨가도 예전 값이 그대로 남는다.
    assert "data_period_start" in _MERGE_SQL
    assert "data_period_end" in _MERGE_SQL
    assert "data_period_start = EXCLUDED.data_period_start" in _MERGE_SQL
    assert "data_period_end = EXCLUDED.data_period_end" in _MERGE_SQL
