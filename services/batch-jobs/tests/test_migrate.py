"""Tests for batch_jobs/migrate.py (#129)."""

from __future__ import annotations

from pathlib import Path

import pytest
from batch_jobs.migrate import run_migrations


class FakeCursor:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self._current: tuple | None = None

    def execute(self, sql: str, params: tuple = ()) -> None:
        self.connection.executed.append(sql.strip())
        normalized = sql.strip()
        if normalized.startswith("SELECT pg_try_advisory_lock"):
            self._current = (True,)
        elif normalized.startswith("SELECT pg_advisory_unlock"):
            self._current = None
        elif normalized.startswith("SELECT checksum FROM schema_migrations"):
            (filename,) = params
            checksum = self.connection.applied.get(filename)
            self._current = (checksum,) if checksum is not None else None
        elif normalized.startswith("INSERT INTO schema_migrations"):
            filename, checksum = params
            self.connection.applied[filename] = checksum
            self._current = None
        else:
            # CREATE TABLE IF NOT EXISTS schema_migrations, 각 마이그레이션
            # 파일 본문 등 — 실행됐다는 사실만 기록하고 결과행은 없다.
            self._current = None

    def fetchone(self) -> tuple | None:
        return self._current

    def close(self) -> None:
        pass


class FakeConnection:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.applied: dict[str, str] = {}
        self.committed = 0
        self.rolled_back = 0

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.committed += 1

    def rollback(self) -> None:
        self.rolled_back += 1


def write_migration(directory: Path, name: str, sql: str) -> None:
    (directory / name).write_text(sql)


def test_applies_new_migration_files_in_order(tmp_path):
    write_migration(tmp_path, "0001_a.sql", "CREATE TABLE a (id INT);")
    write_migration(tmp_path, "0002_b.sql", "CREATE TABLE b (id INT);")
    connection = FakeConnection()

    result = run_migrations(tmp_path, connection)

    assert result.applied == ("0001_a.sql", "0002_b.sql")
    assert result.skipped == ()
    assert set(connection.applied) == {"0001_a.sql", "0002_b.sql"}


def test_skips_already_applied_migration_with_matching_checksum(tmp_path):
    write_migration(tmp_path, "0001_a.sql", "CREATE TABLE a (id INT);")
    connection = FakeConnection()
    run_migrations(tmp_path, connection)

    result = run_migrations(tmp_path, connection)

    assert result.applied == ()
    assert result.skipped == ("0001_a.sql",)


def test_raises_when_an_applied_migration_file_is_modified(tmp_path):
    write_migration(tmp_path, "0001_a.sql", "CREATE TABLE a (id INT);")
    connection = FakeConnection()
    run_migrations(tmp_path, connection)
    write_migration(tmp_path, "0001_a.sql", "CREATE TABLE a (id INT, extra INT);")

    with pytest.raises(ValueError, match="0001_a.sql"):
        run_migrations(tmp_path, connection)
