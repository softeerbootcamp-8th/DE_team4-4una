"""standard_writer의 Spark 밖 구간이 PERF 로그를 남기는지 검증한다 (#461).

`write_standard_comfort_scores`는 Spark JDBC staging write 하나를 빼면 전부
psycopg2 직접 실행이다. 그 구간들은 Spark event log에 흔적이 없어, 베이스라인
수집(#462)이 Postgres 적재 시간을 보려면 이 로그가 유일한 근거다.

`df.write` 체인만 가짜로 바꾸면 SparkSession 없이 전체 흐름을 돌릴 수 있다 — 그래야
JDBC staging write 구간(#527)도 실제 코드 경로로 지나간다.
"""

from __future__ import annotations

import json
import logging

import pytest
from batch_jobs.comfort_score.standard_writer import (
    EXPECTED_STAGING_COLUMNS,
    write_standard_comfort_scores,
)
from de4_core import PERF_LOG_PREFIX

_MERGE_COUNTS = (7, 3)


class FakeCursor:
    """SQL 문자열을 보고 해당 단계가 기대하는 결과를 돌려준다."""

    def __init__(self) -> None:
        self._last_sql = ""

    def execute(self, sql: str, params: tuple = ()) -> None:
        del params
        self._last_sql = sql

    def fetchone(self):
        if "pg_try_advisory_lock" in self._last_sql:
            return (True,)
        if "count(DISTINCT" in self._last_sql:
            return (10, 10)
        if "NaN" in self._last_sql:
            return (0,)
        if "ON CONFLICT" in self._last_sql:
            return _MERGE_COUNTS
        return None

    def fetchall(self):
        return list(EXPECTED_STAGING_COLUMNS.items())

    def close(self) -> None:
        pass


class FakeConnection:
    def __init__(self) -> None:
        self.commits = 0

    def cursor(self) -> FakeCursor:
        return FakeCursor()

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        pass


def _perf_payloads(caplog) -> dict[str, dict]:
    payloads = {}
    for message in caplog.messages:
        if not message.startswith(f"{PERF_LOG_PREFIX} "):
            continue
        payload = json.loads(message[len(PERF_LOG_PREFIX) + 1 :])
        payloads[payload["phase"]] = payload
    return payloads


class FakeWriter:
    """`df.write.format(...).option(...).mode(...).save()` 체인을 그대로 받아 넘긴다."""

    def __init__(self) -> None:
        self.saved = False

    def format(self, _format: str) -> FakeWriter:
        return self

    def option(self, _key: str, _value: str) -> FakeWriter:
        return self

    def mode(self, _mode: str) -> FakeWriter:
        return self

    def save(self) -> None:
        self.saved = True


class FakeDataFrame:
    def __init__(self) -> None:
        self.write = FakeWriter()


def test_merge_phase_logs_elapsed_and_affected_rows(caplog):
    connection = FakeConnection()

    with caplog.at_level(logging.INFO):
        write_standard_comfort_scores(
            df=FakeDataFrame(),
            jdbc_url="jdbc:postgresql://localhost:5432/de4",
            postgres_user="de4",
            postgres_password="unused",
            connection=connection,
        )

    merge = _perf_payloads(caplog)["standard_score.postgres_merge"]
    assert merge["ok"] is True
    assert merge["rows"] == sum(_MERGE_COUNTS)
    assert isinstance(merge["elapsed_s"], float)


def test_every_write_phase_is_logged(caplog):
    """적재 5구간이 전부 남아야 Postgres 적재 시간을 쪼개 볼 수 있다.

    staging write는 Spark JDBC라 event log에도 남지만, MERGE·검증과 같은 축에서
    비교하려면 PERF 줄로도 남아야 한다(#527).
    """
    connection = FakeConnection()

    with caplog.at_level(logging.INFO):
        write_standard_comfort_scores(
            df=FakeDataFrame(),
            jdbc_url="jdbc:postgresql://localhost:5432/de4",
            postgres_user="de4",
            postgres_password="unused",
            connection=connection,
        )

    assert set(_perf_payloads(caplog)) == {
        "standard_score.staging_lock",
        "standard_score.jdbc_staging_write",
        "standard_score.staging_validate",
        "standard_score.postgres_merge",
        "standard_score.staging_truncate",
    }


def test_failed_phase_is_logged_with_ok_false(caplog):
    """중복 행으로 검증이 실패해도 그 구간의 소요 시간은 남아야 한다."""

    class DuplicateRowsCursor(FakeCursor):
        def fetchone(self):
            if "count(DISTINCT" in self._last_sql:
                return (10, 8)
            return super().fetchone()

    connection = FakeConnection()
    connection.cursor = DuplicateRowsCursor  # type: ignore[method-assign]

    with caplog.at_level(logging.INFO), pytest.raises(ValueError, match="duplicate"):
        write_standard_comfort_scores(
            df=FakeDataFrame(),
            jdbc_url="jdbc:postgresql://localhost:5432/de4",
            postgres_user="de4",
            postgres_password="unused",
            connection=connection,
        )

    payloads = _perf_payloads(caplog)
    assert payloads["standard_score.staging_validate"]["ok"] is False
    assert "standard_score.postgres_merge" not in payloads
