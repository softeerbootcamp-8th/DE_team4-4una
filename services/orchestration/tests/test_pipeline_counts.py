"""jobs/pipeline_counts.py의 orchestration 전용 count 조회 로직(#409)을 검증한다.

실제 S3/Postgres 없이 ObjectStore와 psycopg2 connection을 fake로 주입한다 —
실제 값 확인은 로컬 Airflow 수동 검증(README)에서 다룬다.
"""

from __future__ import annotations

import io
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import unquote

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from jobs.pipeline_counts import (
    PostgresConfig,
    count_audit_gold_tables,
    count_standard_score_pipeline_outputs,
)


@dataclass(frozen=True, slots=True)
class _FakeObject:
    uri: str


class _FakeObjectStore:
    def __init__(self, row_counts_by_uri: dict[str, int]):
        self._row_counts_by_uri = row_counts_by_uri

    def list_objects(self, uri: str):
        # Unquote the input URI to handle URL-encoded paths from join_uri
        unquoted_uri = unquote(uri)
        prefix = unquoted_uri.rstrip("/") + "/"
        return [
            _FakeObject(uri=file_uri)
            for file_uri in self._row_counts_by_uri
            if file_uri.startswith(prefix)
        ]

    def read_bytes(self, uri: str) -> bytes:
        # Unquote the input URI to handle URL-encoded paths from join_uri
        unquoted_uri = unquote(uri)
        table = pa.table({"x": list(range(self._row_counts_by_uri[unquoted_uri]))})
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        return buffer.getvalue()


class _FakeCursor:
    def __init__(self, result):
        self.result = result
        self.last_sql: str | None = None
        self.last_params = None

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, sql, params=None):
        self.last_sql = sql
        self.last_params = params

    def fetchone(self):
        return (self.result,)


class _FakeConnection:
    def __init__(self, result):
        self.cursor_obj = _FakeCursor(result)

    def cursor(self):
        return self.cursor_obj


def test_postgres_config_reads_from_env(monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", "db")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "de4")
    monkeypatch.setenv("POSTGRES_USER", "u")
    monkeypatch.setenv("POSTGRES_PASSWORD", "p")

    config = PostgresConfig.from_env()

    assert config.as_connect_kwargs() == {
        "host": "db",
        "port": "5432",
        "dbname": "de4",
        "user": "u",
        "password": "p",
    }


def test_counts_quarantine_feature_and_hourly_comfort_partitions():
    store = _FakeObjectStore(
        {
            "file:///lake/quarantine/target_date=2026-08-18/target_hour=09/part-0.parquet": 3,
            "file:///lake/features/data_period_date=2026-08-18/hour=09/part-0.parquet": 80,
            "file:///lake/hourly_comfort_score/part-0.parquet": 80,
        }
    )
    connection = _FakeConnection(result=100)

    counts = count_standard_score_pipeline_outputs(
        target_hour=datetime(2026, 8, 18, 9, tzinfo=UTC),
        as_of=datetime(2026, 8, 18, 10, tzinfo=UTC),
        quarantine_output_path="file:///lake/quarantine",
        feature_output_path="file:///lake/features",
        hourly_comfort_output_path="file:///lake/hourly_comfort_score",
        connection=connection,
        store=store,
    )

    assert counts.quarantine_count == 3
    assert counts.feature_count == 80
    assert counts.hourly_comfort_score_count == 80
    assert counts.standard_segment_comfort_score_count == 100


def test_empty_partition_counts_as_zero():
    connection = _FakeConnection(result=0)

    counts = count_standard_score_pipeline_outputs(
        target_hour=datetime(2026, 8, 18, 9, tzinfo=UTC),
        as_of=datetime(2026, 8, 18, 10, tzinfo=UTC),
        quarantine_output_path="file:///lake/quarantine",
        feature_output_path="file:///lake/features",
        hourly_comfort_output_path="file:///lake/hourly_comfort_score",
        connection=connection,
        store=_FakeObjectStore({}),
    )

    assert counts.quarantine_count == 0
    assert counts.feature_count == 0
    assert counts.hourly_comfort_score_count == 0


def test_standard_score_query_filters_by_as_of():
    connection = _FakeConnection(result=42)

    count_standard_score_pipeline_outputs(
        target_hour=datetime(2026, 8, 18, 9, tzinfo=UTC),
        as_of=datetime(2026, 8, 18, 10, tzinfo=UTC),
        quarantine_output_path="file:///lake/quarantine",
        feature_output_path="file:///lake/features",
        hourly_comfort_output_path="file:///lake/hourly_comfort_score",
        connection=connection,
        store=_FakeObjectStore({}),
    )

    assert connection.cursor_obj.last_params == (datetime(2026, 8, 18, 10, tzinfo=UTC),)


def test_count_audit_gold_tables_queries_both_tables():
    connection = _FakeConnection(result=7)

    counts = count_audit_gold_tables(connection=connection)

    assert counts == {
        "standard_segment_comfort_score": 7,
        "current_segment_comfort_score": 7,
    }
