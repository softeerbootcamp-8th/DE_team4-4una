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
from de4_core import ObjectStore

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
    """`row_counts_by_uri`가 없는 URI(예: `_SUCCESS`)는 파일로 존재하되 Parquet은 아니다."""

    def __init__(self, row_counts_by_uri: dict[str, int], extra_uris: tuple[str, ...] = ()):
        self._row_counts_by_uri = row_counts_by_uri
        self._extra_uris = extra_uris
        # 행 수 계산이 객체 전량을 내려받지 않는다는 것을 테스트가 직접 확인할 수 있게
        # read_bytes 호출을 기록한다(#470).
        self.read_bytes_calls: list[str] = []

    def list_objects(self, uri: str):
        # Unquote the input URI to handle URL-encoded paths from join_uri
        unquoted_uri = unquote(uri)
        prefix = unquoted_uri.rstrip("/") + "/"
        return [
            _FakeObject(uri=file_uri)
            for file_uri in (*self._row_counts_by_uri, *self._extra_uris)
            if file_uri.startswith(prefix)
        ]

    def read_bytes(self, uri: str) -> bytes:
        self.read_bytes_calls.append(uri)
        return self._parquet_bytes(uri)

    def open_reader(self, uri: str):
        return io.BytesIO(self._parquet_bytes(uri))

    def _parquet_bytes(self, uri: str) -> bytes:
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


def test_counts_sum_across_multiple_parquet_files_in_one_partition():
    store = _FakeObjectStore(
        {
            "file:///lake/features/data_period_date=2026-08-18/hour=09/part-0.parquet": 30,
            "file:///lake/features/data_period_date=2026-08-18/hour=09/part-1.parquet": 12,
        }
    )

    counts = count_standard_score_pipeline_outputs(
        target_hour=datetime(2026, 8, 18, 9, tzinfo=UTC),
        as_of=datetime(2026, 8, 18, 10, tzinfo=UTC),
        quarantine_output_path="file:///lake/quarantine",
        feature_output_path="file:///lake/features",
        hourly_comfort_output_path="file:///lake/hourly_comfort_score",
        connection=_FakeConnection(result=0),
        store=store,
    )

    assert counts.feature_count == 42


def test_non_parquet_markers_are_ignored():
    # Spark 출력 디렉터리에는 _SUCCESS 같은 비-Parquet 파일이 함께 남는다.
    store = _FakeObjectStore(
        {"file:///lake/features/data_period_date=2026-08-18/hour=09/part-0.parquet": 5},
        extra_uris=("file:///lake/features/data_period_date=2026-08-18/hour=09/_SUCCESS",),
    )

    counts = count_standard_score_pipeline_outputs(
        target_hour=datetime(2026, 8, 18, 9, tzinfo=UTC),
        as_of=datetime(2026, 8, 18, 10, tzinfo=UTC),
        quarantine_output_path="file:///lake/quarantine",
        feature_output_path="file:///lake/features",
        hourly_comfort_output_path="file:///lake/hourly_comfort_score",
        connection=_FakeConnection(result=0),
        store=store,
    )

    assert counts.feature_count == 5


def test_counting_does_not_download_whole_objects():
    """행 수는 Parquet footer만 읽어 얻는다 — 객체 전량 다운로드는 이 이슈의 문제였다(#470)."""
    store = _FakeObjectStore(
        {"file:///lake/features/data_period_date=2026-08-18/hour=09/part-0.parquet": 5}
    )

    count_standard_score_pipeline_outputs(
        target_hour=datetime(2026, 8, 18, 9, tzinfo=UTC),
        as_of=datetime(2026, 8, 18, 10, tzinfo=UTC),
        quarantine_output_path="file:///lake/quarantine",
        feature_output_path="file:///lake/features",
        hourly_comfort_output_path="file:///lake/hourly_comfort_score",
        connection=_FakeConnection(result=0),
        store=store,
    )

    assert store.read_bytes_calls == []


class _FakeS3Client:
    """Range 헤더를 존중하고 실제로 전송한 바이트 수를 기록하는 최소 S3 대역."""

    def __init__(self):
        self.objects: dict[tuple[str, str], bytes] = {}
        self.bytes_served = 0

    def put_object(self, **kwargs):
        self.objects[(kwargs["Bucket"], kwargs["Key"])] = kwargs["Body"]

    def get_object(self, **kwargs):
        value = self.objects[(kwargs["Bucket"], kwargs["Key"])]
        byte_range = kwargs.get("Range")
        if byte_range is not None:
            start, _, end = byte_range.removeprefix("bytes=").partition("-")
            value = value[int(start) : int(end) + 1]
        self.bytes_served += len(value)
        return {"Body": io.BytesIO(value)}

    def head_object(self, **kwargs):
        return {"ContentLength": len(self.objects[(kwargs["Bucket"], kwargs["Key"])])}

    def list_objects_v2(self, **kwargs):
        prefix = kwargs["Prefix"]
        return {
            "Contents": [
                {"Key": key, "LastModified": datetime(2026, 8, 18, tzinfo=UTC), "Size": len(body)}
                for (bucket, key), body in self.objects.items()
                if bucket == kwargs["Bucket"] and key.startswith(prefix)
            ],
            "IsTruncated": False,
        }


def test_real_object_store_counts_rows_without_downloading_the_object():
    """fake store가 아닌 실제 ObjectStore로도 footer만 읽는지 확인한다(#470).

    두 테스트 스위트(de4-core의 Range 리더, 위쪽의 fake store)가 각각 한쪽만
    덮고 있어, 실제 Parquet 바이트가 오가는 이음매를 여기서 직접 검증한다.
    """
    client = _FakeS3Client()
    buffer = io.BytesIO()
    pq.write_table(pa.table({"x": list(range(200_000)), "y": ["z" * 20] * 200_000}), buffer)
    payload = buffer.getvalue()
    client.put_object(
        Bucket="lake",
        Key="features/data_period_date=2026-08-18/hour=09/part-0.parquet",
        Body=payload,
    )

    counts = count_standard_score_pipeline_outputs(
        target_hour=datetime(2026, 8, 18, 9, tzinfo=UTC),
        as_of=datetime(2026, 8, 18, 10, tzinfo=UTC),
        quarantine_output_path="s3://lake/quarantine",
        feature_output_path="s3://lake/features",
        hourly_comfort_output_path="s3://lake/hourly_comfort_score",
        connection=_FakeConnection(result=0),
        store=ObjectStore(client),
    )

    assert counts.feature_count == 200_000
    # footer는 파일 크기와 무관하게 꼬리 일부만 필요하다. 전량 다운로드였다면
    # bytes_served가 payload 크기와 같아진다.
    assert len(payload) > 500_000
    assert client.bytes_served < len(payload) // 4
