"""Tests for batch_jobs/comfort_score/standard_storage.py (#265, #343).

같은 as_of로 재실행하면 새 version 경로에 쓰고 검증까지 끝난 뒤에만 manifest를
전환한다는 계약을 검증한다: 기존 version은 절대 건드리지 않고(불변), write/검증
실패 시 manifest와 활성 snapshot이 그대로 남고, manifest resolve는 명확히 실패해야
할 때 명확히 실패한다.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from urllib.parse import unquote

import pytest
from batch_jobs.comfort_score.standard_storage import (
    audit_standard_snapshot,
    read_active_standard_comfort_score_snapshot,
    resolve_active_standard_snapshot_uri,
    standard_manifest_uri,
    standard_snapshot_uri,
    standard_version_uri,
    write_standard_comfort_score_snapshot,
)
from de4_core import ObjectStore
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.readwriter import DataFrameReader, DataFrameWriter

BASE_SCHEMA = "segment_id string, vehicle_profile_id int, comfort_score double"


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("batch-jobs-standard-storage-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def rows_df(spark, as_of: datetime, *rows: tuple):
    # naive datetime을 스키마에 직접 실으면 호스트 타임존에 따라 해석이 갈리므로,
    # job과 동일하게 F.lit(as_of)로 붙인다.
    return spark.createDataFrame(list(rows), BASE_SCHEMA).withColumn(
        "score_as_of", F.lit(as_of)
    )


class TestUriHelpers:
    AS_OF = datetime(2026, 8, 23, 3, 0, 0, tzinfo=UTC)

    def test_same_as_of_always_yields_the_same_root_uri(self):
        assert standard_snapshot_uri("out", self.AS_OF) == standard_snapshot_uri("out", self.AS_OF)

    def test_different_as_of_yields_different_root_uris(self):
        other = self.AS_OF.replace(hour=4)
        assert standard_snapshot_uri("out", self.AS_OF) != standard_snapshot_uri("out", other)

    def test_root_uri_encodes_score_as_of_date_and_score_as_of(self):
        # 로컬 file:// 스킴은 join_uri()가 '='를 %3D로 인코딩하므로 unquote로 확인한다.
        uri = unquote(standard_snapshot_uri("out", self.AS_OF))

        assert "score_as_of_date=2026-08-23" in uri
        assert "score_as_of=2026-08-23T03-00-00Z" in uri

    def test_version_uri_is_nested_under_versions_of_the_root(self):
        root = standard_snapshot_uri("out", self.AS_OF)

        assert unquote(standard_version_uri(root, "v1")) == f"{unquote(root)}/versions/v1"

    def test_manifest_uri_is_named_manifest_json_under_the_root(self):
        root = standard_snapshot_uri("out", self.AS_OF)

        assert unquote(standard_manifest_uri(root)) == f"{unquote(root)}/manifest.json"


class TestAuditStandardSnapshot:
    """행 수와 score_as_of 검사를 aggregation 한 번으로 처리한다."""

    AS_OF = datetime(2026, 8, 23, 3, 0, 0, tzinfo=UTC)

    def test_returns_the_row_count_when_every_row_matches(self, spark) -> None:
        df = rows_df(spark, self.AS_OF, ("seg-1", 1, 50.0), ("seg-2", 1, 60.0))

        assert audit_standard_snapshot(df, self.AS_OF) == 2

    def test_rejects_a_mismatched_score_as_of(self, spark) -> None:
        df = rows_df(spark, self.AS_OF.replace(hour=4), ("seg-1", 1, 50.0))

        with pytest.raises(ValueError, match="score_as_of"):
            audit_standard_snapshot(df, self.AS_OF)

    def test_an_empty_frame_counts_zero(self, spark) -> None:
        """global aggregation은 빈 입력에도 한 행을 돌려주므로 0으로 확정돼야 한다."""
        empty = rows_df(spark, self.AS_OF).limit(0)

        assert audit_standard_snapshot(empty, self.AS_OF) == 0


class TestWriteStandardComfortScoreSnapshot:
    AS_OF = datetime(2026, 8, 23, 3, 0, 0, tzinfo=UTC)

    def test_a_caller_supplied_count_is_used_for_the_read_back_check(
        self, spark, tmp_path
    ) -> None:
        """호출자가 넘긴 행 수가 read-back 대조 기준이 된다 — 틀리면 승격 전에 막힌다."""
        df = rows_df(spark, self.AS_OF, ("seg-1", 1, 50.0), ("seg-2", 1, 60.0))

        with pytest.raises(ValueError, match="read-back row count"):
            write_standard_comfort_score_snapshot(
                spark, df, str(tmp_path), self.AS_OF, expected_count=99
            )

    def test_rejects_naive_as_of_when_writing(self, spark, tmp_path) -> None:
        naive_as_of = datetime(2026, 8, 23, 3, 0, 0)  # noqa: DTZ001
        df = rows_df(spark, naive_as_of.replace(tzinfo=UTC), ("seg-1", 1, 50.0))

        with pytest.raises(ValueError, match="timezone-aware"):
            write_standard_comfort_score_snapshot(spark, df, str(tmp_path), naive_as_of)

    def test_rejects_rows_whose_score_as_of_does_not_match(self, spark, tmp_path) -> None:
        other_as_of = self.AS_OF.replace(hour=4)
        df = rows_df(spark, other_as_of, ("seg-1", 1, 50.0))

        with pytest.raises(ValueError, match="score_as_of"):
            write_standard_comfort_score_snapshot(spark, df, str(tmp_path), self.AS_OF)

    def test_first_write_creates_a_version_and_an_active_manifest(self, spark, tmp_path) -> None:
        output_root = str(tmp_path)
        df = rows_df(spark, self.AS_OF, ("seg-1", 1, 50.0), ("seg-2", 1, 60.0))

        result = write_standard_comfort_score_snapshot(spark, df, output_root, self.AS_OF)

        root = standard_snapshot_uri(output_root, self.AS_OF)
        assert result.version_uri == standard_version_uri(root, result.version_id)
        assert result.row_count == 2
        assert spark.read.parquet(result.version_uri).count() == 2
        assert resolve_active_standard_snapshot_uri(output_root, self.AS_OF) == result.version_uri

    def test_rerun_writes_a_new_version_without_touching_the_old_one(self, spark, tmp_path) -> None:
        output_root = str(tmp_path)
        first_df = rows_df(spark, self.AS_OF, ("seg-1", 1, 50.0))
        second_df = rows_df(spark, self.AS_OF, ("seg-2", 1, 60.0), ("seg-3", 1, 70.0))

        first = write_standard_comfort_score_snapshot(spark, first_df, output_root, self.AS_OF)
        second = write_standard_comfort_score_snapshot(spark, second_df, output_root, self.AS_OF)

        assert second.version_id != first.version_id
        assert second.version_uri != first.version_uri
        old_rows = spark.read.parquet(first.version_uri).collect()
        assert [row["segment_id"] for row in old_rows] == ["seg-1"]

    def test_manifest_points_to_the_latest_version_after_a_successful_rerun(
        self, spark, tmp_path
    ) -> None:
        output_root = str(tmp_path)
        first_df = rows_df(spark, self.AS_OF, ("seg-1", 1, 50.0))
        second_df = rows_df(spark, self.AS_OF, ("seg-2", 1, 60.0))

        write_standard_comfort_score_snapshot(spark, first_df, output_root, self.AS_OF)
        second = write_standard_comfort_score_snapshot(spark, second_df, output_root, self.AS_OF)

        active_uri = resolve_active_standard_snapshot_uri(output_root, self.AS_OF)
        assert active_uri == second.version_uri
        rows = spark.read.parquet(active_uri).collect()
        assert [row["segment_id"] for row in rows] == ["seg-2"]

    def test_write_failure_leaves_the_manifest_and_active_snapshot_untouched(
        self, spark, tmp_path, monkeypatch
    ) -> None:
        output_root = str(tmp_path)
        original = write_standard_comfort_score_snapshot(
            spark, rows_df(spark, self.AS_OF, ("seg-1", 1, 50.0)), output_root, self.AS_OF
        )

        def failing_parquet(self, *args, **kwargs):
            raise RuntimeError("simulated s3 write failure")

        monkeypatch.setattr(DataFrameWriter, "parquet", failing_parquet)

        broken_df = rows_df(spark, self.AS_OF, ("seg-2", 1, 60.0))
        with pytest.raises(RuntimeError, match="simulated s3 write failure"):
            write_standard_comfort_score_snapshot(spark, broken_df, output_root, self.AS_OF)
        monkeypatch.undo()

        assert resolve_active_standard_snapshot_uri(output_root, self.AS_OF) == original.version_uri
        rows = spark.read.parquet(original.version_uri).collect()
        assert [row["segment_id"] for row in rows] == ["seg-1"]

    def test_schema_mismatch_leaves_the_manifest_and_active_snapshot_untouched(
        self, spark, tmp_path, monkeypatch
    ) -> None:
        output_root = str(tmp_path)
        original = write_standard_comfort_score_snapshot(
            spark, rows_df(spark, self.AS_OF, ("seg-1", 1, 50.0)), output_root, self.AS_OF
        )
        original_parquet = DataFrameReader.parquet

        def dropped_column_reader(self, *paths, **options):
            return original_parquet(self, *paths, **options).drop("comfort_score")

        monkeypatch.setattr(DataFrameReader, "parquet", dropped_column_reader)

        broken_df = rows_df(spark, self.AS_OF, ("seg-2", 1, 60.0))
        with pytest.raises(ValueError, match="schema"):
            write_standard_comfort_score_snapshot(spark, broken_df, output_root, self.AS_OF)
        monkeypatch.undo()

        assert resolve_active_standard_snapshot_uri(output_root, self.AS_OF) == original.version_uri
        rows = spark.read.parquet(original.version_uri).collect()
        assert [row["segment_id"] for row in rows] == ["seg-1"]

    def test_row_count_mismatch_leaves_the_manifest_and_active_snapshot_untouched(
        self, spark, tmp_path, monkeypatch
    ) -> None:
        output_root = str(tmp_path)
        original = write_standard_comfort_score_snapshot(
            spark, rows_df(spark, self.AS_OF, ("seg-1", 1, 50.0)), output_root, self.AS_OF
        )
        original_parquet = DataFrameReader.parquet

        def truncated_reader(self, *paths, **options):
            return original_parquet(self, *paths, **options).limit(0)

        monkeypatch.setattr(DataFrameReader, "parquet", truncated_reader)

        broken_df = rows_df(spark, self.AS_OF, ("seg-2", 1, 60.0))
        with pytest.raises(ValueError, match="row count"):
            write_standard_comfort_score_snapshot(spark, broken_df, output_root, self.AS_OF)
        monkeypatch.undo()

        assert resolve_active_standard_snapshot_uri(output_root, self.AS_OF) == original.version_uri
        rows = spark.read.parquet(original.version_uri).collect()
        assert [row["segment_id"] for row in rows] == ["seg-1"]

    def test_manifest_write_failure_leaves_the_existing_manifest_untouched(
        self, spark, tmp_path, monkeypatch
    ) -> None:
        output_root = str(tmp_path)
        original = write_standard_comfort_score_snapshot(
            spark, rows_df(spark, self.AS_OF, ("seg-1", 1, 50.0)), output_root, self.AS_OF
        )

        def failing_write_bytes(self, uri, value):
            raise RuntimeError("simulated manifest write failure")

        monkeypatch.setattr(ObjectStore, "write_bytes", failing_write_bytes)

        # 새 version 자체는 정상적으로 쓰이고 검증도 통과한다 — manifest 갱신만 실패한다.
        broken_df = rows_df(spark, self.AS_OF, ("seg-2", 1, 60.0))
        with pytest.raises(RuntimeError, match="simulated manifest write failure"):
            write_standard_comfort_score_snapshot(spark, broken_df, output_root, self.AS_OF)
        monkeypatch.undo()

        assert resolve_active_standard_snapshot_uri(output_root, self.AS_OF) == original.version_uri
        rows = spark.read.parquet(original.version_uri).collect()
        assert [row["segment_id"] for row in rows] == ["seg-1"]


class TestResolveActiveStandardSnapshot:
    AS_OF = datetime(2026, 8, 23, 3, 0, 0, tzinfo=UTC)

    def _manifest_uri(self, output_root: str) -> str:
        return standard_manifest_uri(standard_snapshot_uri(output_root, self.AS_OF))

    def test_reads_the_version_the_manifest_points_to(self, spark, tmp_path) -> None:
        output_root = str(tmp_path)
        write_standard_comfort_score_snapshot(
            spark, rows_df(spark, self.AS_OF, ("seg-1", 1, 50.0)), output_root, self.AS_OF
        )

        active_df = read_active_standard_comfort_score_snapshot(spark, output_root, self.AS_OF)

        assert [row["segment_id"] for row in active_df.collect()] == ["seg-1"]

    def test_raises_when_no_manifest_exists(self, tmp_path) -> None:
        with pytest.raises(ValueError, match="no manifest found"):
            resolve_active_standard_snapshot_uri(str(tmp_path), self.AS_OF)

    def test_raises_when_manifest_is_not_valid_json(self, tmp_path) -> None:
        output_root = str(tmp_path)
        ObjectStore().write_bytes(self._manifest_uri(output_root), b"not json")

        with pytest.raises(ValueError, match="not valid JSON"):
            resolve_active_standard_snapshot_uri(output_root, self.AS_OF)

    def test_raises_when_manifest_is_missing_required_keys(self, tmp_path) -> None:
        output_root = str(tmp_path)
        ObjectStore().write_bytes(
            self._manifest_uri(output_root), json.dumps({"version_id": "abc"}).encode()
        )

        with pytest.raises(ValueError, match="missing required key"):
            resolve_active_standard_snapshot_uri(output_root, self.AS_OF)

    def test_raises_when_manifest_score_as_of_does_not_match(self, tmp_path) -> None:
        output_root = str(tmp_path)
        other_as_of = self.AS_OF.replace(hour=4)
        root = standard_snapshot_uri(output_root, self.AS_OF)
        ObjectStore().write_bytes(
            self._manifest_uri(output_root),
            json.dumps(
                {
                    "score_as_of": other_as_of.isoformat(),
                    "version_id": "abc",
                    "snapshot_uri": standard_version_uri(root, "abc"),
                    "row_count": 1,
                }
            ).encode(),
        )

        with pytest.raises(ValueError, match="does not match"):
            resolve_active_standard_snapshot_uri(output_root, self.AS_OF)

    def test_raises_when_manifest_row_count_is_not_a_non_negative_integer(self, tmp_path) -> None:
        output_root = str(tmp_path)
        root = standard_snapshot_uri(output_root, self.AS_OF)
        ObjectStore().write_bytes(
            self._manifest_uri(output_root),
            json.dumps(
                {
                    "score_as_of": self.AS_OF.isoformat(),
                    "version_id": "abc",
                    "snapshot_uri": standard_version_uri(root, "abc"),
                    "row_count": "ㅋㅋ",
                }
            ).encode(),
        )

        with pytest.raises(ValueError, match="row_count"):
            resolve_active_standard_snapshot_uri(output_root, self.AS_OF)

    def test_raises_when_manifest_snapshot_uri_does_not_match_its_version_id(
        self, tmp_path
    ) -> None:
        output_root = str(tmp_path)
        ObjectStore().write_bytes(
            self._manifest_uri(output_root),
            json.dumps(
                {
                    "score_as_of": self.AS_OF.isoformat(),
                    "version_id": "abc",
                    "snapshot_uri": "file:///엉뚱한경로",
                    "row_count": 1,
                }
            ).encode(),
        )

        with pytest.raises(ValueError, match="does not match"):
            resolve_active_standard_snapshot_uri(output_root, self.AS_OF)
