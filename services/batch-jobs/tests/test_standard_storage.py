"""Tests for batch_jobs/comfort_score/standard_storage.py (#265).

같은 as_of는 같은 경로로 멱등 재실행되고, 다른 as_of는 별도 경로로 보존되며,
read-back 불일치는 예외로 이어진다는 계약을 검증한다.
"""

from __future__ import annotations

from datetime import UTC, datetime
from urllib.parse import unquote

import pytest
from batch_jobs.comfort_score.standard_storage import (
    standard_snapshot_uri,
    write_standard_comfort_score_snapshot,
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.readwriter import DataFrameReader

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


class TestStandardSnapshotUri:
    def test_same_as_of_always_yields_the_same_uri(self):
        as_of = datetime(2026, 8, 23, 3, 0, 0, tzinfo=UTC)

        first = standard_snapshot_uri("out", as_of)
        second = standard_snapshot_uri("out", as_of)

        assert first == second

    def test_different_as_of_yields_different_uris(self):
        uri_3am = standard_snapshot_uri("out", datetime(2026, 8, 23, 3, 0, 0, tzinfo=UTC))
        uri_4am = standard_snapshot_uri("out", datetime(2026, 8, 23, 4, 0, 0, tzinfo=UTC))

        assert uri_3am != uri_4am

    def test_uri_encodes_score_as_of_date_and_score_as_of(self):
        as_of = datetime(2026, 8, 23, 3, 0, 0, tzinfo=UTC)

        # 로컬 file:// 스킴은 join_uri()가 '='를 %3D로 인코딩하므로 unquote로 확인한다.
        uri = unquote(standard_snapshot_uri("out", as_of))

        assert "score_as_of_date=2026-08-23" in uri
        assert "score_as_of=2026-08-23T03-00-00Z" in uri

    def test_rejects_naive_as_of_when_writing(self, spark, tmp_path) -> None:
        naive_as_of = datetime(2026, 8, 23, 3, 0, 0)  # noqa: DTZ001
        df = rows_df(spark, naive_as_of.replace(tzinfo=UTC), ("seg-1", 1, 50.0))

        with pytest.raises(ValueError, match="timezone-aware"):
            write_standard_comfort_score_snapshot(spark, df, str(tmp_path), naive_as_of)


class TestWriteStandardComfortScoreSnapshot:
    AS_OF = datetime(2026, 8, 23, 3, 0, 0, tzinfo=UTC)

    def test_write_creates_data_at_the_expected_path(self, spark, tmp_path) -> None:
        output_root = str(tmp_path)
        df = rows_df(spark, self.AS_OF, ("seg-1", 1, 50.0), ("seg-2", 1, 60.0))

        result = write_standard_comfort_score_snapshot(spark, df, output_root, self.AS_OF)

        assert result.output_uri == standard_snapshot_uri(output_root, self.AS_OF)
        assert result.row_count == 2
        assert spark.read.parquet(result.output_uri).count() == 2

    def test_rerunning_the_same_as_of_replaces_the_snapshot(self, spark, tmp_path) -> None:
        output_root = str(tmp_path)
        first = rows_df(spark, self.AS_OF, ("seg-1", 1, 50.0))
        second = rows_df(spark, self.AS_OF, ("seg-2", 1, 60.0), ("seg-3", 1, 70.0))

        write_standard_comfort_score_snapshot(spark, first, output_root, self.AS_OF)
        result = write_standard_comfort_score_snapshot(spark, second, output_root, self.AS_OF)

        rows = spark.read.parquet(result.output_uri).collect()
        assert {row["segment_id"] for row in rows} == {"seg-2", "seg-3"}

    def test_other_as_of_snapshots_are_preserved_across_a_rerun(self, spark, tmp_path) -> None:
        output_root = str(tmp_path)
        earlier_as_of = datetime(2026, 8, 23, 2, 0, 0, tzinfo=UTC)
        earlier_df = rows_df(spark, earlier_as_of, ("seg-9", 1, 10.0))
        first = rows_df(spark, self.AS_OF, ("seg-1", 1, 50.0))
        rerun = rows_df(spark, self.AS_OF, ("seg-2", 1, 60.0))

        earlier_result = write_standard_comfort_score_snapshot(
            spark, earlier_df, output_root, earlier_as_of
        )
        write_standard_comfort_score_snapshot(spark, first, output_root, self.AS_OF)
        write_standard_comfort_score_snapshot(spark, rerun, output_root, self.AS_OF)

        rows = spark.read.parquet(earlier_result.output_uri).collect()
        assert [row["segment_id"] for row in rows] == ["seg-9"]

    def test_rejects_rows_whose_score_as_of_does_not_match(self, spark, tmp_path) -> None:
        other_as_of = self.AS_OF.replace(hour=4)
        df = rows_df(spark, other_as_of, ("seg-1", 1, 50.0))

        with pytest.raises(ValueError, match="score_as_of"):
            write_standard_comfort_score_snapshot(spark, df, str(tmp_path), self.AS_OF)

    def test_rejects_a_schema_mismatched_read_back(self, spark, tmp_path, monkeypatch) -> None:
        # spark.read는 매번 새 인스턴스를 만들어서 클래스 메서드를 patch해야 한다.
        output_root = str(tmp_path)
        df = rows_df(spark, self.AS_OF, ("seg-1", 1, 50.0))
        original_parquet = DataFrameReader.parquet

        def dropped_column_reader(self, *paths, **options):
            return original_parquet(self, *paths, **options).drop("comfort_score")

        monkeypatch.setattr(DataFrameReader, "parquet", dropped_column_reader)

        with pytest.raises(ValueError, match="schema"):
            write_standard_comfort_score_snapshot(spark, df, output_root, self.AS_OF)

    def test_rejects_a_row_count_mismatch_on_read_back(self, spark, tmp_path, monkeypatch) -> None:
        output_root = str(tmp_path)
        df = rows_df(spark, self.AS_OF, ("seg-1", 1, 50.0), ("seg-2", 1, 60.0))
        original_parquet = DataFrameReader.parquet

        def truncated_reader(self, *paths, **options):
            return original_parquet(self, *paths, **options).filter(F.col("segment_id") == "seg-1")

        monkeypatch.setattr(DataFrameReader, "parquet", truncated_reader)

        with pytest.raises(ValueError, match="row count"):
            write_standard_comfort_score_snapshot(spark, df, output_root, self.AS_OF)
