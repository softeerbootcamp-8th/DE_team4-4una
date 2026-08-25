"""Tests for batch_jobs/comfort_score/standard_job.py (#290, #265, #343).

TestGoldBeforePostgres는 실행 순서(계산 -> S3 Gold version 저장 -> manifest resolve
-> PostgreSQL)를 실제 Spark 세션으로 검증한다. 단위 동작은 test_standard_storage.py와
기존 standard_writer 테스트가 다루므로, 여기서는 단계를 잇는 wiring만 본다.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import ClassVar
from unittest.mock import Mock

import pytest
from batch_jobs.comfort_score.config import DEFAULT_COMFORT_SCORE_CONFIG_PATH
from batch_jobs.comfort_score.standard_job import (
    POSTGRES_JDBC_PACKAGE,
    StandardComfortScoreJobConfig,
    _postgres_jdbc_spark_config,
    run_standard_comfort_score_job,
)
from batch_jobs.comfort_score.standard_storage import (
    resolve_active_standard_snapshot_uri,
)
from batch_jobs.comfort_score.standard_writer import WriteSummary
from batch_jobs.hourly_comfort_storage import hour_output_path
from de4_core import join_uri
from pyspark.sql import SparkSession


def test_falls_back_to_maven_package_when_jar_uri_is_not_set():
    key, value = _postgres_jdbc_spark_config({})

    assert (key, value) == ("spark.jars.packages", POSTGRES_JDBC_PACKAGE)


def test_uses_s3_jar_uri_when_postgres_jdbc_jar_uri_is_set():
    key, value = _postgres_jdbc_spark_config(
        {"POSTGRES_JDBC_JAR_URI": "s3://de4-artifacts/jars/postgresql-42.7.4.jar"}
    )

    assert (key, value) == (
        "spark.jars",
        "s3://de4-artifacts/jars/postgresql-42.7.4.jar",
    )


class TestStandardComfortScoreJobConfigGoldOutputUri:
    BASE_ENV: ClassVar[dict[str, str]] = {
        "POSTGRES_HOST": "db.local",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "de4",
        "POSTGRES_USER": "app",
        "POSTGRES_PASSWORD": "secret",
    }

    def test_defaults_to_a_gold_path_under_the_data_lake(self):
        config = StandardComfortScoreJobConfig.from_env(
            {**self.BASE_ENV, "STANDARD_COMFORT_SCORE_DATA_LAKE_URI": "data/local-lake"}
        )

        assert config.gold_output_uri == join_uri(
            "data/local-lake", "gold", "standard_segment_comfort_score"
        )

    def test_explicit_gold_output_uri_overrides_the_default(self):
        config = StandardComfortScoreJobConfig.from_env(
            {
                **self.BASE_ENV,
                "STANDARD_COMFORT_SCORE_DATA_LAKE_URI": "data/local-lake",
                "STANDARD_COMFORT_SCORE_GOLD_OUTPUT_URI": "s3://de4-lake/gold/standard",
            }
        )

        assert config.gold_output_uri == "s3://de4-lake/gold/standard"


class TestStandardComfortScoreJobConfigRoadEnvironmentUri:
    BASE_ENV: ClassVar[dict[str, str]] = {
        "POSTGRES_HOST": "db.local",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "de4",
        "POSTGRES_USER": "app",
        "POSTGRES_PASSWORD": "secret",
    }

    def test_falls_back_to_the_data_lake_uri_when_unset(self):
        config = StandardComfortScoreJobConfig.from_env(
            {**self.BASE_ENV, "STANDARD_COMFORT_SCORE_DATA_LAKE_URI": "data/local-lake"}
        )

        assert config.road_environment_uri == "data/local-lake"

    def test_explicit_reference_data_lake_uri_overrides_the_default(self):
        config = StandardComfortScoreJobConfig.from_env(
            {
                **self.BASE_ENV,
                "STANDARD_COMFORT_SCORE_DATA_LAKE_URI": "s3://de4-data-lake",
                "REFERENCE_DATA_LAKE_URI": "s3://de4-reference",
            }
        )

        assert config.road_environment_uri == "s3://de4-reference"


class FakeCursor:
    def __init__(self, vehicle_profile_ids: tuple[int, ...]) -> None:
        self._vehicle_profile_ids = vehicle_profile_ids

    def execute(self, sql: str, params: tuple = ()) -> None:
        del sql, params

    def fetchall(self) -> list[tuple[int]]:
        return [(profile_id,) for profile_id in self._vehicle_profile_ids]

    def close(self) -> None:
        pass


class FakeConnection:
    """load_universe()의 vehicle_profile 조회만 흉내낸다."""

    def __init__(self, vehicle_profile_ids: tuple[int, ...] = (1,)) -> None:
        self._vehicle_profile_ids = vehicle_profile_ids

    def cursor(self) -> FakeCursor:
        return FakeCursor(self._vehicle_profile_ids)


@pytest.fixture(scope="module")
def _standard_job_order_spark():
    session = (
        SparkSession.builder.appName("batch-jobs-standard-job-order-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestGoldBeforePostgres:
    AS_OF = datetime(2026, 8, 20, 3, 0, 0, tzinfo=UTC)
    SEGMENT_ID = "seg-1"
    VEHICLE_PROFILE_ID = 1
    HOURLY_SCHEMA = (
        "segment_id string, vehicle_profile_id int, data_period_start timestamp, "
        "data_period_end timestamp, road_snapshot_date date, vertical_score double, "
        "longitudinal_score double, lateral_score double, scoring_version string, "
        "sample_count long, trip_count long, _run_id string, _processed_at timestamp"
    )

    @pytest.fixture
    def spark(self, _standard_job_order_spark):
        return _standard_job_order_spark

    def _build_config(self, spark, tmp_path: Path) -> StandardComfortScoreJobConfig:
        data_lake_uri = str(tmp_path)
        self._write_universe_environment(spark, tmp_path)
        self._write_hourly_comfort_score(spark, data_lake_uri)
        return StandardComfortScoreJobConfig(
            data_lake_uri=data_lake_uri,
            road_environment_uri=data_lake_uri,
            window_hours=168,
            comfort_score_config_path=DEFAULT_COMFORT_SCORE_CONFIG_PATH,
            gold_output_uri=str(tmp_path / "gold"),
            postgres_host="unused",
            postgres_port=5432,
            postgres_db="unused",
            postgres_user="unused",
            postgres_password="unused",
        )

    def _write_universe_environment(self, spark, tmp_path: Path) -> None:
        # load_universe()가 읽는 pointer -> manifest -> segment artifact 구성.
        segment_dir = tmp_path / "segment_reference"
        spark.createDataFrame(
            [(self.SEGMENT_ID,)], "segment_id string"
        ).write.mode("overwrite").parquet(str(segment_dir))

        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "artifacts": [
                        {
                            "role": "enriched_segment_reference",
                            "uri": segment_dir.resolve().as_uri(),
                        }
                    ]
                }
            )
        )
        pointer_dir = tmp_path / "prepared" / "simulation_environment"
        pointer_dir.mkdir(parents=True)
        (pointer_dir / "active.json").write_text(
            json.dumps({"manifest_uri": manifest_path.resolve().as_uri()})
        )

    def _write_hourly_comfort_score(self, spark, data_lake_uri: str) -> None:
        # 시간 파티션에 쓴다(#469). 타임스탬프는 tz-aware로 넣어야 파티션 경로(UTC 기준)와
        # 행의 data_period_start가 어긋나지 않는다 — naive면 PySpark가 호스트 로컬
        # 타임존으로 해석한다.
        period_start = self.AS_OF - timedelta(hours=1)
        row = (
            self.SEGMENT_ID,
            self.VEHICLE_PROFILE_ID,
            period_start,
            self.AS_OF,
            self.AS_OF.date(),
            80.0,
            40.0,
            20.0,
            "1.0.0",
            10,
            10,
            "run-1",
            self.AS_OF,
        )
        uri = join_uri(data_lake_uri, "silver", "hourly_comfort_score")
        spark.createDataFrame([row], self.HOURLY_SCHEMA).write.parquet(
            hour_output_path(uri, period_start)
        )

    def test_gold_write_failure_prevents_postgres_write(
        self, spark, tmp_path, monkeypatch
    ) -> None:
        config = self._build_config(spark, tmp_path)
        monkeypatch.setattr(
            "batch_jobs.comfort_score.standard_job.write_standard_comfort_score_snapshot",
            Mock(side_effect=RuntimeError("gold write boom")),
        )
        postgres_writer = Mock()
        monkeypatch.setattr(
            "batch_jobs.comfort_score.standard_job.write_standard_comfort_scores",
            postgres_writer,
        )

        with pytest.raises(RuntimeError, match="gold write boom"):
            run_standard_comfort_score_job(spark, config, self.AS_OF, FakeConnection())

        postgres_writer.assert_not_called()

    def test_postgres_write_receives_the_gold_read_back_dataframe(
        self, spark, tmp_path, monkeypatch
    ) -> None:
        config = self._build_config(spark, tmp_path)
        postgres_writer = Mock(
            return_value=WriteSummary(staging_count=1, inserted_count=1, updated_count=0)
        )
        monkeypatch.setattr(
            "batch_jobs.comfort_score.standard_job.write_standard_comfort_scores",
            postgres_writer,
        )

        summary = run_standard_comfort_score_job(spark, config, self.AS_OF, FakeConnection())

        postgres_writer.assert_called_once()
        received_df = postgres_writer.call_args.args[0]
        assert received_df.count() == summary.scored_count

        active_uri = resolve_active_standard_snapshot_uri(config.gold_output_uri, self.AS_OF)
        on_disk_rows = {tuple(r) for r in spark.read.parquet(active_uri).collect()}
        received_rows = {tuple(r) for r in received_df.collect()}
        assert received_rows == on_disk_rows

    def test_postgres_write_reads_through_manifest_resolution_not_the_raw_write_result(
        self, spark, tmp_path, monkeypatch
    ) -> None:
        """gold_result.version_uri를 직접 읽지 않고, manifest를 resolve하는 함수를
        실제로 호출한다는 걸 증명한다(#343) — 단순 내용 일치만으로는 이 wiring을
        보장하지 못한다(우연히 같은 값일 수 있으므로)."""
        config = self._build_config(spark, tmp_path)
        from batch_jobs.comfort_score import standard_storage

        resolve_spy = Mock(wraps=standard_storage.read_active_standard_comfort_score_snapshot)
        monkeypatch.setattr(
            "batch_jobs.comfort_score.standard_job.read_active_standard_comfort_score_snapshot",
            resolve_spy,
        )
        monkeypatch.setattr(
            "batch_jobs.comfort_score.standard_job.write_standard_comfort_scores",
            Mock(return_value=WriteSummary(staging_count=1, inserted_count=1, updated_count=0)),
        )

        run_standard_comfort_score_job(spark, config, self.AS_OF, FakeConnection())

        resolve_spy.assert_called_once_with(spark, config.gold_output_uri, self.AS_OF)
