"""Tests for the standard_segment_comfort_score path (#198).

기존 segment_comfort_score 경로의 테스트는 test_segment_comfort_score.py에 그대로 둔다 —
여기서는 #198이 새로 도입한 방향별 점수, universe materialization, standard writer만
다룬다.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from typing import ClassVar

import psycopg2
import pytest
from batch_jobs.comfort_score.config import (
    DEFAULT_COMFORT_SCORE_CONFIG_PATH,
    ComfortScoreConfig,
)
from batch_jobs.comfort_score.formula import (
    DIRECTION_COLUMNS,
    VEHICLE_AGNOSTIC_VEHICLE_PROFILE_ID,
    compute_segment_comfort_scores,
    compute_standard_comfort_scores,
)
from batch_jobs.comfort_score.standard_job import (
    POSTGRES_JDBC_PACKAGE,
    StandardComfortScoreJobConfig,
    _attach_score_as_of,
    _fill_missing_periods,
    _select_staging_columns,
    build_spark_session,
    run_standard_comfort_score_job,
)
from batch_jobs.comfort_score.standard_writer import (
    _MERGE_SQL,
    EXPECTED_STAGING_COLUMNS,
    STAGING_TABLE,
    TARGET_TABLE,
    _acquire_lock,
    _validate_no_duplicates_or_nan,
)
from batch_jobs.comfort_score.universe import (
    load_vehicle_profile_ids,
    resolve_segment_artifact_uri,
)
from batch_jobs.db_lock_keys import (
    GOLD_JOB_STAGING_LOCK_KEY,
    STANDARD_JOB_STAGING_LOCK_KEY,
)
from batch_jobs.migrate import MigrationConfig, run_migrations
from batch_jobs.schemas import HOURLY_COMFORT_SCORE_SCHEMA
from batch_jobs.sensor_features.config import ProvisionalThreshold
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RUN_INTEGRATION = os.environ.get("RUN_INTEGRATION") == "1"

# 기본 YAML 값(0.5/0.3/0.2, T_min=5, k=10)과 일부러 다르게 둬서, 결과가 이 값들을
# 실제로 반영하는지(하드코딩된 상수가 아닌지) 자연스럽게 검증한다.
TEST_CONFIG = ComfortScoreConfig(
    vertical_weight=ProvisionalThreshold(value=0.6, provisional=True),
    longitudinal_weight=ProvisionalThreshold(value=0.3, provisional=True),
    lateral_weight=ProvisionalThreshold(value=0.1, provisional=True),
    min_traffic_threshold=ProvisionalThreshold(value=5.0, provisional=True),
    shrinkage_k=ProvisionalThreshold(value=4.0, provisional=True),
)

HOURLY_SCHEMA = (
    "segment_id string, vehicle_profile_id int, data_period_start timestamp, "
    "data_period_end timestamp, vertical_score double, longitudinal_score double, "
    "lateral_score double, trip_count long, sample_count long"
)

UNIVERSE_SCHEMA = "segment_id string, vehicle_profile_id int"


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("batch-jobs-standard-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        # 이 파일의 통합 테스트가 쓰는 JDBC 드라이버를 여기서도 같이 올린다.
        # spark.jars.packages는 JVM이 뜰 때 한 번만 해석되므로, 이 fixture가 먼저
        # 만든 세션에 드라이버가 없으면 뒤이어 build_spark_session()을 불러도
        # classpath에 더 붙지 않아 JDBC write가 ClassNotFoundException으로 죽는다.
        .config("spark.jars.packages", POSTGRES_JDBC_PACKAGE)
        .getOrCreate()
    )
    yield session
    session.stop()


class FakeCursor:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self._queued: list[object] = []
        self._current: object = None

    def queue(self, result) -> None:
        """다음 execute() 이후 fetchone()/fetchall()이 반환할 값을 예약한다."""
        self._queued.append(result)

    def execute(self, sql: str, params: tuple = ()) -> None:
        self.executed.append(sql.strip())
        self._current = self._queued.pop(0) if self._queued else None

    def fetchone(self):
        return self._current

    def fetchall(self):
        return self._current or []

    def close(self) -> None:
        pass


def hour(
    segment_id: str = "seg-1",
    vehicle_profile_id: int = 1,
    data_period_start: datetime = datetime(2026, 8, 1, 0, 0, 0),  # noqa: DTZ001
    data_period_end: datetime | None = None,
    vertical_score: float = 0.0,
    longitudinal_score: float = 0.0,
    lateral_score: float = 0.0,
    trip_count: int = 10,
    sample_count: int = 0,
) -> tuple:
    if data_period_end is None:
        data_period_end = data_period_start + timedelta(hours=1)
    return (
        segment_id,
        vehicle_profile_id,
        data_period_start,
        data_period_end,
        vertical_score,
        longitudinal_score,
        lateral_score,
        trip_count,
        sample_count,
    )


def hourly_df(spark, *rows: tuple):
    return spark.createDataFrame(list(rows), HOURLY_SCHEMA)


def universe_df(spark, *pairs: tuple):
    return spark.createDataFrame(list(pairs), UNIVERSE_SCHEMA)


def rows_by_key(result) -> dict:
    return {(row.segment_id, row.vehicle_profile_id): row for row in result.collect()}


def weighted_sum(row, config: ComfortScoreConfig = TEST_CONFIG) -> float:
    return (
        config.vertical_weight.value * row.vertical_score
        + config.longitudinal_weight.value * row.longitudinal_score
        + config.lateral_weight.value * row.lateral_score
    )


class TestDirectionalScores:
    def test_directional_scores_are_reported_per_direction(self, spark):
        df = hourly_df(
            spark,
            hour(vertical_score=100.0, longitudinal_score=50.0, lateral_score=10.0),
        )

        result = compute_standard_comfort_scores(
            df, TEST_CONFIG, universe_df(spark, ("seg-1", 1))
        )

        row = rows_by_key(result)[("seg-1", 1)]
        # 세그먼트가 하나뿐이라 방향별 mu도 관측값과 같아 shrink 후에도 그대로다.
        assert row.vertical_score == pytest.approx(100.0)
        assert row.longitudinal_score == pytest.approx(50.0)
        assert row.lateral_score == pytest.approx(10.0)

    def test_comfort_score_equals_the_weighted_sum_of_directional_scores(self, spark):
        # 방향별로 다른 값을 가진 여러 세그먼트 — shrinkage가 실제로 걸리는 상황에서
        # 선형성이 유지되는지 본다.
        df = hourly_df(
            spark,
            hour(segment_id="seg-x", vertical_score=90.0, longitudinal_score=20.0,
                 lateral_score=70.0),
            hour(segment_id="seg-y", vertical_score=10.0, longitudinal_score=80.0,
                 lateral_score=30.0),
            hour(segment_id="seg-z", vertical_score=55.0, longitudinal_score=45.0,
                 lateral_score=5.0),
        )

        result = compute_standard_comfort_scores(
            df,
            TEST_CONFIG,
            universe_df(spark, ("seg-x", 1), ("seg-y", 1), ("seg-z", 1)),
        )

        for row in result.collect():
            assert row.comfort_score == pytest.approx(weighted_sum(row), abs=1e-5)

    def test_comfort_score_matches_the_existing_gold_path(self, spark):
        """방향별 산출로 바꿔도 기존 segment_comfort_score 값이 달라지지 않는다."""
        rows = (
            hour(segment_id="seg-x", vertical_score=90.0, longitudinal_score=20.0,
                 lateral_score=70.0, sample_count=10),
            hour(segment_id="seg-y", vertical_score=10.0, longitudinal_score=80.0,
                 lateral_score=30.0, sample_count=20),
            # T_min 미달 — 양쪽 경로 모두에서 제외돼야 한다.
            hour(segment_id="seg-y", vertical_score=99.0, trip_count=1, sample_count=99),
        )
        legacy = rows_by_key(compute_segment_comfort_scores(hourly_df(spark, *rows), TEST_CONFIG))
        standard = rows_by_key(
            compute_standard_comfort_scores(
                hourly_df(spark, *rows),
                TEST_CONFIG,
                universe_df(spark, ("seg-x", 1), ("seg-y", 1)),
            )
        )

        for key, legacy_row in legacy.items():
            assert standard[key].comfort_score == pytest.approx(
                legacy_row.comfort_score, abs=1e-5
            )
            assert standard[key].confidence_score == pytest.approx(
                legacy_row.confidence_score, abs=1e-5
            )
            assert standard[key].sample_count == legacy_row.sample_count

    def test_vehicle_agnostic_row_also_carries_directional_scores(self, spark):
        df = hourly_df(
            spark,
            hour(vehicle_profile_id=1, vertical_score=100.0, trip_count=10),
            hour(vehicle_profile_id=2, vertical_score=0.0, trip_count=30),
        )

        result = compute_standard_comfort_scores(
            df, TEST_CONFIG, universe_df(spark, ("seg-1", 1), ("seg-1", 2))
        )

        row = rows_by_key(result)[("seg-1", VEHICLE_AGNOSTIC_VEHICLE_PROFILE_ID)]
        # 트래픽 가중 평균: (10*100 + 30*0)/40 = 25. 세그먼트가 하나라 mu도 25.
        assert row.vertical_score == pytest.approx(25.0)
        assert row.comfort_score == pytest.approx(weighted_sum(row), abs=1e-5)


class TestUniverseMaterialization:
    def test_every_universe_combination_gets_a_row(self, spark):
        df = hourly_df(spark, hour(segment_id="seg-x", vehicle_profile_id=1,
                                   vertical_score=100.0))

        result = compute_standard_comfort_scores(
            df,
            TEST_CONFIG,
            universe_df(spark, ("seg-x", 1), ("seg-x", 2), ("seg-y", 1), ("seg-y", 2)),
        )

        rows = rows_by_key(result)
        for segment_id in ("seg-x", "seg-y"):
            for vehicle_profile_id in (1, 2):
                assert (segment_id, vehicle_profile_id) in rows
            # vehicle-agnostic sentinel 행도 segment마다 하나씩 나온다.
            assert (segment_id, VEHICLE_AGNOSTIC_VEHICLE_PROFILE_ID) in rows

    def test_a_never_observed_combination_falls_back_with_zero_confidence(self, spark):
        df = hourly_df(
            spark,
            hour(segment_id="seg-x", vehicle_profile_id=1, vertical_score=100.0),
            hour(segment_id="seg-y", vehicle_profile_id=1, vertical_score=0.0),
        )

        result = compute_standard_comfort_scores(
            df, TEST_CONFIG, universe_df(spark, ("seg-x", 1), ("seg-y", 1), ("seg-z", 1))
        )

        row = rows_by_key(result)[("seg-z", 1)]
        # N=0 -> Step 4가 mu_p로 수렴한다. mu_p의 vertical = (100+0)/2 = 50.
        assert row.confidence_score == pytest.approx(0.0)
        assert row.vertical_score == pytest.approx(50.0)
        assert row.qualifying_hours == 0
        # 롤업할 qualifying hour가 없어 경계는 NULL로 나오고, 채움은 job의 책임이다.
        assert row.data_period_start is None
        assert row.data_period_end is None

    def test_a_profile_with_no_qualifying_hour_anywhere_uses_the_global_mean(self, spark):
        df = hourly_df(
            spark,
            hour(segment_id="seg-x", vehicle_profile_id=1, vertical_score=100.0,
                 trip_count=10),
            # 프로필 2는 이 윈도우 전체에서 T_min 미달이라 mu_2가 정의되지 않는다.
            hour(segment_id="seg-y", vehicle_profile_id=2, vertical_score=0.0,
                 trip_count=2),
        )

        result = compute_standard_comfort_scores(
            df, TEST_CONFIG, universe_df(spark, ("seg-x", 1), ("seg-y", 2))
        )

        rows = rows_by_key(result)
        # 기존 Gold 경로에서는 행 자체가 생기지 않던 조합이다 (전역 mu로 대체).
        assert ("seg-y", 2) in rows
        assert rows[("seg-y", 2)].vertical_score == pytest.approx(100.0)
        assert rows[("seg-y", 2)].comfort_score is not None

    def test_universe_rejects_the_reserved_sentinel_profile(self, spark):
        df = hourly_df(spark, hour())

        with pytest.raises(ValueError, match="vehicle_profile_id=0"):
            compute_standard_comfort_scores(
                df, TEST_CONFIG, universe_df(spark, ("seg-1", 0))
            )

    def test_universe_requires_both_key_columns(self, spark):
        df = hourly_df(spark, hour())
        broken = spark.createDataFrame([("seg-1",)], "segment_id string")

        with pytest.raises(ValueError, match="vehicle_profile_id"):
            compute_standard_comfort_scores(df, TEST_CONFIG, broken)


class TestUniverseResolution:
    @staticmethod
    def write_environment(tmp_path, artifacts: list[dict]) -> str:
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps({"artifacts": artifacts}))
        pointer_dir = tmp_path / "prepared" / "simulation_environment"
        pointer_dir.mkdir(parents=True)
        (pointer_dir / "active.json").write_text(
            json.dumps({"manifest_uri": manifest_path.as_uri()})
        )
        return str(tmp_path)

    def test_resolves_the_enriched_segment_reference_artifact(self, tmp_path):
        data_lake_uri = self.write_environment(
            tmp_path,
            [
                {"role": "road_segment", "uri": "file:///other.parquet"},
                {"role": "enriched_segment_reference", "uri": "file:///wanted.parquet"},
            ],
        )

        assert resolve_segment_artifact_uri(data_lake_uri) == "file:///wanted.parquet"

    def test_raises_when_the_manifest_has_no_segment_artifact(self, tmp_path):
        data_lake_uri = self.write_environment(
            tmp_path, [{"role": "road_segment", "uri": "file:///other.parquet"}]
        )

        with pytest.raises(ValueError, match="enriched_segment_reference"):
            resolve_segment_artifact_uri(data_lake_uri)

    def test_load_vehicle_profile_ids_excludes_the_sentinel(self):
        cursor = FakeCursor()
        cursor.queue([(1,), (2,), (3,)])
        connection = type("Connection", (), {"cursor": lambda self: cursor})()

        assert load_vehicle_profile_ids(connection) == (1, 2, 3)
        assert "vehicle_profile_id <> %s" in cursor.executed[0]

    def test_load_vehicle_profile_ids_raises_when_no_real_profile_exists(self):
        cursor = FakeCursor()
        cursor.queue([])
        connection = type("Connection", (), {"cursor": lambda self: cursor})()

        with pytest.raises(RuntimeError, match="migrate-database"):
            load_vehicle_profile_ids(connection)


class TestStandardJob:
    def test_fill_missing_periods_uses_the_batch_window_when_null(self, spark):
        as_of = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
        df = spark.createDataFrame(
            [("seg-1", None, None)],
            "segment_id string, data_period_start timestamp, data_period_end timestamp",
        )

        # 절대 시각으로 비교한다 — collect()가 돌려주는 naive datetime은 JVM 로컬
        # 타임존이라 tz-aware 값과 직접 비교하면 오프셋만큼 어긋난다.
        row = (
            _fill_missing_periods(df, as_of, 168)
            .select(
                F.unix_timestamp("data_period_start"), F.unix_timestamp("data_period_end")
            )
            .collect()[0]
        )

        assert row[0] == int((as_of - timedelta(hours=168)).timestamp())
        assert row[1] == int(as_of.timestamp())

    def test_fill_missing_periods_leaves_observed_bounds_untouched(self, spark):
        as_of = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
        observed_start = datetime(2026, 8, 15, 3, 0, tzinfo=UTC)
        observed_end = datetime(2026, 8, 15, 9, 0, tzinfo=UTC)
        df = spark.createDataFrame(
            [("seg-1", observed_start, observed_end)],
            "segment_id string, data_period_start timestamp, data_period_end timestamp",
        )

        row = (
            _fill_missing_periods(df, as_of, 168)
            .select(
                F.unix_timestamp("data_period_start"), F.unix_timestamp("data_period_end")
            )
            .collect()[0]
        )

        assert row[0] == int(observed_start.timestamp())
        assert row[1] == int(observed_end.timestamp())

    def test_attach_score_as_of_uses_the_same_literal_for_every_row(self, spark):
        as_of = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
        df = spark.createDataFrame([("a",), ("b",)], "segment_id string")

        values = {
            row[0]
            for row in _attach_score_as_of(df, as_of)
            .select(F.unix_timestamp("score_as_of"))
            .collect()
        }

        assert values == {int(as_of.timestamp())}

    def test_select_staging_columns_drops_diagnostic_columns(self, spark):
        df = spark.createDataFrame(
            [
                (
                    "seg-1", 1,
                    datetime(2026, 8, 16, tzinfo=UTC),
                    datetime(2026, 8, 15, 12, tzinfo=UTC),
                    datetime(2026, 8, 15, 13, tzinfo=UTC),
                    80.0, 70.0, 60.0, 75.0, 100, 0.9, "1.0.0",
                    datetime(2026, 8, 16, tzinfo=UTC),
                    5, 78.0, 82.0,
                )
            ],
            "segment_id string, vehicle_profile_id int, score_as_of timestamp, "
            "data_period_start timestamp, data_period_end timestamp, "
            "vertical_score double, longitudinal_score double, lateral_score double, "
            "comfort_score double, sample_count long, confidence_score double, "
            "score_version string, calculated_at timestamp, "
            "qualifying_hours long, observed_score double, population_mean double",
        )

        result = _select_staging_columns(df)

        assert result.columns == list(EXPECTED_STAGING_COLUMNS)

    def test_config_falls_back_to_the_gold_job_environment_variables(self):
        config = StandardComfortScoreJobConfig.from_env(
            {
                "SEGMENT_COMFORT_SCORE_DATA_LAKE_URI": "s3://bucket/lake",
                "SEGMENT_COMFORT_SCORE_WINDOW_HOURS": "24",
                "POSTGRES_HOST": "localhost",
                "POSTGRES_PORT": "5432",
                "POSTGRES_DB": "de4",
                "POSTGRES_USER": "de4",
                "POSTGRES_PASSWORD": "secret",
            }
        )

        assert config.data_lake_uri == "s3://bucket/lake"
        assert config.window_hours == 24

    def test_standard_environment_variables_win_over_the_gold_ones(self):
        config = StandardComfortScoreJobConfig.from_env(
            {
                "STANDARD_COMFORT_SCORE_DATA_LAKE_URI": "s3://bucket/standard",
                "SEGMENT_COMFORT_SCORE_DATA_LAKE_URI": "s3://bucket/gold",
                "POSTGRES_HOST": "localhost",
                "POSTGRES_PORT": "5432",
                "POSTGRES_DB": "de4",
                "POSTGRES_USER": "de4",
                "POSTGRES_PASSWORD": "secret",
            }
        )

        assert config.data_lake_uri == "s3://bucket/standard"


class TestStandardWriter:
    def test_uses_a_lock_key_separate_from_the_gold_job(self):
        assert STANDARD_JOB_STAGING_LOCK_KEY != GOLD_JOB_STAGING_LOCK_KEY

    def test_acquire_lock_raises_when_already_held(self):
        cursor = FakeCursor()
        cursor.queue((False,))

        with pytest.raises(RuntimeError, match="holds the staging lock"):
            _acquire_lock(cursor)

    def test_merge_conflict_target_is_the_three_column_primary_key(self):
        assert "ON CONFLICT (segment_id, vehicle_profile_id, score_as_of)" in _MERGE_SQL

    def test_merge_never_reassigns_the_conflict_key_columns(self):
        update_clause = _MERGE_SQL.split("DO UPDATE SET")[1]
        assert "score_as_of = EXCLUDED" not in update_clause
        assert "segment_id = EXCLUDED" not in update_clause
        assert "vehicle_profile_id = EXCLUDED" not in update_clause

    def test_merge_carries_every_directional_score(self):
        for direction in DIRECTION_COLUMNS:
            assert f"{direction} = EXCLUDED.{direction}" in _MERGE_SQL

    def test_duplicate_check_uses_the_three_column_key(self):
        cursor = FakeCursor()
        cursor.queue((3, 2))

        with pytest.raises(ValueError, match="duplicate"):
            _validate_no_duplicates_or_nan(cursor)
        assert "score_as_of" in cursor.executed[0]

    def test_staging_columns_match_the_target_contract(self):
        assert set(EXPECTED_STAGING_COLUMNS) == {
            "segment_id",
            "vehicle_profile_id",
            "score_as_of",
            "data_period_start",
            "data_period_end",
            *DIRECTION_COLUMNS,
            "comfort_score",
            "sample_count",
            "confidence_score",
            "score_version",
            "calculated_at",
        }


def _connect():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def _truncate_standard_tables() -> None:
    connection = _connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"TRUNCATE {TARGET_TABLE}, {STAGING_TABLE} CASCADE")
        connection.commit()
    finally:
        connection.close()


class TestStandardComfortScoreIntegration:
    """Integration tests against a real Postgres (#198).

    RUN_INTEGRATION 미설정 시 skip. 설정됐는데 접속이 실패하면 skip이 아니라 fail한다.

    이 클래스는 자기 pytest 프로세스에서 돌려야 한다.

        RUN_INTEGRATION=1 pytest services/batch-jobs/tests/test_standard_comfort_score.py

    build_spark_session()이 JDBC 드라이버를 spark.jars.packages로 받아오는데, 그
    해석은 JVM이 뜨는 시점에 끝난다. 전체 스위트처럼 앞선 모듈이 이미 SparkSession을
    만든 프로세스에서는 세션을 stop/재생성해도 JVM classpath가 그대로라 드라이버를
    더 붙일 수 없고, JDBC write가 ClassNotFoundException으로 실패한다. 같은 제약이
    test_segment_comfort_score.py의 통합 테스트에도 그대로 적용된다.
    """

    pytestmark = pytest.mark.skipif(
        not RUN_INTEGRATION, reason="set RUN_INTEGRATION=1 to run against a real Postgres"
    )

    PROFILE_ID: ClassVar[int] = 1

    @staticmethod
    @pytest.fixture(scope="class")
    def spark():
        session = build_spark_session()
        yield session
        session.stop()

    @staticmethod
    @pytest.fixture(scope="class", autouse=True)
    def migrated():
        connection = _connect()
        try:
            run_migrations(MigrationConfig.from_env().migrations_dir, connection)
        finally:
            connection.close()

    @staticmethod
    @pytest.fixture(autouse=True)
    def clean_tables():
        _truncate_standard_tables()
        connection = _connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM vehicle_profile WHERE vehicle_profile_id != 0")
                cursor.execute(
                    "INSERT INTO vehicle_profile "
                    "(vehicle_profile_id, profile_name, body_type, size_class, "
                    " vertical_response_factor, longitudinal_response_factor, "
                    " lateral_response_factor, damping_factor, steering_vibration_factor, "
                    " is_active, created_at, updated_at) "
                    "VALUES (1, 'test_profile', 'sedan', 'compact', "
                    " 1.0, 1.0, 1.0, 1.0, 1.0, TRUE, now(), now())"
                )
            connection.commit()
        finally:
            connection.close()
        yield
        # 뒷정리를 반드시 한다. standard_segment_comfort_score는 vehicle_profile을
        # 참조하는 FK를 갖고 있어서, 여기서 행을 남기면 다음 테스트(특히
        # test_segment_comfort_score.py의 fixture)가 실행하는
        # "DELETE FROM vehicle_profile"이 ForeignKeyViolation으로 실패한다.
        _truncate_standard_tables()

    @staticmethod
    def hourly_row(**overrides):
        row = {
            "segment_id": "seg-1",
            "vehicle_profile_id": 1,
            "data_period_start": datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
            "data_period_end": datetime(2026, 8, 15, 13, 0, tzinfo=UTC),
            "road_snapshot_date": datetime(2026, 8, 1).date(),  # noqa: DTZ001
            "vertical_score": 80.0,
            "longitudinal_score": 70.0,
            "lateral_score": 60.0,
            "scoring_version": "1.0.0",
            "sample_count": 100,
            "trip_count": 10,
            "_run_id": "test-run",
            "_processed_at": datetime(2026, 8, 15, 13, 5, tzinfo=UTC),
        }
        row.update(overrides)
        return tuple(row[field.name] for field in HOURLY_COMFORT_SCORE_SCHEMA)

    @classmethod
    def write_data_lake(cls, spark, tmp_path, *segment_ids: str) -> str:
        (
            spark.createDataFrame(
                [cls.hourly_row(segment_id=segment_id) for segment_id in segment_ids],
                HOURLY_COMFORT_SCORE_SCHEMA,
            ).write.parquet(str(tmp_path / "silver" / "hourly_comfort_score"))
        )
        segment_path = tmp_path / "segments"
        spark.createDataFrame(
            [(segment_id,) for segment_id in segment_ids], "segment_id string"
        ).write.parquet(str(segment_path))

        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "artifacts": [
                        {
                            "role": "enriched_segment_reference",
                            "uri": segment_path.as_uri(),
                        }
                    ]
                }
            )
        )
        pointer_dir = tmp_path / "prepared" / "simulation_environment"
        pointer_dir.mkdir(parents=True)
        (pointer_dir / "active.json").write_text(
            json.dumps({"manifest_uri": manifest_path.as_uri()})
        )
        return str(tmp_path)

    @staticmethod
    def make_config(data_lake_uri: str) -> StandardComfortScoreJobConfig:
        env = os.environ
        return StandardComfortScoreJobConfig(
            data_lake_uri=data_lake_uri,
            window_hours=168,
            comfort_score_config_path=DEFAULT_COMFORT_SCORE_CONFIG_PATH,
            postgres_host=env["POSTGRES_HOST"],
            postgres_port=int(env["POSTGRES_PORT"]),
            postgres_db=env["POSTGRES_DB"],
            postgres_user=env["POSTGRES_USER"],
            postgres_password=env["POSTGRES_PASSWORD"],
        )

    @staticmethod
    def fetch(connection) -> list[tuple]:
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT segment_id, vehicle_profile_id, score_as_of, comfort_score "
                f"FROM {TARGET_TABLE} ORDER BY score_as_of, segment_id, vehicle_profile_id"
            )
            return cursor.fetchall()

    def test_rerunning_the_same_score_as_of_updates_instead_of_duplicating(
        self, spark, tmp_path
    ):
        as_of = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
        config = self.make_config(self.write_data_lake(spark, tmp_path, "seg-1"))
        connection = _connect()
        try:
            first = run_standard_comfort_score_job(spark, config, as_of, connection)
            assert first.inserted_count >= 1
            first_rows = self.fetch(connection)

            second = run_standard_comfort_score_job(spark, config, as_of, connection)

            assert second.inserted_count == 0
            assert second.updated_count == first.inserted_count
            assert self.fetch(connection) == first_rows
        finally:
            connection.close()

    def test_a_different_score_as_of_appends_a_new_snapshot(self, spark, tmp_path):
        config = self.make_config(self.write_data_lake(spark, tmp_path, "seg-1"))
        connection = _connect()
        try:
            run_standard_comfort_score_job(
                spark, config, datetime(2026, 8, 16, 0, 0, tzinfo=UTC), connection
            )
            before = len(self.fetch(connection))

            run_standard_comfort_score_job(
                spark, config, datetime(2026, 8, 16, 1, 0, tzinfo=UTC), connection
            )

            assert len(self.fetch(connection)) == before * 2
        finally:
            connection.close()

    def test_data_period_bounds_are_never_null(self, spark, tmp_path):
        as_of = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
        # seg-2는 hourly 데이터가 전혀 없는 segment라 N=0 경로를 탄다.
        config = self.make_config(self.write_data_lake(spark, tmp_path, "seg-1", "seg-2"))
        connection = _connect()
        try:
            run_standard_comfort_score_job(spark, config, as_of, connection)
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT count(*) FROM {TARGET_TABLE} "
                    "WHERE data_period_start IS NULL OR data_period_end IS NULL"
                )
                (null_count,) = cursor.fetchone()
            assert null_count == 0
        finally:
            connection.close()
