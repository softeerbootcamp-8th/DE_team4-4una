"""Tests for comfort_score/*.py (#127, #129)."""

from __future__ import annotations

import os
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import ClassVar

import psycopg2
import pytest
from batch_jobs.comfort_score.config import (
    DEFAULT_COMFORT_SCORE_CONFIG_PATH,
    ComfortScoreConfig,
    load_comfort_score_config,
)
from batch_jobs.comfort_score.formula import compute_segment_comfort_scores
from batch_jobs.comfort_score.gold_job import (
    SegmentComfortScoreJobConfig,
    SegmentComfortScoreJobSummary,
    _attach_calculated_at,
    _fill_missing_periods,
    _select_staging_columns,
    _validate_as_of,
    build_spark_session,
    run_segment_comfort_score_job,
)
from batch_jobs.comfort_score.gold_writer import (
    _MERGE_SQL,
    EXPECTED_STAGING_COLUMNS,
    STAGING_TABLE,
    TARGET_TABLE,
    _acquire_lock,
    _merge,
    _validate_no_duplicates_or_nan,
    _validate_staging_table_shape,
)
from batch_jobs.comfort_score.loader import (
    _filter_window_hours,
    _select_latest_scoring_version,
    _validate_schema,
    load_hourly_comfort_score_for_gold,
)
from batch_jobs.comfort_scoring_config import DEFAULT_HOURLY_SCORING_CONFIG
from batch_jobs.db_lock_keys import GOLD_JOB_STAGING_LOCK_KEY
from batch_jobs.migrate import MigrationConfig, run_migrations
from batch_jobs.schemas import HOURLY_COMFORT_SCORE_SCHEMA
from batch_jobs.sensor_features.config import ProvisionalThreshold
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# collect()가 돌려주는 timestamp는 이 파이썬 프로세스의 로컬 타임존으로 변환되어,
# 고정하지 않으면 실행 머신마다(Asia/Seoul vs UTC) 값이 달라진다.
os.environ["TZ"] = "UTC"
time.tzset()

RUN_INTEGRATION = os.environ.get("RUN_INTEGRATION") == "1"


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("batch-jobs-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestComfortScoreConfig:
    def test_load_comfort_score_config_reads_provisional_thresholds(self) -> None:
        config = load_comfort_score_config()

        assert config.vertical_weight.value == 0.5
        assert config.vertical_weight.provisional is True
        assert config.longitudinal_weight.value == 0.3
        assert config.longitudinal_weight.provisional is True
        assert config.lateral_weight.value == 0.2
        assert config.lateral_weight.provisional is True
        assert config.min_traffic_threshold.value == 5.0
        assert config.min_traffic_threshold.provisional is True
        assert config.shrinkage_k.value == 10.0
        assert config.shrinkage_k.provisional is True


# collect()가 돌려주는 TimestampType 값은 tzinfo가 없는 naive datetime이라
# TZ=UTC 고정 환경에서 naive datetime을 그대로 UTC로 다룬다.
AS_OF = datetime(2026, 8, 16, 0, 0, 0)  # noqa: DTZ001


class TestComfortScoreLoader:
    EXPECTED: ClassVar = StructType(
        [
            StructField("segment_id", StringType(), nullable=False),
            StructField("trip_count", IntegerType(), nullable=False),
        ]
    )

    def test_validate_schema_passes_when_all_columns_and_types_match(self):
        actual = StructType(
            [
                StructField("segment_id", StringType(), nullable=False),
                StructField("trip_count", IntegerType(), nullable=False),
                StructField("extra_column", StringType(), nullable=True),
            ]
        )

        _validate_schema(actual, self.EXPECTED, source="test-source")  # must not raise

    def test_validate_schema_raises_with_missing_column_names(self):
        actual = StructType([StructField("segment_id", StringType(), nullable=False)])

        with pytest.raises(ValueError, match="trip_count"):
            _validate_schema(actual, self.EXPECTED, source="test-source")

    def test_validate_schema_raises_with_type_mismatch_detail(self):
        actual = StructType(
            [
                StructField("segment_id", StringType(), nullable=False),
                StructField("trip_count", StringType(), nullable=False),
            ]
        )

        with pytest.raises(ValueError, match="trip_count"):
            _validate_schema(actual, self.EXPECTED, source="test-source")

    def test_filter_window_hours_keeps_only_the_half_open_168_hour_window(self, spark):
        rows = spark.createDataFrame(
            [
                (AS_OF - timedelta(hours=169),),  # window 시작 1시간 전 — 제외
                (AS_OF - timedelta(hours=168),),  # window 시작 정각 — 포함
                (AS_OF - timedelta(hours=1),),  # window 안 — 포함
                (AS_OF,),  # as_of 자신 — 제외 (배타적 상한)
            ],
            "data_period_start timestamp",
        )

        kept = {
            row["data_period_start"] for row in _filter_window_hours(rows, AS_OF, 168).collect()
        }

        assert kept == {AS_OF - timedelta(hours=168), AS_OF - timedelta(hours=1)}

    def test_select_latest_scoring_version_compares_semver_not_strings(self, spark):
        # 문자열 그대로 비교하면 "10.0.0" < "9.0.0"으로 잘못 판정된다 — 이 케이스가 그걸 잡는다.
        rows = spark.createDataFrame(
            [
                ("seg-1", 1, AS_OF, "9.0.0", 10),
                ("seg-1", 1, AS_OF, "10.0.0", 20),
                ("seg-2", 1, AS_OF, "1.1.1", 30),
            ],
            "segment_id string, vehicle_profile_id int, data_period_start timestamp, "
            "scoring_version string, sample_count long",
        )

        result = {
            (row["segment_id"], row["vehicle_profile_id"], row["data_period_start"]): row[
                "sample_count"
            ]
            for row in _select_latest_scoring_version(rows).collect()
        }

        assert result == {
            ("seg-1", 1, AS_OF): 20,  # "10.0.0"이 이겨야 한다
            ("seg-2", 1, AS_OF): 30,
        }

    def test_select_latest_scoring_version_parses_the_configured_scoring_version(self, spark):
        # 여기서 검증할 값은 하드코딩한 semver가 아니라 hourly_comfort.yaml에 실제로 설정된
        # scoring_version이다 (#152). 이게 SemVer가 아니면 캐스팅이 런타임에 실패한다.
        configured_version = DEFAULT_HOURLY_SCORING_CONFIG.scoring_version
        rows = spark.createDataFrame(
            [("seg-1", 1, AS_OF, configured_version, 100)],
            "segment_id string, vehicle_profile_id int, data_period_start timestamp, "
            "scoring_version string, sample_count long",
        )

        result = _select_latest_scoring_version(rows).collect()

        assert result[0]["scoring_version"] == configured_version

    @staticmethod
    def _write_rows(spark, path: Path, schema: StructType, rows: list[dict]) -> None:
        data = [tuple(row[field.name] for field in schema.fields) for row in rows]
        spark.createDataFrame(data, schema).write.parquet(str(path))

    @staticmethod
    def _comfort_score_row(**overrides: object) -> dict:
        base = {
            "segment_id": "seg-1",
            "vehicle_profile_id": 1,
            "data_period_start": AS_OF - timedelta(hours=1),
            "data_period_end": AS_OF,
            "road_snapshot_date": (AS_OF - timedelta(hours=1)).date(),
            "vertical_score": 80.0,
            "longitudinal_score": 80.0,
            "lateral_score": 80.0,
            "scoring_version": "1.0.0",
            "sample_count": 10,
            "trip_count": 5,
            "_run_id": "run-1",
            "_processed_at": AS_OF,
        }
        return base | overrides

    def test_load_keeps_only_the_window_and_passes_trip_count_through(self, spark, tmp_path):
        self._write_rows(
            spark,
            tmp_path / "silver" / "hourly_comfort_score",
            HOURLY_COMFORT_SCORE_SCHEMA,
            [
                self._comfort_score_row(segment_id="in-window", trip_count=7),
                self._comfort_score_row(
                    segment_id="out-of-window", data_period_start=AS_OF - timedelta(hours=200)
                ),
            ],
        )

        result = {
            row["segment_id"]: row["trip_count"]
            for row in load_hourly_comfort_score_for_gold(spark, str(tmp_path), AS_OF).collect()
        }

        # out-of-window 행은 아예 빠지고, trip_count는 hourly_comfort_score 자신의
        # 값이 그대로 나온다 (더 이상 hourly_segment_features와 join하지 않는다).
        assert result == {"in-window": 7}

    def test_load_raises_clearly_when_hourly_comfort_score_is_missing_a_column(
        self, spark, tmp_path
    ):
        incomplete_schema = StructType(
            [
                field
                for field in HOURLY_COMFORT_SCORE_SCHEMA.fields
                if field.name != "sample_count"
            ]
        )
        self._write_rows(
            spark,
            tmp_path / "silver" / "hourly_comfort_score",
            incomplete_schema,
            [{k: v for k, v in self._comfort_score_row().items() if k != "sample_count"}],
        )

        with pytest.raises(ValueError, match="sample_count"):
            load_hourly_comfort_score_for_gold(spark, str(tmp_path), AS_OF)

    def test_load_raises_clearly_when_hourly_comfort_score_has_a_type_mismatch(
        self, spark, tmp_path
    ):
        mismatched_schema = StructType(
            [
                StructField("sample_count", StringType(), nullable=False)
                if field.name == "sample_count"
                else field
                for field in HOURLY_COMFORT_SCORE_SCHEMA.fields
            ]
        )
        self._write_rows(
            spark,
            tmp_path / "silver" / "hourly_comfort_score",
            mismatched_schema,
            [self._comfort_score_row(sample_count="10")],
        )

        with pytest.raises(ValueError, match="sample_count"):
            load_hourly_comfort_score_for_gold(spark, str(tmp_path), AS_OF)


class TestComfortScoreFormula:
    # 기본 YAML 값(0.5/0.3/0.2, T_min=5, k=10)과 일부러 다르게 둬서, 결과가 이 값들을
    # 실제로 반영하는지(하드코딩된 상수가 아닌지) 자연스럽게 검증한다.
    TEST_CONFIG: ClassVar = ComfortScoreConfig(
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

    @staticmethod
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
        # 기본값은 호출부에서 매번 안 넘겨도 되게 data_period_start + 1시간으로 맞춘다.
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

    def hourly_df(self, spark, *rows: tuple):
        return spark.createDataFrame(list(rows), self.HOURLY_SCHEMA)

    @staticmethod
    def per_vehicle_rows(result) -> dict:
        return {
            (row.segment_id, row.vehicle_profile_id): row
            for row in result.collect()
            if row.vehicle_profile_id != 0
        }

    @staticmethod
    def vehicle_agnostic_row(result, segment_id: str):
        matches = [
            row
            for row in result.collect()
            if row.segment_id == segment_id and row.vehicle_profile_id == 0
        ]
        assert len(matches) == 1, f"expected exactly one vehicle-agnostic row for {segment_id}"
        return matches[0]

    def test_combines_directional_scores_with_configured_weights_for_one_qualifying_hour(
        self, spark
    ):
        df = self.hourly_df(
            spark,
            self.hour(vertical_score=100.0, longitudinal_score=0.0, lateral_score=0.0, trip_count=10),
        )

        result = compute_segment_comfort_scores(df, self.TEST_CONFIG)

        row = self.per_vehicle_rows(result)[("seg-1", 1)]
        # c_h = 0.6*100 + 0.3*0 + 0.1*0 = 60; 세그먼트가 하나뿐이라 mu_p도 60이라 shrink 후에도 60 그대로.
        assert row.comfort_score == pytest.approx(60.0)
        assert row.confidence_score == pytest.approx(1 / 5)  # N=1, k=4 -> 1/(1+4)
        assert row.score_version == "1.0.0"

    def test_hours_below_min_traffic_threshold_are_excluded(self, spark):
        df = self.hourly_df(
            spark,
            self.hour(trip_count=2, vertical_score=0.0, sample_count=999),  # T_min=5 미달 -> 제외
            self.hour(trip_count=10, vertical_score=100.0, sample_count=50),  # 포함
        )

        result = compute_segment_comfort_scores(df, self.TEST_CONFIG)

        row = self.per_vehicle_rows(result)[("seg-1", 1)]
        assert row.qualifying_hours == 1
        assert row.sample_count == 50  # 제외된 시간의 999는 합산되지 않는다
        assert row.comfort_score == pytest.approx(60.0)

    def test_shrinks_toward_the_population_mean_across_segments(self, spark):
        df = self.hourly_df(
            spark,
            self.hour(segment_id="seg-x", vertical_score=100.0, trip_count=10),  # c_h=60
            self.hour(segment_id="seg-y", vertical_score=0.0, trip_count=10),  # c_h=0
        )

        result = compute_segment_comfort_scores(df, self.TEST_CONFIG)
        rows = self.per_vehicle_rows(result)

        # mu_p = (60+0)/2 = 30. ComfortScore = (N*c_obs + k*mu_p)/(N+k), N=1, k=4.
        assert rows[("seg-x", 1)].comfort_score == pytest.approx((1 * 60 + 4 * 30) / 5)
        assert rows[("seg-y", 1)].comfort_score == pytest.approx((1 * 0 + 4 * 30) / 5)

    def test_rolls_up_data_period_bounds_across_qualifying_hours(self, spark):
        df = self.hourly_df(
            spark,
            self.hour(data_period_start=datetime(2026, 8, 1, 3, 0, 0), trip_count=10),  # noqa: DTZ001
            self.hour(data_period_start=datetime(2026, 8, 1, 5, 0, 0), trip_count=10),  # noqa: DTZ001
            # T_min 미달이라 qualify하지 않는 시간 — MIN/MAX 계산에서 제외돼야 한다.
            self.hour(data_period_start=datetime(2026, 8, 1, 0, 0, 0), trip_count=2),  # noqa: DTZ001
            self.hour(data_period_start=datetime(2026, 8, 1, 9, 0, 0), trip_count=2),  # noqa: DTZ001
        )

        result = compute_segment_comfort_scores(df, self.TEST_CONFIG)

        row = self.per_vehicle_rows(result)[("seg-1", 1)]
        assert row.data_period_start == datetime(2026, 8, 1, 3, 0, 0)  # noqa: DTZ001
        assert row.data_period_end == datetime(2026, 8, 1, 6, 0, 0)  # noqa: DTZ001

    def test_a_pair_with_hours_that_never_qualify_falls_back_to_the_population_mean(self, spark):
        df = self.hourly_df(
            spark,
            self.hour(segment_id="seg-x", vertical_score=100.0, trip_count=10),  # c_h=60, qualifies
            self.hour(segment_id="seg-y", vertical_score=0.0, trip_count=10),  # c_h=0, qualifies
            # seg-z: 기록(원본 행)은 있지만 T_min(5) 미달이라 qualifying hour가 0개가 된다.
            self.hour(segment_id="seg-z", vertical_score=999.0, trip_count=2, sample_count=999),
        )

        result = compute_segment_comfort_scores(df, self.TEST_CONFIG)
        rows = self.per_vehicle_rows(result)

        # seg-z는 mu_p(=30, seg-x/seg-y에서만 계산됨)로 그대로 대체되고 confidence는 0이어야 한다.
        z_row = rows[("seg-z", 1)]
        assert z_row.qualifying_hours == 0
        assert z_row.sample_count == 0
        assert z_row.comfort_score == pytest.approx(30.0)
        assert z_row.confidence_score == pytest.approx(0.0)
        # qualifying hour가 0개라 MIN/MAX로 롤업할 시간이 없다 — NULL로 남기고,
        # 실제 배치 윈도우 경계로 채우는 건 gold_job.py의 책임이다.
        assert z_row.data_period_start is None
        assert z_row.data_period_end is None

    def test_vehicle_agnostic_row_pools_profiles_in_the_same_hour_weighted_by_traffic(self, spark):
        same_hour = datetime(2026, 8, 1, 3, 0, 0)  # noqa: DTZ001
        df = self.hourly_df(
            spark,
            self.hour(
                vehicle_profile_id=1,
                data_period_start=same_hour,
                vertical_score=100.0,
                trip_count=10,
                sample_count=5,
            ),  # c_h=60
            self.hour(
                vehicle_profile_id=2,
                data_period_start=same_hour,
                vertical_score=0.0,
                trip_count=30,
                sample_count=15,
            ),  # c_h=0
        )

        result = compute_segment_comfort_scores(df, self.TEST_CONFIG)

        # 이 윈도우엔 이 세그먼트의 이 한 시간뿐이라, pooled c_h가 곧 c_obs이자 mu(전체 population)다.
        # c_h,s = (10*60 + 30*0) / (10+30) = 15
        row = self.vehicle_agnostic_row(result, "seg-1")
        assert row.comfort_score == pytest.approx(15.0)
        assert row.sample_count == 20  # 5 + 15, 두 프로필의 sample_count 합
        assert row.qualifying_hours == 1

        # 차량별 행(profile 1, 2)도 vehicle-agnostic 행과 함께 그대로 남아 있어야 한다.
        by_key = self.per_vehicle_rows(result)
        assert {1, 2} <= {vehicle_profile_id for (_, vehicle_profile_id) in by_key}

    def test_vehicle_agnostic_row_rolls_up_data_period_bounds_across_pooled_profiles(self, spark):
        same_hour = datetime(2026, 8, 1, 3, 0, 0)  # noqa: DTZ001
        df = self.hourly_df(
            spark,
            self.hour(vehicle_profile_id=1, data_period_start=same_hour, trip_count=10),
            self.hour(vehicle_profile_id=2, data_period_start=same_hour, trip_count=30),
        )

        result = compute_segment_comfort_scores(df, self.TEST_CONFIG)

        row = self.vehicle_agnostic_row(result, "seg-1")
        assert row.data_period_start == same_hour
        assert row.data_period_end == same_hour + timedelta(hours=1)

    def test_vehicle_agnostic_row_shrinks_toward_the_global_population_mean(self, spark):
        df = self.hourly_df(
            spark,
            # c_h=60
            self.hour(segment_id="seg-p", vehicle_profile_id=1, vertical_score=100.0, trip_count=10),
            # c_h=0
            self.hour(segment_id="seg-q", vehicle_profile_id=1, vertical_score=0.0, trip_count=10),
        )

        result = compute_segment_comfort_scores(df, self.TEST_CONFIG)
        expected_p = (1 * 60 + 4 * 30) / 5  # 전역 mu = (60+0)/2 = 30. 프로필이 하나뿐이라 pooling은 no-op.
        expected_q = (1 * 0 + 4 * 30) / 5

        assert self.vehicle_agnostic_row(result, "seg-p").comfort_score == pytest.approx(expected_p)
        assert self.vehicle_agnostic_row(result, "seg-q").comfort_score == pytest.approx(expected_q)

    def test_raises_a_clear_error_when_a_required_column_is_missing(self, spark):
        incomplete = spark.createDataFrame(
            [("seg-1", 1)], "segment_id string, vehicle_profile_id int"
        )

        with pytest.raises(ValueError, match="vertical_score"):
            compute_segment_comfort_scores(incomplete, self.TEST_CONFIG)

    def test_raises_when_input_uses_the_reserved_vehicle_agnostic_sentinel_id(self, spark):
        df = self.hourly_df(spark, self.hour(vehicle_profile_id=0))

        with pytest.raises(ValueError, match="reserved"):
            compute_segment_comfort_scores(df, self.TEST_CONFIG)

    def test_a_profile_with_no_qualifying_hour_anywhere_is_omitted_not_null(self, spark):
        df = self.hourly_df(
            spark,
            self.hour(segment_id="seg-x", vehicle_profile_id=1, vertical_score=100.0, trip_count=10),
            # profile 2는 이 윈도우 전체에서 유일한 행이 T_min 미달이라, 어디에도 qualifying hour가
            # 없다 -> mu_2 자체가 정의되지 않는다.
            self.hour(segment_id="seg-y", vehicle_profile_id=2, trip_count=2),
        )

        result = compute_segment_comfort_scores(df, self.TEST_CONFIG)
        rows = self.per_vehicle_rows(result)

        assert ("seg-x", 1) in rows  # profile 1은 정상적으로 산출됨
        assert ("seg-y", 2) not in rows  # mu_2가 없어 계산 불가 -> NULL 대신 행 자체가 없어야 한다

    def test_a_window_with_no_qualifying_hour_at_all_yields_no_rows(self, spark):
        df = self.hourly_df(spark, self.hour(trip_count=2))  # 유일한 행이 T_min 미달 -> qualifying hour 0개

        result = compute_segment_comfort_scores(df, self.TEST_CONFIG)

        assert result.count() == 0


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


class TestGoldWriter:
    def test_acquire_lock_raises_when_already_held(self):
        cursor = FakeCursor()
        cursor.queue((False,))

        with pytest.raises(RuntimeError, match="lock"):
            _acquire_lock(cursor)

    def test_acquire_lock_succeeds_when_available(self):
        cursor = FakeCursor()
        cursor.queue((True,))

        _acquire_lock(cursor)  # must not raise

    def test_validate_staging_table_shape_raises_when_table_missing(self):
        cursor = FakeCursor()
        cursor.queue([])

        with pytest.raises(RuntimeError, match="make migrate"):
            _validate_staging_table_shape(cursor)

    def test_validate_staging_table_shape_raises_on_type_mismatch(self):
        cursor = FakeCursor()
        wrong_columns = dict(EXPECTED_STAGING_COLUMNS)
        wrong_columns["sample_count"] = "integer"  # 실제는 bigint여야 함
        cursor.queue(list(wrong_columns.items()))

        with pytest.raises(RuntimeError, match="sample_count"):
            _validate_staging_table_shape(cursor)

    def test_validate_staging_table_shape_passes_when_columns_match(self):
        cursor = FakeCursor()
        cursor.queue(list(EXPECTED_STAGING_COLUMNS.items()))

        _validate_staging_table_shape(cursor)  # must not raise

    def test_validate_no_duplicates_raises_on_duplicate_keys(self):
        cursor = FakeCursor()
        cursor.queue((3, 2))  # 3 rows, 2 distinct keys -> 1 duplicate

        with pytest.raises(ValueError, match="duplicate"):
            _validate_no_duplicates_or_nan(cursor)

    def test_validate_no_duplicates_raises_on_nan_or_infinity_scores(self):
        cursor = FakeCursor()
        cursor.queue((2, 2))  # no duplicates
        cursor.queue((1,))  # 1 row with NaN/Infinity

        with pytest.raises(ValueError, match="NaN"):
            _validate_no_duplicates_or_nan(cursor)

    def test_validate_no_duplicates_passes_when_clean(self):
        cursor = FakeCursor()
        cursor.queue((2, 2))
        cursor.queue((0,))

        _validate_no_duplicates_or_nan(cursor)  # must not raise

    def test_merge_returns_inserted_and_updated_counts(self):
        cursor = FakeCursor()
        cursor.queue((7, 3))

        inserted, updated = _merge(cursor)

        assert (inserted, updated) == (7, 3)
        assert "ON CONFLICT" in cursor.executed[-1]

    def test_expected_staging_columns_include_data_period_bounds(self):
        assert EXPECTED_STAGING_COLUMNS["data_period_start"] == "timestamp with time zone"
        assert EXPECTED_STAGING_COLUMNS["data_period_end"] == "timestamp with time zone"

    def test_merge_sql_carries_data_period_bounds_through_insert_and_update(self):
        # PK(segment_id, vehicle_profile_id)는 그대로 두되, 기간 컬럼은 매 rerun마다
        # 갱신돼야 한다 — 그렇지 않으면 창이 옮겨가도 예전 값이 그대로 남는다.
        assert "data_period_start" in _MERGE_SQL
        assert "data_period_end" in _MERGE_SQL
        assert "data_period_start = EXCLUDED.data_period_start" in _MERGE_SQL
        assert "data_period_end = EXCLUDED.data_period_end" in _MERGE_SQL


class TestGoldJob:
    @staticmethod
    def make_config(tmp_path, data_lake_uri: str) -> SegmentComfortScoreJobConfig:
        return SegmentComfortScoreJobConfig(
            data_lake_uri=data_lake_uri,
            window_hours=168,
            comfort_score_config_path=DEFAULT_COMFORT_SCORE_CONFIG_PATH,
            postgres_host="unused",
            postgres_port=5432,
            postgres_db="unused",
            postgres_user="unused",
            postgres_password="unused",
        )

    def test_validate_as_of_raises_on_naive_datetime(self):
        with pytest.raises(ValueError, match="timezone-aware"):
            _validate_as_of(datetime(2026, 8, 16, 0, 0))  # noqa: DTZ001

    def test_validate_as_of_accepts_aware_datetime(self):
        _validate_as_of(datetime(2026, 8, 16, 0, 0, tzinfo=UTC))  # must not raise

    def test_attach_calculated_at_uses_the_same_as_of_literal_for_every_row(self, spark):
        as_of = datetime(2026, 8, 16, 3, 0, tzinfo=UTC)
        df = spark.createDataFrame(
            [("seg-1", 1), ("seg-2", 2)], "segment_id string, vehicle_profile_id int"
        )

        result = _attach_calculated_at(df, as_of)

        epochs = [row[0] for row in result.select(F.unix_timestamp("calculated_at")).collect()]
        assert epochs == [int(as_of.timestamp())] * 2

    def test_select_staging_columns_drops_diagnostic_columns_not_in_the_staging_table(self, spark):
        # formula.py의 출력에는 qualifying_hours/observed_score/population_mean처럼
        # staging 테이블에 없는 진단용 컬럼이 섞여 있다 (#152) — 그대로 write하면
        # JDBC write가 컬럼 불일치로 실패한다.
        df = spark.createDataFrame(
            [
                (
                    "seg-1", 1,
                    datetime(2026, 8, 15, 12, tzinfo=UTC), datetime(2026, 8, 15, 13, tzinfo=UTC),
                    80.0, 0.9, 100, 5, 78.0, 82.0, "1.0.0", datetime(2026, 8, 16, tzinfo=UTC),
                )
            ],
            "segment_id string, vehicle_profile_id int, data_period_start timestamp, "
            "data_period_end timestamp, comfort_score double, "
            "confidence_score double, sample_count long, qualifying_hours long, "
            "observed_score double, population_mean double, score_version string, "
            "calculated_at timestamp",
        )

        result = _select_staging_columns(df)

        assert result.columns == list(EXPECTED_STAGING_COLUMNS)

    def test_fill_missing_periods_leaves_existing_bounds_untouched(self, spark):
        as_of = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
        start = datetime(2026, 8, 15, 3, 0, tzinfo=UTC)
        end = datetime(2026, 8, 15, 4, 0, tzinfo=UTC)
        df = spark.createDataFrame(
            [("seg-1", 1, start, end)],
            "segment_id string, vehicle_profile_id int, data_period_start timestamp, "
            "data_period_end timestamp",
        )

        result = _fill_missing_periods(df, as_of, window_hours=168)

        row = result.select(
            F.unix_timestamp("data_period_start"), F.unix_timestamp("data_period_end")
        ).collect()[0]
        assert row[0] == int(start.timestamp())
        assert row[1] == int(end.timestamp())

    def test_fill_missing_periods_uses_the_batch_window_bounds_when_null(self, spark):
        # qualifying_hours=0인 행은 formula.py에서 MIN/MAX로 롤업할 시간이 없어
        # NULL로 나온다 (#163) — 이 행이 실제로 커버하려던 배치 윈도우
        # [as_of - window_hours, as_of)로 채운다.
        as_of = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
        df = spark.createDataFrame(
            [("seg-z", 1, None, None)],
            "segment_id string, vehicle_profile_id int, data_period_start timestamp, "
            "data_period_end timestamp",
        )

        result = _fill_missing_periods(df, as_of, window_hours=168)

        row = result.select(
            F.unix_timestamp("data_period_start"), F.unix_timestamp("data_period_end")
        ).collect()[0]
        window_start = datetime(2026, 8, 9, 0, 0, tzinfo=UTC)
        assert row[0] == int(window_start.timestamp())
        assert row[1] == int(as_of.timestamp())

    def test_returns_zero_merged_count_and_skips_write_when_window_has_no_rows(
        self, spark, tmp_path
    ):
        input_path = tmp_path / "silver" / "hourly_comfort_score"
        spark.createDataFrame([], HOURLY_COMFORT_SCORE_SCHEMA).write.parquet(str(input_path))
        config = self.make_config(tmp_path, str(tmp_path))
        as_of = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)

        summary = run_segment_comfort_score_job(spark, config, as_of, connection=None)

        assert summary == SegmentComfortScoreJobSummary(0, 0, 0, 0)


def _connect():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


class TestSegmentComfortScoreIntegration:
    """Integration tests for segment_comfort_score gold loading (#129).

    RUN_INTEGRATION 미설정 시 skip(로컬 편의). RUN_INTEGRATION=1인데 Postgres
    접속이 실패하면 skip이 아니라 fail한다 — "접속 안 되면 조용히 스킵"은
    CI에서 영원히 초록불이 켜지는 결과를 낳으므로 채택하지 않는다.
    """

    pytestmark = pytest.mark.skipif(
        not RUN_INTEGRATION, reason="set RUN_INTEGRATION=1 to run against a real Postgres"
    )

    @staticmethod
    @pytest.fixture(scope="class")
    def spark():
        session = build_spark_session()
        yield session
        session.stop()

    @staticmethod
    @pytest.fixture(autouse=True)
    def clean_tables():
        # RUN_INTEGRATION=1인데 접속이 안 되면 여기서 바로 fail한다(skip 아님) —
        # 이 fixture가 실패하면 pytest가 해당 테스트를 error로 보고하지 skip으로
        # 보고하지 않는다.
        connection = _connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"TRUNCATE {TARGET_TABLE}, {STAGING_TABLE}")
                cursor.execute(
                    "DELETE FROM vehicle_profile WHERE vehicle_profile_id != 0"
                )
                # hourly_row()의 기본 vehicle_profile_id=1이 FK를 통과하도록 매
                # 테스트 시작 시 되살린다 — segment_comfort_score.vehicle_profile_id는
                # vehicle_profile을 참조하는 FK라 이 행이 없으면 MERGE의 INSERT가
                # ForeignKeyViolation으로 실패한다.
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

    @staticmethod
    @pytest.fixture(scope="class", autouse=True)
    def migrated():
        connection = _connect()
        try:
            run_migrations(MigrationConfig.from_env().migrations_dir, connection)
        finally:
            connection.close()

    @staticmethod
    def hourly_row(**overrides):
        row = {
            "segment_id": "seg-1",
            "vehicle_profile_id": 1,
            "data_period_start": datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
            "data_period_end": datetime(2026, 8, 15, 13, 0, tzinfo=UTC),
            "road_snapshot_date": datetime(2026, 8, 1).date(),  # noqa: DTZ001
            "vertical_score": 80.0,
            "longitudinal_score": 80.0,
            "lateral_score": 80.0,
            "scoring_version": "1.0.0",
            "sample_count": 100,
            "trip_count": 10,
            "_run_id": "test-run",
            "_processed_at": datetime(2026, 8, 15, 13, 5, tzinfo=UTC),
        }
        row.update(overrides)
        return tuple(row[field.name] for field in HOURLY_COMFORT_SCORE_SCHEMA)

    @staticmethod
    def write_hourly_scores(spark, tmp_path, *rows) -> str:
        data_lake_uri = str(tmp_path)
        (
            spark.createDataFrame(list(rows), HOURLY_COMFORT_SCORE_SCHEMA)
            .write.parquet(str(tmp_path / "silver" / "hourly_comfort_score"))
        )
        return data_lake_uri

    @staticmethod
    def make_config(data_lake_uri: str) -> SegmentComfortScoreJobConfig:
        env = os.environ
        return SegmentComfortScoreJobConfig(
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
    def fetch_rows(connection) -> list[tuple]:
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT segment_id, vehicle_profile_id, comfort_score, calculated_at "
                f"FROM {TARGET_TABLE} ORDER BY segment_id, vehicle_profile_id"
            )
            return cursor.fetchall()

    def test_loads_and_upserts_on_rerun_without_duplicating_rows(self, spark, tmp_path):
        as_of = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
        data_lake_uri = self.write_hourly_scores(spark, tmp_path, self.hourly_row())
        config = self.make_config(data_lake_uri)
        connection = _connect()
        try:
            first = run_segment_comfort_score_job(spark, config, as_of, connection)
            assert first.inserted_count >= 1
            first_rows = self.fetch_rows(connection)

            # 같은 조합을 다른 값으로 재실행 -> 행 수는 그대로, 값만 갱신
            later_as_of = as_of + timedelta(hours=1)
            self.write_hourly_scores(
                spark, tmp_path, self.hourly_row(vertical_score=0.0, longitudinal_score=0.0)
            )
            second = run_segment_comfort_score_job(spark, config, later_as_of, connection)
            second_rows = self.fetch_rows(connection)

            assert second.updated_count >= 1
            assert len(second_rows) == len(first_rows)
            assert second_rows != first_rows  # comfort_score/calculated_at이 바뀜
        finally:
            connection.close()

    def test_fk_violation_rejected_for_unknown_vehicle_profile(self, spark, tmp_path):
        as_of = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
        data_lake_uri = self.write_hourly_scores(
            spark, tmp_path, self.hourly_row(vehicle_profile_id=999)  # vehicle_profile에 없는 ID
        )
        config = self.make_config(data_lake_uri)
        connection = _connect()
        try:
            with pytest.raises(psycopg2.errors.ForeignKeyViolation):
                run_segment_comfort_score_job(spark, config, as_of, connection)
        finally:
            connection.rollback()
            connection.close()

    def test_staging_shape_check_fails_clearly_when_staging_table_is_missing(
        self, spark, tmp_path
    ):
        data_lake_uri = self.write_hourly_scores(spark, tmp_path, self.hourly_row())
        config = self.make_config(data_lake_uri)
        connection = _connect()
        try:
            # DROP 후 run_migrations()로 되살리는 방식은 쓰지 않는다 — 0002는 이미
            # schema_migrations에 체크섬이 기록돼 있어 run_migrations()가
            # 파일명/체크섬 기준으로 skip해버리고 CREATE TABLE이 재실행되지
            # 않는다. 대신 이름을 바꿔뒀다가 finally에서 되돌린다.
            with connection.cursor() as cursor:
                cursor.execute(
                    f"ALTER TABLE {STAGING_TABLE} RENAME TO {STAGING_TABLE}_renamed"
                )
            connection.commit()

            with pytest.raises(RuntimeError, match="make migrate"):
                run_segment_comfort_score_job(
                    spark, config, datetime(2026, 8, 16, 0, 0, tzinfo=UTC), connection
                )
        finally:
            connection.rollback()
            with connection.cursor() as cursor:
                cursor.execute(
                    f"ALTER TABLE {STAGING_TABLE}_renamed RENAME TO {STAGING_TABLE}"
                )
            connection.commit()
            connection.close()

    def test_concurrent_run_fails_fast_on_advisory_lock(self, spark, tmp_path):
        as_of = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
        data_lake_uri = self.write_hourly_scores(spark, tmp_path, self.hourly_row())
        config = self.make_config(data_lake_uri)

        holder = _connect()
        blocker_cursor = holder.cursor()
        blocker_cursor.execute(
            "SELECT pg_try_advisory_lock(%s)", (GOLD_JOB_STAGING_LOCK_KEY,)
        )
        assert blocker_cursor.fetchone()[0] is True

        connection = _connect()
        try:
            with pytest.raises(RuntimeError, match="staging lock"):
                run_segment_comfort_score_job(spark, config, as_of, connection)
        finally:
            connection.rollback()
            connection.close()
            blocker_cursor.execute(
                "SELECT pg_advisory_unlock(%s)", (GOLD_JOB_STAGING_LOCK_KEY,)
            )
            holder.close()

    def test_reads_are_never_blocked_while_merge_runs(self, spark, tmp_path):
        as_of = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
        rows = tuple(
            self.hourly_row(segment_id=f"seg-{i}") for i in range(200)
        )  # MERGE를 조금이라도 오래 걸리게
        data_lake_uri = self.write_hourly_scores(spark, tmp_path, *rows)
        config = self.make_config(data_lake_uri)
        connection = _connect()

        read_durations: list[float] = []
        stop = threading.Event()

        def read_loop():
            reader = _connect()
            # autocommit/commit을 따로 호출하지 않아도 안전하다 — 기본 격리
            # 수준인 READ COMMITTED에서는 각 SELECT 문 시작 시점에 새 스냅샷을
            # 보므로, 커밋을 안 해도 다음 반복의 SELECT는 그 시점의 최신 커밋된
            # 데이터를 본다. 여기서 검증하려는 것도 "MERGE 도중 읽기가 오래
            # 블록되지 않는지"이지 read-your-writes 일관성이 아니다.
            try:
                while not stop.is_set():
                    started = time.monotonic()
                    with reader.cursor() as cursor:
                        cursor.execute(f"SELECT count(*) FROM {TARGET_TABLE}")
                        cursor.fetchone()
                    read_durations.append(time.monotonic() - started)
                    time.sleep(0.01)
            finally:
                reader.close()

        reader_thread = threading.Thread(target=read_loop)
        reader_thread.start()
        try:
            run_segment_comfort_score_job(spark, config, as_of, connection)
        finally:
            stop.set()
            reader_thread.join(timeout=5)
            connection.close()

        assert read_durations  # 읽기 스레드가 실제로 최소 한 번은 실행됨
        assert max(read_durations) < 1.0  # 어떤 읽기도 1초 이상 블록되지 않음
