import os
import shutil
import time
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path

import pytest
import shapely
from batch_jobs.hourly_segment_feature_job import (
    HourlySegmentFeatureJobConfig,
    run_hourly_segment_feature_job,
)
from batch_jobs.hourly_segment_feature_storage import (
    hour_output_path,
    write_hourly_segment_features,
)
from batch_jobs.schemas import (
    HOURLY_SEGMENT_FEATURE_SCHEMA,
    PROCESSED_SENSOR_EVENT_SCHEMA,
)
from batch_jobs.sensor_features.aggregation import (
    add_hourly_aggregation_keys,
    aggregate_hourly_event_counts,
    aggregate_hourly_sensor_statistics,
    build_hourly_segment_features,
    validate_hourly_segment_features,
)
from batch_jobs.sensor_features.config import (
    load_event_feature_config,
    load_steering_feature_config,
)
from batch_jobs.sensor_features.events import (
    add_hard_acceleration_event,
    add_hard_braking_event,
    add_sharp_steering_event,
)
from batch_jobs.sensor_features.steering import add_steering_rate, add_steering_reversal
from pyproj import Transformer
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from shapely.geometry import LineString

# collect()가 돌려주는 timestamp는 이 파이썬 프로세스의 로컬 타임존으로 변환되어,
# 고정하지 않으면 실행 머신마다(Asia/Seoul vs UTC) 값이 달라진다.
os.environ["TZ"] = "UTC"
time.tzset()

BASE_EVENT_TIME = datetime(2026, 8, 14, 10, 0, 0, tzinfo=UTC)


def event_time(offset_seconds: float = 0.0) -> datetime:
    return BASE_EVENT_TIME + timedelta(seconds=offset_seconds)


@pytest.fixture(scope="session")
def spark():
    # 세션 전체에서 재사용: SparkSession 기동에 몇 초가 걸린다.
    session = (
        SparkSession.builder.appName("batch-jobs-tests")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestEventConfig:
    def test_load_event_feature_config_reads_provisional_thresholds(self) -> None:
        config = load_event_feature_config()

        assert config.max_gap_seconds.value == 0.5
        assert config.max_gap_seconds.provisional is True
        assert config.hard_accel_threshold_mps2.value == 3.0
        assert config.hard_accel_threshold_mps2.provisional is True
        assert config.hard_brake_threshold_mps2.value == -3.0
        assert config.hard_brake_threshold_mps2.provisional is True
        assert config.min_event_duration_seconds.value == 0.3
        assert config.min_event_duration_seconds.provisional is True
        assert config.sharp_steer_threshold_deg_per_sec.value == 100.0
        assert config.sharp_steer_threshold_deg_per_sec.provisional is True
        assert config.sharp_steer_min_duration_seconds.value == 0.3
        assert config.sharp_steer_min_duration_seconds.provisional is True
        assert config.lookahead_seconds.value == 1.0
        assert config.lookahead_seconds.provisional is True


class TestSteeringConfig:
    def test_load_steering_feature_config_reads_provisional_thresholds(self) -> None:
        config = load_steering_feature_config()

        assert config.max_gap_seconds.value == 0.5
        assert config.max_gap_seconds.provisional is True
        assert config.steering_rate_deadband_deg_per_sec.value == 10.0
        assert config.steering_rate_deadband_deg_per_sec.provisional is True


class TestEvents:
    COLUMNS = ("event_id", "trip_id", "trip_seq", "event_time", "accel_x")
    STEER_COLUMNS = ("event_id", "trip_id", "trip_seq", "event_time", "steering_rate")
    ACCEL_THRESHOLD = 3.0
    BRAKE_THRESHOLD = -3.0
    STEER_THRESHOLD = 100.0

    @staticmethod
    def accel_row(event_id: str, trip_id: str, trip_seq: int, second: float, accel_x) -> Row:
        return Row(
            event_id=event_id,
            trip_id=trip_id,
            trip_seq=trip_seq,
            event_time=event_time(second),
            accel_x=accel_x,
        )

    @staticmethod
    def flags_by_event_id(df, column: str) -> dict:
        return {row["event_id"]: row[column] for row in df.collect()}

    def add_hard_accel(self, df, min_event_duration_seconds: float = 0.0, max_gap_seconds: float = 0.5):
        return add_hard_acceleration_event(
            df, self.ACCEL_THRESHOLD, min_event_duration_seconds, max_gap_seconds
        )

    def add_hard_brake(self, df, min_event_duration_seconds: float = 0.0, max_gap_seconds: float = 0.5):
        return add_hard_braking_event(
            df, self.BRAKE_THRESHOLD, min_event_duration_seconds, max_gap_seconds
        )

    @staticmethod
    def steer_rate_row(event_id: str, trip_seq: int, second: float, steering_rate) -> Row:
        return Row(
            event_id=event_id,
            trip_id="A",
            trip_seq=trip_seq,
            event_time=event_time(second),
            steering_rate=steering_rate,
        )

    def add_sharp_steer(self, df, min_event_duration_seconds: float = 0.0, max_gap_seconds: float = 0.5):
        return add_sharp_steering_event(
            df, self.STEER_THRESHOLD, min_event_duration_seconds, max_gap_seconds
        )

    def test_sustained_hard_acceleration_flags_only_the_start_row(self, spark) -> None:
        rows = [
            self.accel_row("e1", "A", 1, 0.0, 3.5),
            self.accel_row("e2", "A", 2, 0.1, 3.8),
            self.accel_row("e3", "A", 3, 0.2, 4.1),
            self.accel_row("e4", "A", 4, 0.3, 3.7),
        ]
        df = spark.createDataFrame(rows)

        result = self.flags_by_event_id(
            self.add_hard_accel(df, min_event_duration_seconds=0.3), "hard_accel_event_start"
        )

        assert result == {"e1": True, "e2": False, "e3": False, "e4": False}

    def test_short_hard_acceleration_is_not_flagged(self, spark) -> None:
        rows = [
            self.accel_row("e1", "A", 1, 0.0, 3.5),
            self.accel_row("e2", "A", 2, 0.1, 3.8),
            self.accel_row("e3", "A", 3, 0.2, 1.0),
        ]
        df = spark.createDataFrame(rows)

        result = self.flags_by_event_id(
            self.add_hard_accel(df, min_event_duration_seconds=0.3), "hard_accel_event_start"
        )

        assert result == {"e1": False, "e2": False, "e3": False}

    def test_multi_row_hard_braking_counts_once(self, spark) -> None:
        rows = [
            self.accel_row("e1", "A", 1, 0.0, -3.5),
            self.accel_row("e2", "A", 2, 0.1, -3.8),
            self.accel_row("e3", "A", 3, 0.2, -4.1),
            self.accel_row("e4", "A", 4, 0.3, -3.7),
        ]
        df = spark.createDataFrame(rows)

        result = self.flags_by_event_id(
            self.add_hard_brake(df, min_event_duration_seconds=0.3), "hard_brake_event_start"
        )

        assert sum(1 for flagged in result.values() if flagged) == 1
        assert result["e1"] is True

    def test_episode_splits_when_condition_is_released(self, spark) -> None:
        rows = [
            self.accel_row("e1", "A", 1, 0.0, 3.5),
            self.accel_row("e2", "A", 2, 0.1, 3.8),
            self.accel_row("e3", "A", 3, 0.2, 1.0),  # 조건 해제 -> episode 종료
            self.accel_row("e4", "A", 4, 0.3, 3.6),
            self.accel_row("e5", "A", 5, 0.4, 3.9),
        ]
        df = spark.createDataFrame(rows)

        result = self.flags_by_event_id(
            self.add_hard_accel(df, min_event_duration_seconds=0.05), "hard_accel_event_start"
        )

        assert result == {"e1": True, "e2": False, "e3": False, "e4": True, "e5": False}

    def test_different_trips_are_not_connected(self, spark) -> None:
        rows = [
            self.accel_row("e1", "A", 1, 0.0, 3.5),
            self.accel_row("e2", "A", 2, 0.1, 3.8),
            self.accel_row("e3", "B", 1, 0.2, 3.6),
            self.accel_row("e4", "B", 2, 0.3, 3.9),
        ]
        df = spark.createDataFrame(rows)

        result = self.flags_by_event_id(
            self.add_hard_accel(df, min_event_duration_seconds=0.05), "hard_accel_event_start"
        )

        assert result == {"e1": True, "e2": False, "e3": True, "e4": False}

    def test_sampling_gap_splits_the_episode(self, spark) -> None:
        rows = [
            self.accel_row("e1", "A", 1, 0.0, 3.5),
            self.accel_row("e2", "A", 2, 0.1, 3.8),
            self.accel_row("e3", "A", 3, 60.0, 3.6),
            self.accel_row("e4", "A", 4, 60.1, 3.9),
        ]
        df = spark.createDataFrame(rows)

        result = self.flags_by_event_id(
            self.add_hard_accel(df, min_event_duration_seconds=0.05, max_gap_seconds=0.5),
            "hard_accel_event_start",
        )

        assert result == {"e1": True, "e2": False, "e3": True, "e4": False}

    def test_null_accel_x_splits_the_episode(self, spark) -> None:
        rows = [
            self.accel_row("e1", "A", 1, 0.0, 3.5),
            self.accel_row("e1b", "A", 2, 0.1, 3.6),
            self.accel_row("e2", "A", 3, 0.2, None),
            self.accel_row("e3", "A", 4, 0.3, 3.6),
            self.accel_row("e4", "A", 5, 0.4, 3.9),
        ]
        df = spark.createDataFrame(rows)

        result = self.flags_by_event_id(
            self.add_hard_accel(df, min_event_duration_seconds=0.05), "hard_accel_event_start"
        )

        assert result == {"e1": True, "e1b": False, "e2": False, "e3": True, "e4": False}

    def test_result_is_independent_of_input_row_order(self, spark) -> None:
        rows = [
            self.accel_row("e1", "A", 1, 0.0, 3.5),
            self.accel_row("e2", "A", 2, 0.1, 3.8),
            self.accel_row("e3", "A", 3, 0.2, 4.1),
        ]
        in_order = spark.createDataFrame(rows)
        shuffled = spark.createDataFrame(list(reversed(rows)))

        expected = self.flags_by_event_id(
            self.add_hard_accel(in_order, min_event_duration_seconds=0.15), "hard_accel_event_start"
        )
        actual = self.flags_by_event_id(
            self.add_hard_accel(shuffled, min_event_duration_seconds=0.15), "hard_accel_event_start"
        )

        assert actual == expected

    def test_existing_columns_are_preserved(self, spark) -> None:
        rows = [self.accel_row("e1", "A", 1, 0.0, 3.5)]
        df = spark.createDataFrame(rows)

        result = self.add_hard_accel(df)

        assert set(self.COLUMNS).issubset(set(result.columns))
        assert "hard_accel_event_start" in result.columns

    def test_existing_run_id_lineage_column_is_not_overwritten(self, spark) -> None:
        # processed_sensor_event의 lineage 컬럼 _run_id가 episode 계산용 내부 컬럼과
        # 이름이 겹쳐 덮어써지거나 삭제되지 않는지 확인한다.
        rows = [
            Row(
                event_id="e1",
                trip_id="A",
                trip_seq=1,
                event_time=event_time(0.0),
                accel_x=3.5,
                _run_id="nyc-actual-20260814-v1",
            ),
        ]
        df = spark.createDataFrame(rows)

        result = self.add_hard_accel(df)

        assert result.select("_run_id").collect()[0]["_run_id"] == "nyc-actual-20260814-v1"

    @pytest.mark.parametrize("threshold", [0.0, -1.0])
    def test_non_positive_hard_accel_threshold_is_rejected(self, spark, threshold: float) -> None:
        df = spark.createDataFrame([self.accel_row("e1", "A", 1, 0.0, 3.5)])

        with pytest.raises(ValueError, match="hard_accel_threshold_mps2"):
            add_hard_acceleration_event(df, threshold, 0.0, 0.5)

    @pytest.mark.parametrize("threshold", [0.0, 1.0])
    def test_non_negative_hard_brake_threshold_is_rejected(self, spark, threshold: float) -> None:
        df = spark.createDataFrame([self.accel_row("e1", "A", 1, 0.0, -3.5)])

        with pytest.raises(ValueError, match="hard_brake_threshold_mps2"):
            add_hard_braking_event(df, threshold, 0.0, 0.5)

    @pytest.mark.parametrize("max_gap_seconds", [0.0, -1.0])
    def test_non_positive_max_gap_seconds_is_rejected(self, spark, max_gap_seconds: float) -> None:
        df = spark.createDataFrame([self.accel_row("e1", "A", 1, 0.0, 3.5)])

        with pytest.raises(ValueError, match="max_gap_seconds"):
            add_hard_acceleration_event(df, self.ACCEL_THRESHOLD, 0.0, max_gap_seconds)

    def test_negative_min_event_duration_seconds_is_rejected(self, spark) -> None:
        df = spark.createDataFrame([self.accel_row("e1", "A", 1, 0.0, 3.5)])

        with pytest.raises(ValueError, match="min_duration_seconds"):
            add_hard_acceleration_event(df, self.ACCEL_THRESHOLD, -1.0, 0.5)

    def test_positive_steering_rate_triggers_sharp_steer(self, spark) -> None:
        rows = [
            ("e1", "A", 1, event_time(0.0), 120.0),
            ("e2", "A", 2, event_time(0.1), 130.0),
            ("e3", "A", 3, event_time(0.2), 125.0),
        ]
        df = spark.createDataFrame(rows, self.STEER_COLUMNS)

        result = self.flags_by_event_id(
            self.add_sharp_steer(df, min_event_duration_seconds=0.15), "sharp_steer_event_start"
        )

        assert result == {"e1": True, "e2": False, "e3": False}

    def test_negative_steering_rate_triggers_sharp_steer(self, spark) -> None:
        rows = [
            ("e1", "A", 1, event_time(0.0), -120.0),
            ("e2", "A", 2, event_time(0.1), -130.0),
        ]
        df = spark.createDataFrame(rows, self.STEER_COLUMNS)

        result = self.flags_by_event_id(
            self.add_sharp_steer(df, min_event_duration_seconds=0.05), "sharp_steer_event_start"
        )

        assert result == {"e1": True, "e2": False}

    def test_steering_rate_below_threshold_is_excluded(self, spark) -> None:
        rows = [
            ("e1", "A", 1, event_time(0.0), 20.0),
            ("e2", "A", 2, event_time(0.1), 30.0),
        ]
        df = spark.createDataFrame(rows, self.STEER_COLUMNS)

        result = self.flags_by_event_id(
            self.add_sharp_steer(df, min_event_duration_seconds=0.0), "sharp_steer_event_start"
        )

        assert result == {"e1": False, "e2": False}

    def test_sign_reversal_within_threshold_stays_one_episode(self, spark) -> None:
        # 부호가 바뀌어도 절대값이 계속 threshold 이상이면 하나의 episode로 본다
        # (방향 반전 자체는 is_steering_reversal이 별도로 기록한다).
        rows = [
            ("e1", "A", 1, event_time(0.0), 120.0),
            ("e2", "A", 2, event_time(0.1), 130.0),
            ("e3", "A", 3, event_time(0.2), -125.0),
            ("e4", "A", 4, event_time(0.3), -110.0),
        ]
        df = spark.createDataFrame(rows, self.STEER_COLUMNS)

        result = self.flags_by_event_id(
            self.add_sharp_steer(df, min_event_duration_seconds=0.25), "sharp_steer_event_start"
        )

        assert result == {"e1": True, "e2": False, "e3": False, "e4": False}

    def test_null_steering_rate_is_not_a_candidate(self, spark) -> None:
        rows = [
            self.steer_rate_row("e1", 1, 0.0, 120.0),
            self.steer_rate_row("e2", 2, 0.1, None),
        ]
        df = spark.createDataFrame(rows)

        result = self.flags_by_event_id(
            self.add_sharp_steer(df, min_event_duration_seconds=0.0), "sharp_steer_event_start"
        )

        assert result["e2"] is False

    def test_sharp_steer_existing_columns_are_preserved(self, spark) -> None:
        rows = [("e1", "A", 1, event_time(0.0), 120.0)]
        df = spark.createDataFrame(rows, self.STEER_COLUMNS)

        result = self.add_sharp_steer(df)

        assert set(self.STEER_COLUMNS).issubset(set(result.columns))
        assert "sharp_steer_event_start" in result.columns

    @pytest.mark.parametrize("threshold", [0.0, -1.0])
    def test_non_positive_sharp_steer_threshold_is_rejected(self, spark, threshold: float) -> None:
        df = spark.createDataFrame([("e1", "A", 1, event_time(0.0), 120.0)], self.STEER_COLUMNS)

        with pytest.raises(ValueError, match="sharp_steer_threshold_deg_per_sec"):
            add_sharp_steering_event(df, threshold, 0.0, 0.5)


class TestSteering:
    COLUMNS = ("event_id", "trip_id", "trip_seq", "event_time", "steering_angle")
    RATE_COLUMNS = ("event_id", "trip_id", "trip_seq", "event_time", "steering_rate")
    # timestamp -> double 캐스팅에서 생기는 미세한 부동소수점 오차를 흡수한다.
    RATE_TOLERANCE = 1e-3

    @staticmethod
    def _column_by_event_id(df, column: str) -> dict:
        return {row["event_id"]: row[column] for row in df.collect()}

    def rows_by_event_id(self, df) -> dict:
        return self._column_by_event_id(df, "steering_rate")

    def reversal_by_event_id(self, df) -> dict:
        return self._column_by_event_id(df, "is_steering_reversal")

    def test_normal_steering_rate_uses_actual_time_delta(self, spark) -> None:
        rows = [
            ("e1", "A", 1, event_time(0.0), 0.0),
            ("e2", "A", 2, event_time(0.1), 5.0),
            ("e3", "A", 3, event_time(0.2), 8.0),
        ]
        df = spark.createDataFrame(rows, self.COLUMNS)

        result = self.rows_by_event_id(add_steering_rate(df, max_gap_seconds=5.0))

        assert result["e1"] is None
        assert result["e2"] == pytest.approx(50.0, abs=self.RATE_TOLERANCE)
        assert result["e3"] == pytest.approx(30.0, abs=self.RATE_TOLERANCE)

    def test_trip_boundary_does_not_reference_previous_trip(self, spark) -> None:
        rows = [
            ("e1", "A", 1, event_time(0.0), 0.0),
            ("e2", "A", 2, event_time(0.1), 10.0),
            ("e3", "B", 1, event_time(0.2), -2.0),
        ]
        df = spark.createDataFrame(rows, self.COLUMNS)

        result = self.rows_by_event_id(add_steering_rate(df, max_gap_seconds=5.0))

        assert result["e3"] is None

    @pytest.mark.parametrize(
        "offset_seconds, max_gap_seconds, expected_rate",
        [
            (0.5, 5.0, 20.0),  # 실제 event_time 간격(0.5초) 사용, 10Hz를 가정해 0.1초로 나누지 않음
            (60.0, 5.0, None),  # 간격이 max_gap_seconds 초과 -> sampling gap -> NULL
            (4.0, 5.0, 2.5),  # 간격이 max_gap_seconds 이내 -> 정상 계산
        ],
    )
    def test_steering_rate_depends_on_actual_time_gap(
        self, spark, offset_seconds: float, max_gap_seconds: float, expected_rate: float | None
    ) -> None:
        rows = [
            ("e1", "A", 1, event_time(0.0), 0.0),
            ("e2", "A", 2, event_time(offset_seconds), 10.0),
        ]
        df = spark.createDataFrame(rows, self.COLUMNS)

        result = self.rows_by_event_id(add_steering_rate(df, max_gap_seconds=max_gap_seconds))

        if expected_rate is None:
            assert result["e2"] is None
        else:
            assert result["e2"] == pytest.approx(expected_rate, abs=self.RATE_TOLERANCE)

    def test_non_positive_time_delta_is_null(self, spark) -> None:
        rows = [
            ("e1", "A", 1, event_time(0.0), 0.0),
            ("e2", "A", 2, event_time(0.0), 5.0),
            ("e3", "A", 3, event_time(-0.1), 8.0),
        ]
        df = spark.createDataFrame(rows, self.COLUMNS)

        result = self.rows_by_event_id(add_steering_rate(df, max_gap_seconds=5.0))

        assert result["e2"] is None
        assert result["e3"] is None

    @staticmethod
    def sensor_row(event_id: str, trip_seq: int, second: float, steering_angle: float | None) -> Row:
        return Row(
            event_id=event_id,
            trip_id="A",
            trip_seq=trip_seq,
            event_time=event_time(second),
            steering_angle=steering_angle,
        )

    def test_null_steering_angle_is_null(self, spark) -> None:
        rows = [
            self.sensor_row("e1", 1, 0.0, None),
            self.sensor_row("e2", 2, 0.1, 5.0),
            self.sensor_row("e3", 3, 0.2, None),
        ]
        df = spark.createDataFrame(rows)

        result = self.rows_by_event_id(add_steering_rate(df, max_gap_seconds=5.0))

        assert result["e2"] is None
        assert result["e3"] is None

    def test_result_is_independent_of_input_row_order(self, spark) -> None:
        rows = [
            ("e1", "A", 1, event_time(0.0), 0.0),
            ("e2", "A", 2, event_time(0.1), 5.0),
            ("e3", "A", 3, event_time(0.2), 8.0),
        ]
        in_order = spark.createDataFrame(rows, self.COLUMNS)
        shuffled = spark.createDataFrame(list(reversed(rows)), self.COLUMNS)

        expected = self.rows_by_event_id(add_steering_rate(in_order, max_gap_seconds=5.0))
        actual = self.rows_by_event_id(add_steering_rate(shuffled, max_gap_seconds=5.0))

        assert actual.keys() == expected.keys()
        for event_id, expected_rate in expected.items():
            if expected_rate is None:
                assert actual[event_id] is None
            else:
                assert actual[event_id] == pytest.approx(expected_rate, abs=self.RATE_TOLERANCE)

    def test_existing_columns_are_preserved(self, spark) -> None:
        rows = [
            ("e1", "A", 1, event_time(0.0), 0.0),
            ("e2", "A", 2, event_time(0.1), 5.0),
        ]
        df = spark.createDataFrame(rows, self.COLUMNS)

        result = add_steering_rate(df, max_gap_seconds=5.0)

        assert set(self.COLUMNS).issubset(set(result.columns))
        assert "steering_rate" in result.columns

    @pytest.mark.parametrize("max_gap_seconds", [0.0, -1.0])
    def test_non_positive_max_gap_seconds_is_rejected(self, spark, max_gap_seconds: float) -> None:
        rows = [("e1", "A", 1, event_time(0.0), 0.0)]
        df = spark.createDataFrame(rows, self.COLUMNS)

        with pytest.raises(ValueError, match="max_gap_seconds"):
            add_steering_rate(df, max_gap_seconds=max_gap_seconds)

    @staticmethod
    def rate_row(event_id: str, trip_seq: int, second: float, steering_rate: float | None) -> Row:
        return Row(
            event_id=event_id,
            trip_id="A",
            trip_seq=trip_seq,
            event_time=event_time(second),
            steering_rate=steering_rate,
        )

    def test_deadband_excludes_small_changes_from_direction(self, spark) -> None:
        rows = [
            ("e1", "A", 1, event_time(0.0), 15.0),
            ("e2", "A", 2, event_time(0.1), 3.0),
            ("e3", "A", 3, event_time(0.2), -15.0),
        ]
        df = spark.createDataFrame(rows, self.RATE_COLUMNS)

        result = self.reversal_by_event_id(
            add_steering_reversal(df, steering_rate_deadband_deg_per_sec=5.0)
        )

        assert result["e1"] is None
        assert result["e2"] is None
        assert result["e3"] is True

    def test_same_direction_is_not_a_reversal(self, spark) -> None:
        rows = [
            ("e1", "A", 1, event_time(0.0), 15.0),
            ("e2", "A", 2, event_time(0.1), 20.0),
        ]
        df = spark.createDataFrame(rows, self.RATE_COLUMNS)

        result = self.reversal_by_event_id(
            add_steering_reversal(df, steering_rate_deadband_deg_per_sec=5.0)
        )

        assert result["e1"] is None
        assert result["e2"] is False

    def test_reversal_does_not_cross_trip_boundary(self, spark) -> None:
        rows = [
            ("e1", "A", 1, event_time(0.0), 15.0),
            ("e2", "B", 1, event_time(0.1), -15.0),
        ]
        df = spark.createDataFrame(rows, self.RATE_COLUMNS)

        result = self.reversal_by_event_id(
            add_steering_reversal(df, steering_rate_deadband_deg_per_sec=5.0)
        )

        assert result["e2"] is None

    def test_null_steering_rate_breaks_reversal_continuity(self, spark) -> None:
        # gap(e2) 이후 첫 유효 방향(e3)은 gap 이전 방향(e1)과 비교되면 안 된다.
        rows = [
            self.rate_row("e1", 1, 0.0, 15.0),
            self.rate_row("e2", 2, 0.1, None),
            self.rate_row("e3", 3, 0.2, -15.0),
        ]
        df = spark.createDataFrame(rows)

        result = self.reversal_by_event_id(
            add_steering_reversal(df, steering_rate_deadband_deg_per_sec=5.0)
        )

        assert result["e2"] is None
        assert result["e3"] is None

    def test_reversal_existing_columns_are_preserved(self, spark) -> None:
        rows = [
            ("e1", "A", 1, event_time(0.0), 15.0),
            ("e2", "A", 2, event_time(0.1), -15.0),
        ]
        df = spark.createDataFrame(rows, self.RATE_COLUMNS)

        result = add_steering_reversal(df, steering_rate_deadband_deg_per_sec=5.0)

        assert set(self.RATE_COLUMNS).issubset(set(result.columns))
        assert "is_steering_reversal" in result.columns

    def test_negative_steering_rate_deadband_is_rejected(self, spark) -> None:
        rows = [("e1", "A", 1, event_time(0.0), 0.0)]
        df = spark.createDataFrame(rows, self.RATE_COLUMNS)

        with pytest.raises(ValueError, match="steering_rate_deadband_deg_per_sec"):
            add_steering_reversal(df, steering_rate_deadband_deg_per_sec=-1.0)


class TestHourlyAggregationKeys:
    SCHEMA = StructType(
        [
            StructField("event_time", TimestampType(), nullable=False),
            StructField("segment_id", StringType(), nullable=True),
            StructField("vehicle_profile_id", IntegerType(), nullable=False),
            StructField("speed_mps", DoubleType(), nullable=True),
            StructField("accel_x", DoubleType(), nullable=True),
            StructField("accel_y", DoubleType(), nullable=True),
            StructField("accel_z", DoubleType(), nullable=True),
            StructField("jerk_x", DoubleType(), nullable=True),
            StructField("jerk_y", DoubleType(), nullable=True),
            StructField("jerk_z", DoubleType(), nullable=True),
            StructField("steering_rate", DoubleType(), nullable=True),
            StructField("steering_vibration", DoubleType(), nullable=True),
        ]
    )

    @staticmethod
    def event_time(hour: int, minute: int, second: int) -> datetime:
        return datetime(2026, 8, 11, hour, minute, second, tzinfo=UTC)

    @staticmethod
    def expected(hour: int, minute: int, second: int = 0) -> datetime:
        # collect()가 돌려주는 TimestampType 값은 tzinfo가 없는 naive datetime이다.
        return datetime(2026, 8, 11, hour, minute, second)  # noqa: DTZ001

    def sensor_row(
        self,
        hour: int,
        minute: int,
        second: int,
        segment_id: str | None = "S1",
        vehicle_profile_id: int = 1,
        speed_mps: float | None = None,
    ) -> tuple:
        return (
            self.event_time(hour, minute, second),
            segment_id,
            vehicle_profile_id,
            speed_mps,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )

    def sensor_df(self, spark, rows: list[tuple]):
        return spark.createDataFrame(rows, self.SCHEMA)

    def test_rows_in_the_same_hour_share_data_period_start(self, spark) -> None:
        rows = [
            self.sensor_row(10, 0, 0, speed_mps=5.0),
            self.sensor_row(10, 37, 25, speed_mps=6.0),
            self.sensor_row(10, 59, 59, speed_mps=7.0),
        ]

        result = add_hourly_aggregation_keys(self.sensor_df(spark, rows)).collect()

        assert {row["data_period_start"] for row in result} == {self.expected(10, 0)}

    def test_hour_boundary_splits_into_different_periods(self, spark) -> None:
        rows = [self.sensor_row(10, 59, 59), self.sensor_row(11, 0, 0)]

        result = (
            add_hourly_aggregation_keys(self.sensor_df(spark, rows)).orderBy("event_time").collect()
        )

        assert result[0]["data_period_start"] == self.expected(10, 0)
        assert result[1]["data_period_start"] == self.expected(11, 0)

    def test_data_period_end_is_exactly_one_hour_after_start(self, spark) -> None:
        rows = [self.sensor_row(10, 37, 25)]

        row = add_hourly_aggregation_keys(self.sensor_df(spark, rows)).first()

        assert row["data_period_end"] == self.expected(11, 0)

    def test_unmatched_events_without_segment_id_are_excluded(self, spark) -> None:
        rows = [self.sensor_row(10, 0, 0, segment_id="S1"), self.sensor_row(10, 0, 0, segment_id=None)]

        result = add_hourly_aggregation_keys(self.sensor_df(spark, rows)).collect()

        assert len(result) == 1
        assert result[0]["segment_id"] == "S1"

    def test_different_segment_ids_are_kept_separate(self, spark) -> None:
        rows = [self.sensor_row(10, 0, 0, segment_id="S1"), self.sensor_row(10, 0, 0, segment_id="S2")]

        result = add_hourly_aggregation_keys(self.sensor_df(spark, rows)).collect()

        assert {row["segment_id"] for row in result} == {"S1", "S2"}

    def test_different_vehicle_profile_ids_are_kept_separate(self, spark) -> None:
        rows = [
            self.sensor_row(10, 0, 0, vehicle_profile_id=1),
            self.sensor_row(10, 0, 0, vehicle_profile_id=2),
        ]

        result = add_hourly_aggregation_keys(self.sensor_df(spark, rows)).collect()

        assert {row["vehicle_profile_id"] for row in result} == {1, 2}

    def test_existing_sensor_columns_are_retained(self, spark) -> None:
        rows = [self.sensor_row(10, 0, 0, speed_mps=5.0)]

        row = add_hourly_aggregation_keys(self.sensor_df(spark, rows)).first()

        assert row["speed_mps"] == pytest.approx(5.0)

    def test_missing_required_column_is_rejected(self, spark) -> None:
        incomplete_schema = StructType([StructField("event_time", TimestampType(), nullable=False)])
        df = spark.createDataFrame([(self.event_time(10, 0, 0),)], incomplete_schema)

        with pytest.raises(ValueError, match="segment_id"):
            add_hourly_aggregation_keys(df)


class TestHourlyEventCounts:
    SNAPSHOT = date(2026, 8, 11)
    SENSOR_SCHEMA = StructType(
        [
            StructField("event_time", TimestampType(), nullable=False),
            StructField("segment_id", StringType(), nullable=True),
            StructField("vehicle_profile_id", IntegerType(), nullable=False),
            StructField("trip_id", StringType(), nullable=True),
            StructField("road_snapshot_date", DateType(), nullable=True),
            StructField("speed_mps", DoubleType(), nullable=True),
            StructField("accel_x", DoubleType(), nullable=True),
            StructField("accel_y", DoubleType(), nullable=True),
            StructField("accel_z", DoubleType(), nullable=True),
            StructField("jerk_x", DoubleType(), nullable=True),
            StructField("jerk_y", DoubleType(), nullable=True),
            StructField("jerk_z", DoubleType(), nullable=True),
            StructField("steering_rate", DoubleType(), nullable=True),
            StructField("steering_vibration", DoubleType(), nullable=True),
            StructField("hard_brake_event_start", BooleanType(), nullable=True),
            StructField("hard_accel_event_start", BooleanType(), nullable=True),
            StructField("sharp_steer_event_start", BooleanType(), nullable=True),
            StructField("is_steering_reversal", BooleanType(), nullable=True),
        ]
    )

    @staticmethod
    def event_time(minute: int = 0, hour: int = 10) -> datetime:
        return datetime(2026, 8, 11, hour, minute, 0, tzinfo=UTC)

    def sensor_row(
        self,
        minute: int = 0,
        hour: int = 10,
        segment_id: str | None = "S1",
        vehicle_profile_id: int = 1,
        trip_id: str = "T1",
        hard_brake_event_start: bool | None = False,
        hard_accel_event_start: bool | None = False,
        sharp_steer_event_start: bool | None = False,
        is_steering_reversal: bool | None = False,
    ) -> tuple:
        return (
            self.event_time(minute, hour=hour),
            segment_id,
            vehicle_profile_id,
            trip_id,
            self.SNAPSHOT,
            10.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            hard_brake_event_start,
            hard_accel_event_start,
            sharp_steer_event_start,
            is_steering_reversal,
        )

    def sensor_df(self, spark, rows: list[tuple]):
        return spark.createDataFrame(rows, self.SENSOR_SCHEMA)

    def one_group(self, spark, rows: list[tuple]):
        result = aggregate_hourly_event_counts(self.sensor_df(spark, rows)).collect()
        assert len(result) == 1
        return result[0]

    def test_event_and_sample_counts_are_accurate(self, spark) -> None:
        rows = [
            self.sensor_row(0, trip_id="T1", hard_brake_event_start=True),  # 급제동 episode 시작
            self.sensor_row(1, trip_id="T1", hard_brake_event_start=False),  # 같은 episode의 나머지 행
            self.sensor_row(2, trip_id="T1", hard_accel_event_start=True),
            self.sensor_row(3, trip_id="T2", sharp_steer_event_start=True),
            self.sensor_row(4, trip_id="T2", is_steering_reversal=True),
        ]

        row = self.one_group(spark, rows)

        assert row["hard_brake_count"] == 1  # 여러 행짜리 episode도 1회
        assert row["hard_accel_count"] == 1
        assert row["sharp_steer_count"] == 1
        assert row["steer_reversal_count"] == 1
        assert row["sample_count"] == 5
        assert row["trip_count"] == 2

    def test_null_event_flags_are_treated_as_zero(self, spark) -> None:
        rows = [self.sensor_row(0, hard_brake_event_start=None)]

        row = self.one_group(spark, rows)

        assert row["hard_brake_count"] == 0

    def test_group_keys_separate_and_unmatched_rows_excluded(self, spark) -> None:
        rows = [
            self.sensor_row(0, segment_id="S1", vehicle_profile_id=1),
            self.sensor_row(0, segment_id="S2", vehicle_profile_id=1),
            self.sensor_row(0, segment_id="S1", vehicle_profile_id=2),
            self.sensor_row(0, hour=11, segment_id="S1", vehicle_profile_id=1),
            self.sensor_row(0, segment_id=None),  # 미매칭 행은 별도 그룹을 만들지 않고 제외되어야 함
        ]

        result = aggregate_hourly_event_counts(self.sensor_df(spark, rows)).collect()

        assert len(result) == 4


class TestHourlySensorStatistics:
    SCHEMA = StructType(
        [
            StructField("event_time", TimestampType(), nullable=False),
            StructField("segment_id", StringType(), nullable=True),
            StructField("vehicle_profile_id", IntegerType(), nullable=False),
            StructField("speed_mps", DoubleType(), nullable=True),
            StructField("accel_x", DoubleType(), nullable=True),
            StructField("accel_y", DoubleType(), nullable=True),
            StructField("accel_z", DoubleType(), nullable=True),
            StructField("jerk_x", DoubleType(), nullable=True),
            StructField("jerk_y", DoubleType(), nullable=True),
            StructField("jerk_z", DoubleType(), nullable=True),
            StructField("steering_rate", DoubleType(), nullable=True),
            StructField("steering_vibration", DoubleType(), nullable=True),
        ]
    )

    @staticmethod
    def event_time(hour: int = 10, minute: int = 0, second: int = 0) -> datetime:
        return datetime(2026, 8, 11, hour, minute, second, tzinfo=UTC)

    @staticmethod
    def expected(hour: int, minute: int = 0) -> datetime:
        # collect()가 돌려주는 TimestampType 값은 tzinfo가 없는 naive datetime이다.
        return datetime(2026, 8, 11, hour, minute, 0)  # noqa: DTZ001

    def sensor_row(
        self,
        minute: int = 0,
        hour: int = 10,
        second: int = 0,
        segment_id: str | None = "S1",
        vehicle_profile_id: int = 1,
        speed_mps: float | None = 10.0,
        accel_x: float | None = None,
    ) -> tuple:
        return (
            self.event_time(hour, minute, second),
            segment_id,
            vehicle_profile_id,
            speed_mps,
            accel_x,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )

    def sensor_df(self, spark, rows: list[tuple]):
        return spark.createDataFrame(rows, self.SCHEMA)

    def one_group(self, spark, rows: list[tuple]):
        result = aggregate_hourly_sensor_statistics(self.sensor_df(spark, rows)).collect()
        assert len(result) == 1
        return result[0]

    def test_rms_matches_the_formula(self, spark) -> None:
        rows = [self.sensor_row(0, accel_x=3.0), self.sensor_row(1, accel_x=4.0)]

        row = self.one_group(spark, rows)

        expected_rms = ((3.0**2 + 4.0**2) / 2) ** 0.5
        assert row["rms_accel_x"] == pytest.approx(expected_rms)

    def test_p95_uses_absolute_value(self, spark) -> None:
        values = [1.0, 2.0, 3.0, 4.0, -10.0]
        rows = [self.sensor_row(minute, accel_x=value) for minute, value in enumerate(values)]

        row = self.one_group(spark, rows)

        # 부호를 그대로 쓰면 0.95 분위수는 4 근처에 그치지만, 절댓값 기준이면
        # -10의 크기(10)가 반영되어 훨씬 커야 한다.
        assert row["p95_abs_accel_x"] > 5.0

    def test_avg_speed_is_computed_correctly(self, spark) -> None:
        rows = [self.sensor_row(0, speed_mps=10.0), self.sensor_row(1, speed_mps=20.0)]

        row = self.one_group(spark, rows)

        assert row["avg_speed_mps"] == pytest.approx(15.0)

    def test_steering_signals_only_have_rms_not_p95(self, spark) -> None:
        rows = [self.sensor_row(0)]

        row = self.one_group(spark, rows)

        assert "rms_steering_rate" in row.asDict()
        assert "rms_steering_vibration" in row.asDict()
        assert "p95_abs_steering_rate" not in row.asDict()
        assert "p95_abs_steering_vibration" not in row.asDict()

    def test_different_group_keys_are_aggregated_separately(self, spark) -> None:
        rows = [
            self.sensor_row(0, segment_id="S1", vehicle_profile_id=1),
            self.sensor_row(0, segment_id="S2", vehicle_profile_id=1),
            self.sensor_row(0, segment_id="S1", vehicle_profile_id=2),
        ]

        result = aggregate_hourly_sensor_statistics(self.sensor_df(spark, rows)).collect()

        keys = {(row["segment_id"], row["vehicle_profile_id"]) for row in result}
        assert keys == {("S1", 1), ("S2", 1), ("S1", 2)}

    def test_hour_boundary_is_aggregated_into_different_groups(self, spark) -> None:
        rows = [
            self.sensor_row(minute=59, second=59),
            self.sensor_row(hour=11, minute=0, second=0),
        ]

        result = aggregate_hourly_sensor_statistics(self.sensor_df(spark, rows)).collect()

        periods = {row["data_period_start"] for row in result}
        assert periods == {self.expected(10), self.expected(11)}

    def test_unmatched_events_without_segment_id_are_excluded(self, spark) -> None:
        rows = [self.sensor_row(0, segment_id="S1"), self.sensor_row(0, segment_id=None)]

        result = aggregate_hourly_sensor_statistics(self.sensor_df(spark, rows)).collect()

        assert len(result) == 1
        assert result[0]["segment_id"] == "S1"

    def test_partial_null_values_are_excluded_from_statistics(self, spark) -> None:
        rows = [
            self.sensor_row(0, accel_x=3.0),
            self.sensor_row(1, accel_x=None),
            self.sensor_row(2, accel_x=4.0),
        ]

        row = self.one_group(spark, rows)

        expected_rms = ((3.0**2 + 4.0**2) / 2) ** 0.5
        assert row["rms_accel_x"] == pytest.approx(expected_rms)

    def test_all_null_values_produce_null_statistics(self, spark) -> None:
        rows = [self.sensor_row(0, accel_x=None), self.sensor_row(1, accel_x=None)]

        row = self.one_group(spark, rows)

        assert row["rms_accel_x"] is None
        assert row["p95_abs_accel_x"] is None

    def test_result_is_independent_of_input_row_order(self, spark) -> None:
        rows = [self.sensor_row(0, accel_x=3.0), self.sensor_row(1, accel_x=4.0)]

        forward = self.one_group(spark, rows)
        reversed_row = self.one_group(spark, list(reversed(rows)))

        assert forward["rms_accel_x"] == pytest.approx(reversed_row["rms_accel_x"])


class TestHourlySegmentFeatures:
    SNAPSHOT = date(2026, 8, 11)
    RUN_ID = "run-1"
    FEATURE_VERSION = "v1"
    PROCESSED_AT = datetime(2026, 8, 11, 12, 0, 0, tzinfo=UTC)

    SENSOR_SCHEMA = StructType(
        [
            StructField("event_time", TimestampType(), nullable=False),
            StructField("segment_id", StringType(), nullable=True),
            StructField("vehicle_profile_id", IntegerType(), nullable=False),
            StructField("trip_id", StringType(), nullable=True),
            StructField("road_snapshot_date", DateType(), nullable=True),
            StructField("speed_mps", DoubleType(), nullable=True),
            StructField("accel_x", DoubleType(), nullable=True),
            StructField("accel_y", DoubleType(), nullable=True),
            StructField("accel_z", DoubleType(), nullable=True),
            StructField("jerk_x", DoubleType(), nullable=True),
            StructField("jerk_y", DoubleType(), nullable=True),
            StructField("jerk_z", DoubleType(), nullable=True),
            StructField("steering_rate", DoubleType(), nullable=True),
            StructField("steering_vibration", DoubleType(), nullable=True),
            StructField("hard_brake_event_start", BooleanType(), nullable=True),
            StructField("hard_accel_event_start", BooleanType(), nullable=True),
            StructField("sharp_steer_event_start", BooleanType(), nullable=True),
            StructField("is_steering_reversal", BooleanType(), nullable=True),
        ]
    )

    @classmethod
    def event_time(cls, minute: int = 0, hour: int = 10) -> datetime:
        return datetime(2026, 8, 11, hour, minute, 0, tzinfo=UTC)

    def sensor_row(
        self,
        minute: int = 0,
        trip_id: str = "T1",
        road_snapshot_date: date = SNAPSHOT,
        hard_accel_event_start: bool = False,
    ) -> tuple:
        return (
            self.event_time(minute),
            "S1",
            1,
            trip_id,
            road_snapshot_date,
            10.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            False,
            hard_accel_event_start,
            False,
            False,
        )

    def sensor_df(self, spark, rows: list[tuple]):
        return spark.createDataFrame(rows, self.SENSOR_SCHEMA)

    def test_build_combines_statistics_and_event_counts_into_canonical_schema(self, spark) -> None:
        rows = [
            self.sensor_row(0, hard_accel_event_start=True),
            self.sensor_row(1, trip_id="T2"),
        ]

        result = build_hourly_segment_features(
            self.sensor_df(spark, rows),
            feature_version=self.FEATURE_VERSION,
            run_id=self.RUN_ID,
            processed_at=self.PROCESSED_AT,
        )

        expected_fields = HOURLY_SEGMENT_FEATURE_SCHEMA.fields
        assert result.columns == [field.name for field in expected_fields]
        actual_types = {field.name: field.dataType for field in result.schema.fields}
        assert all(actual_types[field.name] == field.dataType for field in expected_fields)

        rows = result.collect()
        assert len(rows) == 1
        row = rows[0]
        assert row["avg_speed_mps"] == pytest.approx(10.0)
        assert row["sample_count"] == 2
        assert row["trip_count"] == 2
        assert row["hard_accel_count"] == 1
        assert row["road_snapshot_date"] == self.SNAPSHOT
        assert row["feature_version"] == self.FEATURE_VERSION
        assert row["_run_id"] == self.RUN_ID

    def test_conflicting_road_snapshot_date_for_same_pk_is_rejected(self, spark) -> None:
        # aggregate_hourly_event_counts는 road_snapshot_date별로도 나뉘어 집계되므로, 같은
        # 시간·Segment·차량 프로필에 서로 다른 snapshot이 섞이면 PK가 중복되어야 한다.
        rows = [
            self.sensor_row(0, road_snapshot_date=self.SNAPSHOT),
            self.sensor_row(1, road_snapshot_date=date(2026, 8, 12)),
        ]

        with pytest.raises(ValueError, match="duplicate"):
            build_hourly_segment_features(
                self.sensor_df(spark, rows),
                feature_version=self.FEATURE_VERSION,
                run_id=self.RUN_ID,
                processed_at=self.PROCESSED_AT,
            )

    def feature_row(self, **overrides: object) -> dict[str, object]:
        row = {
            "segment_id": "S1",
            "vehicle_profile_id": 1,
            "data_period_start": self.event_time(0),
            "data_period_end": self.event_time(0).replace(hour=11),
            "road_snapshot_date": self.SNAPSHOT,
            "avg_speed_mps": 10.0,
            "rms_accel_x": 1.0,
            "rms_accel_y": 1.0,
            "rms_accel_z": 1.0,
            "p95_abs_accel_x": 1.0,
            "p95_abs_accel_y": 1.0,
            "p95_abs_accel_z": 1.0,
            "rms_jerk_x": 1.0,
            "rms_jerk_y": 1.0,
            "rms_jerk_z": 1.0,
            "p95_abs_jerk_x": 1.0,
            "p95_abs_jerk_y": 1.0,
            "p95_abs_jerk_z": 1.0,
            "hard_brake_count": 0,
            "hard_accel_count": 0,
            "sharp_steer_count": 0,
            "steer_reversal_count": 0,
            "rms_steering_rate": 1.0,
            "rms_steering_vibration": 1.0,
            "sample_count": 10,
            "trip_count": 2,
            "feature_version": self.FEATURE_VERSION,
            "_processed_at": self.PROCESSED_AT,
            "_run_id": self.RUN_ID,
        }
        row.update(overrides)
        return row

    # 검증 대상 컬럼 순서는 항상 스키마와 같아야 하므로, dict 대신 스키마 순서 그대로 tuple을 만든다
    def feature_rows_df(self, spark, rows: list[dict[str, object]]):
        ordered = [
            tuple(row[field.name] for field in HOURLY_SEGMENT_FEATURE_SCHEMA.fields) for row in rows
        ]
        # nullable=False인 컬럼도 테스트에서는 의도적으로 NULL을 넣어봐야 하므로 전부 nullable로 둔다
        nullable_schema = StructType(
            [
                StructField(field.name, field.dataType, nullable=True)
                for field in HOURLY_SEGMENT_FEATURE_SCHEMA.fields
            ]
        )
        return spark.createDataFrame(ordered, nullable_schema)

    def test_valid_output_passes_validation(self, spark) -> None:
        df = self.feature_rows_df(spark, [self.feature_row()])

        validate_hourly_segment_features(df)

    def test_cache_is_released_after_successful_validation(self, spark) -> None:
        df = self.feature_rows_df(spark, [self.feature_row()])

        validate_hourly_segment_features(df)

        assert df.storageLevel.useMemory is False

    def test_cache_is_released_after_validation_error(self, spark) -> None:
        df = self.feature_rows_df(spark, [self.feature_row(), self.feature_row()])  # 중복 PK -> 검증 실패

        with pytest.raises(ValueError, match="duplicate"):
            validate_hourly_segment_features(df)

        assert df.storageLevel.useMemory is False

    @pytest.mark.parametrize(
        "build_df, match",
        [
            (lambda self, spark: self.feature_rows_df(spark, [self.feature_row(segment_id=None)]), "NULL"),
            (
                lambda self, spark: self.feature_rows_df(
                    spark, [self.feature_row(data_period_end=self.event_time(0))]
                ),
                "one hour",
            ),
            (
                lambda self, spark: self.feature_rows_df(
                    spark, [self.feature_row(hard_brake_count=-1)]
                ),
                "invalid count",
            ),
            (
                lambda self, spark: self.feature_rows_df(
                    spark, [self.feature_row(sample_count=1, trip_count=2)]
                ),
                "invalid count",
            ),
            (
                lambda self, spark: self.feature_rows_df(
                    spark, [self.feature_row(sample_count=1, hard_brake_count=2)]
                ),
                "invalid count",
            ),
            (
                lambda self, spark: self.feature_rows_df(
                    spark, [self.feature_row(), self.feature_row()]
                ),
                "duplicate",
            ),
            (
                lambda self, spark: self.feature_rows_df(spark, [self.feature_row()]).drop(
                    "feature_version"
                ),
                "do not match the canonical schema",
            ),
        ],
        ids=[
            "null-required",
            "bad-period-end",
            "negative-count",
            "trip>sample",
            "event>sample",
            "duplicate-pk",
            "column-mismatch",
        ],
    )
    def test_rejects_invalid_rows(self, spark, build_df, match: str) -> None:
        with pytest.raises(ValueError, match=match):
            validate_hourly_segment_features(build_df(self, spark))


class TestHourlySegmentFeatureJob:
    SNAPSHOT = date(2026, 8, 13)
    TARGET_HOUR = datetime(2026, 8, 16, 10, 0, 0, tzinfo=UTC)
    PROCESSED_AT = datetime(2026, 8, 16, 12, 0, 0, tzinfo=UTC)
    FEATURE_VERSION = "v1"
    RUN_ID = "run-1"

    _TRANSFORMER = Transformer.from_crs("EPSG:4326", "EPSG:32118", always_xy=True)
    BASE_LAT, BASE_LON = 40.7484, -73.9857

    ROAD_SEGMENT_COLUMNS = (
        "segment_id",
        "snapshot_date",
        "geometry_wkb",
        "traffic_direction",
        "from_node_id",
        "to_node_id",
    )

    @classmethod
    def _base_xy(cls) -> tuple[float, float]:
        return cls._TRANSFORMER.transform(cls.BASE_LON, cls.BASE_LAT)

    def write_road_segment(self, spark, tmp_path) -> str:
        # 모든 센서 포인트가 이 하나의 도로(양방향)와 정확히 같은 좌표라 항상 매칭된다.
        base_x, base_y = self._base_xy()
        line = LineString([(base_x, base_y - 50.0), (base_x, base_y + 50.0)])
        row = ("S1", self.SNAPSHOT, shapely.to_wkb(line), "T", "N1", "N2")
        path = str(tmp_path / "road_segment")
        spark.createDataFrame([row], self.ROAD_SEGMENT_COLUMNS).write.mode("overwrite").parquet(
            f"{path}/snapshot_date={self.SNAPSHOT.isoformat()}/data.parquet"
        )
        return path

    def sensor_row(
        self,
        event_time: datetime,
        event_id: str,
        trip_id: str = "T1",
        trip_seq: int = 0,
        vehicle_profile_id: int = 1,
        accel_x: float = 0.0,
        latitude: float = BASE_LAT,
        longitude: float = BASE_LON,
    ) -> tuple:
        return (
            event_id,
            vehicle_profile_id,
            trip_id,
            trip_seq,
            event_time,
            latitude,
            longitude,
            10.0,
            0.0,
            accel_x,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            self.PROCESSED_AT,
            self.RUN_ID,
        )

    def sensor_events(self, spark, rows: list[tuple]):
        return spark.createDataFrame(rows, PROCESSED_SENSOR_EVENT_SCHEMA)

    def build_config(self, spark, tmp_path) -> HourlySegmentFeatureJobConfig:
        road_segment_path = self.write_road_segment(spark, tmp_path)
        return HourlySegmentFeatureJobConfig.from_env(
            {
                "HOURLY_SEGMENT_FEATURE_ROAD_SEGMENT_PATH": road_segment_path,
                "HOURLY_SEGMENT_FEATURE_OUTPUT_PATH": str(tmp_path / "hourly_segment_features"),
            }
        )

    def run_job(self, spark, tmp_path, rows: list[tuple]):
        config = self.build_config(spark, tmp_path)
        summary = run_hourly_segment_feature_job(
            spark,
            self.sensor_events(spark, rows),
            config,
            self.TARGET_HOUR,
            self.SNAPSHOT,
            self.FEATURE_VERSION,
            self.RUN_ID,
            self.PROCESSED_AT,
        )
        return summary, spark.read.parquet(summary.output_path).collect()

    def test_matches_and_aggregates_one_target_hour(self, spark, tmp_path) -> None:
        rows = [
            self.sensor_row(self.TARGET_HOUR + timedelta(seconds=0), "e1", trip_seq=0),
            self.sensor_row(self.TARGET_HOUR + timedelta(seconds=1), "e2", trip_seq=1),
            self.sensor_row(self.TARGET_HOUR + timedelta(seconds=2), "e3", trip_seq=2),
        ]

        summary, result = self.run_job(spark, tmp_path, rows)

        assert summary.result_count == 1
        assert summary.target_hour == self.TARGET_HOUR
        assert summary.run_id == self.RUN_ID
        assert len(result) == 1
        row = result[0]
        assert row["segment_id"] == "S1"
        assert row["road_snapshot_date"] == self.SNAPSHOT
        assert row["data_period_start"] == self.TARGET_HOUR.replace(tzinfo=None)
        assert row["sample_count"] == 3
        assert row["trip_count"] == 1
        assert row["feature_version"] == self.FEATURE_VERSION
        assert row["_run_id"] == self.RUN_ID

        stored_schema = spark.read.parquet(summary.output_path).schema
        actual_types = {field.name: field.dataType for field in stored_schema.fields}
        assert stored_schema.fieldNames() == HOURLY_SEGMENT_FEATURE_SCHEMA.fieldNames()
        assert all(
            actual_types[field.name] == field.dataType
            for field in HOURLY_SEGMENT_FEATURE_SCHEMA.fields
        )

    def test_episode_starting_before_target_hour_is_not_recounted(self, spark, tmp_path) -> None:
        rows = [
            # 09:59:59.7에 급제동이 시작해 대상 시간(10시) 안까지 이어짐
            self.sensor_row(
                self.TARGET_HOUR - timedelta(milliseconds=300), "e0", trip_seq=0, accel_x=-5.0
            ),
            self.sensor_row(
                self.TARGET_HOUR + timedelta(milliseconds=100), "e1", trip_seq=1, accel_x=-5.0
            ),
        ]

        _, result = self.run_job(spark, tmp_path, rows)

        assert len(result) == 1
        assert result[0]["hard_brake_count"] == 0  # 시작 행은 09시에 속하므로 10시엔 없어야 함

    def test_lookback_rows_are_excluded_from_the_final_result(self, spark, tmp_path) -> None:
        rows = [
            # target_hour 0.2초 전: lookback 범위 안이라 읽히지만 최종 집계에는 포함되면 안 됨
            self.sensor_row(
                self.TARGET_HOUR - timedelta(milliseconds=200), "lookback", trip_id="LOOKBACK"
            ),
            self.sensor_row(self.TARGET_HOUR + timedelta(seconds=0), "e1", trip_seq=0),
            self.sensor_row(self.TARGET_HOUR + timedelta(seconds=1), "e2", trip_seq=1),
        ]

        _, result = self.run_job(spark, tmp_path, rows)

        assert len(result) == 1  # lookback 행이 별도의 이전 시간 그룹을 만들지 않는다
        assert result[0]["data_period_start"] == self.TARGET_HOUR.replace(tzinfo=None)
        assert result[0]["sample_count"] == 2

    def test_episode_spanning_the_hour_boundary_is_counted_via_lookahead(self, spark, tmp_path) -> None:
        rows = [
            self.sensor_row(self.TARGET_HOUR + timedelta(seconds=30), "e0", trip_seq=0, accel_x=0.0),
            # 급제동이 대상 시간 끝나기 0.2초 전에 시작해 다음 시간으로 0.1초 더 이어짐
            self.sensor_row(
                self.TARGET_HOUR + timedelta(minutes=59, seconds=59, milliseconds=800),
                "e1",
                trip_seq=1,
                accel_x=-5.0,
            ),
            self.sensor_row(
                self.TARGET_HOUR + timedelta(hours=1, milliseconds=100),
                "e2",
                trip_seq=2,
                accel_x=-5.0,
            ),
        ]

        _, result = self.run_job(spark, tmp_path, rows)

        assert len(result) == 1
        assert result[0]["hard_brake_count"] == 1
        # 11시의 e2는 Episode 판단(lookahead)에는 쓰이지만 최종 sample_count에는 들어가면 안 된다.
        assert result[0]["sample_count"] == 2

    def test_unmatched_events_are_excluded_from_the_result(self, spark, tmp_path) -> None:
        rows = [
            self.sensor_row(self.TARGET_HOUR + timedelta(seconds=0), "e1", trip_seq=0),
            # 도로에서 멀리 떨어진 위치라 어떤 Segment와도 매칭되지 않는다
            self.sensor_row(
                self.TARGET_HOUR + timedelta(seconds=1),
                "e2",
                trip_seq=1,
                latitude=self.BASE_LAT + 1.0,
                longitude=self.BASE_LON,
            ),
        ]

        _, result = self.run_job(spark, tmp_path, rows)

        assert len(result) == 1
        assert result[0]["segment_id"] == "S1"
        assert result[0]["sample_count"] == 1

    def test_rerunning_the_same_hour_replaces_the_stored_result(self, spark, tmp_path) -> None:
        config = self.build_config(spark, tmp_path)

        first = run_hourly_segment_feature_job(
            spark,
            self.sensor_events(spark, [self.sensor_row(self.TARGET_HOUR, "e1")]),
            config,
            self.TARGET_HOUR,
            self.SNAPSHOT,
            self.FEATURE_VERSION,
            "run-1",
            self.PROCESSED_AT,
        )
        rows = [
            self.sensor_row(self.TARGET_HOUR, "e2"),
            self.sensor_row(self.TARGET_HOUR + timedelta(seconds=1), "e3", vehicle_profile_id=2),
        ]
        second = run_hourly_segment_feature_job(
            spark,
            self.sensor_events(spark, rows),
            config,
            self.TARGET_HOUR,
            self.SNAPSHOT,
            self.FEATURE_VERSION,
            "run-2",
            self.PROCESSED_AT,
        )

        assert second.output_path == first.output_path
        result = spark.read.parquet(second.output_path).collect()
        assert len(result) == 2  # 이전 실행(run-1)의 결과가 아니라 최신 결과로 교체됨
        assert {row["vehicle_profile_id"] for row in result} == {1, 2}

    @pytest.mark.parametrize(
        "target_hour, feature_version, run_id, processed_at",
        [
            (TARGET_HOUR.replace(tzinfo=None), FEATURE_VERSION, RUN_ID, PROCESSED_AT),  # naive
            (TARGET_HOUR.replace(minute=30), FEATURE_VERSION, RUN_ID, PROCESSED_AT),  # 정각 아님
            (TARGET_HOUR, "", RUN_ID, PROCESSED_AT),  # feature_version 공백
            (TARGET_HOUR, FEATURE_VERSION, "", PROCESSED_AT),  # run_id 공백
            (TARGET_HOUR, FEATURE_VERSION, RUN_ID, PROCESSED_AT.replace(tzinfo=None)),  # naive
        ],
        ids=["naive-target-hour", "not-truncated", "blank-version", "blank-run-id", "naive-processed-at"],
    )
    def test_rejects_invalid_arguments(
        self, spark, tmp_path, target_hour, feature_version, run_id, processed_at
    ) -> None:
        config = self.build_config(spark, tmp_path)
        sensor_df = self.sensor_events(
            spark, [self.sensor_row(self.TARGET_HOUR, "e1")]
        )

        with pytest.raises(ValueError):
            run_hourly_segment_feature_job(
                spark,
                sensor_df,
                config,
                target_hour,
                self.SNAPSHOT,
                feature_version,
                run_id,
                processed_at,
            )


@contextmanager
def host_timezone(tz_name: str):
    # Spark JVM은 이미 떠 있어도, naive datetime 변환은 파이썬 프로세스의 현재 TZ를 그대로 따른다.
    previous = os.environ.get("TZ")
    os.environ["TZ"] = tz_name
    time.tzset()
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = previous
        time.tzset()


class TestHourlySegmentFeatureStorage:
    SNAPSHOT = date(2026, 8, 13)
    TARGET_HOUR = datetime(2026, 8, 16, 10, 0, 0, tzinfo=UTC)
    RUN_ID = "run-1"

    def feature_row(
        self, target_hour: datetime = TARGET_HOUR, segment_id: str = "S1", **overrides: object
    ) -> dict[str, object]:
        row = {
            "segment_id": segment_id,
            "vehicle_profile_id": 1,
            # 실제 파이프라인은 date_trunc()로 이 값을 만들어 UTC 순간을 그대로 보존하므로,
            # 테스트도 tzinfo를 떼지 않고 UTC-aware로 둬야 호스트 타임존과 무관하게 재현된다.
            "data_period_start": target_hour,
            "data_period_end": target_hour + timedelta(hours=1),
            "road_snapshot_date": self.SNAPSHOT,
            "avg_speed_mps": 10.0,
            "rms_accel_x": 1.0,
            "rms_accel_y": 1.0,
            "rms_accel_z": 1.0,
            "p95_abs_accel_x": 1.0,
            "p95_abs_accel_y": 1.0,
            "p95_abs_accel_z": 1.0,
            "rms_jerk_x": 1.0,
            "rms_jerk_y": 1.0,
            "rms_jerk_z": 1.0,
            "p95_abs_jerk_x": 1.0,
            "p95_abs_jerk_y": 1.0,
            "p95_abs_jerk_z": 1.0,
            "hard_brake_count": 0,
            "hard_accel_count": 0,
            "sharp_steer_count": 0,
            "steer_reversal_count": 0,
            "rms_steering_rate": 1.0,
            "rms_steering_vibration": 1.0,
            "sample_count": 10,
            "trip_count": 2,
            "feature_version": "v1",
            "_processed_at": self.TARGET_HOUR,
            "_run_id": self.RUN_ID,
        }
        row.update(overrides)
        return row

    def feature_rows_df(self, spark, rows: list[dict[str, object]]):
        non_null_schema = StructType(
            [
                StructField(field.name, field.dataType, nullable=True)
                for field in HOURLY_SEGMENT_FEATURE_SCHEMA.fields
            ]
        )
        ordered = [
            tuple(row[field.name] for field in HOURLY_SEGMENT_FEATURE_SCHEMA.fields) for row in rows
        ]
        return spark.createDataFrame(ordered, non_null_schema)

    @staticmethod
    def read_back(spark, path: str):
        return spark.read.parquet(path).collect()

    @staticmethod
    def assert_staging_is_empty(output_root: str) -> None:
        staging_root = Path(output_root) / "_staging"
        assert not staging_root.exists() or not any(staging_root.iterdir())

    def test_hour_output_path_is_isolated_per_hour(self) -> None:
        path_10 = hour_output_path("out", self.TARGET_HOUR)
        path_11 = hour_output_path("out", self.TARGET_HOUR.replace(hour=11))

        assert path_10 == "out/data_period_date=2026-08-16/hour=10"
        assert path_10 != path_11

    def test_write_creates_data_at_the_expected_path(self, spark, tmp_path) -> None:
        output_root = str(tmp_path / "hourly_segment_features")
        df = self.feature_rows_df(spark, [self.feature_row(), self.feature_row(segment_id="S2")])

        result = write_hourly_segment_features(spark, df, output_root, self.TARGET_HOUR, self.RUN_ID)

        assert result.output_path == hour_output_path(output_root, self.TARGET_HOUR)
        assert result.row_count == 2
        assert len(self.read_back(spark, result.output_path)) == 2

    def test_rerunning_same_hour_replaces_data(self, spark, tmp_path) -> None:
        output_root = str(tmp_path / "hourly_segment_features")
        first = self.feature_rows_df(spark, [self.feature_row(segment_id="S1")])
        second = self.feature_rows_df(
            spark, [self.feature_row(segment_id="S2"), self.feature_row(segment_id="S3")]
        )

        write_hourly_segment_features(spark, first, output_root, self.TARGET_HOUR, "run-1")
        result = write_hourly_segment_features(spark, second, output_root, self.TARGET_HOUR, "run-2")

        rows = self.read_back(spark, result.output_path)
        assert {row["segment_id"] for row in rows} == {"S2", "S3"}

    def test_other_hours_are_not_touched_even_after_a_rerun(self, spark, tmp_path) -> None:
        output_root = str(tmp_path / "hourly_segment_features")
        hour_9 = self.TARGET_HOUR.replace(hour=9)
        hour_9_df = self.feature_rows_df(spark, [self.feature_row(target_hour=hour_9, segment_id="S9")])
        hour_10_df = self.feature_rows_df(
            spark, [self.feature_row(target_hour=self.TARGET_HOUR, segment_id="S1")]
        )
        hour_10_rerun_df = self.feature_rows_df(
            spark, [self.feature_row(target_hour=self.TARGET_HOUR, segment_id="S2")]
        )

        result_9 = write_hourly_segment_features(spark, hour_9_df, output_root, hour_9, "run-1")
        write_hourly_segment_features(spark, hour_10_df, output_root, self.TARGET_HOUR, "run-2")
        # 10시를 다른 내용으로 재실행해도 9시 결과는 영향받지 않아야 한다.
        write_hourly_segment_features(spark, hour_10_rerun_df, output_root, self.TARGET_HOUR, "run-3")

        rows = self.read_back(spark, result_9.output_path)
        assert [row["segment_id"] for row in rows] == ["S9"]

    def test_staging_is_cleaned_up_after_a_successful_write(self, spark, tmp_path) -> None:
        output_root = str(tmp_path / "hourly_segment_features")
        df = self.feature_rows_df(spark, [self.feature_row()])

        write_hourly_segment_features(spark, df, output_root, self.TARGET_HOUR, self.RUN_ID)

        self.assert_staging_is_empty(output_root)

    def test_rejects_rows_outside_the_target_hour(self, spark, tmp_path) -> None:
        output_root = str(tmp_path / "hourly_segment_features")
        other_hour = self.TARGET_HOUR.replace(hour=11)
        df = self.feature_rows_df(spark, [self.feature_row(target_hour=other_hour)])

        with pytest.raises(ValueError, match="target_hour"):
            write_hourly_segment_features(spark, df, output_root, self.TARGET_HOUR, self.RUN_ID)

        assert not (tmp_path / "hourly_segment_features").exists()

    @pytest.mark.parametrize("host_tz", ["UTC", "Asia/Seoul"])
    def test_write_succeeds_regardless_of_host_timezone(self, spark, tmp_path, host_tz) -> None:
        with host_timezone(host_tz):
            output_root = str(tmp_path / "hourly_segment_features")
            df = self.feature_rows_df(spark, [self.feature_row()])

            result = write_hourly_segment_features(spark, df, output_root, self.TARGET_HOUR, self.RUN_ID)

            assert result.row_count == 1
            assert result.output_path == hour_output_path(output_root, self.TARGET_HOUR)

    @pytest.mark.parametrize("host_tz", ["UTC", "Asia/Seoul"])
    def test_rejects_rows_outside_the_target_hour_regardless_of_host_timezone(
        self, spark, tmp_path, host_tz
    ) -> None:
        with host_timezone(host_tz):
            output_root = str(tmp_path / "hourly_segment_features")
            other_hour = self.TARGET_HOUR.replace(hour=11)
            df = self.feature_rows_df(spark, [self.feature_row(target_hour=other_hour)])

            with pytest.raises(ValueError, match="target_hour"):
                write_hourly_segment_features(spark, df, output_root, self.TARGET_HOUR, self.RUN_ID)

    @pytest.mark.parametrize(
        "invalid_target_hour",
        [
            TARGET_HOUR.replace(tzinfo=None),  # naive
            TARGET_HOUR.astimezone(timezone(timedelta(hours=9))),  # UTC가 아닌 offset(KST)
            TARGET_HOUR.replace(minute=30),  # 정각이 아님
        ],
        ids=["naive", "non-utc-offset", "not-truncated"],
    )
    def test_rejects_invalid_target_hour(self, spark, tmp_path, invalid_target_hour) -> None:
        output_root = str(tmp_path / "hourly_segment_features")
        df = self.feature_rows_df(spark, [self.feature_row()])

        with pytest.raises(ValueError):
            write_hourly_segment_features(spark, df, output_root, invalid_target_hour, self.RUN_ID)

    def test_hour_output_path_uses_utc_date_and_hour_regardless_of_host_timezone(self) -> None:
        with host_timezone("Asia/Seoul"):
            path = hour_output_path("out", self.TARGET_HOUR)

        assert path == "out/data_period_date=2026-08-16/hour=10"

    def test_rejects_an_empty_result_without_touching_existing_data(self, spark, tmp_path) -> None:
        output_root = str(tmp_path / "hourly_segment_features")
        original = self.feature_rows_df(spark, [self.feature_row(segment_id="ORIGINAL")])
        write_hourly_segment_features(spark, original, output_root, self.TARGET_HOUR, "run-1")

        empty = self.feature_rows_df(spark, [])
        with pytest.raises(ValueError, match="empty"):
            write_hourly_segment_features(spark, empty, output_root, self.TARGET_HOUR, "run-2")

        rows = self.read_back(spark, hour_output_path(output_root, self.TARGET_HOUR))
        assert [row["segment_id"] for row in rows] == ["ORIGINAL"]
        self.assert_staging_is_empty(output_root)

    def test_rejects_a_schema_mismatched_result(self, spark, tmp_path) -> None:
        output_root = str(tmp_path / "hourly_segment_features")
        original = self.feature_rows_df(spark, [self.feature_row(segment_id="ORIGINAL")])
        write_hourly_segment_features(spark, original, output_root, self.TARGET_HOUR, "run-1")

        malformed = self.feature_rows_df(spark, [self.feature_row(segment_id="BROKEN")]).drop(
            "feature_version"
        )
        with pytest.raises(ValueError, match="schema"):
            write_hourly_segment_features(spark, malformed, output_root, self.TARGET_HOUR, "run-2")

        rows = self.read_back(spark, hour_output_path(output_root, self.TARGET_HOUR))
        assert [row["segment_id"] for row in rows] == ["ORIGINAL"]
        self.assert_staging_is_empty(output_root)

    def test_rejects_a_result_with_duplicate_primary_keys(self, spark, tmp_path) -> None:
        output_root = str(tmp_path / "hourly_segment_features")
        original = self.feature_rows_df(spark, [self.feature_row(segment_id="ORIGINAL")])
        write_hourly_segment_features(spark, original, output_root, self.TARGET_HOUR, "run-1")

        duplicated = self.feature_rows_df(
            spark, [self.feature_row(segment_id="BROKEN"), self.feature_row(segment_id="BROKEN")]
        )
        with pytest.raises(ValueError, match="duplicate"):
            write_hourly_segment_features(spark, duplicated, output_root, self.TARGET_HOUR, "run-2")

        rows = self.read_back(spark, hour_output_path(output_root, self.TARGET_HOUR))
        assert [row["segment_id"] for row in rows] == ["ORIGINAL"]
        self.assert_staging_is_empty(output_root)

    @pytest.mark.parametrize(
        "unsafe_run_id",
        ["../escape", "a/b", "", "run id", "../../danger", "abc/def", "abc\\def"],
    )
    def test_rejects_unsafe_run_id(self, spark, tmp_path, unsafe_run_id: str) -> None:
        output_root = str(tmp_path / "hourly_segment_features")
        df = self.feature_rows_df(spark, [self.feature_row()])

        with pytest.raises(ValueError, match="run_id"):
            write_hourly_segment_features(spark, df, output_root, self.TARGET_HOUR, unsafe_run_id)

    @pytest.mark.parametrize(
        "run_id",
        [
            "manual__2026-08-18T07:00:00+00:00",
            "scheduled__2026-08-18T07:00:00+00:00",
        ],
    )
    def test_accepts_airflow_style_run_ids(self, spark, tmp_path, run_id: str) -> None:
        # Airflow의 기본 run_id 형식(manual__/scheduled__ + ISO8601 timestamp)은
        # ':'와 '+'를 포함한다 — 이 값을 그대로 run_id로 받아도 안전하게 써져야 한다.
        output_root = str(tmp_path / "hourly_segment_features")
        df = self.feature_rows_df(spark, [self.feature_row()])

        result = write_hourly_segment_features(spark, df, output_root, self.TARGET_HOUR, run_id)

        assert result.row_count == 1

    def test_recovers_from_a_stale_backup_before_writing(self, spark, tmp_path) -> None:
        output_root = str(tmp_path / "hourly_segment_features")
        original = self.feature_rows_df(spark, [self.feature_row(segment_id="ORIGINAL")])
        write_hourly_segment_features(spark, original, output_root, self.TARGET_HOUR, "run-1")

        # 직전 실행이 final -> backup 이동 직후 죽은 상태를 재현한다.
        final = Path(hour_output_path(output_root, self.TARGET_HOUR))
        backup = final.with_name(final.name + ".bak")
        shutil.move(str(final), str(backup))
        assert not final.exists()

        # 이번 쓰기는 빈 결과라 실패하지만, 복구는 쓰기 시도 이전에 이미 끝나 있어야 한다.
        # 새 결과가 우연히 ORIGINAL과 겹쳐서 통과하는 게 아니라는 걸 증명하기 위해 저장을 실패시킨다.
        empty = self.feature_rows_df(spark, [])
        with pytest.raises(ValueError, match="empty"):
            write_hourly_segment_features(spark, empty, output_root, self.TARGET_HOUR, "run-2")

        assert not backup.exists()
        rows = self.read_back(spark, str(final))
        assert [row["segment_id"] for row in rows] == ["ORIGINAL"]

    def test_backup_is_restored_when_the_final_swap_fails(self, spark, tmp_path, monkeypatch) -> None:
        output_root = str(tmp_path / "hourly_segment_features")
        original = self.feature_rows_df(spark, [self.feature_row(segment_id="ORIGINAL")])
        write_hourly_segment_features(spark, original, output_root, self.TARGET_HOUR, "run-1")

        real_move = shutil.move
        calls = {"count": 0}

        def failing_move(source, destination):
            calls["count"] += 1
            if calls["count"] == 2:  # 두 번째 move(staging -> final)만 실패시킨다
                raise OSError("simulated failure")
            return real_move(source, destination)

        monkeypatch.setattr(shutil, "move", failing_move)

        broken = self.feature_rows_df(spark, [self.feature_row(segment_id="BROKEN")])
        with pytest.raises(OSError, match="simulated failure"):
            write_hourly_segment_features(spark, broken, output_root, self.TARGET_HOUR, "run-2")

        monkeypatch.undo()
        rows = self.read_back(spark, hour_output_path(output_root, self.TARGET_HOUR))
        assert [row["segment_id"] for row in rows] == ["ORIGINAL"]
        self.assert_staging_is_empty(output_root)
