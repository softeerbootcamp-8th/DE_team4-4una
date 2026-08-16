import os
import shutil
import time
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from batch_jobs.hourly_segment_feature_storage import (
    hour_output_path,
    write_hourly_segment_features,
)
from batch_jobs.schemas import HOURLY_SEGMENT_FEATURE_SCHEMA
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType

# collect()가 돌려주는 timestamp는 이 파이썬 프로세스의 로컬 타임존으로 변환되어,
# 고정하지 않으면 실행 머신마다(Asia/Seoul vs UTC) 값이 달라진다.
os.environ["TZ"] = "UTC"
time.tzset()

SNAPSHOT = date(2026, 8, 13)
TARGET_HOUR = datetime(2026, 8, 16, 10, 0, 0, tzinfo=UTC)
RUN_ID = "run-1"


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("batch-jobs-tests")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def feature_row(
    target_hour: datetime = TARGET_HOUR, segment_id: str = "S1", **overrides: object
) -> dict[str, object]:
    row = {
        "segment_id": segment_id,
        "vehicle_profile_id": 1,
        "data_period_start": target_hour.replace(tzinfo=None),
        "data_period_end": target_hour.replace(tzinfo=None, hour=(target_hour.hour + 1) % 24),
        "road_snapshot_date": SNAPSHOT,
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
        "_processed_at": TARGET_HOUR,
        "_run_id": RUN_ID,
    }
    row.update(overrides)
    return row


def feature_rows_df(spark, rows: list[dict[str, object]]):
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


def read_back(spark, path: str):
    return spark.read.parquet(path).collect()


def test_hour_output_path_is_isolated_per_hour() -> None:
    path_10 = hour_output_path("out", TARGET_HOUR)
    path_11 = hour_output_path("out", TARGET_HOUR.replace(hour=11))

    assert path_10 == "out/data_period_date=2026-08-16/hour=10"
    assert path_10 != path_11


def test_write_creates_data_at_the_expected_path(spark, tmp_path) -> None:
    output_root = str(tmp_path / "hourly_segment_features")
    df = feature_rows_df(spark, [feature_row(), feature_row(segment_id="S2")])

    result = write_hourly_segment_features(spark, df, output_root, TARGET_HOUR, RUN_ID)

    assert result.output_path == hour_output_path(output_root, TARGET_HOUR)
    assert result.row_count == 2
    assert len(read_back(spark, result.output_path)) == 2


def test_rerunning_same_hour_replaces_data(spark, tmp_path) -> None:
    output_root = str(tmp_path / "hourly_segment_features")
    first = feature_rows_df(spark, [feature_row(segment_id="S1")])
    second = feature_rows_df(spark, [feature_row(segment_id="S2"), feature_row(segment_id="S3")])

    write_hourly_segment_features(spark, first, output_root, TARGET_HOUR, "run-1")
    result = write_hourly_segment_features(spark, second, output_root, TARGET_HOUR, "run-2")

    rows = read_back(spark, result.output_path)
    assert {row["segment_id"] for row in rows} == {"S2", "S3"}


def test_other_hours_are_not_touched(spark, tmp_path) -> None:
    output_root = str(tmp_path / "hourly_segment_features")
    hour_10 = feature_rows_df(spark, [feature_row(target_hour=TARGET_HOUR, segment_id="S1")])
    hour_11 = feature_rows_df(
        spark, [feature_row(target_hour=TARGET_HOUR.replace(hour=11), segment_id="S2")]
    )

    result_10 = write_hourly_segment_features(spark, hour_10, output_root, TARGET_HOUR, "run-1")
    write_hourly_segment_features(
        spark, hour_11, output_root, TARGET_HOUR.replace(hour=11), "run-2"
    )

    rows = read_back(spark, result_10.output_path)
    assert [row["segment_id"] for row in rows] == ["S1"]


def test_staging_is_cleaned_up_after_a_successful_write(spark, tmp_path) -> None:
    output_root = str(tmp_path / "hourly_segment_features")
    df = feature_rows_df(spark, [feature_row()])

    write_hourly_segment_features(spark, df, output_root, TARGET_HOUR, RUN_ID)

    staging_root = tmp_path / "hourly_segment_features" / "_staging"
    assert not staging_root.exists() or not any(staging_root.iterdir())


def test_rejects_rows_outside_the_target_hour(spark, tmp_path) -> None:
    output_root = str(tmp_path / "hourly_segment_features")
    other_hour = TARGET_HOUR.replace(hour=11)
    df = feature_rows_df(spark, [feature_row(target_hour=other_hour)])

    with pytest.raises(ValueError, match="target_hour"):
        write_hourly_segment_features(spark, df, output_root, TARGET_HOUR, RUN_ID)

    assert not (tmp_path / "hourly_segment_features").exists()


def test_rejects_an_empty_result_without_touching_existing_data(spark, tmp_path) -> None:
    output_root = str(tmp_path / "hourly_segment_features")
    original = feature_rows_df(spark, [feature_row(segment_id="ORIGINAL")])
    write_hourly_segment_features(spark, original, output_root, TARGET_HOUR, "run-1")

    empty = feature_rows_df(spark, [])
    with pytest.raises(ValueError, match="empty"):
        write_hourly_segment_features(spark, empty, output_root, TARGET_HOUR, "run-2")

    rows = read_back(spark, hour_output_path(output_root, TARGET_HOUR))
    assert [row["segment_id"] for row in rows] == ["ORIGINAL"]


@pytest.mark.parametrize("unsafe_run_id", ["../escape", "a/b", "", "run id"])
def test_rejects_unsafe_run_id(spark, tmp_path, unsafe_run_id: str) -> None:
    output_root = str(tmp_path / "hourly_segment_features")
    df = feature_rows_df(spark, [feature_row()])

    with pytest.raises(ValueError, match="run_id"):
        write_hourly_segment_features(spark, df, output_root, TARGET_HOUR, unsafe_run_id)


def test_recovers_from_a_stale_backup_before_writing(spark, tmp_path) -> None:
    output_root = str(tmp_path / "hourly_segment_features")
    original = feature_rows_df(spark, [feature_row(segment_id="ORIGINAL")])
    write_hourly_segment_features(spark, original, output_root, TARGET_HOUR, "run-1")

    # 직전 실행이 final -> backup 이동 직후 죽은 상태를 재현한다.
    final = Path(hour_output_path(output_root, TARGET_HOUR))
    backup = final.with_name(final.name + ".bak")
    shutil.move(str(final), str(backup))
    assert not final.exists()

    updated = feature_rows_df(spark, [feature_row(segment_id="UPDATED")])
    result = write_hourly_segment_features(spark, updated, output_root, TARGET_HOUR, "run-2")

    assert not backup.exists()
    rows = read_back(spark, result.output_path)
    assert [row["segment_id"] for row in rows] == ["UPDATED"]


def test_backup_is_restored_when_the_final_swap_fails(spark, tmp_path, monkeypatch) -> None:
    output_root = str(tmp_path / "hourly_segment_features")
    original = feature_rows_df(spark, [feature_row(segment_id="ORIGINAL")])
    write_hourly_segment_features(spark, original, output_root, TARGET_HOUR, "run-1")

    real_move = shutil.move
    calls = {"count": 0}

    def failing_move(source, destination):
        calls["count"] += 1
        if calls["count"] == 2:  # 두 번째 move(staging -> final)만 실패시킨다
            raise OSError("simulated failure")
        return real_move(source, destination)

    monkeypatch.setattr(shutil, "move", failing_move)

    broken = feature_rows_df(spark, [feature_row(segment_id="BROKEN")])
    with pytest.raises(OSError, match="simulated failure"):
        write_hourly_segment_features(spark, broken, output_root, TARGET_HOUR, "run-2")

    monkeypatch.undo()
    rows = read_back(spark, hour_output_path(output_root, TARGET_HOUR))
    assert [row["segment_id"] for row in rows] == ["ORIGINAL"]
