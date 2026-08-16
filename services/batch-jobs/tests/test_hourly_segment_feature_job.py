import json
import os
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
import shapely
from batch_jobs.cli import build_parser, run_hourly_segment_feature_building
from batch_jobs.hourly_segment_feature_job import (
    HourlySegmentFeatureJobConfig,
    HourlySegmentFeatureJobSummary,
    run_hourly_segment_feature_job,
)
from batch_jobs.hourly_segment_feature_storage import hour_output_path
from batch_jobs.schemas import (
    HOURLY_SEGMENT_FEATURE_SCHEMA,
    PROCESSED_SENSOR_EVENT_SCHEMA,
)
from pyproj import Transformer
from pyspark.sql import SparkSession
from shapely.geometry import LineString

# collect()가 돌려주는 timestamp는 이 파이썬 프로세스의 로컬 타임존으로 변환되어,
# 고정하지 않으면 실행 머신마다(Asia/Seoul vs UTC) 값이 달라진다.
os.environ["TZ"] = "UTC"
time.tzset()

SNAPSHOT = date(2026, 8, 13)
TARGET_HOUR = datetime(2026, 8, 16, 10, 0, 0, tzinfo=UTC)
PROCESSED_AT = datetime(2026, 8, 16, 12, 0, 0, tzinfo=UTC)
FEATURE_VERSION = "v1"
RUN_ID = "run-1"

BASE_LAT, BASE_LON = 40.7484, -73.9857
_TRANSFORMER = Transformer.from_crs("EPSG:4326", "EPSG:32118", always_xy=True)
_BASE_X, _BASE_Y = _TRANSFORMER.transform(BASE_LON, BASE_LAT)

ROAD_SEGMENT_COLUMNS = (
    "segment_id",
    "snapshot_date",
    "geometry_wkb",
    "traffic_direction",
    "from_node_id",
    "to_node_id",
)


@pytest.fixture(scope="module")
def spark():
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


def write_road_segment(spark, tmp_path) -> str:
    # 모든 센서 포인트가 이 하나의 도로(양방향)와 정확히 같은 좌표라 항상 매칭된다.
    line = LineString([(_BASE_X, _BASE_Y - 50.0), (_BASE_X, _BASE_Y + 50.0)])
    row = ("S1", SNAPSHOT, shapely.to_wkb(line), "T", "N1", "N2")
    path = str(tmp_path / "road_segment")
    spark.createDataFrame([row], ROAD_SEGMENT_COLUMNS).write.mode("overwrite").parquet(
        f"{path}/snapshot_date={SNAPSHOT.isoformat()}/data.parquet"
    )
    return path


def sensor_row(
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
        event_time.date(),
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
        PROCESSED_AT,
        RUN_ID,
    )


def write_sensor_events(spark, tmp_path, rows: list[tuple]) -> str:
    path = str(tmp_path / "processed_sensor_event")
    spark.createDataFrame(rows, PROCESSED_SENSOR_EVENT_SCHEMA).write.mode("overwrite").parquet(
        path
    )
    return path


def build_config(spark, tmp_path, rows: list[tuple]) -> HourlySegmentFeatureJobConfig:
    sensor_path = write_sensor_events(spark, tmp_path, rows)
    road_segment_path = write_road_segment(spark, tmp_path)
    return HourlySegmentFeatureJobConfig.from_env(
        {
            "HOURLY_SEGMENT_FEATURE_INPUT_PATH": sensor_path,
            "HOURLY_SEGMENT_FEATURE_ROAD_SEGMENT_PATH": road_segment_path,
            "HOURLY_SEGMENT_FEATURE_OUTPUT_PATH": str(tmp_path / "hourly_segment_features"),
        }
    )


def run_job(spark, tmp_path, rows: list[tuple]):
    config = build_config(spark, tmp_path, rows)
    summary = run_hourly_segment_feature_job(
        spark, config, TARGET_HOUR, SNAPSHOT, FEATURE_VERSION, RUN_ID, PROCESSED_AT
    )
    return summary, spark.read.parquet(summary.output_path).collect()


def test_matches_and_aggregates_one_target_hour(spark, tmp_path) -> None:
    rows = [
        sensor_row(TARGET_HOUR + timedelta(seconds=0), "e1", trip_seq=0),
        sensor_row(TARGET_HOUR + timedelta(seconds=1), "e2", trip_seq=1),
        sensor_row(TARGET_HOUR + timedelta(seconds=2), "e3", trip_seq=2),
    ]

    summary, result = run_job(spark, tmp_path, rows)

    assert summary.result_count == 1
    assert summary.target_hour == TARGET_HOUR
    assert summary.run_id == RUN_ID
    assert len(result) == 1
    row = result[0]
    assert row["segment_id"] == "S1"
    assert row["road_snapshot_date"] == SNAPSHOT
    assert row["data_period_start"] == TARGET_HOUR.replace(tzinfo=None)
    assert row["sample_count"] == 3
    assert row["trip_count"] == 1
    assert row["feature_version"] == FEATURE_VERSION
    assert row["_run_id"] == RUN_ID

    stored_schema = spark.read.parquet(summary.output_path).schema
    actual_types = {field.name: field.dataType for field in stored_schema.fields}
    assert stored_schema.fieldNames() == HOURLY_SEGMENT_FEATURE_SCHEMA.fieldNames()
    assert all(
        actual_types[field.name] == field.dataType
        for field in HOURLY_SEGMENT_FEATURE_SCHEMA.fields
    )


def test_episode_starting_before_target_hour_is_not_recounted(spark, tmp_path) -> None:
    rows = [
        # 09:59:59.7에 급제동이 시작해 대상 시간(10시) 안까지 이어짐
        sensor_row(TARGET_HOUR - timedelta(milliseconds=300), "e0", trip_seq=0, accel_x=-5.0),
        sensor_row(TARGET_HOUR + timedelta(milliseconds=100), "e1", trip_seq=1, accel_x=-5.0),
    ]

    _, result = run_job(spark, tmp_path, rows)

    assert len(result) == 1
    assert result[0]["hard_brake_count"] == 0  # 시작 행은 09시에 속하므로 10시엔 없어야 함


def test_lookback_rows_are_excluded_from_the_final_result(spark, tmp_path) -> None:
    rows = [
        # target_hour 0.2초 전: lookback 범위 안이라 읽히지만 최종 집계에는 포함되면 안 됨
        sensor_row(TARGET_HOUR - timedelta(milliseconds=200), "lookback", trip_id="LOOKBACK"),
        sensor_row(TARGET_HOUR + timedelta(seconds=0), "e1", trip_seq=0),
        sensor_row(TARGET_HOUR + timedelta(seconds=1), "e2", trip_seq=1),
    ]

    _, result = run_job(spark, tmp_path, rows)

    assert len(result) == 1  # lookback 행이 별도의 이전 시간 그룹을 만들지 않는다
    assert result[0]["data_period_start"] == TARGET_HOUR.replace(tzinfo=None)
    assert result[0]["sample_count"] == 2


def test_episode_spanning_the_hour_boundary_is_counted_via_lookahead(spark, tmp_path) -> None:
    rows = [
        sensor_row(TARGET_HOUR + timedelta(seconds=30), "e0", trip_seq=0, accel_x=0.0),
        # 급제동이 대상 시간 끝나기 0.2초 전에 시작해 다음 시간으로 0.1초 더 이어짐
        sensor_row(
            TARGET_HOUR + timedelta(minutes=59, seconds=59, milliseconds=800),
            "e1",
            trip_seq=1,
            accel_x=-5.0,
        ),
        sensor_row(
            TARGET_HOUR + timedelta(hours=1, milliseconds=100),
            "e2",
            trip_seq=2,
            accel_x=-5.0,
        ),
    ]

    _, result = run_job(spark, tmp_path, rows)

    assert len(result) == 1
    assert result[0]["hard_brake_count"] == 1
    # 11시의 e2는 Episode 판단(lookahead)에는 쓰이지만 최종 sample_count에는 들어가면 안 된다.
    assert result[0]["sample_count"] == 2


def test_unmatched_events_are_excluded_from_the_result(spark, tmp_path) -> None:
    rows = [
        sensor_row(TARGET_HOUR + timedelta(seconds=0), "e1", trip_seq=0),
        # 도로에서 멀리 떨어진 위치라 어떤 Segment와도 매칭되지 않는다
        sensor_row(
            TARGET_HOUR + timedelta(seconds=1),
            "e2",
            trip_seq=1,
            latitude=BASE_LAT + 1.0,
            longitude=BASE_LON,
        ),
    ]

    _, result = run_job(spark, tmp_path, rows)

    assert len(result) == 1
    assert result[0]["segment_id"] == "S1"
    assert result[0]["sample_count"] == 1


def test_rerunning_the_same_hour_replaces_the_stored_result(spark, tmp_path) -> None:
    config = build_config(spark, tmp_path, [sensor_row(TARGET_HOUR, "e1")])

    first = run_hourly_segment_feature_job(
        spark, config, TARGET_HOUR, SNAPSHOT, FEATURE_VERSION, "run-1", PROCESSED_AT
    )
    rows = [
        sensor_row(TARGET_HOUR, "e2"),
        sensor_row(TARGET_HOUR + timedelta(seconds=1), "e3", vehicle_profile_id=2),
    ]
    config = build_config(spark, tmp_path, rows)
    second = run_hourly_segment_feature_job(
        spark, config, TARGET_HOUR, SNAPSHOT, FEATURE_VERSION, "run-2", PROCESSED_AT
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
    spark, tmp_path, target_hour, feature_version, run_id, processed_at
) -> None:
    config = build_config(spark, tmp_path, [sensor_row(TARGET_HOUR, "e1")])

    with pytest.raises(ValueError):
        run_hourly_segment_feature_job(
            spark, config, target_hour, SNAPSHOT, feature_version, run_id, processed_at
        )


def cli_args(sensor_path: str, road_segment_path: str, run_id: str, output_path: str | None):
    arguments = [
        "build-hourly-segment-features",
        "--target-hour",
        TARGET_HOUR.isoformat(),
        "--road-snapshot-date",
        SNAPSHOT.isoformat(),
        "--feature-version",
        FEATURE_VERSION,
        "--run-id",
        run_id,
        "--input-path",
        sensor_path,
        "--road-segment-path",
        road_segment_path,
    ]
    if output_path is not None:
        arguments += ["--output-path", output_path]
    return build_parser().parse_args(arguments)


def test_cli_passes_output_path_argument_to_the_job(spark, tmp_path, monkeypatch) -> None:
    sensor_path = write_sensor_events(spark, tmp_path, [sensor_row(TARGET_HOUR, "e1")])
    road_segment_path = write_road_segment(spark, tmp_path)
    output_path = str(tmp_path / "cli_output")
    monkeypatch.setattr(spark, "stop", lambda: None)

    args = cli_args(sensor_path, road_segment_path, "cli-run-1", output_path)
    run_hourly_segment_feature_building(args)

    assert Path(hour_output_path(output_path, TARGET_HOUR)).exists()


def test_cli_falls_back_to_env_output_path_when_not_given(
    spark, tmp_path, monkeypatch
) -> None:
    sensor_path = write_sensor_events(spark, tmp_path, [sensor_row(TARGET_HOUR, "e1")])
    road_segment_path = write_road_segment(spark, tmp_path)
    env_output_path = str(tmp_path / "env_output")
    monkeypatch.setenv("HOURLY_SEGMENT_FEATURE_OUTPUT_PATH", env_output_path)
    monkeypatch.setattr(spark, "stop", lambda: None)

    # --output-path를 주지 않으면 환경변수 기본값을 써야 한다.
    args = cli_args(sensor_path, road_segment_path, "cli-run-2", output_path=None)
    run_hourly_segment_feature_building(args)

    assert Path(hour_output_path(env_output_path, TARGET_HOUR)).exists()


def test_cli_prints_the_job_summary_as_json_without_extra_spark_actions(
    spark, tmp_path, monkeypatch, capsys
) -> None:
    summary = HourlySegmentFeatureJobSummary(
        result_count=3, output_path="/fake/path", target_hour=TARGET_HOUR, run_id="cli-run-3"
    )
    calls = []

    def fake_run_job(*args, **kwargs):
        calls.append((args, kwargs))
        return summary

    monkeypatch.setattr(
        "batch_jobs.hourly_segment_feature_job.run_hourly_segment_feature_job", fake_run_job
    )
    monkeypatch.setattr(spark, "stop", lambda: None)

    args = cli_args("unused", "unused", "cli-run-3", output_path="unused")
    run_hourly_segment_feature_building(args)

    # Job이 요약을 반환한 뒤 CLI가 result.count() 같은 추가 Spark Action 없이 그대로 출력하는지 확인한다.
    assert len(calls) == 1
    printed = json.loads(capsys.readouterr().out)
    assert printed == {"result_count": 3, "output_path": "/fake/path", "run_id": "cli-run-3"}
