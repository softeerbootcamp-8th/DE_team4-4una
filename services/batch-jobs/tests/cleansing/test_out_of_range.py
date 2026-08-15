import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest
from cleansing.reader import read_bronze_sensor_events
from cleansing.rules import load_cleansing_config
from cleansing.validate import OUT_OF_RANGE, split_out_of_range_values
from pyspark.sql import SparkSession

os.environ["TZ"] = "UTC"
time.tzset()

RUN_ID = "cleansing-20260814-001"
REJECTED_AT = datetime(2026, 8, 14, 12, 0, 0, tzinfo=UTC)

VALID_EVENT = {
    "event_id": "555be813-1030-53a5-883c-8503913fef75",
    "vehicle_profile_id": 1,
    "trip_id": "992f18800c372eda4baadecf",
    "trip_seq": 47,
    "event_time": "2024-02-01T05:39:41.700000+00:00",
    "latitude": 40.67435479381055,
    "longitude": -73.97047625909869,
    "speed_mps": 3.596217902773873,
    "heading": 207.39041389728519,
    "accel_x": 0.9379198195238754,
    "accel_y": -2.9267790433598155,
    "accel_z": -0.0144224106250461,
    "jerk_x": -0.0722557532695145,
    "jerk_y": -29.267792529966073,
    "jerk_z": 0.416818166875222,
    "steering_vibration": 0.2797693197309389,
    "steering_angle": -35.0,
    "_ingested_at": "2026-08-13T10:23:24.730637+00:00",
    "_run_id": "nyc-actual-20240201-v4",
}


@pytest.fixture(scope="session")
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


def write_bronze_parquet(spark, directory: Path, *values: str) -> Path:
    """stream-processor가 적재하는 형태로 Parquet을 쓴다."""
    path = directory / "bronze"
    rows = [(value,) for value in values]
    spark.createDataFrame(rows, "value string").write.parquet(str(path))
    return path


def valid_value(**overrides: object) -> str:
    return json.dumps(VALID_EVENT | overrides)


def split(spark, path):
    bronze = read_bronze_sensor_events(spark, path)
    return split_out_of_range_values(bronze, load_cleansing_config(), RUN_ID, REJECTED_AT)


def test_value_above_its_maximum_is_quarantined(spark, tmp_path):
    # 상한을 넘은 속도가 OUT_OF_RANGE 사유로 격리되는지 확인한다.
    path = write_bronze_parquet(spark, tmp_path, valid_value(), valid_value(speed_mps=99.0))

    result = split(spark, path)

    assert [row["reject_reason"] for row in result.quarantined.collect()] == [OUT_OF_RANGE]
    assert len(result.passed.collect()) == 1


def test_negative_direction_values_are_not_quarantined(spark, tmp_path):
    # 가속도, jerk, 조향각, 경도의 음수는 방향을 담은 정상 값이라 격리되지 않는다.
    path = write_bronze_parquet(
        spark,
        tmp_path,
        valid_value(
            accel_x=-29.9, accel_y=-2.9, accel_z=-0.01, jerk_x=-299.0, steering_angle=-35.0
        ),
    )

    result = split(spark, path)

    assert result.quarantined.collect() == []
    assert len(result.passed.collect()) == 1


def test_zero_speed_is_not_quarantined(spark, tmp_path):
    # 정차 상태의 속도 0은 하한과 같은 값이라 격리되지 않는다.
    path = write_bronze_parquet(spark, tmp_path, valid_value(speed_mps=0.0))

    assert split(spark, path).quarantined.collect() == []


def test_null_optional_value_is_not_quarantined(spark, tmp_path):
    # 필수가 아닌 컬럼이 비어 있는 것은 범위 위반이 아니다.
    path = write_bronze_parquet(spark, tmp_path, valid_value(heading=None, accel_x=None))

    assert split(spark, path).quarantined.collect() == []


def test_reject_detail_names_the_violating_columns_and_values(spark, tmp_path):
    # 판정 상세에 위반한 컬럼과 실제 값이 설정 순서대로 들어가는지 확인한다.
    path = write_bronze_parquet(spark, tmp_path, valid_value(latitude=10.0, speed_mps=99.0))

    rows = split(spark, path).quarantined.collect()

    assert rows[0]["reject_detail"] == "latitude=10.0, speed_mps=99.0"
