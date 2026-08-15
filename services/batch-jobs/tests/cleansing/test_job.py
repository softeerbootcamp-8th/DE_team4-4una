import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest
from cleansing.config import CleansingJobConfig
from cleansing.job import run_cleansing_job
from pyspark.sql import SparkSession

os.environ["TZ"] = "UTC"
time.tzset()

RUN_ID = "cleansing-20260815-001"
PROCESSED_AT = datetime(2026, 8, 15, 12, 0, 0, tzinfo=UTC)

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

MALFORMED_VALUE = '{"event_id":"a1b2","trip_seq":1,"event_time":"2024-02-01T05:39'


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


def job_config(directory: Path, bronze_path: Path) -> CleansingJobConfig:
    return CleansingJobConfig.from_env(
        {
            "CLEANSING_BRONZE_INPUT_PATH": str(bronze_path),
            "CLEANSING_SILVER_OUTPUT_PATH": str(directory / "processed"),
            "CLEANSING_QUARANTINE_OUTPUT_PATH": str(directory / "quarantine"),
        }
    )


def test_job_writes_both_outputs(spark, tmp_path):
    # 한 번 실행으로 통과 행과 격리 행이 각자의 경로에 저장되는지 확인한다.
    bronze = write_bronze_parquet(
        spark,
        tmp_path,
        valid_value(),
        valid_value(event_id="other"),
        MALFORMED_VALUE,
        valid_value(event_id="too-fast", speed_mps=999.0),
    )
    config = job_config(tmp_path, bronze)

    run_cleansing_job(spark, config, RUN_ID, PROCESSED_AT)

    assert spark.read.parquet(config.processed_output_path).count() == 2
    assert spark.read.parquet(config.quarantine_output_path).count() == 2


def test_job_loses_no_row(spark, tmp_path):
    # 저장된 통과 행과 격리 행을 합치면 입력 행 수와 같은지 확인한다.
    bronze = write_bronze_parquet(
        spark, tmp_path, valid_value(), MALFORMED_VALUE, valid_value(event_id="other")
    )
    config = job_config(tmp_path, bronze)

    run_cleansing_job(spark, config, RUN_ID, PROCESSED_AT)

    processed = spark.read.parquet(config.processed_output_path).count()
    quarantined = spark.read.parquet(config.quarantine_output_path).count()
    assert processed + quarantined == 3
