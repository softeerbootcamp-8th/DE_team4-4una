import json
import os
import time
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from batch_jobs.schemas import (
    PROCESSED_SENSOR_EVENT_SCHEMA,
    SENSOR_EVENT_QUARANTINE_SCHEMA,
)
from cleansing.config import CleansingJobConfig
from cleansing.reader import read_bronze_sensor_events
from cleansing.rules import load_cleansing_config
from cleansing.sink import (
    to_processed_sensor_events,
    write_processed_sensor_events,
    write_quarantined_events,
)
from cleansing.validate import cleanse_sensor_events
from pyspark.sql import SparkSession

os.environ["TZ"] = "UTC"
time.tzset()

CONFIG = CleansingJobConfig.from_env({})
RUN_ID = "cleansing-20260815-001"
PROCESSED_AT = datetime(2026, 8, 15, 12, 0, 0, tzinfo=UTC)
EVENT_TIME = datetime(2024, 2, 1, 5, 39, 41, 700000, tzinfo=UTC)

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


def cleanse(spark, path):
    bronze = read_bronze_sensor_events(spark, path)
    return cleanse_sensor_events(bronze, load_cleansing_config(), RUN_ID, PROCESSED_AT)


def typed_columns(df) -> set[tuple[str, str]]:
    return {(field.name, field.dataType.typeName()) for field in df.schema.fields}


def schema_columns(schema) -> set[tuple[str, str]]:
    return {(field.name, field.dataType.typeName()) for field in schema.fields}


def partition_dirs(path: Path, column: str) -> list[str]:
    return sorted(entry.name for entry in path.iterdir() if entry.name.startswith(f"{column}="))


def test_transform_matches_the_silver_schema(spark, tmp_path):
    # 변환 결과의 컬럼과 타입이 Silver 스키마와 같은지 확인한다.
    path = write_bronze_parquet(spark, tmp_path, valid_value())

    silver = to_processed_sensor_events(cleanse(spark, path).passed, RUN_ID, PROCESSED_AT)

    assert typed_columns(silver) == schema_columns(PROCESSED_SENSOR_EVENT_SCHEMA)


def test_transform_casts_event_time_and_overwrites_run_id(spark, tmp_path):
    # event_time 캐스팅, event_date 파생, ETL 실행 식별자 덮어쓰기를 확인한다.
    path = write_bronze_parquet(spark, tmp_path, valid_value())

    row = to_processed_sensor_events(
        cleanse(spark, path).passed, RUN_ID, PROCESSED_AT
    ).collect()[0]

    assert row["event_time"] == EVENT_TIME.replace(tzinfo=None)
    assert row["event_date"] == date(2024, 2, 1)
    assert row["_run_id"] == RUN_ID
    assert row["_processed_at"] == PROCESSED_AT.replace(tzinfo=None)


def test_written_silver_is_split_by_event_date(spark, tmp_path):
    # 저장된 통과 행이 event_date 디렉터리로 나뉘고 스키마와 일치하는지 확인한다.
    bronze = write_bronze_parquet(
        spark,
        tmp_path,
        valid_value(),
        valid_value(event_id="other", event_time="2024-02-02T01:00:00+00:00"),
    )
    silver_path = tmp_path / "silver"

    silver = to_processed_sensor_events(cleanse(spark, bronze).passed, RUN_ID, PROCESSED_AT)
    write_processed_sensor_events(silver, silver_path, CONFIG.processed_partition_column)

    assert partition_dirs(silver_path, "event_date") == [
        "event_date=2024-02-01",
        "event_date=2024-02-02",
    ]
    assert typed_columns(spark.read.parquet(str(silver_path))) == schema_columns(
        PROCESSED_SENSOR_EVENT_SCHEMA
    )


def test_written_quarantine_is_split_by_rejected_date(spark, tmp_path):
    # 저장된 격리 행이 rejected_date 디렉터리로 나뉘고 스키마와 일치하는지 확인한다.
    bronze = write_bronze_parquet(spark, tmp_path, valid_value(), MALFORMED_VALUE)
    quarantine_path = tmp_path / "quarantine"

    write_quarantined_events(
        cleanse(spark, bronze).quarantined, quarantine_path, CONFIG.quarantine_partition_column
    )

    assert partition_dirs(quarantine_path, "rejected_date") == ["rejected_date=2026-08-15"]
    assert typed_columns(spark.read.parquet(str(quarantine_path))) == schema_columns(
        SENSOR_EVENT_QUARANTINE_SCHEMA
    )
