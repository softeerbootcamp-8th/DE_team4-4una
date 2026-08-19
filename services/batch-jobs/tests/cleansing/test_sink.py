from datetime import UTC, datetime
from pathlib import Path

from batch_jobs.cleansing.hourly_storage import (
    quarantine_hour_path,
    write_hourly_quarantine,
)
from batch_jobs.cleansing.reader import read_bronze_sensor_events
from batch_jobs.cleansing.rules import load_cleansing_config
from batch_jobs.cleansing.sink import to_processed_sensor_events
from batch_jobs.cleansing.validate import cleanse_sensor_events
from batch_jobs.schemas import (
    PROCESSED_SENSOR_EVENT_SCHEMA,
    SENSOR_EVENT_QUARANTINE_SCHEMA,
)
from bronze_samples import MALFORMED_VALUE, valid_value, write_bronze_parquet

RUN_ID = "cleansing-20260815-001"
PROCESSED_AT = datetime(2026, 8, 15, 12, 0, 0, tzinfo=UTC)
EVENT_TIME = datetime(2024, 2, 1, 5, 39, 41, 700000, tzinfo=UTC)
TARGET_HOUR = EVENT_TIME.replace(minute=0, second=0, microsecond=0)


def cleanse(spark, path):
    bronze = read_bronze_sensor_events(spark, path)
    return cleanse_sensor_events(bronze, load_cleansing_config(), RUN_ID, PROCESSED_AT)


def typed_columns(df) -> set[tuple[str, str]]:
    return {(field.name, field.dataType.typeName()) for field in df.schema.fields}


def schema_columns(schema) -> set[tuple[str, str]]:
    return {(field.name, field.dataType.typeName()) for field in schema.fields}


def test_transform_matches_the_in_memory_feature_schema(spark, tmp_path):
    path = write_bronze_parquet(spark, tmp_path, valid_value())

    processed = to_processed_sensor_events(
        cleanse(spark, path).passed, RUN_ID, PROCESSED_AT
    )

    assert typed_columns(processed) == schema_columns(PROCESSED_SENSOR_EVENT_SCHEMA)
    assert "event_date" not in processed.columns
    assert "event_hour" not in processed.columns


def test_transform_casts_event_time_and_overwrites_lineage(spark, tmp_path):
    path = write_bronze_parquet(spark, tmp_path, valid_value())

    row = to_processed_sensor_events(
        cleanse(spark, path).passed, RUN_ID, PROCESSED_AT
    ).collect()[0]

    assert row["event_time"] == EVENT_TIME.replace(tzinfo=None)
    assert row["_run_id"] == RUN_ID
    assert row["_processed_at"] == PROCESSED_AT.replace(tzinfo=None)


def test_quarantine_is_replaced_at_the_target_hour_path(spark, tmp_path):
    bronze = write_bronze_parquet(spark, tmp_path, valid_value(), MALFORMED_VALUE)
    quarantine_root = str(tmp_path / "quarantine")
    result = cleanse(spark, bronze)

    write_hourly_quarantine(
        spark,
        result.quarantined,
        quarantine_root,
        TARGET_HOUR,
        RUN_ID,
    )

    hour_path = Path(quarantine_hour_path(quarantine_root, TARGET_HOUR))
    assert hour_path == tmp_path / "quarantine/target_date=2024-02-01/target_hour=05"
    stored = spark.read.parquet(str(hour_path))
    assert typed_columns(stored) == schema_columns(SENSOR_EVENT_QUARANTINE_SCHEMA)
    assert stored.count() == 1
