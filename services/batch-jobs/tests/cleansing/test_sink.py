from datetime import UTC, date, datetime
from pathlib import Path

from batch_jobs.cleansing.reader import read_bronze_sensor_events
from batch_jobs.cleansing.rules import load_cleansing_config
from batch_jobs.cleansing.sink import to_processed_sensor_events
from batch_jobs.cleansing.hourly_storage import (
    PROCESSED_SENSOR_EVENT_FILE_SCHEMA,
    PROCESSED_SENSOR_EVENT_PARTITIONED_SCHEMA,
    processed_hour_path,
    quarantine_hour_path,
    write_hourly_cleansing_results,
)
from batch_jobs.cleansing.validate import cleanse_sensor_events
from batch_jobs.schemas import SENSOR_EVENT_QUARANTINE_SCHEMA
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


def test_transform_matches_the_silver_schema(spark, tmp_path):
    # 변환 결과의 컬럼과 타입이 Silver 스키마와 같은지 확인한다.
    path = write_bronze_parquet(spark, tmp_path, valid_value())

    silver = to_processed_sensor_events(cleanse(spark, path).passed, RUN_ID, PROCESSED_AT)

    assert typed_columns(silver) == schema_columns(
        PROCESSED_SENSOR_EVENT_PARTITIONED_SCHEMA
    )


def test_transform_casts_event_time_and_overwrites_run_id(spark, tmp_path):
    # event_time 캐스팅, event_date 파생, ETL 실행 식별자 덮어쓰기를 확인한다.
    path = write_bronze_parquet(spark, tmp_path, valid_value())

    row = to_processed_sensor_events(
        cleanse(spark, path).passed, RUN_ID, PROCESSED_AT
    ).collect()[0]

    assert row["event_time"] == EVENT_TIME.replace(tzinfo=None)
    assert row["event_date"] == date(2024, 2, 1)
    assert row["event_hour"] == 5
    assert row["_run_id"] == RUN_ID
    assert row["_processed_at"] == PROCESSED_AT.replace(tzinfo=None)


def test_partitioned_silver_separates_file_and_dataset_schemas(spark, tmp_path):
    bronze = write_bronze_parquet(
        spark,
        tmp_path,
        valid_value(),
    )
    silver_root = str(tmp_path / "silver")
    quarantine_root = str(tmp_path / "quarantine")
    result = cleanse(spark, bronze)

    silver = to_processed_sensor_events(result.passed, RUN_ID, PROCESSED_AT)
    write_hourly_cleansing_results(
        spark,
        silver,
        result.quarantined,
        silver_root,
        quarantine_root,
        TARGET_HOUR,
        RUN_ID,
    )

    hour_path = Path(processed_hour_path(silver_root, TARGET_HOUR))
    assert hour_path == tmp_path / "silver/event_date=2024-02-01/event_hour=05"
    parquet_files = sorted(hour_path.glob("part-*.parquet"))
    assert parquet_files

    stored = spark.read.parquet(*(str(path) for path in parquet_files))
    assert typed_columns(stored) == schema_columns(PROCESSED_SENSOR_EVENT_FILE_SCHEMA)

    restored = spark.read.parquet(silver_root)
    assert typed_columns(restored) == schema_columns(
        PROCESSED_SENSOR_EVENT_PARTITIONED_SCHEMA
    )
    assert restored.first()["event_hour"] == 5


def test_quarantine_is_replaced_at_the_target_hour_path(spark, tmp_path):
    bronze = write_bronze_parquet(spark, tmp_path, valid_value(), MALFORMED_VALUE)
    silver_root = str(tmp_path / "silver")
    quarantine_root = str(tmp_path / "quarantine")
    result = cleanse(spark, bronze)
    silver = to_processed_sensor_events(result.passed, RUN_ID, PROCESSED_AT)

    write_hourly_cleansing_results(
        spark,
        silver,
        result.quarantined,
        silver_root,
        quarantine_root,
        TARGET_HOUR,
        RUN_ID,
    )

    hour_path = Path(quarantine_hour_path(quarantine_root, TARGET_HOUR))
    assert hour_path == tmp_path / "quarantine/target_date=2024-02-01/target_hour=05"
    stored = spark.read.parquet(str(hour_path))
    assert typed_columns(stored) == schema_columns(SENSOR_EVENT_QUARANTINE_SCHEMA)
    assert stored.count() == 1
