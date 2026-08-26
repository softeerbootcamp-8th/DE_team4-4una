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
from pyspark import StorageLevel

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


def test_quarantine_input_is_cached_for_the_write_then_released(spark, tmp_path, monkeypatch):
    """쓰기 전 count()와 write가 같은 계보를 각각 걷지 않아야 한다(#529).

    캐시가 없으면 write 질의가 입력을 처음부터 다시 만든다. 2026-08-26T04:00 실행
    실측에서 격리가 70행뿐인데도 격리 쓰기가 37.4초였고, 그중 26초가 parquet
    write(8.3초) 이전 스테이지였다 — 비용이 데이터가 아니라 재계산에서 나왔다.
    """
    from batch_jobs.cleansing import hourly_storage

    bronze = write_bronze_parquet(spark, tmp_path, valid_value(), MALFORMED_VALUE)
    quarantined = cleanse(spark, bronze).quarantined
    original_stage = hourly_storage._stage_quarantine
    seen: dict[str, object] = {}

    def spy(spark_session, frame, *args, **kwargs):
        seen["storage_level"] = frame.storageLevel
        return original_stage(spark_session, frame, *args, **kwargs)

    monkeypatch.setattr(hourly_storage, "_stage_quarantine", spy)

    result = write_hourly_quarantine(
        spark, quarantined, str(tmp_path / "quarantine"), TARGET_HOUR, RUN_ID
    )

    assert result.row_count == 1
    # write가 캐시를 보고 들어가야 계보를 다시 걷지 않는다.
    storage_level = seen["storage_level"]
    assert storage_level.useMemory or storage_level.useDisk
    # 이 캐시는 쓰기 한 번을 위한 것이라 함수를 벗어나면 남지 않는다.
    assert quarantined.storageLevel == StorageLevel.NONE


def test_caller_owned_cache_is_not_dropped_by_the_write(spark, tmp_path):
    """호출자가 이미 캐시한 프레임을 넘기면 그 캐시를 건드리지 않는다.

    DataFrame.persist()는 새 객체가 아니라 같은 객체를 돌려주므로, 여기서 무조건
    unpersist하면 호출자의 캐시가 사라진다.
    """
    bronze = write_bronze_parquet(spark, tmp_path, valid_value(), MALFORMED_VALUE)
    quarantined = cleanse(spark, bronze).quarantined.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        write_hourly_quarantine(
            spark, quarantined, str(tmp_path / "quarantine"), TARGET_HOUR, RUN_ID
        )

        assert quarantined.storageLevel != StorageLevel.NONE
    finally:
        quarantined.unpersist()


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
