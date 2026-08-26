from datetime import UTC, datetime, timedelta
from pathlib import Path

from batch_jobs.cleansing.reader import (
    SOURCE_TIMESTAMP_COLUMN,
    filter_bronze_sensor_events_for_hour,
    read_bronze_sensor_events,
)
from batch_jobs.schemas import (
    BRONZE_SENSOR_EVENT_SCHEMA,
    PARSE_FAILED_COLUMN,
    RAW_RECORD_COLUMN,
)
from bronze_samples import (
    BRONZE_TIMESTAMP,
    MALFORMED_VALUE,
    VALID_EVENT,
    valid_value,
    write_bronze_parquet,
)


def _write_partition(spark, root: Path, target_hour: datetime, *values: str) -> None:
    """stream-processor의 event_date=/hour= 파티션 레이아웃을 그대로 재현한다."""
    path = f"{root}/event_date={target_hour.date().isoformat()}/hour={target_hour.hour:02d}"
    rows = [(value, BRONZE_TIMESTAMP) for value in values]
    (
        spark.createDataFrame(rows, "value string, timestamp timestamp")
        .write.mode("overwrite")
        .parquet(path)
    )


def test_reads_every_row_with_the_declared_columns(spark, tmp_path):
    # 입력 두 행이 두 행으로 읽히고, 스키마 컬럼에 원본과 파싱 실패 컬럼이 붙는지 확인한다.
    path = write_bronze_parquet(
        spark, tmp_path, valid_value(trip_seq=47), valid_value(trip_seq=48)
    )

    df = read_bronze_sensor_events(spark, path, _target_hour())

    assert df.count() == 2
    assert df.columns == [
        *[field.name for field in BRONZE_SENSOR_EVENT_SCHEMA.fields],
        RAW_RECORD_COLUMN,
        PARSE_FAILED_COLUMN,
        SOURCE_TIMESTAMP_COLUMN,
    ]


def test_malformed_value_does_not_raise_and_keeps_the_row(spark, tmp_path):
    # 깨진 값이 섞여도 예외 없이 읽히고, 그 행이 버려지지 않는지 확인한다.
    path = write_bronze_parquet(
        spark, tmp_path, valid_value(), MALFORMED_VALUE, valid_value(trip_seq=48)
    )

    rows = read_bronze_sensor_events(spark, path, _target_hour()).collect()

    assert len(rows) == 3


def test_malformed_value_is_flagged_and_kept_as_raw_record(spark, tmp_path):
    # 깨진 값이 파싱 실패로 표시되고 원본 문자열이 그대로 남는지 확인한다.
    path = write_bronze_parquet(spark, tmp_path, valid_value(), MALFORMED_VALUE)

    rows = read_bronze_sensor_events(spark, path, _target_hour()).collect()

    failed = [row for row in rows if row[PARSE_FAILED_COLUMN]]
    assert len(failed) == 1
    assert failed[0][RAW_RECORD_COLUMN] == MALFORMED_VALUE


def test_parsed_row_keeps_its_original_value_as_raw_record(spark, tmp_path):
    # 파싱에 성공한 행도 적재된 원본 문자열을 그대로 들고 있는지 확인한다.
    value = valid_value()
    path = write_bronze_parquet(spark, tmp_path, value)

    row = read_bronze_sensor_events(spark, path, _target_hour()).collect()[0]

    assert row[PARSE_FAILED_COLUMN] is False
    assert row[RAW_RECORD_COLUMN] == value
    assert row["event_id"] == VALID_EVENT["event_id"]


def test_filters_valid_and_malformed_rows_to_one_target_hour(spark, tmp_path):
    path = write_bronze_parquet(
        spark,
        tmp_path,
        valid_value(event_id="target"),
        valid_value(event_id="other", event_time="2024-02-01T06:00:00+00:00"),
        MALFORMED_VALUE,
    )
    bronze = read_bronze_sensor_events(spark, path, _target_hour())

    result = filter_bronze_sensor_events_for_hour(
        bronze, BRONZE_TIMESTAMP.replace(minute=0, second=0, microsecond=0)
    ).collect()

    assert {row["event_id"] for row in result} == {"target", None}


def test_target_hour_reads_only_the_matching_partition_directory(spark, tmp_path):
    root = tmp_path / "bronze"
    hour_5 = datetime(2024, 2, 1, 5, tzinfo=UTC)
    hour_6 = datetime(2024, 2, 1, 6, tzinfo=UTC)
    _write_partition(spark, root, hour_5, valid_value(event_id="in-hour-5"))
    _write_partition(spark, root, hour_6, valid_value(event_id="in-hour-6"))

    rows = read_bronze_sensor_events(spark, root, hour_5).collect()

    assert {row["event_id"] for row in rows} == {"in-hour-5"}


def test_target_hour_partition_is_read_even_if_event_time_disagrees(spark, tmp_path):
    # 파티션 배정과 event_time이 어긋나는 경우(늦은 도착 등)에도 pruning 단계는
    # 파티션 위치만으로 읽는다. 실제 시간 필터링은 이후 in-memory 필터가 맡는다.
    root = tmp_path / "bronze"
    hour_5 = datetime(2024, 2, 1, 5, tzinfo=UTC)
    mismatched = valid_value(event_id="mismatched", event_time="2024-02-01T09:00:00+00:00")
    _write_partition(spark, root, hour_5, mismatched)

    rows = read_bronze_sensor_events(spark, root, hour_5).collect()

    assert {row["event_id"] for row in rows} == {"mismatched"}


def test_target_hour_returns_empty_when_partition_directory_is_missing(spark, tmp_path):
    root = tmp_path / "bronze"
    hour_5 = datetime(2024, 2, 1, 5, tzinfo=UTC)
    hour_6 = hour_5 + timedelta(hours=1)
    _write_partition(spark, root, hour_6, valid_value())

    result = read_bronze_sensor_events(spark, root, hour_5)

    assert result.count() == 0
    assert result.columns == read_bronze_sensor_events(spark, root, hour_6).columns


def _target_hour() -> datetime:
    return BRONZE_TIMESTAMP.replace(minute=0, second=0, microsecond=0)
