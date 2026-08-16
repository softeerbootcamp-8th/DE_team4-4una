from batch_jobs.cleansing.reader import read_bronze_sensor_events
from batch_jobs.schemas import (
    BRONZE_SENSOR_EVENT_SCHEMA,
    PARSE_FAILED_COLUMN,
    RAW_RECORD_COLUMN,
)
from bronze_samples import (
    MALFORMED_VALUE,
    VALID_EVENT,
    valid_value,
    write_bronze_parquet,
)


def test_reads_every_row_with_the_declared_columns(spark, tmp_path):
    # 입력 두 행이 두 행으로 읽히고, 스키마 컬럼에 원본과 파싱 실패 컬럼이 붙는지 확인한다.
    path = write_bronze_parquet(
        spark, tmp_path, valid_value(trip_seq=47), valid_value(trip_seq=48)
    )

    df = read_bronze_sensor_events(spark, path)

    assert df.count() == 2
    assert df.columns == [
        *[field.name for field in BRONZE_SENSOR_EVENT_SCHEMA.fields],
        RAW_RECORD_COLUMN,
        PARSE_FAILED_COLUMN,
    ]


def test_malformed_value_does_not_raise_and_keeps_the_row(spark, tmp_path):
    # 깨진 값이 섞여도 예외 없이 읽히고, 그 행이 버려지지 않는지 확인한다.
    path = write_bronze_parquet(
        spark, tmp_path, valid_value(), MALFORMED_VALUE, valid_value(trip_seq=48)
    )

    rows = read_bronze_sensor_events(spark, path).collect()

    assert len(rows) == 3


def test_malformed_value_is_flagged_and_kept_as_raw_record(spark, tmp_path):
    # 깨진 값이 파싱 실패로 표시되고 원본 문자열이 그대로 남는지 확인한다.
    path = write_bronze_parquet(spark, tmp_path, valid_value(), MALFORMED_VALUE)

    rows = read_bronze_sensor_events(spark, path).collect()

    failed = [row for row in rows if row[PARSE_FAILED_COLUMN]]
    assert len(failed) == 1
    assert failed[0][RAW_RECORD_COLUMN] == MALFORMED_VALUE


def test_parsed_row_keeps_its_original_value_as_raw_record(spark, tmp_path):
    # 파싱에 성공한 행도 적재된 원본 문자열을 그대로 들고 있는지 확인한다.
    value = valid_value()
    path = write_bronze_parquet(spark, tmp_path, value)

    row = read_bronze_sensor_events(spark, path).collect()[0]

    assert row[PARSE_FAILED_COLUMN] is False
    assert row[RAW_RECORD_COLUMN] == value
    assert row["event_id"] == VALID_EVENT["event_id"]
