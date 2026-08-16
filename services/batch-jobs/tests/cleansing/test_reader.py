import json
from pathlib import Path

from batch_jobs.schemas import (
    BRONZE_SENSOR_EVENT_SCHEMA,
    PARSE_FAILED_COLUMN,
    RAW_RECORD_COLUMN,
)
from cleansing.reader import read_bronze_sensor_events

# 실제 sensor-producer 출력에서 가져온 한 행. 컬럼이 서로 뒤바뀌는 실수가 드러나도록
# 값이 모두 다른 행을 골랐다.
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

# 중괄호와 문자열이 닫히지 않은 채 잘린 값
MALFORMED_VALUE = '{"event_id":"a1b2","trip_seq":1,"event_time":"2024-02-01T05:39'


def write_bronze_parquet(spark, directory: Path, *values: str) -> Path:
    """stream-processor가 적재하는 형태로 Parquet을 쓴다."""
    path = directory / "bronze"
    rows = [(value,) for value in values]
    spark.createDataFrame(rows, "value string").write.parquet(str(path))
    return path


def valid_value(**overrides: object) -> str:
    return json.dumps(VALID_EVENT | overrides)


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
