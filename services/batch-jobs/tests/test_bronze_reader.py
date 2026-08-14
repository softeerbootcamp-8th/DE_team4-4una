import json
import os
import time
from pathlib import Path

import pytest
from batch_jobs.bronze_reader import read_bronze_sensor_events
from batch_jobs.schemas import BRONZE_SENSOR_EVENT_SCHEMA, CORRUPT_RECORD_COLUMN
from pyspark.sql import SparkSession

# collect()가 돌려주는 timestamp는 이 파이썬 프로세스의 로컬 타임존으로 변환되어,
# 고정하지 않으면 실행 머신마다(Asia/Seoul vs UTC) 값이 달라진다.
os.environ["TZ"] = "UTC"
time.tzset()

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
    "_run_id": "nyc-actual-20240201-v4",
}

# 중괄호와 문자열이 닫히지 않은 채 잘린 줄
MALFORMED_LINE = '{"event_id":"a1b2","trip_seq":1,"event_time":"2024-02-01T05:39'


@pytest.fixture(scope="session")
def spark():
    # 세션 전체에서 재사용: SparkSession 기동에 몇 초가 걸린다.
    session = (
        SparkSession.builder.appName("batch-jobs-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def write_jsonl(directory: Path, *lines: str) -> Path:
    path = directory / "sensor_event.jsonl"
    path.write_text("\n".join(lines) + "\n")
    return path


def valid_line(**overrides: object) -> str:
    return json.dumps(VALID_EVENT | overrides)


def test_reads_every_row_with_the_declared_columns(spark, tmp_path):
    # 입력 두 줄이 두 행으로 읽히고, 컬럼이 선언한 스키마와 정확히 같은지 확인한다.
    path = write_jsonl(tmp_path, valid_line(trip_seq=47), valid_line(trip_seq=48))

    df = read_bronze_sensor_events(spark, path)

    assert df.count() == 2
    assert df.columns == [field.name for field in BRONZE_SENSOR_EVENT_SCHEMA.fields]


def test_malformed_line_does_not_raise_and_keeps_the_row(spark, tmp_path):
    # 깨진 줄이 섞여도 예외 없이 읽히고, 그 줄도 버려지지 않고 행으로 남는지 확인한다.
    # read()는 계획만 세우므로 collect()까지 해야 파싱이 실제로 실행된다.
    path = write_jsonl(tmp_path, valid_line(), MALFORMED_LINE, valid_line(trip_seq=48))

    rows = read_bronze_sensor_events(spark, path).collect()

    assert len(rows) == 3


def test_malformed_line_is_preserved_in_the_corrupt_record_column(spark, tmp_path):
    # 깨진 줄의 원본 문자열이 corrupt record 컬럼에 그대로 보존되는지 확인한다.
    path = write_jsonl(tmp_path, valid_line(), MALFORMED_LINE)

    rows = read_bronze_sensor_events(spark, path).collect()

    corrupt = [row for row in rows if row[CORRUPT_RECORD_COLUMN] is not None]
    assert len(corrupt) == 1
    assert corrupt[0][CORRUPT_RECORD_COLUMN] == MALFORMED_LINE
