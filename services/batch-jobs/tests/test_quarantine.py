import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest
from batch_jobs.bronze_reader import read_bronze_sensor_events
from batch_jobs.cleansing_config import load_cleansing_config
from batch_jobs.quarantine import (
    MALFORMED_JSON,
    MISSING_REQUIRED_FIELD,
    split_required_field_failures,
)
from batch_jobs.schemas import SENSOR_EVENT_QUARANTINE_SCHEMA
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
    "_run_id": "nyc-actual-20240201-v4",
}

# 중괄호와 문자열이 닫히지 않은 채 잘린 줄
MALFORMED_LINE = '{"event_id":"a1b2","trip_seq":1,"event_time":"2024-02-01T05:39'


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


def write_jsonl(directory: Path, *lines: str) -> Path:
    path = directory / "sensor_event.jsonl"
    path.write_text("\n".join(lines) + "\n")
    return path


def valid_line(**overrides: object) -> str:
    return json.dumps(VALID_EVENT | overrides)


def split(spark, path):
    bronze = read_bronze_sensor_events(spark, path)
    return split_required_field_failures(bronze, load_cleansing_config(), RUN_ID, REJECTED_AT)


def test_parse_failure_is_quarantined_with_its_reason(spark, tmp_path):
    # 잘린 줄이 MALFORMED_JSON 사유로 격리되고 원본 문자열이 그대로 담기는지 확인한다.
    path = write_jsonl(tmp_path, valid_line(), MALFORMED_LINE)

    result = split(spark, path)

    rows = result.quarantined.collect()
    assert [row["reject_reason"] for row in rows] == [MALFORMED_JSON]
    assert rows[0]["raw_record"] == MALFORMED_LINE
    assert result.passed.count() == 1


def test_null_required_column_is_quarantined_with_its_reason(spark, tmp_path):
    # 필수 컬럼이 NULL인 행이 MISSING_REQUIRED_FIELD 사유로 격리되는지 확인한다.
    path = write_jsonl(tmp_path, valid_line(), valid_line(event_id=None))

    result = split(spark, path)

    rows = result.quarantined.collect()
    assert [row["reject_reason"] for row in rows] == [MISSING_REQUIRED_FIELD]
    assert result.passed.count() == 1


def test_reject_detail_names_the_violating_columns(spark, tmp_path):
    # 판정 상세에 NULL이었던 컬럼명이 모두 들어가는지 확인한다.
    path = write_jsonl(tmp_path, valid_line(event_id=None, speed_mps=None))

    rows = split(spark, path).quarantined.collect()

    assert rows[0]["reject_detail"] == "event_id, speed_mps"


def test_no_row_is_lost_and_quarantine_matches_its_schema(spark, tmp_path):
    # 통과 행과 격리 행을 합치면 입력 행 수와 같고, 격리 행이 스키마와 일치하는지 확인한다.
    path = write_jsonl(
        tmp_path, valid_line(), MALFORMED_LINE, valid_line(trip_id=None), valid_line(trip_seq=48)
    )

    result = split(spark, path)

    assert len(result.passed.collect()) == 2
    assert len(result.quarantined.collect()) == 2
    assert result.quarantined.columns == [
        field.name for field in SENSOR_EVENT_QUARANTINE_SCHEMA.fields
    ]
