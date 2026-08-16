"""Shared Bronze input samples and helpers for the cleansing tests."""

import json
from pathlib import Path

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
