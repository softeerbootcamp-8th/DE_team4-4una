import json
from datetime import UTC, datetime
from pathlib import Path

from cleansing.reader import read_bronze_sensor_events
from cleansing.rules import load_cleansing_config
from cleansing.validate import (
    DUPLICATE_EVENT,
    MISSING_REQUIRED_FIELD,
    cleanse_sensor_events,
)

RUN_ID = "cleansing-20260814-001"
REJECTED_AT = datetime(2026, 8, 14, 12, 0, 0, tzinfo=UTC)

EARLIER = "2026-08-13T10:23:24.000000+00:00"
LATER = "2026-08-13T10:23:25.000000+00:00"

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
    "_ingested_at": EARLIER,
    "_run_id": "nyc-actual-20240201-v4",
}


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
    return cleanse_sensor_events(bronze, load_cleansing_config(), RUN_ID, REJECTED_AT)


def test_only_the_latest_ingested_duplicate_survives(spark, tmp_path):
    # event_id가 같은 두 행 중 _ingested_at이 최신인 행(trip_seq=2)만 남는지 확인한다.
    path = write_bronze_parquet(
        spark,
        tmp_path,
        valid_value(_ingested_at=EARLIER, trip_seq=1),
        valid_value(_ingested_at=LATER, trip_seq=2),
    )

    result = cleanse(spark, path)

    passed = result.passed.collect()
    assert len(passed) == 1
    assert passed[0]["trip_seq"] == 2


def test_dropped_duplicate_is_quarantined_with_its_reason(spark, tmp_path):
    # 탈락한 행이 DUPLICATE_EVENT 사유로 격리되고 판정 상세에 키가 남는지 확인한다.
    path = write_bronze_parquet(
        spark, tmp_path, valid_value(_ingested_at=EARLIER), valid_value(_ingested_at=LATER)
    )

    rows = cleanse(spark, path).quarantined.collect()

    assert [row["reject_reason"] for row in rows] == [DUPLICATE_EVENT]
    assert rows[0]["reject_detail"] == f"event_id={VALID_EVENT['event_id']}"


def test_distinct_event_ids_are_all_kept(spark, tmp_path):
    # event_id가 다르면 중복이 아니므로 두 행 모두 남는다.
    path = write_bronze_parquet(
        spark, tmp_path, valid_value(), valid_value(event_id="other-event-id")
    )

    result = cleanse(spark, path)

    assert len(result.passed.collect()) == 2
    assert result.quarantined.collect() == []


def test_null_event_ids_are_not_treated_as_duplicates(spark, tmp_path):
    # event_id가 NULL인 두 행은 서로 중복이 아니라 각각 필수 컬럼 위반으로 격리된다.
    path = write_bronze_parquet(
        spark, tmp_path, valid_value(event_id=None), valid_value(event_id=None)
    )

    rows = cleanse(spark, path).quarantined.collect()

    assert [row["reject_reason"] for row in rows] == [MISSING_REQUIRED_FIELD] * 2
