"""Spark schemas for the Bronze-to-Silver sensor-event cleansing job."""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# JSON 파싱 자체가 실패한 행을 담는 Spark PERMISSIVE 모드 전용 컬럼명
CORRUPT_RECORD_COLUMN = "_corrupt_record"

# event_time은 포맷 불일치 시 값이 조용히 NULL되는 것을 막기 위해 STRING으로 선언, 캐스팅은 Silver에서 수행
BRONZE_SENSOR_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("vehicle_profile_id", IntegerType(), nullable=False),
        StructField("trip_id", StringType(), nullable=False),
        StructField("trip_seq", LongType(), nullable=False),
        StructField("event_time", StringType(), nullable=False),
        StructField("latitude", DoubleType(), nullable=False),
        StructField("longitude", DoubleType(), nullable=False),
        StructField("speed_mps", DoubleType(), nullable=False),
        StructField("heading", DoubleType(), nullable=True),
        StructField("accel_x", DoubleType(), nullable=True),
        StructField("accel_y", DoubleType(), nullable=True),
        StructField("accel_z", DoubleType(), nullable=False),
        StructField("jerk_x", DoubleType(), nullable=True),
        StructField("jerk_y", DoubleType(), nullable=True),
        StructField("jerk_z", DoubleType(), nullable=True),
        StructField("steering_vibration", DoubleType(), nullable=True),
        StructField("steering_angle", DoubleType(), nullable=True),
        StructField("_run_id", StringType(), nullable=False),
        StructField(CORRUPT_RECORD_COLUMN, StringType(), nullable=True),
    ]
)

# jerk_x/y/z는 이 클렌징 단계에서 재계산하지 않고 Bronze 값을 그대로 통과시킨다
PROCESSED_SENSOR_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("vehicle_profile_id", IntegerType(), nullable=False),
        StructField("trip_id", StringType(), nullable=False),
        StructField("trip_seq", LongType(), nullable=False),
        StructField("event_time", TimestampType(), nullable=False),
        StructField("event_date", DateType(), nullable=False),
        StructField("latitude", DoubleType(), nullable=False),
        StructField("longitude", DoubleType(), nullable=False),
        StructField("speed_mps", DoubleType(), nullable=False),
        StructField("heading", DoubleType(), nullable=True),
        StructField("accel_x", DoubleType(), nullable=True),
        StructField("accel_y", DoubleType(), nullable=True),
        StructField("accel_z", DoubleType(), nullable=False),
        StructField("jerk_x", DoubleType(), nullable=True),
        StructField("jerk_y", DoubleType(), nullable=True),
        StructField("jerk_z", DoubleType(), nullable=True),
        StructField("steering_vibration", DoubleType(), nullable=True),
        StructField("steering_angle", DoubleType(), nullable=True),
        StructField("_processed_at", TimestampType(), nullable=False),
        StructField("_run_id", StringType(), nullable=False),
    ]
)

# event_id/trip_id/event_date는 파싱 실패 시 추출 자체가 안 될 수 있어 nullable, event_date로 파티셔닝
SENSOR_EVENT_QUARANTINE_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=True),
        StructField("trip_id", StringType(), nullable=True),
        StructField("event_date", DateType(), nullable=True),
        StructField("reject_reason", StringType(), nullable=False),
        StructField("reject_detail", StringType(), nullable=True),
        StructField("raw_record", StringType(), nullable=False),
        StructField("_run_id", StringType(), nullable=False),
        StructField("_rejected_at", TimestampType(), nullable=False),
    ]
)
