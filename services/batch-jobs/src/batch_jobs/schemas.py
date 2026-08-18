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

# 적재된 원본 JSON 문자열을 담는 컬럼명
RAW_RECORD_COLUMN = "_raw_record"

# 원본 JSON의 파싱 실패 여부를 담는 컬럼명
PARSE_FAILED_COLUMN = "_parse_failed"

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
        StructField("_ingested_at", TimestampType(), nullable=False),
        StructField("_run_id", StringType(), nullable=False),
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

# event_date/event_hour는 Silver1 디렉터리에서 복원되는 파티션 컬럼이다.
# event_hour는 저장 직전에 생성되므로 위 논리 행 스키마에는 없고, 두 컬럼 모두
# 개별 Parquet 파일의 물리 스키마에서는 제외된다.
PROCESSED_SENSOR_EVENT_FILE_SCHEMA = StructType(
    [
        field
        for field in PROCESSED_SENSOR_EVENT_SCHEMA.fields
        if field.name != "event_date"
    ]
)

# event_id/trip_id/event_date는 파싱 실패 시 추출 자체가 안 될 수 있어 nullable,
# 격리 시각에서 파생한 rejected_date로 파티셔닝
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
        StructField("rejected_date", DateType(), nullable=False),
    ]
)

HOURLY_SEGMENT_FEATURE_SCHEMA = StructType(
    [
        StructField("segment_id", StringType(), nullable=False),
        StructField("vehicle_profile_id", IntegerType(), nullable=False),
        StructField("data_period_start", TimestampType(), nullable=False),
        StructField("data_period_end", TimestampType(), nullable=False),
        StructField("road_snapshot_date", DateType(), nullable=False),
        StructField("avg_speed_mps", DoubleType(), nullable=True),
        StructField("rms_accel_x", DoubleType(), nullable=True),
        StructField("rms_accel_y", DoubleType(), nullable=True),
        StructField("rms_accel_z", DoubleType(), nullable=True),
        StructField("p95_abs_accel_x", DoubleType(), nullable=True),
        StructField("p95_abs_accel_y", DoubleType(), nullable=True),
        StructField("p95_abs_accel_z", DoubleType(), nullable=True),
        StructField("rms_jerk_x", DoubleType(), nullable=True),
        StructField("rms_jerk_y", DoubleType(), nullable=True),
        StructField("rms_jerk_z", DoubleType(), nullable=True),
        StructField("p95_abs_jerk_x", DoubleType(), nullable=True),
        StructField("p95_abs_jerk_y", DoubleType(), nullable=True),
        StructField("p95_abs_jerk_z", DoubleType(), nullable=True),
        StructField("hard_brake_count", IntegerType(), nullable=False),
        StructField("hard_accel_count", IntegerType(), nullable=False),
        StructField("sharp_steer_count", IntegerType(), nullable=False),
        StructField("steer_reversal_count", IntegerType(), nullable=False),
        StructField("rms_steering_rate", DoubleType(), nullable=True),
        StructField("rms_steering_vibration", DoubleType(), nullable=True),
        StructField("sample_count", LongType(), nullable=False),
        StructField("trip_count", LongType(), nullable=False),
        StructField("feature_version", StringType(), nullable=False),
        StructField("_processed_at", TimestampType(), nullable=False),
        StructField("_run_id", StringType(), nullable=False),
    ]
)

HOURLY_COMFORT_SCORE_SCHEMA = StructType(
    [
        *HOURLY_SEGMENT_FEATURE_SCHEMA.fields[:5],
        StructField("vertical_score", DoubleType(), nullable=False),
        StructField("longitudinal_score", DoubleType(), nullable=False),
        StructField("lateral_score", DoubleType(), nullable=False),
        StructField("scoring_version", StringType(), nullable=False),
        StructField("sample_count", LongType(), nullable=False),
        StructField("trip_count", LongType(), nullable=False),
        StructField("_run_id", StringType(), nullable=False),
        StructField("_processed_at", TimestampType(), nullable=False),
    ]
)
