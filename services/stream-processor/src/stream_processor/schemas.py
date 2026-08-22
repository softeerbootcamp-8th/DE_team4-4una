"""Explicit schemas used at the Kafka-to-Bronze boundary."""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Kafka value의 필드와 타입을 고정해 JSON 재직렬화 시 타입 변형을 막는다.
SENSOR_EVENT_VALUE_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("vehicle_id", StringType()),
        StructField("vehicle_profile_id", IntegerType()),
        StructField("trip_id", StringType()),
        StructField("trip_seq", LongType()),
        StructField("event_time", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("speed_mps", DoubleType()),
        StructField("heading", DoubleType()),
        StructField("steering_angle", DoubleType()),
        StructField("accel_x", DoubleType()),
        StructField("accel_y", DoubleType()),
        StructField("accel_z", DoubleType()),
        StructField("jerk", DoubleType()),
        StructField("jerk_x", DoubleType()),
        StructField("jerk_y", DoubleType()),
        StructField("jerk_z", DoubleType()),
        StructField("steering_vibration", DoubleType()),
        StructField("_run_id", StringType()),
    ]
)
