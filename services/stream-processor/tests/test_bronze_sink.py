import json
from dataclasses import fields
from datetime import UTC, datetime

from de4_core.sensor import SensorEvent
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from stream_processor.bronze_sink import (
    add_ingestion_time_to_value,
    write_bronze_stream,
)
from stream_processor.config import StreamConfig
from stream_processor.schemas import SENSOR_EVENT_VALUE_SCHEMA

KAFKA_RECORD_SCHEMA = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
    ]
)

SENSOR_VALUE = {
    "event_id": "event-1",
    "vehicle_id": "vehicle-1",
    "vehicle_profile_id": 1,
    "trip_id": "trip-1",
    "trip_seq": 0,
    "event_time": "2026-08-14T05:00:00+00:00",
    "latitude": 40.75,
    "longitude": -73.98,
    "speed_mps": 8.5,
    "heading": 90.0,
    "steering_angle": 1.5,
    "accel_x": 0.1,
    "accel_y": 0.2,
    "accel_z": 0.3,
    "jerk": 0.4,
    "jerk_x": 0.4,
    "jerk_y": 0.5,
    "jerk_z": 0.6,
    "steering_vibration": 0.7,
    "_run_id": "run-1",
}


def stream_config(output_path: str, checkpoint_path: str) -> StreamConfig:
    return StreamConfig(
        bootstrap_servers="localhost:9092",
        topic="sensor-events",
        trigger_interval_seconds=0.1,
        bronze_output_path=output_path,
        bronze_checkpoint_location=checkpoint_path,
        starting_offsets="earliest",
        min_offsets_per_trigger=0,
        max_trigger_delay="5m",
        bronze_output_partitions=1,
    )


def test_value_schema_tracks_shared_sensor_contract() -> None:
    assert [field.name for field in SENSOR_EVENT_VALUE_SCHEMA] == [
        field.name for field in fields(SensorEvent)
    ]


def test_add_ingestion_time_inside_sensor_value(spark) -> None:
    loaded_at = datetime(2026, 8, 14, 5, 7, tzinfo=UTC)
    records = spark.createDataFrame([Row(key="trip-1", value=json.dumps(SENSOR_VALUE))])

    bronze_records = add_ingestion_time_to_value(records, F.lit(loaded_at))
    result = bronze_records.collect()[0]
    value = json.loads(result.value)

    assert bronze_records.columns == ["key", "value"]
    assert {key: value[key] for key in SENSOR_VALUE} == SENSOR_VALUE
    assert datetime.fromisoformat(value["_ingested_at"]) == loaded_at


def test_malformed_value_is_preserved_for_later_quarantine(spark) -> None:
    records = spark.createDataFrame([Row(value='{"event_id":')])

    result = add_ingestion_time_to_value(records).collect()[0]

    assert result.value == '{"event_id":'


def test_parquet_sink_resumes_from_checkpoint_without_rewriting_input(spark, tmp_path) -> None:
    input_path = tmp_path / "input"
    output_path = tmp_path / "bronze"
    checkpoint_path = tmp_path / "checkpoint"
    input_path.mkdir()
    config = stream_config(str(output_path), str(checkpoint_path))

    def write_input(filename: str, offset: int) -> None:
        record = {
            "key": "trip-1",
            "value": json.dumps(SENSOR_VALUE | {"event_id": f"event-{offset}", "trip_seq": offset}),
            "topic": "sensor-events",
            "partition": 0,
            "offset": offset,
            "timestamp": "2026-08-14T05:00:00Z",
        }
        (input_path / filename).write_text(json.dumps(record))

    def start_query():
        records = spark.readStream.schema(KAFKA_RECORD_SCHEMA).json(str(input_path))
        return write_bronze_stream(records, config)

    write_input("batch-1.json", 1)
    first_query = start_query()
    first_query.processAllAvailable()
    first_query.stop()

    write_input("batch-2.json", 2)
    restarted_query = start_query()
    restarted_query.processAllAvailable()
    restarted_query.stop()

    rows = spark.read.parquet(str(output_path)).orderBy("offset").collect()
    assert [row.offset for row in rows] == [1, 2]
    assert [row.key for row in rows] == ["trip-1", "trip-1"]
    assert [row.topic for row in rows] == ["sensor-events", "sensor-events"]
    assert [row.partition for row in rows] == [0, 0]
    assert [json.loads(row.value)["event_id"] for row in rows] == ["event-1", "event-2"]
    assert all(json.loads(row.value)["_ingested_at"] for row in rows)
    assert list(rows[0].asDict()) == [
        "key",
        "value",
        "topic",
        "partition",
        "offset",
        "timestamp",
    ]


def test_one_micro_batch_writes_one_file_per_output_partition(spark, tmp_path) -> None:
    input_path = tmp_path / "input"
    output_path = tmp_path / "bronze"
    input_path.mkdir()
    config = stream_config(str(output_path), str(tmp_path / "checkpoint"))

    for offset in range(1, 6):
        record = {
            "key": "trip-1",
            "value": json.dumps(SENSOR_VALUE | {"event_id": f"event-{offset}"}),
            "topic": "sensor-events",
            "partition": offset,
            "offset": offset,
            "timestamp": "2026-08-14T05:00:00Z",
        }
        (input_path / f"batch-{offset}.json").write_text(json.dumps(record))

    # 작은 파일은 기본 설정에서 한 partition으로 묶여버려서 coalesce가 있으나 없으나
    # 결과가 같아진다. 파일 하나가 partition 하나가 되도록 강제해야 검증이 성립한다.
    # 가장 큰 파일보다 1바이트 큰 값이면 파일 하나는 담기고 둘은 안 담긴다.
    largest_input = max(path.stat().st_size for path in input_path.glob("*.json"))
    previous_max_bytes = spark.conf.get("spark.sql.files.maxPartitionBytes")
    spark.conf.set("spark.sql.files.maxPartitionBytes", str(largest_input + 1))
    spark.conf.set("spark.sql.files.openCostInBytes", "0")
    try:
        source = spark.read.schema(KAFKA_RECORD_SCHEMA).json(str(input_path))
        # coalesce가 없으면 이 partition 수만큼(=5개) 파일이 쏟아진다는 뜻이다.
        assert source.rdd.getNumPartitions() == 5

        records = spark.readStream.schema(KAFKA_RECORD_SCHEMA).json(str(input_path))
        query = write_bronze_stream(records, config)
        query.processAllAvailable()
        query.stop()
    finally:
        spark.conf.set("spark.sql.files.maxPartitionBytes", previous_max_bytes)
        spark.conf.unset("spark.sql.files.openCostInBytes")

    assert config.bronze_output_partitions == 1
    assert len(list(output_path.glob("*.parquet"))) == 1
    assert spark.read.parquet(str(output_path)).count() == 5
