from datetime import UTC, datetime

from pyspark.sql import Row
from stream_processor.config import StreamConfig
from stream_processor.kafka_source import kafka_read_options, select_sensor_columns


def stream_config(**overrides: str) -> StreamConfig:
    return StreamConfig.from_env(
        {"KAFKA_BOOTSTRAP_SERVERS": "broker:9092", "KAFKA_SENSOR_TOPIC": "sensor-events"}
        | overrides
    )


def test_kafka_read_options_omits_batching_when_disabled() -> None:
    options = kafka_read_options(stream_config())

    assert options == {
        "kafka.bootstrap.servers": "broker:9092",
        "subscribe": "sensor-events",
        "startingOffsets": "earliest",
    }


def test_kafka_read_options_pairs_min_offsets_with_max_trigger_delay() -> None:
    options = kafka_read_options(
        stream_config(STREAM_MIN_OFFSETS_PER_TRIGGER="600000", STREAM_MAX_TRIGGER_DELAY="2m")
    )

    # 둘은 반드시 함께 걸려야 한다. minOffsetsPerTrigger만 있으면 한산할 때 배치가 멈춘다.
    assert options["minOffsetsPerTrigger"] == "600000"
    assert options["maxTriggerDelay"] == "2m"


def raw_kafka_row() -> Row:
    # Kafka 소스가 실제로 내려주는 컬럼 모양(key/value는 binary)을 그대로 흉내낸다.
    return Row(
        key=b"trip-1",
        value=b'{"trip_seq": 0}',
        topic="sensor-events",
        partition=0,
        offset=42,
        timestamp=datetime(2024, 2, 1, tzinfo=UTC),
        timestampType=0,
    )


def test_select_sensor_columns_decodes_key_and_value_to_utf8(spark) -> None:
    raw = spark.createDataFrame([raw_kafka_row()])

    result = select_sensor_columns(raw)

    row = result.collect()[0]
    assert row.key == "trip-1"
    assert row.value == '{"trip_seq": 0}'
    assert row.topic == "sensor-events"
    assert row.partition == 0
    assert row.offset == 42
    # conftest가 프로세스 타임존을 UTC로 고정했기 때문에 collect() 결과는 naive UTC 값으로 온다.
    assert row.timestamp == datetime(2024, 2, 1)  # noqa: DTZ001


def test_select_sensor_columns_output_schema_matches_expected_columns(spark) -> None:
    raw = spark.createDataFrame([raw_kafka_row()])

    result = select_sensor_columns(raw)

    assert result.columns == ["key", "value", "topic", "partition", "offset", "timestamp"]
