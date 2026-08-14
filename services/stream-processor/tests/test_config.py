from stream_processor.config import StreamConfig


def test_from_env_uses_provided_values() -> None:
    config = StreamConfig.from_env(
        {
            "KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
            "KAFKA_SENSOR_TOPIC": "custom-topic",
            "STREAM_TRIGGER_INTERVAL_SECONDS": "10",
            "STREAM_BRONZE_OUTPUT_PATH": "/tmp/bronze",
            "STREAM_BRONZE_CHECKPOINT_LOCATION": "/tmp/checkpoint",
            "KAFKA_STARTING_OFFSETS": "latest",
        }
    )

    assert config.bootstrap_servers == "broker:9092"
    assert config.topic == "custom-topic"
    assert config.trigger_interval_seconds == 10.0
    assert config.bronze_output_path == "/tmp/bronze"
    assert config.bronze_checkpoint_location == "/tmp/checkpoint"
    assert config.starting_offsets == "latest"


def test_from_env_applies_defaults_when_missing() -> None:
    config = StreamConfig.from_env({})

    assert config.bootstrap_servers == "localhost:9092"
    assert config.topic == "sensor-events"
    assert config.trigger_interval_seconds == 5.0
    assert config.bronze_output_path == "data/local-lake/bronze/sensor-events"
    assert config.bronze_checkpoint_location == "checkpoints/bronze-sensor-events"
    assert config.starting_offsets == "earliest"
