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
            "STREAM_MIN_OFFSETS_PER_TRIGGER": "600000",
            "STREAM_MAX_TRIGGER_DELAY": "2m",
            "STREAM_MAX_OFFSETS_PER_TRIGGER": "900000",
            "STREAM_BRONZE_OUTPUT_PARTITIONS": "4",
            "STREAM_DRIVER_MEMORY": "8g",
        }
    )

    assert config.bootstrap_servers == "broker:9092"
    assert config.topic == "custom-topic"
    assert config.trigger_interval_seconds == 10.0
    assert config.bronze_output_path == "/tmp/bronze"
    assert config.bronze_checkpoint_location == "/tmp/checkpoint"
    assert config.starting_offsets == "latest"
    assert config.min_offsets_per_trigger == 600000
    assert config.max_trigger_delay == "2m"
    assert config.max_offsets_per_trigger == 900000
    assert config.bronze_output_partitions == 4
    assert config.driver_memory == "8g"


def test_from_env_applies_defaults_when_missing() -> None:
    config = StreamConfig.from_env({})

    assert config.bootstrap_servers == "localhost:9092"
    assert config.topic == "sensor-events"
    assert config.trigger_interval_seconds == 30.0
    assert config.bronze_output_path == "data/local-lake/bronze/sensor-events"
    assert config.bronze_checkpoint_location == "checkpoints/bronze-sensor-events"
    assert config.starting_offsets == "earliest"
    # 기본으로 켜 둔다. 600,000건이 parquet 약 128MB다.
    assert config.min_offsets_per_trigger == 600000
    # 복구 배치 상한도 기본으로 켜 둔다 -- 하한의 2배(#482).
    assert config.max_offsets_per_trigger == 1200000
    # Spark 기본값 1 GiB로는 복구 배치를 못 버틴다(#482).
    assert config.driver_memory == "4g"
    assert config.max_trigger_delay == "30s"
    assert config.bronze_output_partitions == 2


def test_deploy_workflow_uses_the_same_streaming_defaults() -> None:
    from pathlib import Path

    repo_root = Path(__file__).resolve().parents[3]
    workflow = (repo_root / ".github/workflows/deploy-stream-processor.yml").read_text()

    assert "STREAM_TRIGGER_INTERVAL_SECONDS || '30'" in workflow
    assert "STREAM_MIN_OFFSETS_PER_TRIGGER || '600000'" in workflow
    assert "STREAM_MAX_OFFSETS_PER_TRIGGER || '1200000'" in workflow
    assert "STREAM_MAX_TRIGGER_DELAY || '30s'" in workflow
    assert "STREAM_BRONZE_OUTPUT_PARTITIONS || '2'" in workflow
