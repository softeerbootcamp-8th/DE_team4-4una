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
    assert config.max_offsets_per_trigger == 120000
    # Spark 기본값 1 GiB로는 복구 배치를 못 버틴다(#482).
    assert config.driver_memory == "4g"
    assert config.max_trigger_delay == "30s"
    assert config.bronze_output_partitions == 2


def test_from_env_treats_empty_values_as_unset() -> None:
    # docker compose는 호스트 .env에 값이 없으면 빈 문자열을 주입하고(#409),
    # .env.example:16-27이 STREAM_* 를 전부 빈 값으로 배포한다. 예전에는 숫자 키에서
    # int("")로 기동이 죽었다(#592).
    empty = dict.fromkeys(
        [
            "KAFKA_BOOTSTRAP_SERVERS",
            "KAFKA_SENSOR_TOPIC",
            "KAFKA_STARTING_OFFSETS",
            "STREAM_TRIGGER_INTERVAL_SECONDS",
            "STREAM_BRONZE_OUTPUT_PATH",
            "STREAM_BRONZE_CHECKPOINT_LOCATION",
            "STREAM_MIN_OFFSETS_PER_TRIGGER",
            "STREAM_MAX_TRIGGER_DELAY",
            "STREAM_MAX_OFFSETS_PER_TRIGGER",
            "STREAM_BRONZE_OUTPUT_PARTITIONS",
            "STREAM_DRIVER_MEMORY",
        ],
        "",
    )

    # 키가 아예 없을 때와 완전히 같은 설정이어야 한다.
    assert StreamConfig.from_env(empty) == StreamConfig.from_env({})


def test_explicit_zero_is_not_replaced_by_the_default() -> None:
    # `or`는 문자열 단계에서 걸리고 "0"은 truthy라 그대로 통과한다. 0은 각각
    # "트리거 하한 끄기"(로컬 스모크)와 "복구 배치 상한 없음"을 뜻하는 유효한 설정이라,
    # 빈 값 처리가 이 값을 삼키면 안 된다.
    config = StreamConfig.from_env(
        {
            "STREAM_MIN_OFFSETS_PER_TRIGGER": "0",
            "STREAM_MAX_OFFSETS_PER_TRIGGER": "0",
            "STREAM_BRONZE_OUTPUT_PARTITIONS": "0",
            "STREAM_TRIGGER_INTERVAL_SECONDS": "0",
        }
    )

    assert config.min_offsets_per_trigger == 0
    assert config.max_offsets_per_trigger == 0
    assert config.bronze_output_partitions == 0
    assert config.trigger_interval_seconds == 0.0
