"""Environment-driven configuration for the Kafka console-sink stream."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class StreamConfig:
    """Runtime settings for `read_kafka_stream`, sourced entirely from the environment."""

    bootstrap_servers: str
    topic: str
    trigger_interval_seconds: float
    checkpoint_location: str
    starting_offsets: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> StreamConfig:
        # 테스트에서 실제 환경변수를 건드리지 않고 값을 주입할 수 있도록 Mapping을 받는다.
        # 운영 코드는 env를 생략해 os.environ을 그대로 사용한다.
        source = env if env is not None else os.environ
        return cls(
            # sensor-producer가 이미 쓰는 환경변수 이름과 동일하게 맞춰 두 서비스가
            # 별도 변환 없이 같은 Kafka 엔드포인트/토픽을 공유하게 한다.
            bootstrap_servers=source.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic=source.get("KAFKA_SENSOR_TOPIC", "sensor-events"),
            trigger_interval_seconds=float(source.get("STREAM_TRIGGER_INTERVAL_SECONDS", "5")),
            checkpoint_location=source.get(
                "STREAM_CHECKPOINT_LOCATION", "checkpoints/stream-processor"
            ),
            # 체크포인트가 없을 때만 적용되며, 체크포인트가 있으면 이 값과 무관하게
            # Spark가 체크포인트 기준으로 재개한다 (재시작 시 재개 조건 충족).
            starting_offsets=source.get("KAFKA_STARTING_OFFSETS", "earliest"),
        )
