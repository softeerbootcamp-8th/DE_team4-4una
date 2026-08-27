"""Environment-driven configuration for the Kafka-to-Bronze stream."""

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
    bronze_output_path: str
    bronze_checkpoint_location: str
    starting_offsets: str
    min_offsets_per_trigger: int
    max_trigger_delay: str
    max_offsets_per_trigger: int
    bronze_output_partitions: int
    driver_memory: str

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
            trigger_interval_seconds=float(source.get("STREAM_TRIGGER_INTERVAL_SECONDS", "30")),
            bronze_output_path=source.get(
                "STREAM_BRONZE_OUTPUT_PATH", "data/local-lake/bronze/sensor-events"
            ),
            bronze_checkpoint_location=source.get(
                "STREAM_BRONZE_CHECKPOINT_LOCATION",
                "checkpoints/bronze-sensor-events",
            ),
            # 체크포인트가 없을 때만 적용되며, 체크포인트가 있으면 이 값과 무관하게
            # Spark가 체크포인트 기준으로 재개한다 (재시작 시 재개 조건 충족).
            starting_offsets=source.get("KAFKA_STARTING_OFFSETS", "earliest"),
            # Kafka에 이만큼 쌓일 때까지 micro-batch를 미룬다(partition별이 아니라 전체 합계).
            # 기본 600,000건은 parquet 약 128MB다 — 우리 Bronze 실측으로 디스크에서
            # 행당 223B. 데이터가 적은 로컬 스모크에서는 0으로 꺼야 max_trigger_delay를
            # 기다리지 않는다.
            min_offsets_per_trigger=int(
                source.get("STREAM_MIN_OFFSETS_PER_TRIGGER", "600000")
            ),
            # min_offsets_per_trigger만 걸면 트래픽이 적을 때 스트림이 아무것도 쓰지 않는다.
            # 양이 모자라도 이 시간이 지나면 배치를 실행시키는 상한이다.
            max_trigger_delay=source.get("STREAM_MAX_TRIGGER_DELAY", "30s"),
            # 복구 배치 상한(#482). min_offsets_per_trigger는 하한이라, 상한이 없으면
            # 장애 후 재시작할 때 첫 micro-batch가 그동안 Kafka에 쌓인 offset을 전부
            # 소비하지 않도록 범위를 나눈다. 30초 canary에서는 1,200,000건으로
            # batch duration과 복구 지연을 함께 제한한다. 0이면 상한 없음(이전 동작).
            max_offsets_per_trigger=int(
                source.get("STREAM_MAX_OFFSETS_PER_TRIGGER", "1200000")
            ),
            # 2 vCPU 실행 환경에서는 write task를 두 개로 두면 coalesce(1)의 단일
            # S3 writer 병목을 피하면서 파일 수가 과도하게 늘지 않는다.
            bronze_output_partitions=int(source.get("STREAM_BRONZE_OUTPUT_PARTITIONS", "2")),
            # Spark 기본 driver heap은 1 GiB다(로컬에서 직접 확인). local[*]에서는
            # driver가 곧 executor이므로 micro-batch 전체가 이 heap 안에서 파싱된다 --
            # 위 상한과 짝이다(#482).
            driver_memory=source.get("STREAM_DRIVER_MEMORY", "4g"),
        )
