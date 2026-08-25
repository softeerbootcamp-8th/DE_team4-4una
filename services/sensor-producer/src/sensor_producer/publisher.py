"""Kafka and test publishers for sensor-event messages."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Protocol

import orjson
from de4_core import SensorEvent


class EventPublisher(Protocol):
    def publish(self, event: SensorEvent) -> None: ...

    def flush(self) -> None: ...


class MemoryPublisher:
    def __init__(self) -> None:
        self.events: list[SensorEvent] = []

    def publish(self, event: SensorEvent) -> None:
        self.events.append(event)

    def flush(self) -> None:
        return


class JsonlPublisher:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._stream = path.open("wb")

    def publish(self, event: SensorEvent) -> None:
        self._stream.write(event.to_json() + b"\n")

    def flush(self) -> None:
        self._stream.flush()
        self._stream.close()


class KafkaPublisher:
    def __init__(
        self,
        bootstrap_servers: Iterable[str],
        topic: str,
    ):
        from confluent_kafka import Producer

        self.topic = topic
        self._delivery_errors: list[str] = []
        self._producer = Producer(
            {
                "bootstrap.servers": ",".join(bootstrap_servers),
                "acks": "all",
                "enable.idempotence": True,
                "linger.ms": 20,
                "batch.size": 131_072,
                # Kafka는 개별 레코드가 아니라 record batch 단위로 압축한다. 실측
                # payload 10만 건 기준 lz4는 2.53배에서 멈추지만 zstd는 4.33배까지
                # 짜내, broker 저장량이 226 -> 132 B/record가 된다(#476).
                # linger.ms는 그대로 둔다 -- zstd는 batch가 33건뿐일 때도 상한 근처까지
                # 압축하므로, 20ms를 500ms로 늘려도 3pp만 더 줄어든다.
                "compression.type": "zstd",
                "queue.buffering.max.messages": 1_000_000,
            }
        )

    def publish(self, event: SensorEvent) -> None:
        self._raise_delivery_error()
        while True:
            try:
                self._producer.produce(
                    self.topic,
                    key=event.message_key,
                    value=orjson.dumps(event.to_dict(), option=orjson.OPT_SORT_KEYS),
                    on_delivery=self._on_delivery,
                )
                return
            except BufferError:
                # 내부 큐가 찼을 때 메시지를 버리지 않고 delivery callback을 비운다
                self._producer.poll(0.05)

    def flush(self) -> None:
        remaining = self._producer.flush(30)
        self._raise_delivery_error()
        if remaining:
            raise RuntimeError(f"Kafka flush timed out with {remaining} messages")

    def _on_delivery(self, error: object | None, _message: object) -> None:
        if error is not None:
            self._delivery_errors.append(str(error))

    def _raise_delivery_error(self) -> None:
        if self._delivery_errors:
            raise RuntimeError(f"Kafka delivery failed: {self._delivery_errors[0]}")
