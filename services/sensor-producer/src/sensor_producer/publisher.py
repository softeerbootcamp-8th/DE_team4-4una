"""Kafka and test publishers for sensor-event messages."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Protocol

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
    def __init__(self, bootstrap_servers: Iterable[str], topic: str):
        from kafka import KafkaProducer

        self.topic = topic
        self._producer = KafkaProducer(
            bootstrap_servers=list(bootstrap_servers),
            acks="all",
            retries=10,
            max_in_flight_requests_per_connection=1,
        )

    def publish(self, event: SensorEvent) -> None:
        self._producer.send(
            self.topic,
            key=event.message_key,
            value=event.to_json(),
        )

    def flush(self) -> None:
        self._producer.flush()
        self._producer.close()
