import json
import sys
from datetime import UTC, datetime
from types import SimpleNamespace

from de4_core import SensorEvent
from sensor_producer.publisher import JsonlPublisher, KafkaPublisher


def event() -> SensorEvent:
    return SensorEvent(
        event_id="event-1",
        vehicle_id="vehicle-1",
        vehicle_profile_id=1,
        trip_id="trip-1",
        trip_seq=0,
        event_time=datetime(2024, 2, 1, tzinfo=UTC),
        latitude=40.67,
        longitude=-73.98,
        speed_mps=1.0,
        heading=0.0,
        steering_angle=3.5,
        accel_x=0.0,
        accel_y=0.0,
        accel_z=0.0,
        jerk=0.0,
        jerk_x=0.0,
        jerk_y=0.0,
        jerk_z=0.0,
        steering_vibration=0.12,
        _run_id="run-1",
    )


def test_jsonl_publisher_writes_sensor_event_contract(tmp_path) -> None:
    output = tmp_path / "events.jsonl"
    publisher = JsonlPublisher(output)

    publisher.publish(event())
    publisher.flush()

    value = json.loads(output.read_text())
    assert value["trip_id"] == "trip-1"
    assert value["jerk"] == value["jerk_x"]
    assert {"jerk_x", "jerk_y", "jerk_z"} <= value.keys()
    assert value["steering_vibration"] == 0.12
    assert value["steering_angle"] == 3.5
    assert "_ingested_at" not in value
    assert "segment_id" not in value


def test_kafka_publisher_keys_records_by_trip(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, object]]] = []
    configurations: list[dict[str, object]] = []

    class FakeProducer:
        def __init__(self, configuration: dict[str, object]):
            configurations.append(configuration)

        def produce(
            self,
            topic: str,
            **message: object,
        ) -> None:
            calls.append((topic, message))

        def poll(self, timeout: float) -> None:
            return None

        def flush(self, timeout: float) -> int:
            return 0

    monkeypatch.setitem(
        sys.modules, "confluent_kafka", SimpleNamespace(Producer=FakeProducer)
    )
    publisher = KafkaPublisher(["localhost:9092"], "sensor-events")

    publisher.publish(event())
    publisher.flush()

    assert calls[0][0] == "sensor-events"
    assert calls[0][1]["key"] == b"trip-1"
    assert json.loads(calls[0][1]["value"])["trip_seq"] == 0
    assert "timestamp_ms" not in calls[0][1]
    assert configurations[0]["enable.idempotence"] is True
    assert configurations[0]["compression.type"] == "lz4"
