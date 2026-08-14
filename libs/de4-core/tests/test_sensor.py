from datetime import UTC, datetime

import pytest
from de4_core import SensorEvent


def make_event(**overrides: object) -> SensorEvent:
    values: dict[str, object] = {
        "event_id": "event-1",
        "vehicle_id": "vehicle-1",
        "vehicle_profile_id": 1,
        "trip_id": "trip-1",
        "trip_seq": 0,
        "event_time": datetime(2024, 2, 1, tzinfo=UTC),
        "latitude": 40.67,
        "longitude": -73.98,
        "speed_mps": 4.0,
        "heading": 90.0,
        "steering_angle": 5.0,
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
    values.update(overrides)
    return SensorEvent(**values)  # type: ignore[arg-type]


def test_sensor_event_serializes_agreed_schema() -> None:
    event = make_event()

    assert event.message_key == b"trip-1"
    assert event.to_dict()["trip_seq"] == 0
    assert event.to_dict()["jerk_x"] == 0.4
    assert event.to_dict()["jerk_y"] == 0.5
    assert event.to_dict()["jerk_z"] == 0.6
    assert event.to_dict()["steering_angle"] == 5.0
    assert event.to_dict()["steering_vibration"] == 0.7
    assert "_ingested_at" not in event.to_dict()
    assert b'"event_time":"2024-02-01T00:00:00+00:00"' in event.to_json()


def test_sensor_event_requires_legacy_jerk_to_match_jerk_x() -> None:
    with pytest.raises(ValueError, match="jerk must equal"):
        make_event(jerk_x=0.5)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("trip_seq", -1),
        ("latitude", 91.0),
        ("longitude", -181.0),
        ("speed_mps", -0.1),
        ("heading", 360.0),
        ("steering_angle", 36.0),
        ("steering_angle", float("nan")),
        ("steering_vibration", -0.1),
    ],
)
def test_sensor_event_rejects_invalid_values(field: str, value: object) -> None:
    with pytest.raises(ValueError):
        make_event(**{field: value})
