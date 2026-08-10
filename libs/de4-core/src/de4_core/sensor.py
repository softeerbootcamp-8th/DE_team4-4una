"""Executable contracts for vehicle sensor events."""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class SensorEvent:
    """One immutable Bronze vehicle sensor measurement.

    Timestamps are timezone-aware UTC values. Acceleration values use m/s²,
    speed uses m/s, heading uses degrees, and jerk is longitudinal m/s³.
    """

    event_id: str
    vehicle_id: str
    vehicle_profile_id: int
    trip_id: str
    trip_seq: int
    event_time: datetime
    latitude: float
    longitude: float
    speed_mps: float
    heading: float | None
    accel_x: float | None
    accel_y: float | None
    accel_z: float
    jerk: float
    _ingested_at: datetime
    _run_id: str

    def __post_init__(self) -> None:
        if not self.event_id or not self.trip_id or not self.vehicle_id:
            raise ValueError("event, trip, and vehicle identifiers must be non-empty")
        if self.vehicle_profile_id <= 0:
            raise ValueError("vehicle_profile_id must be positive")
        if self.trip_seq < 0:
            raise ValueError("trip_seq must be non-negative")
        if self.event_time.utcoffset() is None or self._ingested_at.utcoffset() is None:
            raise ValueError("sensor timestamps must be timezone-aware")
        if not -90 <= self.latitude <= 90:
            raise ValueError("latitude must be between -90 and 90")
        if not -180 <= self.longitude <= 180:
            raise ValueError("longitude must be between -180 and 180")
        if self.speed_mps < 0 or not math.isfinite(self.speed_mps):
            raise ValueError("speed_mps must be finite and non-negative")
        if self.heading is not None and not 0 <= self.heading < 360:
            raise ValueError("heading must be in [0, 360)")
        for name in ("accel_x", "accel_y", "accel_z", "jerk"):
            value = getattr(self, name)
            if value is not None and not math.isfinite(value):
                raise ValueError(f"{name} must be finite when present")

    @property
    def message_key(self) -> bytes:
        """Keep every trip in one Kafka partition for ordered delivery."""

        return self.trip_id.encode()

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-compatible dictionary using the agreed column names."""

        value = asdict(self)
        value["event_time"] = self.event_time.isoformat()
        value["_ingested_at"] = self._ingested_at.isoformat()
        return value

    def to_json(self) -> bytes:
        """Serialize deterministically for Kafka and fixture comparisons."""

        return json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
