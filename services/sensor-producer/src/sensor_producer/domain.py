"""Domain objects used by the deterministic driving simulation."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from shapely.geometry import LineString


@dataclass(frozen=True, slots=True)
class TripRecord:
    """Minimal TLC trip projection required by the motion simulation."""

    trip_id: str
    request_datetime: datetime
    pickup_datetime: datetime
    dropoff_datetime: datetime
    pu_location_id: int
    do_location_id: int

    def __post_init__(self) -> None:
        if not self.trip_id:
            raise ValueError("trip_id must be non-empty")
        if not self.request_datetime <= self.pickup_datetime < self.dropoff_datetime:
            raise ValueError("trip timestamps must satisfy request <= pickup < dropoff")

    @property
    def passenger_duration_seconds(self) -> float:
        return (self.dropoff_datetime - self.pickup_datetime).total_seconds()


@dataclass(frozen=True, slots=True)
class VehicleProfile:
    vehicle_profile_id: int
    vehicle_name: str
    vertical_response: float
    damping: float
    longitudinal_response: float = 1.0
    lateral_response: float = 1.0
    steering_vibration_response: float = 1.0


VEHICLE_PROFILES: dict[int, VehicleProfile] = {
    1: VehicleProfile(1, "genesis", 0.72, 0.82, 0.90, 0.90, 0.72),
    2: VehicleProfile(2, "grandeur", 0.82, 0.76, 0.95, 0.95, 0.82),
    3: VehicleProfile(3, "avante", 1.08, 0.62, 1.05, 1.08, 1.08),
    4: VehicleProfile(4, "ev5", 0.94, 0.72, 0.88, 1.02, 0.94),
}


@dataclass(slots=True)
class RoadSegment:
    segment_id: str
    from_node_id: str
    to_node_id: str
    traffic_direction: str
    street_name: str
    geometry: LineString
    length_m: float
    posted_speed_mph: float | None
    curve_radius_m: float | None
    pavement_rating: float | None = None
    hump_fractions: list[float] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class RouteLeg:
    segment_id: str
    geometry: LineString
    length_m: float
    posted_speed_mph: float | None
    curve_radius_m: float | None
    pavement_rating: float | None
    hump_distances_m: tuple[float, ...]


@dataclass(frozen=True, slots=True)
class RoutePlan:
    trip_id: str
    planned_at: datetime
    start_node_id: str
    end_node_id: str
    legs: tuple[RouteLeg, ...]
    total_length_m: float

    @property
    def segment_ids(self) -> tuple[str, ...]:
        return tuple(leg.segment_id for leg in self.legs)


@dataclass(frozen=True, slots=True)
class SimulationConfig:
    run_id: str
    sample_hz: int = 10
    time_scale: float = 1.0
    seed: int = 4
    vehicle_profile_id: int = 1
    pavement_model_version: str = "pavement-v1"
    hump_model_version: str = "hump-v1"
    motion_model_version: str = "motion-v1"

    def __post_init__(self) -> None:
        if not self.run_id:
            raise ValueError("run_id must be non-empty")
        if self.sample_hz <= 0 or self.sample_hz > 100:
            raise ValueError("sample_hz must be in [1, 100]")
        if self.time_scale < 0:
            raise ValueError("time_scale must be non-negative")
        if self.vehicle_profile_id not in VEHICLE_PROFILES:
            raise ValueError("unknown vehicle_profile_id")

    @property
    def interval_seconds(self) -> float:
        return 1 / self.sample_hz
