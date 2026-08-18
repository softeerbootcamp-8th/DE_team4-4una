"""Domain objects used by the deterministic driving simulation."""

from __future__ import annotations

import math
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
    trip_miles: float

    def __post_init__(self) -> None:
        if not self.trip_id:
            raise ValueError("trip_id must be non-empty")
        if not self.request_datetime <= self.pickup_datetime < self.dropoff_datetime:
            raise ValueError("trip timestamps must satisfy request <= pickup < dropoff")
        if self.trip_miles <= 0 or not math.isfinite(self.trip_miles):
            raise ValueError("trip_miles must be finite and positive")

    @property
    def passenger_duration_seconds(self) -> float:
        return (self.dropoff_datetime - self.pickup_datetime).total_seconds()


@dataclass(frozen=True, slots=True)
class VehicleProfile:
    vehicle_profile_id: int
    profile_name: str
    vertical_response: float
    damping: float
    longitudinal_response: float = 1.0
    lateral_response: float = 1.0
    steering_vibration_response: float = 1.0


# 제조사/모델 대신 차체 유형 x 크기 등급으로 재정의한다 (#170).
VEHICLE_PROFILES: dict[int, VehicleProfile] = {
    1: VehicleProfile(1, "VP_SEDAN_COMPACT", 1.05, 0.68, 1.00, 1.00, 1.03),
    2: VehicleProfile(2, "VP_SEDAN_LARGE", 1.00, 0.77, 1.00, 1.00, 1.00),
    3: VehicleProfile(3, "VP_SUV_COMPACT", 1.08, 0.70, 1.00, 1.06, 1.04),
    4: VehicleProfile(4, "VP_SUV_LARGE", 1.01, 0.66, 1.00, 1.08, 1.00),
    5: VehicleProfile(5, "VP_MPV_LARGE", 0.96, 0.61, 1.00, 1.10, 0.98),
}

# 차체 흔들림 항을 정규화하는 기준 차량. 세 반응계수가 모두 1.00인 프로필이다.
BASELINE_DAMPING = VEHICLE_PROFILES[2].damping

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
