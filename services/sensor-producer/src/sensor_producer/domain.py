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

# 배정 비율 세트의 버전. 비율이 바뀌면 함께 올려 run_summary에 남긴다.
VEHICLE_MIX_VERSION = "v1-heuristic"

# trip별 차종 배정 비율. 세단 우위와 MPV 희소를 표현한 가정치이며 실측 분포가 아니다.
VEHICLE_MIXES: dict[str, tuple[tuple[int, float], ...]] = {
    "nyc-hvfhv-v1": (
        (1, 0.42),  # VP_SEDAN_COMPACT
        (2, 0.12),  # VP_SEDAN_LARGE
        (3, 0.26),  # VP_SUV_COMPACT
        (4, 0.16),  # VP_SUV_LARGE
        (5, 0.04),  # VP_MPV_LARGE
    ),
}


def _validate_vehicle_mixes() -> None:
    """CDF 순서가 dict 작성 순서에 좌우되지 않도록 id 오름차순까지 함께 검증한다."""
    for name, shares in VEHICLE_MIXES.items():
        if not shares:
            raise ValueError(f"vehicle mix {name} must not be empty")
        profile_ids = [profile_id for profile_id, _ in shares]
        if profile_ids != sorted(profile_ids):
            raise ValueError(f"vehicle mix {name} must be ordered by vehicle_profile_id")
        if len(set(profile_ids)) != len(profile_ids):
            raise ValueError(f"vehicle mix {name} repeats a vehicle_profile_id")
        if unknown := [value for value in profile_ids if value not in VEHICLE_PROFILES]:
            raise ValueError(f"vehicle mix {name} references unknown profiles: {unknown}")
        if any(share <= 0 for _, share in shares):
            raise ValueError(f"vehicle mix {name} shares must be positive")
        if abs(sum(share for _, share in shares) - 1.0) > 1e-9:
            raise ValueError(f"vehicle mix {name} shares must sum to 1")


_validate_vehicle_mixes()


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
    # 배정 모드는 배타적이다. vehicle_profile_id는 모든 trip에 한 프로필을 고정하고,
    # vehicle_mix는 trip마다 결정론적으로 프로필을 뽑는다.
    vehicle_profile_id: int | None = 1
    vehicle_mix: str | None = None
    pavement_model_version: str = "pavement-v1"
    hump_model_version: str = "hump-v1"
    motion_model_version: str = "motion-v1"
    vehicle_mix_version: str = VEHICLE_MIX_VERSION

    def __post_init__(self) -> None:
        if not self.run_id:
            raise ValueError("run_id must be non-empty")
        if self.sample_hz <= 0 or self.sample_hz > 100:
            raise ValueError("sample_hz must be in [1, 100]")
        if self.time_scale < 0:
            raise ValueError("time_scale must be non-negative")
        if (self.vehicle_profile_id is None) == (self.vehicle_mix is None):
            raise ValueError("set exactly one of vehicle_profile_id and vehicle_mix")
        if (
            self.vehicle_profile_id is not None
            and self.vehicle_profile_id not in VEHICLE_PROFILES
        ):
            raise ValueError("unknown vehicle_profile_id")
        if self.vehicle_mix is not None and self.vehicle_mix not in VEHICLE_MIXES:
            raise ValueError("unknown vehicle_mix")

    @property
    def interval_seconds(self) -> float:
        return 1 / self.sample_hz
