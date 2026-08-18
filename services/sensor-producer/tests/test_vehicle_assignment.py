"""Per-trip vehicle-profile assignment and damping-driven vertical response."""

from collections import Counter
from datetime import UTC, datetime, timedelta

import pytest
from sensor_producer.domain import (
    VEHICLE_MIXES,
    VEHICLE_PROFILES,
    RouteLeg,
    RoutePlan,
    SimulationConfig,
    TripRecord,
)
from sensor_producer.publisher import MemoryPublisher
from sensor_producer.simulation import (
    MotionSimulator,
    ReplayCoordinator,
    assign_vehicle_profile,
    deterministic_phase,
    uniform01,
)
from shapely.geometry import LineString

MIX_NAME = "nyc-hvfhv-v1"
BASELINE = VEHICLE_PROFILES[2]
SOFTEST = VEHICLE_PROFILES[5]


def trip(trip_id: str, duration_seconds: int = 4) -> TripRecord:
    pickup = datetime(2024, 2, 1, 10, 5, tzinfo=UTC)
    return TripRecord(
        trip_id=trip_id,
        request_datetime=pickup - timedelta(minutes=5),
        pickup_datetime=pickup,
        dropoff_datetime=pickup + timedelta(seconds=duration_seconds),
        pu_location_id=181,
        do_location_id=181,
        trip_miles=0.1,
    )


def leg(humps: tuple[float, ...] = (), length_m: float = 40.0) -> RouteLeg:
    return RouteLeg(
        segment_id="segment-1",
        geometry=LineString([(-73.99, 40.67), (-73.989, 40.67)]),
        length_m=length_m,
        posted_speed_mph=25.0,
        curve_radius_m=None,
        pavement_rating=4.0,
        hump_distances_m=humps,
    )


def plan(humps: tuple[float, ...] = (), length_m: float = 10.0) -> RoutePlan:
    return RoutePlan(
        trip_id="trip-1",
        planned_at=trip("trip-1").request_datetime,
        start_node_id="n1",
        end_node_id="n2",
        legs=(leg(humps, length_m),),
        total_length_m=length_m,
    )


class StubRouter:
    def plan_for_zones(
        self,
        trip_id: str,
        planned_at: datetime,
        pickup_zone: object,
        dropoff_zone: object,
        target_distance_m: float | None = None,
    ) -> RoutePlan:
        return plan()


# --- 결정론적 배정 ------------------------------------------------------------


def test_assignment_is_deterministic() -> None:
    for trip_id in ("a", "b", "trip-42"):
        assert assign_vehicle_profile(trip_id, 4, MIX_NAME) is assign_vehicle_profile(
            trip_id, 4, MIX_NAME
        )


def test_assignment_depends_on_the_seed() -> None:
    trip_ids = [f"trip-{index}" for index in range(200)]
    assert [assign_vehicle_profile(value, 4, MIX_NAME) for value in trip_ids] != [
        assign_vehicle_profile(value, 5, MIX_NAME) for value in trip_ids
    ]


def test_assignment_follows_the_configured_shares() -> None:
    total = 4000
    counts = Counter(
        assign_vehicle_profile(f"trip-{index}", 4, MIX_NAME).vehicle_profile_id
        for index in range(total)
    )
    for profile_id, share in VEHICLE_MIXES[MIX_NAME]:
        assert counts[profile_id] / total == pytest.approx(share, abs=0.02)


def test_mix_draw_does_not_share_a_hash_with_the_motion_phase() -> None:
    """두 결정론적 추출이 같은 해시를 쓰면 배정과 진동 위상에 상관이 생긴다."""
    assert uniform01("vehicle-mix", 4, "trip-1") != deterministic_phase("trip-1", 4)


def test_deterministic_phase_hash_input_is_frozen() -> None:
    """기대값은 sha256(f"{seed}:{trip_id}")를 직접 계산한 결과다.

    여기에 namespace를 덧붙이면 기록된 실행의 accel_z 위상이 전부 달라진다.
    """
    assert deterministic_phase("trip-1", 4) == pytest.approx(5.431920011344631)


# --- 배정 모드 ----------------------------------------------------------------


def test_replay_with_a_mix_emits_more_than_one_profile() -> None:
    publisher = MemoryPublisher()
    coordinator = ReplayCoordinator(
        StubRouter(),  # type: ignore[arg-type]
        {181: object()},
        publisher,
        SimulationConfig(
            "test-run",
            sample_hz=1,
            time_scale=0,
            vehicle_profile_id=None,
            vehicle_mix=MIX_NAME,
        ),
        MotionSimulator(),
    )

    result = coordinator.replay([trip(f"trip-{index}") for index in range(60)])

    assert len({event.vehicle_profile_id for event in publisher.events}) > 1
    assert sum(result.profile_trip_counts.values()) == result.trips_planned


def test_replay_with_a_fixed_profile_reports_one_profile() -> None:
    coordinator = ReplayCoordinator(
        StubRouter(),  # type: ignore[arg-type]
        {181: object()},
        MemoryPublisher(),
        SimulationConfig("test-run", sample_hz=1, time_scale=0, vehicle_profile_id=4),
    )

    result = coordinator.replay([trip(f"trip-{index}") for index in range(10)])

    assert result.profile_trip_counts == {"VP_SUV_LARGE": 10}


def test_config_requires_exactly_one_assignment_mode() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        SimulationConfig("test-run", vehicle_profile_id=1, vehicle_mix=MIX_NAME)
    with pytest.raises(ValueError, match="exactly one"):
        SimulationConfig("test-run", vehicle_profile_id=None)


def test_config_rejects_unknown_mix() -> None:
    with pytest.raises(ValueError, match="unknown vehicle_mix"):
        SimulationConfig("test-run", vehicle_profile_id=None, vehicle_mix="nope")


def test_every_mix_is_a_valid_distribution() -> None:
    for shares in VEHICLE_MIXES.values():
        assert sum(share for _, share in shares) == pytest.approx(1.0)
        # 0은 Gold의 vehicle-agnostic sentinel 예약값이라 배정 대상이 아니다.
        assert all(profile_id > 0 for profile_id, _ in shares)
        assert [profile_id for profile_id, _ in shares] == sorted(
            profile_id for profile_id, _ in shares
        )
