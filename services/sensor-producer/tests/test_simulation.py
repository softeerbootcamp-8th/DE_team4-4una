from datetime import UTC, datetime, timedelta
from itertools import pairwise

import pytest
from sensor_producer.domain import (
    VEHICLE_PROFILES,
    RouteLeg,
    RoutePlan,
    SimulationConfig,
    TripRecord,
)
from sensor_producer.errors import TripInfeasibleError, TripSkipReason
from sensor_producer.publisher import MemoryPublisher
from sensor_producer.simulation import MotionSimulator, ReplayCoordinator, SpeedProfile
from shapely.geometry import LineString


def trip(
    duration_seconds: int = 2,
    *,
    trip_id: str = "trip-1",
    pu_location_id: int = 181,
    do_location_id: int = 181,
) -> TripRecord:
    pickup = datetime(2024, 2, 1, 10, 5, tzinfo=UTC)
    return TripRecord(
        trip_id=trip_id,
        request_datetime=pickup - timedelta(minutes=5),
        pickup_datetime=pickup,
        dropoff_datetime=pickup + timedelta(seconds=duration_seconds),
        pu_location_id=pu_location_id,
        do_location_id=do_location_id,
        trip_miles=0.1,
    )


def route(
    pavement_rating: float,
    humps: tuple[float, ...] = (),
    length_m: float = 100.0,
) -> RoutePlan:
    line = LineString([(-73.99, 40.67), (-73.989, 40.67)])
    leg = RouteLeg(
        segment_id="segment-1",
        geometry=line,
        length_m=length_m,
        posted_speed_mph=25.0,
        curve_radius_m=None,
        pavement_rating=pavement_rating,
        hump_distances_m=humps,
    )
    return RoutePlan(
        trip_id="trip-1",
        planned_at=trip().request_datetime,
        start_node_id="n1",
        end_node_id="n2",
        legs=(leg,),
        total_length_m=length_m,
    )


def turning_route() -> RoutePlan:
    first = RouteLeg(
        segment_id="segment-1",
        geometry=LineString([(-73.99, 40.67), (-73.9895, 40.67)]),
        length_m=50.0,
        posted_speed_mph=25.0,
        curve_radius_m=None,
        pavement_rating=8.0,
        hump_distances_m=(),
    )
    second = RouteLeg(
        segment_id="segment-2",
        geometry=LineString([(-73.9895, 40.67), (-73.9895, 40.6705)]),
        length_m=50.0,
        posted_speed_mph=25.0,
        curve_radius_m=None,
        pavement_rating=8.0,
        hump_distances_m=(),
    )
    return RoutePlan(
        trip_id="trip-1",
        planned_at=trip().request_datetime,
        start_node_id="n1",
        end_node_id="n3",
        legs=(first, second),
        total_length_m=100.0,
    )


def mixed_speed_route() -> RoutePlan:
    first = RouteLeg(
        segment_id="segment-slow",
        geometry=LineString([(-73.99, 40.67), (-73.9898, 40.67)]),
        length_m=20.0,
        posted_speed_mph=10.0,
        curve_radius_m=None,
        pavement_rating=8.0,
        hump_distances_m=(),
    )
    second = RouteLeg(
        segment_id="segment-fast",
        geometry=LineString([(-73.9898, 40.67), (-73.988, 40.67)]),
        length_m=180.0,
        posted_speed_mph=30.0,
        curve_radius_m=None,
        pavement_rating=8.0,
        hump_distances_m=(),
    )
    return RoutePlan(
        trip_id="trip-1",
        planned_at=trip().request_datetime,
        start_node_id="n1",
        end_node_id="n3",
        legs=(first, second),
        total_length_m=200.0,
    )


def simulate(plan: RoutePlan):
    return list(
        MotionSimulator().generate(
            trip(duration_seconds=20),
            plan,
            VEHICLE_PROFILES[1],
            SimulationConfig("test-run", sample_hz=10, time_scale=0),
        )
    )


def test_sensor_samples_start_at_pickup_and_have_stable_sequence() -> None:
    events = simulate(route(8.0))

    assert len(events) == 201
    assert events[0].event_time == trip().pickup_datetime
    assert events[-1].event_time == trip(duration_seconds=20).dropoff_datetime
    assert [event.trip_seq for event in events] == list(range(201))
    assert (events[1].event_time - events[0].event_time).total_seconds() == 0.1
    assert all(event.to_dict().get("segment_id") is None for event in events)
    assert all(abs(event.accel_y or 0) <= 4.0 for event in events)
    assert events[0].jerk == events[0].jerk_x == 0.0
    assert events[0].jerk_y == events[0].jerk_z == 0.0
    assert events[0].steering_angle == 0.0
    assert all(-35.0 <= event.steering_angle <= 35.0 for event in events)
    assert events[0].steering_vibration == 0.0
    assert all(event.steering_vibration >= 0 for event in events)


def test_speed_profile_has_consistent_distance_and_respects_limit() -> None:
    plan = route(8.0)
    duration_seconds = 20
    interval_seconds = 0.1
    profile = SpeedProfile.for_route(plan, duration_seconds)
    states = [
        profile.state_at(sequence * interval_seconds)
        for sequence in range(duration_seconds * 10 + 1)
    ]

    integrated_distance = sum(
        (previous.speed_mps + current.speed_mps) / 2 * interval_seconds
        for previous, current in pairwise(states)
    )
    assert states[0].speed_mps == states[-1].speed_mps == 0.0
    assert states[0].distance_m == 0.0
    assert states[-1].distance_m == plan.total_length_m
    assert integrated_distance == pytest.approx(plan.total_length_m, abs=0.01)
    assert all(
        previous.distance_m <= current.distance_m
        for previous, current in pairwise(states)
    )
    assert max(state.speed_mps for state in states) <= profile.leg_speed_limits_mps[0]
    assert sum(state.speed_mps == profile.leg_speeds_mps[0] for state in states) > 1


def test_speed_profile_applies_limits_per_segment() -> None:
    profile = SpeedProfile.for_route(mixed_speed_route(), duration_seconds=30)
    states = [profile.state_at(sequence * 0.1) for sequence in range(301)]
    slow_limit, fast_limit = profile.leg_speed_limits_mps
    slow_segment = [state for state in states if state.distance_m < 20.0]
    fast_segment = [state for state in states if 20.0 < state.distance_m < 200.0]

    assert max(state.speed_mps for state in slow_segment) <= slow_limit
    assert max(state.speed_mps for state in fast_segment) > slow_limit
    assert max(state.speed_mps for state in fast_segment) <= fast_limit


def test_speed_profile_rejects_route_that_requires_speeding() -> None:
    with pytest.raises(ValueError, match="posted speed limit"):
        SpeedProfile.for_route(route(8.0), duration_seconds=2)


def test_three_axis_jerk_is_derived_from_published_acceleration() -> None:
    events = simulate(turning_route())
    interval_seconds = 0.1

    assert any(abs(event.jerk_y) > 0 for event in events[1:])
    assert any(abs(event.jerk_z) > 0 for event in events[1:])
    for previous, current in pairwise(events):
        assert previous.accel_x is not None
        assert previous.accel_y is not None
        assert current.accel_x is not None
        assert current.accel_y is not None
        assert current.accel_x == pytest.approx(
            (current.speed_mps - previous.speed_mps)
            / interval_seconds
            * VEHICLE_PROFILES[1].longitudinal_response
        )
        assert current.jerk_x == pytest.approx(
            (current.accel_x - previous.accel_x) / interval_seconds
        )
        assert current.jerk_y == pytest.approx(
            (current.accel_y - previous.accel_y) / interval_seconds
        )
        assert current.jerk_z == pytest.approx(
            (current.accel_z - previous.accel_z) / interval_seconds
        )
        assert current.jerk == current.jerk_x


def test_poor_pavement_increases_vertical_motion() -> None:
    poor = simulate(route(2.0))
    good = simulate(route(9.0))

    poor_mean = sum(abs(event.accel_z) for event in poor) / len(poor)
    good_mean = sum(abs(event.accel_z) for event in good) / len(good)

    assert poor_mean > good_mean * 2


def test_poor_pavement_increases_steering_vibration() -> None:
    poor = simulate(route(2.0))
    good = simulate(route(9.0))

    poor_mean = sum(event.steering_vibration for event in poor) / len(poor)
    good_mean = sum(event.steering_vibration for event in good) / len(good)

    assert poor_mean > good_mean * 2


def test_turning_increases_peak_steering_vibration() -> None:
    straight = simulate(route(8.0))
    turning = simulate(turning_route())

    assert max(event.steering_vibration for event in turning) > max(
        event.steering_vibration for event in straight
    )


def test_steering_angle_reflects_route_direction_change() -> None:
    straight = simulate(route(8.0))
    turning = simulate(turning_route())

    assert all(event.steering_angle == pytest.approx(0.0) for event in straight)
    assert max(abs(event.steering_angle) for event in turning) > 1.0


def test_speed_hump_creates_visible_vertical_impact() -> None:
    smooth = simulate(route(8.0))
    with_hump = simulate(route(8.0, (50.0,)))
    replayed = simulate(route(8.0, (50.0,)))

    assert max(event.accel_z for event in with_hump) > max(
        event.accel_z for event in smooth
    ) + 1.0
    assert [event.event_id for event in with_hump] == [event.event_id for event in replayed]
    assert [event.steering_vibration for event in with_hump] == [
        event.steering_vibration for event in replayed
    ]
    assert [event.speed_mps for event in with_hump] == [
        event.speed_mps for event in replayed
    ]
    assert max(event.steering_vibration for event in with_hump) > max(
        event.steering_vibration for event in smooth
    )


def test_replay_plans_at_request_and_publishes_from_pickup() -> None:
    class RecordingClock:
        def __init__(self) -> None:
            self.times: list[datetime] = []

        def wait_until(self, event_time: datetime) -> None:
            self.times.append(event_time)

    class RecordingRouter:
        def __init__(self, clock: RecordingClock) -> None:
            self.clock = clock
            self.planned_at: datetime | None = None

        def plan_for_zones(
            self,
            trip_id: str,
            planned_at: datetime,
            pickup_zone: object,
            dropoff_zone: object,
            target_distance_m: float | None = None,
        ) -> RoutePlan:
            assert self.clock.times[-1] == planned_at
            self.planned_at = planned_at
            return route(8.0, length_m=10.0)

    source_trip = trip()
    clock = RecordingClock()
    router = RecordingRouter(clock)
    publisher = MemoryPublisher()
    coordinator = ReplayCoordinator(
        router,  # type: ignore[arg-type]
        {181: object()},
        publisher,
        SimulationConfig("test-run", sample_hz=1, time_scale=0),
        clock=clock,  # type: ignore[arg-type]
    )

    result = coordinator.replay([source_trip])

    assert router.planned_at == source_trip.request_datetime
    assert clock.times == [
        source_trip.request_datetime,
        source_trip.pickup_datetime,
        source_trip.pickup_datetime + timedelta(seconds=1),
        source_trip.dropoff_datetime,
    ]
    assert [event.event_time for event in publisher.events] == clock.times[1:]
    assert result.events_published == 3


def test_replay_rejects_an_out_of_order_streaming_trip_source() -> None:
    source = iter(
        [
            trip(trip_id="trip-later"),
            TripRecord(
                trip_id="trip-earlier",
                request_datetime=trip().request_datetime - timedelta(minutes=1),
                pickup_datetime=trip().pickup_datetime,
                dropoff_datetime=trip().dropoff_datetime,
                pu_location_id=181,
                do_location_id=181,
                trip_miles=0.1,
            ),
        ]
    )
    coordinator = ReplayCoordinator(
        object(),  # type: ignore[arg-type]
        {181: object()},
        MemoryPublisher(),
        SimulationConfig("test-run", sample_hz=1, time_scale=0),
    )

    with pytest.raises(ValueError, match="sorted by request time"):
        coordinator.replay(source)


def test_replay_skips_infeasible_trip_and_continues(caplog) -> None:
    class SelectiveRouter:
        def plan_for_zones(
            self,
            trip_id: str,
            planned_at: datetime,
            pickup_zone: object,
            dropoff_zone: object,
            target_distance_m: float | None = None,
        ) -> RoutePlan:
            if trip_id == "trip-bad":
                raise TripInfeasibleError(
                    TripSkipReason.NO_DIRECTED_ROUTE,
                    "test route is disconnected",
                )
            return route(8.0, length_m=10.0)

    publisher = MemoryPublisher()
    coordinator = ReplayCoordinator(
        SelectiveRouter(),  # type: ignore[arg-type]
        {181: object()},
        publisher,
        SimulationConfig("test-run", sample_hz=1, time_scale=0),
    )

    result = coordinator.replay(
        [trip(trip_id="trip-bad"), trip(trip_id="trip-good")]
    )

    assert {event.trip_id for event in publisher.events} == {"trip-good"}
    assert result.trips_attempted == 2
    assert result.trips_planned == 1
    assert result.trips_skipped == 1
    assert result.trip_skip_ratio == 0.5
    assert result.skip_reason_counts == {"NO_DIRECTED_ROUTE": 1}
    assert "trip_id=trip-bad" in caplog.text
    assert "reason=NO_DIRECTED_ROUTE" in caplog.text


def test_replay_aggregates_missing_zone_reason() -> None:
    class RecordingRouter:
        def __init__(self) -> None:
            self.calls = 0

        def plan_for_zones(
            self,
            trip_id: str,
            planned_at: datetime,
            pickup_zone: object,
            dropoff_zone: object,
            target_distance_m: float | None = None,
        ) -> RoutePlan:
            self.calls += 1
            return route(8.0, length_m=10.0)

    router = RecordingRouter()
    coordinator = ReplayCoordinator(
        router,  # type: ignore[arg-type]
        {181: object()},
        MemoryPublisher(),
        SimulationConfig("test-run", sample_hz=1, time_scale=0),
    )

    result = coordinator.replay(
        [trip(trip_id="trip-missing", pu_location_id=999), trip(trip_id="trip-valid")]
    )

    assert router.calls == 1
    assert result.skip_reason_counts == {"PICKUP_ZONE_NOT_FOUND": 1}


def test_replay_skips_infeasible_speed_profile() -> None:
    class LongRouteRouter:
        def plan_for_zones(
            self,
            trip_id: str,
            planned_at: datetime,
            pickup_zone: object,
            dropoff_zone: object,
            target_distance_m: float | None = None,
        ) -> RoutePlan:
            return route(8.0, length_m=100.0)

    publisher = MemoryPublisher()
    coordinator = ReplayCoordinator(
        LongRouteRouter(),  # type: ignore[arg-type]
        {181: object()},
        publisher,
        SimulationConfig("test-run", sample_hz=1, time_scale=0),
    )

    result = coordinator.replay([trip(duration_seconds=2)])

    assert publisher.events == []
    assert result.skip_reason_counts == {"SPEED_PROFILE_INFEASIBLE": 1}


def test_replay_does_not_hide_publisher_failure() -> None:
    class RecordingRouter:
        def plan_for_zones(
            self,
            trip_id: str,
            planned_at: datetime,
            pickup_zone: object,
            dropoff_zone: object,
            target_distance_m: float | None = None,
        ) -> RoutePlan:
            return route(8.0, length_m=10.0)

    class FailingPublisher:
        def publish(self, event: object) -> None:
            raise RuntimeError("Kafka unavailable")

        def flush(self) -> None:
            return

    coordinator = ReplayCoordinator(
        RecordingRouter(),  # type: ignore[arg-type]
        {181: object()},
        FailingPublisher(),  # type: ignore[arg-type]
        SimulationConfig("test-run", sample_hz=1, time_scale=0),
    )

    with pytest.raises(RuntimeError, match="Kafka unavailable"):
        coordinator.replay([trip()])


def test_vehicle_profiles_use_canonical_ids() -> None:
    assert set(VEHICLE_PROFILES) == {1, 2, 3, 4, 5}


def test_vehicle_profiles_use_expected_profile_names() -> None:
    names = {profile_id: profile.profile_name for profile_id, profile in VEHICLE_PROFILES.items()}
    assert names == {
        1: "VP_SEDAN_COMPACT",
        2: "VP_SEDAN_LARGE",
        3: "VP_SUV_COMPACT",
        4: "VP_SUV_LARGE",
        5: "VP_MPV_LARGE",
    }


def test_simulation_config_accepts_every_canonical_vehicle_profile_id() -> None:
    for profile_id in VEHICLE_PROFILES:
        SimulationConfig("test-run", vehicle_profile_id=profile_id)


def test_simulation_config_rejects_unknown_vehicle_profile_id() -> None:
    with pytest.raises(ValueError, match="vehicle_profile_id"):
        SimulationConfig("test-run", vehicle_profile_id=999)


def test_sensor_response_differs_by_vehicle_profile() -> None:
    plan = route(2.0, (50.0,))

    def mean_abs_accel_z(profile_id: int) -> float:
        events = list(
            MotionSimulator().generate(
                trip(duration_seconds=20),
                plan,
                VEHICLE_PROFILES[profile_id],
                SimulationConfig(
                    "test-run", vehicle_profile_id=profile_id, sample_hz=10, time_scale=0
                ),
            )
        )
        return sum(abs(event.accel_z) for event in events) / len(events)

    means = {profile_id: mean_abs_accel_z(profile_id) for profile_id in VEHICLE_PROFILES}

    assert len({round(value, 6) for value in means.values()}) == len(VEHICLE_PROFILES)
