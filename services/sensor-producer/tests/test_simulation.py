from datetime import UTC, datetime, timedelta
from itertools import pairwise

import numpy as np
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
from sensor_producer.simulation import (
    MotionSimulator,
    ReplayClock,
    ReplayCoordinator,
    ReplayTimeline,
    SpeedProfile,
    rebase_trip_to_replay_timeline,
)
from shapely.geometry import LineString


def trip(
    duration_seconds: int = 2,
    *,
    trip_id: str = "trip-1",
    pu_location_id: int = 181,
    do_location_id: int = 181,
    start_offset_seconds: int = 0,
) -> TripRecord:
    pickup = datetime(2024, 2, 1, 10, 5, tzinfo=UTC) + timedelta(
        seconds=start_offset_seconds
    )
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
            self.planned_at = planned_at
            return route(8.0, length_m=10.0)

    source_trip = trip()
    dispatched_at = datetime(2026, 8, 23, 23, 50, tzinfo=UTC)
    clock = RecordingClock()

    def utc_now() -> datetime:
        assert clock.times[-1] == source_trip.request_datetime
        return dispatched_at

    router = RecordingRouter(clock)
    publisher = MemoryPublisher()
    coordinator = ReplayCoordinator(
        router,  # type: ignore[arg-type]
        {181: object()},
        publisher,
        SimulationConfig("test-run", sample_hz=1, time_scale=0),
        clock=clock,  # type: ignore[arg-type]
        utc_now=utc_now,
    )

    result = coordinator.replay([source_trip])

    assert router.planned_at == datetime(2026, 8, 23, 23, 50, tzinfo=UTC)
    assert clock.times == [
        source_trip.request_datetime,
        source_trip.pickup_datetime,
        source_trip.pickup_datetime + timedelta(seconds=1),
        source_trip.dropoff_datetime,
    ]
    assert [event.event_time for event in publisher.events] == [
        datetime(2026, 8, 23, 23, 55, tzinfo=UTC),
        datetime(2026, 8, 23, 23, 55, 1, tzinfo=UTC),
        datetime(2026, 8, 23, 23, 55, 2, tzinfo=UTC),
    ]
    assert result.events_published == 3


def test_replay_clock_reports_lag_and_enforces_limit(monkeypatch) -> None:
    moments = iter([100.0, 102.0, 100.0, 102.0])
    monkeypatch.setattr(
        "sensor_producer.simulation.time.monotonic",
        lambda: next(moments),
    )
    clock = ReplayClock(time_scale=1)
    source_time = datetime(2024, 2, 1, 10, tzinfo=UTC)

    clock.wait_until(source_time)
    clock.wait_until(source_time + timedelta(seconds=1))

    assert clock.final_lag_seconds == pytest.approx(1.0)
    assert clock.max_lag_seconds == pytest.approx(1.0)

    clock = ReplayClock(time_scale=1, max_lag_seconds=0.5)
    clock.wait_until(source_time)
    with pytest.raises(RuntimeError, match="replay lag 1.000s"):
        clock.wait_until(source_time + timedelta(seconds=1))


def test_vectorized_speed_profile_matches_scalar_states() -> None:
    planned_route = route(8.0, length_m=100.0)
    profile = SpeedProfile.for_route(planned_route, duration_seconds=20.0)
    elapsed = np.linspace(0.0, 20.0, 201)

    distances, speeds = profile.states_for(elapsed)
    scalar = [profile.state_at(float(value)) for value in elapsed]

    assert distances == pytest.approx([state.distance_m for state in scalar])
    assert speeds == pytest.approx([state.speed_mps for state in scalar])


def test_rebase_trip_preserves_source_offsets_on_one_run_timeline() -> None:
    source_trip = TripRecord(
        trip_id="overnight-trip",
        request_datetime=datetime(2024, 2, 1, 23, 58, tzinfo=UTC),
        pickup_datetime=datetime(2024, 2, 2, 0, 1, tzinfo=UTC),
        dropoff_datetime=datetime(2024, 2, 2, 0, 3, tzinfo=UTC),
        pu_location_id=181,
        do_location_id=181,
        trip_miles=0.5,
    )

    timeline = ReplayTimeline(
        source_trip.request_datetime,
        datetime(2026, 8, 23, 15, tzinfo=UTC),
    )
    replay_trip = rebase_trip_to_replay_timeline(
        source_trip,
        timeline,
    )

    assert replay_trip.request_datetime == datetime(2026, 8, 23, 15, tzinfo=UTC)
    assert replay_trip.pickup_datetime == datetime(2026, 8, 23, 15, 3, tzinfo=UTC)
    assert replay_trip.dropoff_datetime == datetime(2026, 8, 23, 15, 5, tzinfo=UTC)
    assert (
        replay_trip.passenger_duration_seconds == source_trip.passenger_duration_seconds
    )


def test_replay_uses_one_utc_anchor_for_every_dispatch() -> None:
    class StaticRouter:
        def plan_for_zones(
            self,
            trip_id: str,
            planned_at: datetime,
            pickup_zone: object,
            dropoff_zone: object,
            target_distance_m: float | None = None,
        ) -> RoutePlan:
            return route(8.0, length_m=10.0)

    utc_calls = 0

    def utc_now() -> datetime:
        nonlocal utc_calls
        utc_calls += 1
        return datetime(2026, 8, 23, 23, 59, 59, tzinfo=UTC)

    publisher = MemoryPublisher()
    coordinator = ReplayCoordinator(
        StaticRouter(),  # type: ignore[arg-type]
        {181: object()},
        publisher,
        SimulationConfig("test-run", sample_hz=1, time_scale=0),
        utc_now=utc_now,
    )

    coordinator.replay(
        [
            trip(trip_id="trip-before-midnight"),
            trip(trip_id="trip-after-midnight", start_offset_seconds=1),
        ]
    )

    first_event = next(
        event for event in publisher.events if event.trip_id == "trip-before-midnight"
    )
    second_event = next(
        event for event in publisher.events if event.trip_id == "trip-after-midnight"
    )
    assert second_event.event_time - first_event.event_time == timedelta(seconds=1)
    assert utc_calls == 1


def test_replay_timeline_rejects_naive_anchor() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        ReplayTimeline(
            trip().request_datetime,
            datetime(2026, 8, 23),  # noqa: DTZ001
        )


def test_replay_consumes_only_the_next_pending_dispatch() -> None:
    class StaticRouter:
        def plan_for_zones(
            self,
            trip_id: str,
            planned_at: datetime,
            pickup_zone: object,
            dropoff_zone: object,
            target_distance_m: float | None = None,
        ) -> RoutePlan:
            return route(8.0, length_m=10.0)

    publisher = MemoryPublisher()

    def trip_stream():
        yield trip(trip_id="trip-1")
        yield trip(trip_id="trip-2", start_offset_seconds=600)
        # 세 번째 Trip을 요구할 때는 첫 번째 차량의 이벤트가 이미 발행되어야 한다
        assert publisher.events
        yield trip(trip_id="trip-3", start_offset_seconds=1200)

    coordinator = ReplayCoordinator(
        StaticRouter(),  # type: ignore[arg-type]
        {181: object()},
        publisher,
        SimulationConfig("test-run", sample_hz=1, time_scale=0),
    )

    result = coordinator.replay(trip_stream())

    assert result.trips_attempted == 3
    assert {event.trip_id for event in publisher.events} == {
        "trip-1",
        "trip-2",
        "trip-3",
    }


def test_replay_rejects_decreasing_request_time() -> None:
    coordinator = ReplayCoordinator(
        object(),  # type: ignore[arg-type]
        {181: object()},
        MemoryPublisher(),
        SimulationConfig("test-run", sample_hz=1, time_scale=0),
    )
    trips = iter(
        [
            trip(trip_id="trip-later", start_offset_seconds=60),
            trip(trip_id="trip-earlier"),
        ]
    )

    with pytest.raises(ValueError, match="ordered by request_datetime"):
        coordinator.replay(trips)


def test_replay_interleaves_overlapping_trips() -> None:
    class StaticRouter:
        def plan_for_zones(
            self,
            trip_id: str,
            planned_at: datetime,
            pickup_zone: object,
            dropoff_zone: object,
            target_distance_m: float | None = None,
        ) -> RoutePlan:
            return route(8.0, length_m=10.0)

    publisher = MemoryPublisher()
    coordinator = ReplayCoordinator(
        StaticRouter(),  # type: ignore[arg-type]
        {181: object()},
        publisher,
        SimulationConfig("test-run", sample_hz=1, time_scale=0),
    )

    coordinator.replay(
        iter(
            [
                trip(trip_id="trip-a"),
                trip(trip_id="trip-b", start_offset_seconds=1),
            ]
        )
    )

    assert [event.event_time for event in publisher.events] == sorted(
        event.event_time for event in publisher.events
    )
    assert [event.trip_seq for event in publisher.events if event.trip_id == "trip-a"] == [
        0,
        1,
        2,
    ]
    assert [event.trip_seq for event in publisher.events if event.trip_id == "trip-b"] == [
        0,
        1,
        2,
    ]


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
