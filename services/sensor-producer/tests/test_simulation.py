from datetime import UTC, datetime, timedelta

from sensor_producer.domain import (
    VEHICLE_PROFILES,
    RouteLeg,
    RoutePlan,
    SimulationConfig,
    TripRecord,
)
from sensor_producer.publisher import MemoryPublisher
from sensor_producer.simulation import MotionSimulator, ReplayCoordinator
from shapely.geometry import LineString


def trip() -> TripRecord:
    pickup = datetime(2024, 2, 1, 10, 5, tzinfo=UTC)
    return TripRecord(
        trip_id="trip-1",
        request_datetime=pickup - timedelta(minutes=5),
        pickup_datetime=pickup,
        dropoff_datetime=pickup + timedelta(seconds=2),
        pu_location_id=181,
        do_location_id=181,
        trip_miles=0.1,
        trip_time=2,
    )


def route(pavement_rating: float, humps: tuple[float, ...] = ()) -> RoutePlan:
    line = LineString([(-73.99, 40.67), (-73.989, 40.67)])
    leg = RouteLeg(
        segment_id="segment-1",
        geometry=line,
        length_m=100.0,
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
        total_length_m=100.0,
    )


def simulate(plan: RoutePlan):
    return list(
        MotionSimulator().generate(
            trip(),
            plan,
            VEHICLE_PROFILES[1],
            SimulationConfig("test-run", sample_hz=10, time_scale=0),
        )
    )


def test_sensor_samples_start_at_pickup_and_have_stable_sequence() -> None:
    events = simulate(route(8.0))

    assert len(events) == 21
    assert events[0].event_time == trip().pickup_datetime
    assert events[-1].event_time == trip().dropoff_datetime
    assert [event.trip_seq for event in events] == list(range(21))
    assert (events[1].event_time - events[0].event_time).total_seconds() == 0.1
    assert all(event.to_dict().get("segment_id") is None for event in events)
    assert all(abs(event.accel_y or 0) <= 4.0 for event in events)


def test_poor_pavement_increases_vertical_motion() -> None:
    poor = simulate(route(2.0))
    good = simulate(route(9.0))

    poor_mean = sum(abs(event.accel_z) for event in poor) / len(poor)
    good_mean = sum(abs(event.accel_z) for event in good) / len(good)

    assert poor_mean > good_mean * 2


def test_speed_hump_creates_visible_vertical_impact() -> None:
    smooth = simulate(route(8.0))
    with_hump = simulate(route(8.0, (50.0,)))

    assert max(event.accel_z for event in with_hump) > max(
        event.accel_z for event in smooth
    ) + 1.0
    assert [event.event_id for event in with_hump] == [
        event.event_id for event in simulate(route(8.0, (50.0,)))
    ]


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
        ) -> RoutePlan:
            assert self.clock.times[-1] == planned_at
            self.planned_at = planned_at
            return route(8.0)

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
