from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sensor_producer.domain import RoadSegment, SimulationConfig
from sensor_producer.nyc_data import NYC_TIMEZONE
from sensor_producer.prepared_replay import (
    MANIFEST_FILE_NAME,
    ROUTES_FILE_NAME,
    TRIPS_FILE_NAME,
    ReplayBounds,
    iter_prepared_replay,
    prepare_replay_bundle,
    source_anchor_for_wall_time,
    stable_worker_index,
)
from sensor_producer.publisher import MemoryPublisher
from sensor_producer.routing import RoadRouter
from sensor_producer.simulation import ReplayCoordinator
from shapely.geometry import LineString, box


def source_time(minute: int) -> datetime:
    return datetime(2024, 2, 1, 10, minute)  # noqa: DTZ001


def source_row(minute: int, pickup_zone: int = 181) -> dict[str, object]:
    request_time = source_time(minute)
    return {
        "request_datetime": request_time,
        "pickup_datetime": request_time + timedelta(minutes=1),
        "dropoff_datetime": request_time + timedelta(minutes=1, seconds=20),
        "PULocationID": pickup_zone,
        "DOLocationID": 181,
        "trip_miles": 100 / 1609.344,
    }


def write_source(path: Path) -> None:
    pq.write_table(
        pa.Table.from_pylist([source_row(2), source_row(1, 999), source_row(0)]),
        path,
    )


def road_router() -> RoadRouter:
    segment = RoadSegment(
        segment_id="segment-1",
        from_node_id="node-1",
        to_node_id="node-2",
        traffic_direction="T",
        street_name="TEST STREET",
        geometry=LineString([(-74.0, 40.0), (-73.999, 40.0)]),
        length_m=100.0,
        posted_speed_mph=25.0,
        curve_radius_m=None,
        pavement_rating=8.0,
    )
    return RoadRouter([segment])


def prepare_test_bundle(tmp_path: Path, name: str = "replay"):
    source_path = tmp_path / "source.parquet"
    if not source_path.exists():
        write_source(source_path)
    road_path = tmp_path / "road.parquet"
    zone_path = tmp_path / "zones.parquet"
    for path in (road_path, zone_path):
        path.write_bytes(b"fixture")

    router = road_router()
    zones = {181: box(-74.001, 39.999, -73.998, 40.001)}
    output_dir = tmp_path / name
    manifest = prepare_replay_bundle(
        source_path,
        router,
        zones,
        output_dir,
        date(2026, 8, 19),
        road_environment_path=road_path,
        taxi_zone_path=zone_path,
    )
    return output_dir, router, zones, manifest


def test_prepared_replay_sorts_trips_and_reuses_one_route_per_od(
    tmp_path: Path,
) -> None:
    output_dir, router, _, manifest = prepare_test_bundle(tmp_path)

    trip_rows = pq.read_table(output_dir / TRIPS_FILE_NAME).to_pylist()
    route_rows = pq.read_table(output_dir / ROUTES_FILE_NAME).to_pylist()
    prepared = list(iter_prepared_replay(output_dir, router, date(2026, 8, 19)))

    assert [row["request_datetime"].minute for row in trip_rows] == [0, 1, 2]
    assert len(route_rows) == 2
    assert [row["skip_reason"] for row in route_rows] == [
        None,
        "PICKUP_ZONE_NOT_FOUND",
    ]
    planned = [item for item in prepared if item.route is not None]
    assert len(planned) == 2
    assert planned[0].route.segment_ids == planned[1].route.segment_ids
    assert manifest["trip_count"] == 3
    assert manifest["route_template_count"] == 2
    assert manifest["planned_trip_count"] == 2
    assert (output_dir / MANIFEST_FILE_NAME).is_file()
    with pytest.raises(ValueError, match="road snapshot"):
        list(iter_prepared_replay(output_dir, router, date(2026, 8, 20)))


def test_prepared_replay_is_deterministic(tmp_path: Path) -> None:
    first_dir, _, _, _ = prepare_test_bundle(tmp_path, "first")
    second_dir, _, _, _ = prepare_test_bundle(tmp_path, "second")

    assert pq.read_table(first_dir / TRIPS_FILE_NAME).equals(
        pq.read_table(second_dir / TRIPS_FILE_NAME)
    )
    assert pq.read_table(first_dir / ROUTES_FILE_NAME).equals(
        pq.read_table(second_dir / ROUTES_FILE_NAME)
    )


def test_prepared_replay_assigns_each_trip_to_one_stable_worker(tmp_path: Path) -> None:
    output_dir, router, _, _ = prepare_test_bundle(tmp_path)
    workers = [
        list(
            iter_prepared_replay(
                output_dir,
                router,
                date(2026, 8, 19),
                worker_index=worker_index,
                worker_count=2,
            )
        )
        for worker_index in range(2)
    ]
    trip_ids = [item.trip.trip_id for items in workers for item in items]

    assert len(trip_ids) == 3
    assert len(set(trip_ids)) == 3
    assert all(
        stable_worker_index(item.trip.trip_id, 2) == index
        for index, items in enumerate(workers)
        for item in items
    )


def test_source_anchor_matches_current_new_york_weekday_and_clock() -> None:
    bounds = ReplayBounds(
        datetime(2024, 2, 1, 0, tzinfo=NYC_TIMEZONE),
        datetime(2024, 2, 29, 23, 59, tzinfo=NYC_TIMEZONE),
    )

    source_anchor = source_anchor_for_wall_time(
        bounds,
        datetime(2026, 8, 24, 10, 15, 30, tzinfo=UTC),
    )

    assert source_anchor.weekday() == 0
    assert (source_anchor.hour, source_anchor.minute, source_anchor.second) == (
        6,
        15,
        30,
    )
    assert source_anchor.date() == date(2024, 2, 26)


def test_source_anchor_uses_actual_replay_interval_across_month_boundary() -> None:
    bounds = ReplayBounds(
        datetime(2024, 1, 31, 23, 42, tzinfo=NYC_TIMEZONE),
        datetime(2024, 2, 29, 23, 59, tzinfo=NYC_TIMEZONE),
    )

    source_anchor = source_anchor_for_wall_time(
        bounds,
        datetime(2026, 8, 24, 10, 15, 30, tzinfo=UTC),
    )

    assert source_anchor == datetime(
        2024, 2, 26, 6, 15, 30, tzinfo=NYC_TIMEZONE
    )
    assert bounds.cycle_start == datetime(
        2024, 1, 31, 23, 42, tzinfo=NYC_TIMEZONE
    )
    assert bounds.cycle_hours == 697


def test_prepared_replay_rotates_the_month_from_source_anchor(tmp_path: Path) -> None:
    output_dir, router, _, _ = prepare_test_bundle(tmp_path)

    replay = list(
        iter_prepared_replay(
            output_dir,
            router,
            date(2026, 8, 19),
            start_at=datetime(2024, 2, 1, 10, 1, 30, tzinfo=NYC_TIMEZONE),
        )
    )

    request_times = [item.trip.request_datetime for item in replay]
    assert request_times == sorted(request_times)
    assert [value.minute for value in request_times] == [2, 2, 3]
    assert request_times[1] > source_time(2).replace(tzinfo=NYC_TIMEZONE)


def test_prepared_replay_bypasses_runtime_route_search(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_dir, router, zones, _ = prepare_test_bundle(tmp_path)
    prepared = list(iter_prepared_replay(output_dir, router, date(2026, 8, 19)))

    def fail_if_called(*args, **kwargs):
        raise AssertionError("runtime route search must be bypassed")

    monkeypatch.setattr(router, "plan_for_zones", fail_if_called)
    publisher = MemoryPublisher()
    result = ReplayCoordinator(
        router,
        zones,
        publisher,
        SimulationConfig("prepared-run", sample_hz=1, time_scale=0),
        utc_now=lambda: datetime(2026, 8, 24, 1, tzinfo=UTC),
    ).replay(prepared)

    assert result.trips_attempted == 3
    assert result.trips_planned == 2
    assert result.trips_skipped == 1
    assert {event.trip_id for event in publisher.events} == {
        item.trip.trip_id for item in prepared if item.route is not None
    }
