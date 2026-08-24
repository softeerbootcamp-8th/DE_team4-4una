from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sensor_producer.domain import TripRecord
from sensor_producer.sampling import (
    HourlySamplingPlan,
    build_hourly_sampling_plan,
    source_hour,
)

NYC = ZoneInfo("America/New_York")


def trip(
    trip_id: str,
    pickup: datetime,
    duration_seconds: int,
) -> TripRecord:
    return TripRecord(
        trip_id=trip_id,
        request_datetime=pickup - timedelta(minutes=1),
        pickup_datetime=pickup,
        dropoff_datetime=pickup + timedelta(seconds=duration_seconds),
        pu_location_id=181,
        do_location_id=182,
        trip_miles=1.0,
    )


def test_sampling_plan_caps_busy_hours_without_filling_quiet_hours(
    tmp_path: Path,
) -> None:
    path = tmp_path / "trips.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "pickup_datetime": datetime(2024, 2, 1, 10, 0),  # noqa: DTZ001
                    "dropoff_datetime": datetime(2024, 2, 1, 10, 1, 40),  # noqa: DTZ001
                },
                {
                    "pickup_datetime": datetime(2024, 2, 1, 10, 10),  # noqa: DTZ001
                    "dropoff_datetime": datetime(2024, 2, 1, 10, 11, 40),  # noqa: DTZ001
                },
                {
                    "pickup_datetime": datetime(2024, 2, 1, 11, 0),  # noqa: DTZ001
                    "dropoff_datetime": datetime(2024, 2, 1, 11, 0, 50),  # noqa: DTZ001
                },
            ]
        ),
        path,
    )

    plan = build_hourly_sampling_plan(
        path,
        sample_hz=10,
        target_events_per_hour=1_000,
        seed=4,
        cycle_hours=2,
        prepared=True,
    )

    assert plan.maximum_projected_events <= 1_000
    hour_10 = source_hour(datetime(2024, 2, 1, 10, tzinfo=NYC))
    hour_11 = source_hour(datetime(2024, 2, 1, 11, tzinfo=NYC))
    assert plan.hour_ratios[hour_10] < plan.base_ratio
    assert plan.hour_ratios[hour_11] == pytest.approx(
        plan.base_ratio
    )


def test_trip_sampling_is_deterministic_and_uses_the_strictest_hour() -> None:
    pickup = datetime(2024, 2, 1, 10, 59, tzinfo=NYC)
    plan = HourlySamplingPlan(
        sample_hz=10,
        target_events_per_hour=1_000,
        seed=4,
        base_ratio=0.5,
        hour_ratios={
            source_hour(datetime(2024, 2, 1, 10, tzinfo=NYC)): 0.4,
            source_hour(datetime(2024, 2, 1, 11, tzinfo=NYC)): 0.2,
        },
        full_event_count=2_000,
        cycle_hours=2,
        maximum_projected_events=1_000,
    )
    selected_trip = trip("trip-a", pickup, duration_seconds=120)

    assert plan.probability_for(selected_trip) == pytest.approx(0.2)
    assert plan.includes(selected_trip) == plan.includes(selected_trip)
