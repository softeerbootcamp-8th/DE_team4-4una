"""Build a deterministic Trip sample that respects an hourly event budget."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import duckdb

from sensor_producer.domain import TripRecord
from sensor_producer.nyc_data import NYC_TIMEZONE, parse_nyc_datetime

DEFAULT_HOURLY_EVENT_TARGET = 10_000_000
SAMPLING_POLICY = "hourly-trip-budget-v1"


@dataclass(frozen=True, slots=True)
class HourlySamplingPlan:
    """Trip-level probabilities derived from full-volume hourly estimates."""

    sample_hz: int
    target_events_per_hour: int
    seed: int
    base_ratio: float
    hour_ratios: dict[datetime, float]
    full_event_count: int
    cycle_hours: int
    maximum_projected_events: int

    def probability_for(self, trip: TripRecord) -> float:
        # 여러 시간에 걸친 Trip은 가장 혼잡한 시간의 비율을 적용해 상한을 보호한다
        current = source_hour(trip.pickup_datetime)
        last = source_hour(trip.dropoff_datetime)
        probability = self.base_ratio
        while current <= last:
            probability = min(
                probability,
                self.hour_ratios.get(current, self.base_ratio),
            )
            current += timedelta(hours=1)
        return probability

    def includes(self, trip: TripRecord) -> bool:
        digest = hashlib.blake2b(
            f"{SAMPLING_POLICY}:{self.seed}:{trip.trip_id}".encode(),
            digest_size=8,
        ).digest()
        draw = int.from_bytes(digest, "big") / 2**64
        return draw < self.probability_for(trip)

    def summary(self) -> dict[str, object]:
        return {
            "policy": SAMPLING_POLICY,
            "sample_hz": self.sample_hz,
            "target_events_per_hour": self.target_events_per_hour,
            "seed": self.seed,
            "base_ratio": self.base_ratio,
            "full_event_count": self.full_event_count,
            "cycle_hours": self.cycle_hours,
            "maximum_projected_events": self.maximum_projected_events,
        }


def build_hourly_sampling_plan(
    trips_path: Path,
    *,
    sample_hz: int,
    target_events_per_hour: int,
    seed: int,
    cycle_hours: int,
    prepared: bool,
) -> HourlySamplingPlan:
    if sample_hz <= 0:
        raise ValueError("sample_hz must be positive")
    if target_events_per_hour <= 0:
        raise ValueError("target_events_per_hour must be positive")
    if cycle_hours <= 0:
        raise ValueError("cycle_hours must be positive")

    pickup_column = "pickup_datetime"
    dropoff_column = "dropoff_datetime"
    validity = ""
    if not prepared:
        validity = """
            WHERE request_datetime IS NOT NULL
              AND pickup_datetime IS NOT NULL
              AND dropoff_datetime IS NOT NULL
              AND PULocationID IS NOT NULL
              AND DOLocationID IS NOT NULL
              AND trip_miles IS NOT NULL
              AND request_datetime <= pickup_datetime
              AND pickup_datetime < dropoff_datetime
              AND trip_miles > 0
              AND isfinite(trip_miles)
        """

    escaped_path = str(trips_path).replace("'", "''")
    query = f"""
        WITH valid AS (
            SELECT {pickup_column} AS pickup_at, {dropoff_column} AS dropoff_at
            FROM read_parquet('{escaped_path}')
            {validity}
        ), expanded AS (
            SELECT
                hour_start,
                pickup_at,
                dropoff_at
            FROM valid,
            UNNEST(
                generate_series(
                    date_trunc('hour', pickup_at),
                    date_trunc('hour', dropoff_at),
                    INTERVAL 1 HOUR
                )
            ) AS generated(hour_start)
        )
        SELECT
            hour_start,
            SUM(
                GREATEST(
                    0,
                    epoch(
                        LEAST(dropoff_at, hour_start + INTERVAL 1 HOUR)
                        - GREATEST(pickup_at, hour_start)
                    )
                ) * ?
                + CASE
                    WHEN dropoff_at >= hour_start
                     AND dropoff_at < hour_start + INTERVAL 1 HOUR
                    THEN 1 ELSE 0
                  END
            ) AS expected_events
        FROM expanded
        GROUP BY hour_start
        ORDER BY hour_start
    """
    connection = duckdb.connect()
    try:
        rows = connection.execute(query, [sample_hz]).fetchall()
    finally:
        connection.close()
    if not rows:
        raise ValueError("cannot build a sampling plan from an empty replay")

    hourly_events = {
        source_hour(parse_nyc_datetime(hour)): max(0, round(events))
        for hour, events in rows
    }
    full_event_count = sum(hourly_events.values())
    average_events = full_event_count / cycle_hours
    base_ratio = min(1.0, target_events_per_hour / average_events)
    hour_ratios = {
        hour: min(
            base_ratio,
            1.0 if events == 0 else target_events_per_hour / events,
        )
        for hour, events in hourly_events.items()
    }
    maximum_projected_events = max(
        round(events * hour_ratios[hour])
        for hour, events in hourly_events.items()
    )
    return HourlySamplingPlan(
        sample_hz=sample_hz,
        target_events_per_hour=target_events_per_hour,
        seed=seed,
        base_ratio=base_ratio,
        hour_ratios=hour_ratios,
        full_event_count=full_event_count,
        cycle_hours=cycle_hours,
        maximum_projected_events=maximum_projected_events,
    )


def source_hour(value: datetime) -> datetime:
    return value.astimezone(NYC_TIMEZONE).replace(
        minute=0,
        second=0,
        microsecond=0,
        tzinfo=None,
    )
