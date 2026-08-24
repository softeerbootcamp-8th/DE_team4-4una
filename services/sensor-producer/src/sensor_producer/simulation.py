"""Deterministic vehicle motion and wall-clock replay coordination."""

from __future__ import annotations

import hashlib
import heapq
import logging
import math
import time
import uuid
from collections import Counter
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta

import numpy as np
import shapely
from de4_core import SensorEvent

from sensor_producer.domain import (
    BASELINE_DAMPING,
    VEHICLE_MIXES,
    VEHICLE_PROFILES,
    PreparedTrip,
    RouteLeg,
    RoutePlan,
    SimulationConfig,
    TripRecord,
    VehicleProfile,
)
from sensor_producer.errors import TripInfeasibleError, TripSkipReason
from sensor_producer.publisher import EventPublisher
from sensor_producer.routing import METERS_PER_MILE, RoadRouter

EVENT_NAMESPACE = uuid.UUID("a8ad2dcf-cbb4-4ca8-9173-a48958caa85e")
TIMESTAMP_POLICY = "current-ny-clock-to-run-utc-anchor-v2"
MPH_TO_MPS = 0.44704
DEFAULT_SPEED_LIMIT_MPH = 25.0
MAX_ACCELERATION_PHASE_SECONDS = 8.0
# 차량별 보정값이 없는 PoC용 bicycle model 가정이다.
REPRESENTATIVE_WHEELBASE_M = 2.8
MIN_STEERING_SPEED_MPS = 0.5
# 차체 흔들림은 휠 레벨 거칠기(1.7 rad/m)보다 훨씬 긴 파장으로 나타나는 저주파 성분이다.
SWAY_WAVENUMBER_RAD_PER_M = 0.35
SWAY_WEIGHT = 0.25
MAX_STEERING_ANGLE_DEG = 35.0
STEERING_ANGLE_DEADBAND_DEG = 0.05
SAMPLE_BATCH_SIZE = 512
logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ReplayResult:
    trips_attempted: int
    trips_planned: int
    trips_skipped: int
    skip_reason_counts: dict[str, int]
    events_published: int
    unique_segments: int
    rated_samples: int
    hump_samples: int
    profile_trip_counts: dict[str, int]
    final_replay_lag_seconds: float = 0.0
    max_replay_lag_seconds: float = 0.0
    observed_segment_ids: frozenset[str] = frozenset()

    @property
    def trip_skip_ratio(self) -> float:
        if self.trips_attempted == 0:
            return 0.0
        return self.trips_skipped / self.trips_attempted


@dataclass(frozen=True, slots=True)
class ReplayTimeline:
    """Map one historical source timeline onto one shared UTC run timeline."""

    source_anchor: datetime
    wall_anchor: datetime

    def __post_init__(self) -> None:
        if self.source_anchor.utcoffset() is None:
            raise ValueError("source_anchor must be timezone-aware")
        if self.wall_anchor.utcoffset() is None:
            raise ValueError("wall_anchor must be timezone-aware")

    def map(self, source_time: datetime) -> datetime:
        if source_time.utcoffset() is None:
            raise ValueError("source_time must be timezone-aware")
        return (self.wall_anchor + (source_time - self.source_anchor)).astimezone(UTC)


def rebase_trip_to_replay_timeline(
    trip: TripRecord,
    timeline: ReplayTimeline,
) -> TripRecord:
    return TripRecord(
        trip_id=trip.trip_id,
        request_datetime=timeline.map(trip.request_datetime),
        pickup_datetime=timeline.map(trip.pickup_datetime),
        dropoff_datetime=timeline.map(trip.dropoff_datetime),
        pu_location_id=trip.pu_location_id,
        do_location_id=trip.do_location_id,
        trip_miles=trip.trip_miles,
    )


def source_sensor_schedule_time(
    source_trip: TripRecord,
    replay_trip: TripRecord,
    event: SensorEvent,
) -> datetime:
    elapsed = event.event_time - replay_trip.pickup_datetime
    return source_trip.pickup_datetime + elapsed


@dataclass(frozen=True, slots=True)
class SamplePosition:
    leg: RouteLeg
    distance_in_leg_m: float


@dataclass(frozen=True, slots=True)
class MotionState:
    distance_m: float
    speed_mps: float


@dataclass(frozen=True, slots=True)
class SimulatedSample:
    event: SensorEvent
    rated: bool
    near_hump: bool


@dataclass(frozen=True, slots=True)
class SpeedProfile:
    """승객 운행 한 건의 가속·정속·감속 프로파일."""

    route_length_m: float
    duration_seconds: float
    ramp_seconds: float
    leg_lengths_m: tuple[float, ...]
    leg_speeds_mps: tuple[float, ...]
    leg_speed_limits_mps: tuple[float, ...]

    @classmethod
    def for_route(cls, route: RoutePlan, duration_seconds: float) -> SpeedProfile:
        leg_lengths_m = tuple(leg.length_m for leg in route.legs)
        leg_speed_limits_mps = tuple(
            (
                leg.posted_speed_mph
                if leg.posted_speed_mph is not None and leg.posted_speed_mph > 0
                else DEFAULT_SPEED_LIMIT_MPH
            )
            * MPH_TO_MPS
            for leg in route.legs
        )
        ramp_distance_limit = (
            leg_lengths_m[0] / leg_speed_limits_mps[0]
            if len(leg_lengths_m) == 1
            else min(
                2 * leg_lengths_m[0] / leg_speed_limits_mps[0],
                2 * leg_lengths_m[-1] / leg_speed_limits_mps[-1],
            )
        )
        ramp_seconds = min(
            MAX_ACCELERATION_PHASE_SECONDS,
            duration_seconds / 3,
            ramp_distance_limit,
        )
        cruise_seconds = duration_seconds - ramp_seconds
        if sum(
            length / limit
            for length, limit in zip(leg_lengths_m, leg_speed_limits_mps, strict=True)
        ) > cruise_seconds:
            raise TripInfeasibleError(
                TripSkipReason.SPEED_PROFILE_INFEASIBLE,
                "route cannot be completed within TLC duration and posted speed limit",
            )

        lower_speed = 0.0
        upper_speed = max(leg_speed_limits_mps)
        # 각 구간의 제한속도를 지키면서 TLC 운행 시간을 맞출 목표속도를 찾는다.
        for _ in range(60):
            target_speed = (lower_speed + upper_speed) / 2
            travel_seconds = sum(
                length / min(target_speed, limit)
                for length, limit in zip(
                    leg_lengths_m, leg_speed_limits_mps, strict=True
                )
            )
            if travel_seconds > cruise_seconds:
                lower_speed = target_speed
            else:
                upper_speed = target_speed
        leg_speeds_mps = tuple(
            min(upper_speed, limit) for limit in leg_speed_limits_mps
        )
        return cls(
            route.total_length_m,
            duration_seconds,
            ramp_seconds,
            leg_lengths_m,
            leg_speeds_mps,
            leg_speed_limits_mps,
        )

    def state_at(self, elapsed_seconds: float) -> MotionState:
        elapsed = clamp(elapsed_seconds, 0.0, self.duration_seconds)
        first_speed = self.leg_speeds_mps[0]
        last_speed = self.leg_speeds_mps[-1]
        acceleration_distance = first_speed * self.ramp_seconds / 2
        deceleration_distance = last_speed * self.ramp_seconds / 2
        if elapsed >= self.duration_seconds:
            return MotionState(self.route_length_m, 0.0)
        if elapsed < self.ramp_seconds:
            progress = elapsed / self.ramp_seconds
            speed = first_speed * smoothstep(progress)
            # smoothstep 적분값을 사용해 속도와 누적 이동거리를 일치시킨다.
            distance = first_speed * self.ramp_seconds * smoothstep_integral(
                progress
            )
            return MotionState(distance, speed)
        if elapsed < self.duration_seconds - self.ramp_seconds:
            remaining_seconds = elapsed - self.ramp_seconds
            distance = acceleration_distance
            last_index = len(self.leg_lengths_m) - 1
            for index, (length, speed) in enumerate(
                zip(self.leg_lengths_m, self.leg_speeds_mps, strict=True)
            ):
                traversable = length
                if index == 0:
                    traversable -= acceleration_distance
                if index == last_index:
                    traversable -= deceleration_distance
                phase_seconds = traversable / speed
                if remaining_seconds <= phase_seconds:
                    return MotionState(distance + remaining_seconds * speed, speed)
                distance += traversable
                remaining_seconds -= phase_seconds
            return MotionState(self.route_length_m - deceleration_distance, last_speed)

        progress = (
            elapsed - (self.duration_seconds - self.ramp_seconds)
        ) / self.ramp_seconds
        distance_in_phase = last_speed * self.ramp_seconds * (
            progress - smoothstep_integral(progress)
        )
        return MotionState(
            self.route_length_m - deceleration_distance + distance_in_phase,
            last_speed * (1 - smoothstep(progress)),
        )

    def states_for(self, elapsed_seconds: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        """Calculate one chunk of distance and speed values in native array code."""
        elapsed = np.clip(elapsed_seconds, 0.0, self.duration_seconds)
        distances = np.empty_like(elapsed)
        speeds = np.empty_like(elapsed)
        first_speed = self.leg_speeds_mps[0]
        last_speed = self.leg_speeds_mps[-1]
        acceleration_distance = first_speed * self.ramp_seconds / 2
        deceleration_distance = last_speed * self.ramp_seconds / 2

        finished = elapsed >= self.duration_seconds
        accelerating = (~finished) & (elapsed < self.ramp_seconds)
        decelerating = (~finished) & (
            elapsed >= self.duration_seconds - self.ramp_seconds
        )
        cruising = ~(finished | accelerating | decelerating)

        progress = elapsed[accelerating] / self.ramp_seconds
        smooth = 3 * progress**2 - 2 * progress**3
        distances[accelerating] = (
            first_speed * self.ramp_seconds * (progress**3 - progress**4 / 2)
        )
        speeds[accelerating] = first_speed * smooth

        if np.any(cruising):
            leg_lengths = np.asarray(self.leg_lengths_m, dtype=np.float64)
            leg_speeds = np.asarray(self.leg_speeds_mps, dtype=np.float64)
            traversable = leg_lengths.copy()
            traversable[0] -= acceleration_distance
            traversable[-1] -= deceleration_distance
            phase_seconds = traversable / leg_speeds
            cumulative_seconds = np.cumsum(phase_seconds)
            cumulative_distances = np.cumsum(traversable)
            remaining = elapsed[cruising] - self.ramp_seconds
            leg_indices = np.searchsorted(cumulative_seconds, remaining, side="left")
            leg_indices = np.clip(leg_indices, 0, len(leg_speeds) - 1)
            previous_seconds = np.where(
                leg_indices == 0,
                0.0,
                cumulative_seconds[np.maximum(0, leg_indices - 1)],
            )
            previous_distances = np.where(
                leg_indices == 0,
                0.0,
                cumulative_distances[np.maximum(0, leg_indices - 1)],
            )
            speeds[cruising] = leg_speeds[leg_indices]
            distances[cruising] = (
                acceleration_distance
                + previous_distances
                + (remaining - previous_seconds) * leg_speeds[leg_indices]
            )

        progress = (
            elapsed[decelerating] - (self.duration_seconds - self.ramp_seconds)
        ) / self.ramp_seconds
        smooth = 3 * progress**2 - 2 * progress**3
        distances[decelerating] = (
            self.route_length_m
            - deceleration_distance
            + last_speed
            * self.ramp_seconds
            * (progress - (progress**3 - progress**4 / 2))
        )
        speeds[decelerating] = last_speed * (1 - smooth)
        distances[finished] = self.route_length_m
        speeds[finished] = 0.0
        return distances, speeds


class MotionSimulator:
    """Generate plausible, deterministic signals rather than calibrated physics."""

    def generate(
        self,
        trip: TripRecord,
        route: RoutePlan,
        profile: VehicleProfile,
        config: SimulationConfig,
    ) -> Iterator[SensorEvent]:
        for sample in self.generate_with_metadata(trip, route, profile, config):
            yield sample.event

    def generate_with_metadata(
        self,
        trip: TripRecord,
        route: RoutePlan,
        profile: VehicleProfile,
        config: SimulationConfig,
    ) -> Iterator[SimulatedSample]:
        duration = trip.passenger_duration_seconds
        sample_count = max(2, int(duration * config.sample_hz) + 1)
        previous_speed = 0.0
        previous_accel_x = 0.0
        previous_accel_y = 0.0
        previous_accel_z = 0.0
        previous_heading: float | None = None
        phase = deterministic_phase(trip.trip_id, config.seed)
        speed_profile = SpeedProfile.for_route(route, duration)

        for chunk_start in range(0, sample_count, SAMPLE_BATCH_SIZE):
            chunk_stop = min(sample_count, chunk_start + SAMPLE_BATCH_SIZE)
            sequences = np.arange(chunk_start, chunk_stop, dtype=np.int64)
            elapsed = np.minimum(duration, sequences * config.interval_seconds)
            distances, speeds = speed_profile.states_for(elapsed)
            leg_indices, local_distances, longitudes, latitudes, headings = (
                route_sample_arrays(route, distances)
            )

            previous_speeds = np.concatenate(([previous_speed], speeds[:-1]))
            accel_x = (
                (speeds - previous_speeds)
                / config.interval_seconds
                * profile.longitudinal_response
            )
            previous_headings = np.concatenate(
                (
                    [headings[0] if previous_heading is None else previous_heading],
                    headings[:-1],
                )
            )
            heading_delta = (headings - previous_headings + 180.0) % 360.0 - 180.0
            yaw_rate = np.radians(heading_delta) / config.interval_seconds
            accel_y = np.clip(
                speeds * yaw_rate * profile.lateral_response,
                -4.0,
                4.0,
            )
            steering_angles = steering_angle_array(speeds, yaw_rate)
            accel_z, near_hump = vertical_acceleration_array(
                route,
                leg_indices,
                local_distances,
                distances,
                speeds,
                elapsed,
                phase,
                profile,
            )
            steering_vibration = steering_vibration_array(
                speeds,
                accel_y,
                accel_z,
                elapsed,
                phase,
                profile,
            )
            if chunk_start == 0:
                accel_x[0] = 0.0
                accel_y[0] = 0.0
                steering_angles[0] = 0.0
            previous_accel_x_values = np.concatenate(([previous_accel_x], accel_x[:-1]))
            previous_accel_y_values = np.concatenate(([previous_accel_y], accel_y[:-1]))
            previous_accel_z_values = np.concatenate(([previous_accel_z], accel_z[:-1]))
            jerk_x = (accel_x - previous_accel_x_values) / config.interval_seconds
            jerk_y = (accel_y - previous_accel_y_values) / config.interval_seconds
            jerk_z = (accel_z - previous_accel_z_values) / config.interval_seconds
            if chunk_start == 0:
                jerk_x[0] = 0.0
                jerk_y[0] = 0.0
                jerk_z[0] = 0.0

            for offset, sequence_value in enumerate(sequences):
                sequence = int(sequence_value)
                event_time = (
                    trip.pickup_datetime + timedelta(seconds=float(elapsed[offset]))
                ).astimezone(UTC)
                event_id = str(
                    uuid.uuid5(
                        EVENT_NAMESPACE,
                        f"{config.run_id}:{trip.trip_id}:"
                        f"{profile.vehicle_profile_id}:{sequence}",
                    )
                )
                event = SensorEvent(
                    event_id=event_id,
                    vehicle_id=f"vehicle-{profile.vehicle_profile_id}-{trip.trip_id[:8]}",
                    vehicle_profile_id=profile.vehicle_profile_id,
                    trip_id=trip.trip_id,
                    trip_seq=sequence,
                    event_time=event_time,
                    latitude=float(latitudes[offset]),
                    longitude=float(longitudes[offset]),
                    speed_mps=max(0.0, float(speeds[offset])),
                    heading=float(headings[offset]),
                    steering_angle=float(steering_angles[offset]),
                    accel_x=float(accel_x[offset]),
                    accel_y=float(accel_y[offset]),
                    accel_z=float(accel_z[offset]),
                    jerk=float(jerk_x[offset]),
                    jerk_x=float(jerk_x[offset]),
                    jerk_y=float(jerk_y[offset]),
                    jerk_z=float(jerk_z[offset]),
                    steering_vibration=float(steering_vibration[offset]),
                    _run_id=config.run_id,
                )
                leg = route.legs[int(leg_indices[offset])]
                yield SimulatedSample(
                    event,
                    rated=leg.pavement_rating is not None,
                    near_hump=bool(near_hump[offset]),
                )

            previous_speed = float(speeds[-1])
            previous_accel_x = float(accel_x[-1])
            previous_accel_y = float(accel_y[-1])
            previous_accel_z = float(accel_z[-1])
            previous_heading = float(headings[-1])


def route_sample_arrays(
    route: RoutePlan,
    distances_m: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Locate a chunk on the route without scanning every leg per sample."""
    leg_lengths = np.asarray([leg.length_m for leg in route.legs], dtype=np.float64)
    cumulative = np.cumsum(leg_lengths)
    clipped = np.clip(distances_m, 0.0, route.total_length_m)
    leg_indices = np.searchsorted(cumulative, clipped, side="left")
    leg_indices = np.clip(leg_indices, 0, len(route.legs) - 1)
    previous = np.concatenate(([0.0], cumulative[:-1]))
    local_distances = clipped - previous[leg_indices]
    longitudes = np.empty_like(clipped)
    latitudes = np.empty_like(clipped)
    headings = np.empty_like(clipped)

    for leg_index in np.unique(leg_indices):
        mask = leg_indices == leg_index
        leg = route.legs[int(leg_index)]
        fractions = np.clip(local_distances[mask] / leg.length_m, 0.0, 1.0)
        points = shapely.line_interpolate_point(
            leg.geometry,
            fractions,
            normalized=True,
        )
        delta = min(0.001, 1 / max(1000, len(leg.geometry.coords) * 100))
        before = shapely.line_interpolate_point(
            leg.geometry,
            np.clip(fractions - delta, 0.0, 1.0),
            normalized=True,
        )
        after = shapely.line_interpolate_point(
            leg.geometry,
            np.clip(fractions + delta, 0.0, 1.0),
            normalized=True,
        )
        before_lon = shapely.get_x(before)
        before_lat = shapely.get_y(before)
        after_lon = shapely.get_x(after)
        after_lat = shapely.get_y(after)
        lon1 = np.radians(before_lon)
        lat1 = np.radians(before_lat)
        lon2 = np.radians(after_lon)
        lat2 = np.radians(after_lat)
        delta_lon = lon2 - lon1
        x = np.sin(delta_lon) * np.cos(lat2)
        y = np.cos(lat1) * np.sin(lat2) - (
            np.sin(lat1) * np.cos(lat2) * np.cos(delta_lon)
        )
        longitudes[mask] = shapely.get_x(points)
        latitudes[mask] = shapely.get_y(points)
        headings[mask] = np.degrees(np.arctan2(x, y)) % 360.0
    return leg_indices, local_distances, longitudes, latitudes, headings


def vertical_acceleration_array(
    route: RoutePlan,
    leg_indices: np.ndarray,
    local_distances_m: np.ndarray,
    route_distances_m: np.ndarray,
    speeds_mps: np.ndarray,
    elapsed_seconds: np.ndarray,
    phase: float,
    profile: VehicleProfile,
) -> tuple[np.ndarray, np.ndarray]:
    ratings_by_leg = np.asarray(
        [
            np.nan if leg.pavement_rating is None else leg.pavement_rating
            for leg in route.legs
        ]
    )
    ratings = ratings_by_leg[leg_indices]
    roughness = np.where(np.isnan(ratings), 0.12, 0.08 + (10 - ratings) / 9 * 0.52)
    pavement = (
        profile.vertical_response
        * roughness
        * (
            np.sin(route_distances_m * 1.7 + phase)
            + 0.35 * np.sin(route_distances_m * 4.1 + phase / 2)
        )
    )
    sway = (
        profile.vertical_response
        * roughness
        * SWAY_WEIGHT
        * (BASELINE_DAMPING / profile.damping)
        * np.sin(route_distances_m * SWAY_WAVENUMBER_RAD_PER_M + phase / 3)
    )
    hump_response = np.zeros_like(route_distances_m)
    near_hump = np.zeros(route_distances_m.shape, dtype=np.bool_)
    for leg_index in np.unique(leg_indices):
        leg_mask = leg_indices == leg_index
        leg = route.legs[int(leg_index)]
        for hump_distance in leg.hump_distances_m:
            offset = local_distances_m - hump_distance
            near_hump[leg_mask & (np.abs(offset) <= 2.0)] = True
            active = leg_mask & (np.abs(offset) <= 8.0)
            if not np.any(active):
                continue
            selected_offset = offset[active]
            impact = np.exp(-((selected_offset / 1.8) ** 2))
            ring = np.where(
                selected_offset > 0,
                np.sin(elapsed_seconds[active] * 18)
                * np.exp(-selected_offset * profile.damping),
                0.0,
            )
            hump_response[active] += (
                profile.vertical_response
                * np.maximum(0.5, speeds_mps[active] / 5)
                * (1.8 * impact + 0.35 * ring)
            )
    return pavement + sway + hump_response, near_hump


def steering_vibration_array(
    speeds_mps: np.ndarray,
    accel_y: np.ndarray,
    accel_z: np.ndarray,
    elapsed_seconds: np.ndarray,
    phase: float,
    profile: VehicleProfile,
) -> np.ndarray:
    moving_factor = np.clip(speeds_mps / 1.5, 0.0, 1.0)
    speed_factor = np.clip(speeds_mps / 8.0, 0.0, 1.5)
    road_component = np.abs(accel_z) * (0.20 + 0.25 * speed_factor) * moving_factor
    steering_component = np.abs(accel_y) * 0.14
    carrier = 0.85 + 0.15 * np.abs(np.sin(elapsed_seconds * 28.0 + phase))
    return (
        profile.steering_vibration_response
        * (road_component + steering_component)
        * carrier
    )


def steering_angle_array(speeds_mps: np.ndarray, yaw_rate: np.ndarray) -> np.ndarray:
    safe_speed = np.maximum(speeds_mps, MIN_STEERING_SPEED_MPS)
    angles = np.degrees(np.arctan(REPRESENTATIVE_WHEELBASE_M * yaw_rate / safe_speed))
    angles = np.where(speeds_mps < MIN_STEERING_SPEED_MPS, 0.0, angles)
    angles = np.where(np.abs(angles) < STEERING_ANGLE_DEADBAND_DEG, 0.0, angles)
    return np.clip(angles, -MAX_STEERING_ANGLE_DEG, MAX_STEERING_ANGLE_DEG)


class ReplayClock:
    def __init__(
        self,
        time_scale: float,
        *,
        source_anchor: datetime | None = None,
        wall_anchor: datetime | None = None,
        max_lag_seconds: float | None = None,
    ):
        if (source_anchor is None) != (wall_anchor is None):
            raise ValueError("source_anchor and wall_anchor must be set together")
        if source_anchor is not None and source_anchor.utcoffset() is None:
            raise ValueError("source_anchor must be timezone-aware")
        if wall_anchor is not None and wall_anchor.utcoffset() is None:
            raise ValueError("wall_anchor must be timezone-aware")
        if max_lag_seconds is not None and max_lag_seconds < 0:
            raise ValueError("max_lag_seconds must be non-negative")
        self.time_scale = time_scale
        self.max_lag_seconds_limit = max_lag_seconds
        self._event_anchor = source_anchor
        self._monotonic_anchor = (
            time.monotonic()
            + (wall_anchor.astimezone(UTC) - datetime.now(UTC)).total_seconds()
            if wall_anchor is not None
            else None
        )
        self.final_lag_seconds = 0.0
        self.max_lag_seconds = 0.0

    def wait_until(self, event_time: datetime) -> None:
        if self.time_scale == 0:
            return
        if self._event_anchor is None:
            self._event_anchor = event_time
            self._monotonic_anchor = time.monotonic()
            return
        simulated_elapsed = (event_time - self._event_anchor).total_seconds()
        target = self._monotonic_anchor + simulated_elapsed / self.time_scale  # type: ignore[operator]
        now = time.monotonic()
        remaining = target - now
        if remaining > 0:
            time.sleep(remaining)
            now = time.monotonic()
        self.final_lag_seconds = max(0.0, now - target)
        self.max_lag_seconds = max(self.max_lag_seconds, self.final_lag_seconds)
        if (
            self.max_lag_seconds_limit is not None
            and self.final_lag_seconds > self.max_lag_seconds_limit
        ):
            raise RuntimeError(
                "replay lag "
                f"{self.final_lag_seconds:.3f}s exceeds maximum "
                f"{self.max_lag_seconds_limit:.3f}s"
            )


class ReplayCoordinator:
    def __init__(
        self,
        router: RoadRouter,
        taxi_zones: dict[int, object],
        publisher: EventPublisher,
        config: SimulationConfig,
        simulator: MotionSimulator | None = None,
        clock: ReplayClock | None = None,
        utc_now: Callable[[], datetime] | None = None,
        timeline: ReplayTimeline | None = None,
    ):
        self.router = router
        self.taxi_zones = taxi_zones
        self.publisher = publisher
        self.config = config
        self.simulator = simulator or MotionSimulator()
        self.clock = clock or ReplayClock(config.time_scale)
        self.utc_now = utc_now or (lambda: datetime.now(UTC))
        self.timeline = timeline

    def replay(self, trips: Iterable[TripRecord | PreparedTrip]) -> ReplayResult:
        queue: list[tuple[datetime, int, str, object]] = []
        trip_iterator = iter(trips)
        counter = 0
        previous_request_time: datetime | None = None

        def enqueue_next_dispatch() -> None:
            nonlocal counter, previous_request_time
            try:
                replay_input = next(trip_iterator)
            except StopIteration:
                return
            trip = (
                replay_input.trip
                if isinstance(replay_input, PreparedTrip)
                else replay_input
            )
            if (
                previous_request_time is not None
                and trip.request_datetime < previous_request_time
            ):
                raise ValueError("trips must be ordered by request_datetime")
            previous_request_time = trip.request_datetime
            heapq.heappush(
                queue,
                (trip.request_datetime, counter, "dispatch", replay_input),
            )
            counter += 1

        # 아직 배차되지 않은 Trip은 다음 한 건만 유지해 입력 크기만큼 메모리가 늘지 않게 한다
        enqueue_next_dispatch()

        trips_planned = 0
        trips_attempted = 0
        skip_reasons: Counter[TripSkipReason] = Counter()
        events_published = 0
        segments: set[str] = set()
        rated_samples = 0
        hump_samples = 0
        profile_trip_counts: Counter[str] = Counter()

        while queue:
            action_time, _, action, value = heapq.heappop(queue)
            self.clock.wait_until(action_time)
            if action == "dispatch":
                replay_input = value
                assert isinstance(replay_input, TripRecord | PreparedTrip)
                source_trip = (
                    replay_input.trip
                    if isinstance(replay_input, PreparedTrip)
                    else replay_input
                )
                if self.timeline is None:
                    # 첫 실제 배차를 기준으로 모든 Trip이 하나의 시간축을 공유한다
                    self.timeline = ReplayTimeline(
                        source_trip.request_datetime, self.utc_now()
                    )
                replay_trip = rebase_trip_to_replay_timeline(source_trip, self.timeline)
                enqueue_next_dispatch()
                trips_attempted += 1
                try:
                    if isinstance(replay_input, PreparedTrip):
                        if replay_input.skip_reason is not None:
                            raise TripInfeasibleError(
                                replay_input.skip_reason,
                                "trip was rejected while preparing replay input",
                            )
                        assert replay_input.route is not None
                        # 경로는 사전에 계산하되 planned_at은 실제 배차 시각으로 기록한다
                        route = replace(
                            replay_input.route,
                            planned_at=replay_trip.request_datetime,
                        )
                    else:
                        pickup_zone = self._taxi_zone(
                            source_trip.pu_location_id,
                            TripSkipReason.PICKUP_ZONE_NOT_FOUND,
                        )
                        dropoff_zone = self._taxi_zone(
                            source_trip.do_location_id,
                            TripSkipReason.DROPOFF_ZONE_NOT_FOUND,
                        )
                        route = self.router.plan_for_zones(
                            replay_trip.trip_id,
                            replay_trip.request_datetime,
                            pickup_zone,
                            dropoff_zone,
                            target_distance_m=replay_trip.trip_miles * METERS_PER_MILE,
                        )
                    profile = self._profile_for(replay_trip)
                    sample_iterator = self.simulator.generate_with_metadata(
                        replay_trip, route, profile, self.config
                    )
                    # 첫 샘플 생성 시 속도 프로파일까지 검증한다
                    first_sample = next(sample_iterator)
                except TripInfeasibleError as error:
                    skip_reasons[error.reason] += 1
                    logger.warning(
                        "trip skipped trip_id=%s reason=%s detail=%s",
                        replay_trip.trip_id,
                        error.reason,
                        error,
                    )
                    continue
                except StopIteration:
                    reason = TripSkipReason.EMPTY_SENSOR_STREAM
                    skip_reasons[reason] += 1
                    logger.warning(
                        "trip skipped trip_id=%s reason=%s detail=no sensor samples",
                        replay_trip.trip_id,
                        reason,
                    )
                    continue
                trips_planned += 1
                # skip된 trip을 세면 요약이 실제 발행 이벤트와 어긋난다.
                profile_trip_counts[profile.profile_name] += 1
                segments.update(route.segment_ids)
                heapq.heappush(
                    queue,
                    (
                        source_sensor_schedule_time(
                            source_trip,
                            replay_trip,
                            first_sample.event,
                        ),
                        counter,
                        "sensor",
                        (
                            first_sample,
                            route,
                            sample_iterator,
                            source_trip,
                            replay_trip,
                        ),
                    ),
                )
                counter += 1
            else:
                (
                    sample,
                    route,
                    sample_iterator,
                    source_trip,
                    replay_trip,
                ) = value
                event = sample.event
                assert isinstance(sample, SimulatedSample)
                assert isinstance(event, SensorEvent)
                assert isinstance(route, RoutePlan)
                assert isinstance(source_trip, TripRecord)
                assert isinstance(replay_trip, TripRecord)
                self.publisher.publish(event)
                events_published += 1
                try:
                    next_sample = next(sample_iterator)
                except StopIteration:
                    pass
                else:
                    heapq.heappush(
                        queue,
                        (
                            source_sensor_schedule_time(
                                source_trip,
                                replay_trip,
                                next_sample.event,
                            ),
                            counter,
                            "sensor",
                            (
                                next_sample,
                                route,
                                sample_iterator,
                                source_trip,
                                replay_trip,
                            ),
                        ),
                    )
                    counter += 1
                if sample.rated:
                    rated_samples += 1
                if sample.near_hump:
                    hump_samples += 1

        self.publisher.flush()
        return ReplayResult(
            trips_attempted=trips_attempted,
            trips_planned=trips_planned,
            trips_skipped=sum(skip_reasons.values()),
            skip_reason_counts={
                reason.value: count
                for reason, count in sorted(skip_reasons.items(), key=lambda item: item[0].value)
            },
            events_published=events_published,
            unique_segments=len(segments),
            rated_samples=rated_samples,
            hump_samples=hump_samples,
            profile_trip_counts=dict(sorted(profile_trip_counts.items())),
            final_replay_lag_seconds=float(
                getattr(self.clock, "final_lag_seconds", 0.0)
            ),
            max_replay_lag_seconds=float(getattr(self.clock, "max_lag_seconds", 0.0)),
            observed_segment_ids=frozenset(segments),
        )

    def _profile_for(self, trip: TripRecord) -> VehicleProfile:
        if self.config.vehicle_mix is not None:
            return assign_vehicle_profile(
                trip.trip_id, self.config.seed, self.config.vehicle_mix
            )
        assert self.config.vehicle_profile_id is not None
        return VEHICLE_PROFILES[self.config.vehicle_profile_id]

    def _taxi_zone(self, location_id: int, reason: TripSkipReason) -> object:
        try:
            return self.taxi_zones[location_id]
        except KeyError as error:
            raise TripInfeasibleError(
                reason,
                f"taxi zone {location_id} is not present in the environment",
            ) from error


def distance_for_event(
    trip_seq: int,
    route: RoutePlan,
    config: SimulationConfig,
    trip: TripRecord,
) -> float:
    elapsed = min(trip.passenger_duration_seconds, trip_seq * config.interval_seconds)
    return SpeedProfile.for_route(route, trip.passenger_duration_seconds).state_at(
        elapsed
    ).distance_m


def locate(route: RoutePlan, distance_m: float) -> SamplePosition:
    remaining = min(route.total_length_m, max(0.0, distance_m))
    for leg in route.legs:
        if remaining <= leg.length_m:
            return SamplePosition(leg, remaining)
        remaining -= leg.length_m
    return SamplePosition(route.legs[-1], route.legs[-1].length_m)


def vertical_acceleration(
    position: SamplePosition,
    route_distance_m: float,
    speed_mps: float,
    elapsed_seconds: float,
    phase: float,
    profile: VehicleProfile,
) -> tuple[float, bool]:
    rating = position.leg.pavement_rating
    roughness = 0.12 if rating is None else 0.08 + (10 - rating) / 9 * 0.52
    pavement = (
        profile.vertical_response
        * roughness
        * (
            math.sin(route_distance_m * 1.7 + phase)
            + 0.35 * math.sin(route_distance_m * 4.1 + phase / 2)
        )
    )
    # 감쇠계수가 낮은 차량일수록 노면 입력이 차체 흔들림으로 더 오래 남는다. 정상상태
    # 사인파에는 지속 시간이 없으므로 지속을 진폭으로 근사한다(감쇠비가 아니다).
    sway = (
        profile.vertical_response
        * roughness
        * SWAY_WEIGHT
        * (BASELINE_DAMPING / profile.damping)
        * math.sin(route_distance_m * SWAY_WAVENUMBER_RAD_PER_M + phase / 3)
    )
    hump_response = 0.0
    near_hump = False
    for hump_distance in position.leg.hump_distances_m:
        offset = position.distance_in_leg_m - hump_distance
        if abs(offset) > 8:
            continue
        near_hump = True
        width = 1.8
        impact = math.exp(-((offset / width) ** 2))
        # 잔진동은 요철을 지난 뒤에만 남는다. 통과 전 구간에는 충격만 있다.
        ring = 0.0
        if offset > 0:
            ring = math.sin(elapsed_seconds * 18) * math.exp(-offset * profile.damping)
        hump_response += (
            profile.vertical_response
            * max(0.5, speed_mps / 5)
            * (1.8 * impact + 0.35 * ring)
        )
    return pavement + sway + hump_response, near_hump


def steering_vibration_amplitude(
    speed_mps: float,
    accel_y: float,
    accel_z: float,
    elapsed_seconds: float,
    phase: float,
    profile: VehicleProfile,
) -> float:
    """Approximate the non-negative steering-wheel vibration amplitude in m/s².

    This is an RMS-like engineering signal rather than a calibrated steering
    column model. Road vibration reaches the wheel only while moving, lateral
    acceleration contributes during steering, and the carrier gives the signal
    a deterministic high-frequency texture.
    """

    moving_factor = clamp(speed_mps / 1.5, 0.0, 1.0)
    speed_factor = clamp(speed_mps / 8.0, 0.0, 1.5)
    road_component = abs(accel_z) * (0.20 + 0.25 * speed_factor) * moving_factor
    steering_component = abs(accel_y) * 0.14
    carrier = 0.85 + 0.15 * abs(math.sin(elapsed_seconds * 28.0 + phase))
    return (
        profile.steering_vibration_response
        * (road_component + steering_component)
        * carrier
    )


def steering_angle_degrees(speed_mps: float, yaw_rate_rad_s: float) -> float:
    """간단한 bicycle model로 부호가 있는 전륜 조향각을 근사한다.

    heading은 시계 방향으로 증가하므로 양수는 우회전, 음수는 좌회전이다.
    """

    # 정지에 가까우면 heading 기반 조향각이 불안정하므로 중립값을 사용한다.
    if speed_mps < MIN_STEERING_SPEED_MPS:
        return 0.0
    angle = math.degrees(
        math.atan(REPRESENTATIVE_WHEELBASE_M * yaw_rate_rad_s / speed_mps)
    )
    if abs(angle) < STEERING_ANGLE_DEADBAND_DEG:
        return 0.0
    return clamp(angle, -MAX_STEERING_ANGLE_DEG, MAX_STEERING_ANGLE_DEG)


def smoothstep(value: float) -> float:
    return 3 * value**2 - 2 * value**3


def smoothstep_integral(value: float) -> float:
    return value**3 - value**4 / 2


def signed_heading_delta(previous: float, current: float) -> float:
    return (current - previous + 180) % 360 - 180


def uniform01(namespace: str, seed: int, trip_id: str) -> float:
    """Map one trip onto a reproducible value in [0, 1).

    `namespace` keeps independent draws from correlating. Do not reuse it for
    `deterministic_phase`: that hash input is frozen so recorded runs stay
    reproducible.
    """

    digest = hashlib.sha256(f"{namespace}:{seed}:{trip_id}".encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64


def assign_vehicle_profile(trip_id: str, seed: int, mix_name: str) -> VehicleProfile:
    """Draw one vehicle profile for a trip from the configured share table.

    The draw is deterministic, not random. `mix_name` is deliberately absent from
    the namespace, so adjusting one share moves only the trips near the shifted
    boundary instead of reshuffling every assignment.
    """

    shares = VEHICLE_MIXES[mix_name]
    value = uniform01("vehicle-mix", seed, trip_id)
    cumulative = 0.0
    for profile_id, share in shares:
        cumulative += share
        if value < cumulative:
            return VEHICLE_PROFILES[profile_id]
    # 비율 합이 1이어도 부동소수 잔차로 마지막 구간을 넘길 수 있다.
    return VEHICLE_PROFILES[shares[-1][0]]


def deterministic_phase(trip_id: str, seed: int) -> float:
    digest = hashlib.sha256(f"{seed}:{trip_id}".encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64 * 2 * math.pi


def clamp(value: float, minimum: float, maximum: float) -> float:
    return min(maximum, max(minimum, value))
