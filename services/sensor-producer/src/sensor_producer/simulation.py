"""Deterministic vehicle motion and wall-clock replay coordination."""

from __future__ import annotations

import hashlib
import heapq
import math
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from de4_core import SensorEvent

from sensor_producer.domain import (
    VEHICLE_PROFILES,
    RouteLeg,
    RoutePlan,
    SimulationConfig,
    TripRecord,
    VehicleProfile,
)
from sensor_producer.geo import point_and_heading
from sensor_producer.publisher import EventPublisher
from sensor_producer.routing import METERS_PER_MILE, RoadRouter

EVENT_NAMESPACE = uuid.UUID("a8ad2dcf-cbb4-4ca8-9173-a48958caa85e")
MPH_TO_MPS = 0.44704
DEFAULT_SPEED_LIMIT_MPH = 25.0
MAX_ACCELERATION_PHASE_SECONDS = 8.0
# 차량별 보정값이 없는 PoC용 bicycle model 가정이다.
REPRESENTATIVE_WHEELBASE_M = 2.8
MIN_STEERING_SPEED_MPS = 0.5
MAX_STEERING_ANGLE_DEG = 35.0
STEERING_ANGLE_DEADBAND_DEG = 0.05


@dataclass(frozen=True, slots=True)
class ReplayResult:
    trips_planned: int
    events_published: int
    unique_segments: int
    rated_samples: int
    hump_samples: int


@dataclass(frozen=True, slots=True)
class SamplePosition:
    leg: RouteLeg
    distance_in_leg_m: float


@dataclass(frozen=True, slots=True)
class MotionState:
    distance_m: float
    speed_mps: float


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
            raise ValueError(
                "route cannot be completed within TLC duration and posted speed limit"
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


class MotionSimulator:
    """Generate plausible, deterministic signals rather than calibrated physics."""

    def generate(
        self,
        trip: TripRecord,
        route: RoutePlan,
        profile: VehicleProfile,
        config: SimulationConfig,
    ) -> Iterator[SensorEvent]:
        duration = trip.passenger_duration_seconds
        sample_count = max(2, int(duration * config.sample_hz) + 1)
        previous_speed = 0.0
        previous_accel_x = 0.0
        previous_accel_y = 0.0
        previous_accel_z = 0.0
        previous_heading: float | None = None
        phase = deterministic_phase(trip.trip_id, config.seed)
        speed_profile = SpeedProfile.for_route(route, duration)

        for sequence in range(sample_count):
            elapsed = min(duration, sequence * config.interval_seconds)
            motion = speed_profile.state_at(elapsed)
            distance = motion.distance_m
            speed = motion.speed_mps
            position = locate(route, distance)
            fraction = (
                position.distance_in_leg_m / position.leg.length_m
                if position.leg.length_m
                else 0.0
            )
            point, heading = point_and_heading(position.leg.geometry, fraction)

            if sequence == 0:
                accel_x = 0.0
                accel_y = 0.0
                steering_angle = 0.0
            else:
                accel_x = (
                    (speed - previous_speed)
                    / config.interval_seconds
                    * profile.longitudinal_response
                )
                assert previous_heading is not None
                heading_delta = signed_heading_delta(previous_heading, heading)
                yaw_rate = math.radians(heading_delta) / config.interval_seconds
                accel_y = clamp(speed * yaw_rate * profile.lateral_response, -4.0, 4.0)
                steering_angle = steering_angle_degrees(speed, yaw_rate)

            accel_z, _near_hump = vertical_acceleration(
                position,
                distance,
                speed,
                elapsed,
                phase,
                profile,
            )
            steering_vibration = steering_vibration_amplitude(
                speed,
                accel_y,
                accel_z,
                elapsed,
                phase,
                profile,
            )
            if sequence == 0:
                jerk_x = 0.0
                jerk_y = 0.0
                jerk_z = 0.0
            else:
                jerk_x = (accel_x - previous_accel_x) / config.interval_seconds
                jerk_y = (accel_y - previous_accel_y) / config.interval_seconds
                jerk_z = (accel_z - previous_accel_z) / config.interval_seconds
            event_time = (trip.pickup_datetime + timedelta(seconds=elapsed)).astimezone(UTC)
            event_id = str(
                uuid.uuid5(
                    EVENT_NAMESPACE,
                    f"{config.run_id}:{trip.trip_id}:{profile.vehicle_profile_id}:{sequence}",
                )
            )
            yield SensorEvent(
                event_id=event_id,
                vehicle_id=f"vehicle-{profile.vehicle_profile_id}-{trip.trip_id[:8]}",
                vehicle_profile_id=profile.vehicle_profile_id,
                trip_id=trip.trip_id,
                trip_seq=sequence,
                event_time=event_time,
                latitude=point.y,
                longitude=point.x,
                speed_mps=max(0.0, speed),
                heading=heading,
                steering_angle=steering_angle,
                accel_x=accel_x,
                accel_y=accel_y,
                accel_z=accel_z,
                jerk=jerk_x,
                jerk_x=jerk_x,
                jerk_y=jerk_y,
                jerk_z=jerk_z,
                steering_vibration=steering_vibration,
                _run_id=config.run_id,
            )
            previous_speed = speed
            previous_accel_x = accel_x
            previous_accel_y = accel_y
            previous_accel_z = accel_z
            previous_heading = heading


class ReplayClock:
    def __init__(self, time_scale: float):
        self.time_scale = time_scale
        self._event_anchor: datetime | None = None
        self._monotonic_anchor: float | None = None

    def wait_until(self, event_time: datetime) -> None:
        if self.time_scale == 0:
            return
        if self._event_anchor is None:
            self._event_anchor = event_time
            self._monotonic_anchor = time.monotonic()
            return
        simulated_elapsed = (event_time - self._event_anchor).total_seconds()
        target = self._monotonic_anchor + simulated_elapsed / self.time_scale  # type: ignore[operator]
        remaining = target - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)


class ReplayCoordinator:
    def __init__(
        self,
        router: RoadRouter,
        taxi_zones: dict[int, object],
        publisher: EventPublisher,
        config: SimulationConfig,
        simulator: MotionSimulator | None = None,
        clock: ReplayClock | None = None,
    ):
        self.router = router
        self.taxi_zones = taxi_zones
        self.publisher = publisher
        self.config = config
        self.simulator = simulator or MotionSimulator()
        self.clock = clock or ReplayClock(config.time_scale)

    def replay(self, trips: list[TripRecord]) -> ReplayResult:
        queue: list[tuple[datetime, int, str, object]] = []
        counter = 0
        for trip in sorted(trips, key=lambda value: (value.request_datetime, value.trip_id)):
            heapq.heappush(queue, (trip.request_datetime, counter, "dispatch", trip))
            counter += 1

        trips_planned = 0
        events_published = 0
        segments: set[str] = set()
        rated_samples = 0
        hump_samples = 0
        profile = VEHICLE_PROFILES[self.config.vehicle_profile_id]

        while queue:
            action_time, _, action, value = heapq.heappop(queue)
            self.clock.wait_until(action_time)
            if action == "dispatch":
                trip = value
                assert isinstance(trip, TripRecord)
                route = self.router.plan_for_zones(
                    trip.trip_id,
                    trip.request_datetime,
                    self.taxi_zones[trip.pu_location_id],
                    self.taxi_zones[trip.do_location_id],
                    target_distance_m=trip.trip_miles * METERS_PER_MILE,
                )
                trips_planned += 1
                segments.update(route.segment_ids)
                event_iterator = self.simulator.generate(trip, route, profile, self.config)
                first_event = next(event_iterator)
                heapq.heappush(
                    queue,
                    (
                        first_event.event_time,
                        counter,
                        "sensor",
                        (first_event, route, event_iterator, trip),
                    ),
                )
                counter += 1
            else:
                event, route, event_iterator, trip = value
                assert isinstance(event, SensorEvent)
                assert isinstance(route, RoutePlan)
                assert isinstance(trip, TripRecord)
                self.publisher.publish(event)
                events_published += 1
                try:
                    next_event = next(event_iterator)
                except StopIteration:
                    pass
                else:
                    heapq.heappush(
                        queue,
                        (
                            next_event.event_time,
                            counter,
                            "sensor",
                            (next_event, route, event_iterator, trip),
                        ),
                    )
                    counter += 1
                position = locate(
                    route,
                    distance_for_event(event.trip_seq, route, self.config, trip),
                )
                if position.leg.pavement_rating is not None:
                    rated_samples += 1
                if any(
                    abs(position.distance_in_leg_m - hump_distance) <= 2.0
                    for hump_distance in position.leg.hump_distances_m
                ):
                    hump_samples += 1

        self.publisher.flush()
        return ReplayResult(
            trips_planned=trips_planned,
            events_published=events_published,
            unique_segments=len(segments),
            rated_samples=rated_samples,
            hump_samples=hump_samples,
        )


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
    pavement = profile.vertical_response * roughness * (
        math.sin(route_distance_m * 1.7 + phase)
        + 0.35 * math.sin(route_distance_m * 4.1 + phase / 2)
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
        ring = math.sin(elapsed_seconds * 18) * math.exp(-abs(offset) * profile.damping)
        hump_response += profile.vertical_response * max(0.5, speed_mps / 5) * (
            1.8 * impact + 0.35 * ring
        )
    return pavement + hump_response, near_hump


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
    return profile.steering_vibration_response * (
        road_component + steering_component
    ) * carrier


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


def deterministic_phase(trip_id: str, seed: int) -> float:
    digest = hashlib.sha256(f"{seed}:{trip_id}".encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64 * 2 * math.pi


def clamp(value: float, minimum: float, maximum: float) -> float:
    return min(maximum, max(minimum, value))
