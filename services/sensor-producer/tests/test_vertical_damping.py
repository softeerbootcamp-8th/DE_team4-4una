"""Damping-driven vertical response: body sway and post-hump ringing."""


from sensor_producer.domain import (
    BASELINE_DAMPING,
    VEHICLE_PROFILES,
    RouteLeg,
    VehicleProfile,
)
from sensor_producer.simulation import (
    SamplePosition,
    vertical_acceleration,
)
from shapely.geometry import LineString

BASELINE = VEHICLE_PROFILES[2]
SOFTEST = VEHICLE_PROFILES[5]


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


# --- damping이 실제로 신호에 도달하는지 ---------------------------------------


def accel_z(profile: VehicleProfile, route_distance_m: float, humps=()) -> float:
    value, _ = vertical_acceleration(
        SamplePosition(leg(humps), route_distance_m),
        route_distance_m,
        8.0,
        route_distance_m / 8.0,
        0.3,
        profile,
    )
    return value


def test_damping_changes_the_vertical_signal_without_any_hump() -> None:
    """요철이 없는 노면에서도 damping이 차체 흔들림으로 발현되어야 한다.

    기록된 스모크 실행에서 hump 근처 샘플은 전체의 0.34%뿐이었다. damping이 요철
    항에만 걸려 있으면 프로필별 감쇠계수 차이가 사실상 아무 효과도 내지 못한다.
    """
    baseline = [accel_z(BASELINE, float(step)) for step in range(40)]
    softest = [accel_z(SOFTEST, float(step)) for step in range(40)]

    assert baseline != softest
    # damping이 낮은 프로필이 저주파 흔들림 성분을 더 크게 만든다.
    assert max(abs(value) for value in softest) > max(abs(value) for value in baseline)


def test_baseline_profile_normalises_the_sway_term() -> None:
    assert BASELINE_DAMPING == BASELINE.damping
    assert BASELINE.profile_name == "VP_SEDAN_LARGE"


def hump_asymmetry(profile: VehicleProfile) -> float:
    """요철 전후 같은 거리에서의 신호 차이.

    `route_distance_m`을 고정하면 노면·차체 흔들림 항이 두 지점에서 동일하고, 충격
    항은 offset에 대칭이므로 남는 차이는 통과 후 잔진동뿐이다.
    """
    humps = (20.0,)
    readings = []
    for distance_in_leg_m in (17.0, 23.0):
        value, _ = vertical_acceleration(
            SamplePosition(leg(humps), distance_in_leg_m),
            20.0,
            8.0,
            2.0,
            0.3,
            profile,
        )
        readings.append(value)
    return readings[1] - readings[0]


def test_hump_ringing_only_appears_after_the_impact() -> None:
    """통과 전에는 잔진동이 없으므로 신호가 요철을 기준으로 비대칭이어야 한다."""
    assert hump_asymmetry(BASELINE) != 0.0


def test_lower_damping_leaves_a_larger_hump_ring() -> None:
    assert SOFTEST.damping < BASELINE.damping
    assert abs(hump_asymmetry(SOFTEST)) > abs(hump_asymmetry(BASELINE))
