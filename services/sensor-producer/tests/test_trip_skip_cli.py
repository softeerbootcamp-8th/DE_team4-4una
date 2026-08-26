import argparse
from pathlib import Path

import pytest
from sensor_producer.cli import (
    build_parser,
    enforce_trip_skip_ratio,
    local_parquet,
    positive_int,
    ratio,
)
from sensor_producer.sampling import DEFAULT_HOURLY_EVENT_TARGET
from sensor_producer.simulation import ReplayResult


def replay_result(skipped: int) -> ReplayResult:
    return ReplayResult(
        trips_attempted=2,
        trips_planned=2 - skipped,
        trips_skipped=skipped,
        skip_reason_counts={"NO_DIRECTED_ROUTE": skipped} if skipped else {},
        events_published=3,
        unique_segments=1,
        rated_samples=3,
        hump_samples=0,
        profile_trip_counts={"VP_SEDAN_COMPACT": 2 - skipped},
    )


def test_trip_skip_threshold_allows_equal_ratio() -> None:
    enforce_trip_skip_ratio(replay_result(skipped=1), maximum=0.5)


def test_trip_skip_threshold_exits_when_ratio_is_exceeded() -> None:
    with pytest.raises(SystemExit, match="exceeds maximum"):
        enforce_trip_skip_ratio(replay_result(skipped=1), maximum=0.49)


@pytest.mark.parametrize("value", ["-0.1", "1.1", "nan"])
def test_ratio_rejects_values_outside_unit_interval(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        ratio(value)


@pytest.mark.parametrize("value", ["0", "-1"])
def test_positive_int_rejects_non_positive_values(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        positive_int(value)


def test_local_parquet_accepts_an_existing_parquet_file(tmp_path: Path) -> None:
    path = tmp_path / "input.parquet"
    path.touch()

    assert local_parquet(str(path)) == path.resolve()


@pytest.mark.parametrize("value", ["missing.parquet", "input.json"])
def test_local_parquet_rejects_missing_or_non_parquet_input(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError, match="local Parquet"):
        local_parquet(value)


def test_run_requires_all_local_parquet_inputs() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["run"])


def test_run_requires_an_explicit_vehicle_assignment(tmp_path: Path) -> None:
    # 기본값으로 프로필 1이 되면 전 구간이 한 프로필 데이터가 되어 조용히 망가진다.
    paths = [
        tmp_path / name for name in ("trips.parquet", "roads.parquet", "zones.parquet")
    ]
    for path in paths:
        path.touch()
    base = [
        "run",
        "--trips-path", str(paths[0]),
        "--road-environment-path", str(paths[1]),
        "--taxi-zone-path", str(paths[2]),
    ]

    with pytest.raises(SystemExit):
        build_parser().parse_args(base)

    assert build_parser().parse_args([*base, "--vehicle-mix", "nyc-hvfhv-v1"]).vehicle_mix
    assert build_parser().parse_args([*base, "--vehicle-profile-id", "3"]).vehicle_profile_id == 3


def test_run_defaults_to_the_hourly_event_budget(
    tmp_path: Path,
) -> None:
    paths = [
        tmp_path / name for name in ("trips.parquet", "roads.parquet", "zones.parquet")
    ]
    for path in paths:
        path.touch()

    arguments = build_parser().parse_args(
        [
            "run",
            "--trips-path",
            str(paths[0]),
            "--road-environment-path",
            str(paths[1]),
            "--taxi-zone-path",
            str(paths[2]),
            "--vehicle-mix",
            "nyc-hvfhv-v1",
        ]
    )

    assert not hasattr(arguments, "source_date")
    assert not hasattr(arguments, "max_trips")
    assert arguments.sample_hz == 10
    assert arguments.hourly_event_target == DEFAULT_HOURLY_EVENT_TARGET
