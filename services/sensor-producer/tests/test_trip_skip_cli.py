import argparse
from pathlib import Path

import pytest
from sensor_producer.cli import (
    build_parser,
    enforce_trip_skip_ratio,
    local_parquet,
    ratio,
)
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
