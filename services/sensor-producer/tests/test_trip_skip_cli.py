import argparse

import pytest
from sensor_producer.cli import (
    build_parser,
    enforce_trip_skip_ratio,
    limit_trips,
    ratio,
    resolve_trips,
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


def test_limit_trips_does_not_consume_beyond_maximum() -> None:
    consumed: list[object] = []

    def source():
        for value in (object(), object(), object()):
            consumed.append(value)
            yield value

    selected = list(limit_trips(source(), 2))  # type: ignore[arg-type]

    assert selected == consumed
    assert len(consumed) == 2


def test_parquet_trip_uri_requires_source_date() -> None:
    arguments = build_parser().parse_args(
        ["run", "--trips-uri", "s3://bucket/fhvhv.parquet"]
    )

    with pytest.raises(SystemExit, match="--source-date is required"):
        resolve_trips(arguments, input_dir=None)
