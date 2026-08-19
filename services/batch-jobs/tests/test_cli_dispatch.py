"""Tests for batch_jobs/cli.py::main() command dispatch (#152)."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime

from batch_jobs import cli


def test_cleanse_command_requires_and_parses_target_hour() -> None:
    arguments = cli.build_parser().parse_args(
        [
            "cleanse-sensor-events",
            "--run-id",
            "scheduled__2026-08-18T05:00:00+00:00",
            "--target-hour",
            "2026-08-18T05:00:00+00:00",
        ]
    )

    assert arguments.target_hour == datetime(2026, 8, 18, 5, tzinfo=UTC)


def test_main_returns_after_load_segment_comfort_score_without_falling_through(
    monkeypatch,
) -> None:
    # load-segment-comfort-score 분기에 return이 없으면, 이 분기 처리 후 뒤에 있는
    # build-hourly-segment-features/fetch-reference-data/build-road-environment
    # 코드까지 그대로 실행되어 이 커맨드에는 없는 --build-id 등을 찾다가
    # AttributeError로 비정상 종료된다.
    calls: list[argparse.Namespace] = []
    monkeypatch.setattr(cli, "run_segment_comfort_score_loading", calls.append)

    cli.main(["load-segment-comfort-score", "--as-of", "2026-08-16T00:00:00+00:00"])

    assert len(calls) == 1


def test_collect_weather_snapshots_command_parses_target_time() -> None:
    arguments = cli.build_parser().parse_args(
        ["collect-weather-snapshots", "--target-time", "2026-08-19T10:15:00+00:00"]
    )

    assert arguments.target_time == "2026-08-19T10:15:00+00:00"


def test_main_returns_after_collect_weather_snapshots_without_falling_through(
    monkeypatch,
) -> None:
    calls: list[argparse.Namespace] = []
    monkeypatch.setattr(cli, "run_weather_snapshot_collection", calls.append)

    cli.main(["collect-weather-snapshots", "--target-time", "2026-08-19T10:15:00+00:00"])

    assert len(calls) == 1
