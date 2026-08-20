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
            "--road-snapshot-date",
            "2026-08-18",
            "--feature-version",
            "v1",
        ]
    )

    assert arguments.target_hour == datetime(2026, 8, 18, 5, tzinfo=UTC)


def test_cleanse_command_accepts_pipeline_paths() -> None:
    arguments = cli.build_parser().parse_args(
        [
            "cleanse-sensor-events",
            "--run-id",
            "run-1",
            "--target-hour",
            "2026-08-18T05:00:00+00:00",
            "--road-snapshot-date",
            "2026-08-18",
            "--feature-version",
            "v1",
            "--bronze-input-path",
            "/bronze",
            "--quarantine-output-path",
            "/quarantine",
            "--road-segment-path",
            "/roads",
            "--output-path",
            "/features",
        ]
    )

    assert arguments.bronze_input_path == "/bronze"
    assert arguments.quarantine_output_path == "/quarantine"
    assert arguments.road_segment_path == "/roads"
    assert arguments.output_path == "/features"


def test_validate_sensor_processing_command_requires_and_parses_target_hour() -> None:
    arguments = cli.build_parser().parse_args(
        [
            "validate-sensor-processing",
            "--target-hour",
            "2026-08-18T05:00:00+00:00",
        ]
    )

    assert arguments.target_hour == datetime(2026, 8, 18, 5, tzinfo=UTC)


def test_validate_sensor_processing_command_accepts_path_overrides() -> None:
    arguments = cli.build_parser().parse_args(
        [
            "validate-sensor-processing",
            "--target-hour",
            "2026-08-18T05:00:00+00:00",
            "--output-path",
            "/features",
            "--quarantine-output-path",
            "/quarantine",
        ]
    )

    assert arguments.output_path == "/features"
    assert arguments.quarantine_output_path == "/quarantine"


def test_main_returns_after_validate_sensor_processing_without_falling_through(
    monkeypatch,
) -> None:
    calls: list[argparse.Namespace] = []
    monkeypatch.setattr(cli, "run_sensor_processing_validation_cli", calls.append)

    cli.main(["validate-sensor-processing", "--target-hour", "2026-08-18T05:00:00+00:00"])

    assert len(calls) == 1


def test_main_returns_after_load_standard_segment_comfort_score_without_falling_through(
    monkeypatch,
) -> None:
    # 이 분기에 return이 없으면, 처리 후 뒤에 있는 build-hourly-segment-features/
    # fetch-reference-data/build-road-environment 코드까지 그대로 실행되어 이
    # 커맨드에는 없는 --build-id 등을 찾다가 AttributeError로 비정상 종료된다.
    calls: list[argparse.Namespace] = []
    monkeypatch.setattr(cli, "run_standard_comfort_score_loading", calls.append)

    cli.main(
        ["load-standard-segment-comfort-score", "--as-of", "2026-08-16T00:00:00+00:00"]
    )

    assert len(calls) == 1
