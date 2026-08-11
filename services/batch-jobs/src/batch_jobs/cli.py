"""Command-line entry points for monthly road-environment builds."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path

from batch_jobs.pipeline import build_and_publish_environment
from batch_jobs.sources import fetch_reference_sources


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build versioned NYC road environments")
    subparsers = parser.add_subparsers(dest="command", required=True)

    fetch_parser = subparsers.add_parser("fetch-reference-data")
    fetch_parser.add_argument("--output-dir", type=Path, required=True)
    fetch_parser.add_argument("--snapshot-date", type=date.fromisoformat, required=True)
    add_bbox_argument(fetch_parser)

    build_parser = subparsers.add_parser("build-road-environment")
    add_build_arguments(build_parser)
    build_parser.add_argument("--source-dir", type=Path, required=True)

    monthly_parser = subparsers.add_parser("run-monthly")
    add_build_arguments(monthly_parser)
    add_bbox_argument(monthly_parser)
    return parser


def add_build_arguments(parser: argparse.ArgumentParser) -> None:
    configured_data_lake = os.getenv("REFERENCE_DATA_LAKE_URI")
    parser.add_argument(
        "--data-lake-uri",
        default=configured_data_lake,
        required=configured_data_lake is None,
    )
    parser.add_argument("--reference-date", type=date.fromisoformat, required=True)
    parser.add_argument("--road-snapshot-date", type=date.fromisoformat)
    parser.add_argument("--build-id")
    parser.add_argument("--activate", action="store_true")
    parser.add_argument("--min-pavement-match-rate", type=float, default=0.0)
    parser.add_argument("--min-hump-match-rate", type=float, default=0.0)


def add_bbox_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--bbox",
        type=float,
        nargs=4,
        metavar=("WEST", "SOUTH", "EAST", "NORTH"),
        help="Optional WGS84 smoke-test extent; omit it for the complete source",
    )


def main(argv: list[str] | None = None) -> None:
    arguments = build_parser().parse_args(argv)
    if arguments.command == "fetch-reference-data":
        manifest_path = fetch_reference_sources(
            arguments.output_dir,
            arguments.snapshot_date,
            tuple(arguments.bbox) if arguments.bbox else None,
        )
        print(manifest_path)
        return

    build_id = arguments.build_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    road_snapshot_date = arguments.road_snapshot_date or arguments.reference_date
    if arguments.command == "build-road-environment":
        result = build_and_publish_environment(
            arguments.source_dir,
            arguments.data_lake_uri,
            arguments.reference_date,
            road_snapshot_date,
            build_id,
            activate=arguments.activate,
            minimum_pavement_segment_match_rate=arguments.min_pavement_match_rate,
            minimum_hump_source_match_rate=arguments.min_hump_match_rate,
        )
    else:
        with tempfile.TemporaryDirectory(prefix="de4-reference-source-") as temporary:
            source_dir = Path(temporary)
            fetch_reference_sources(
                source_dir,
                road_snapshot_date,
                tuple(arguments.bbox) if arguments.bbox else None,
            )
            result = build_and_publish_environment(
                source_dir,
                arguments.data_lake_uri,
                arguments.reference_date,
                road_snapshot_date,
                build_id,
                activate=arguments.activate,
                minimum_pavement_segment_match_rate=arguments.min_pavement_match_rate,
                minimum_hump_source_match_rate=arguments.min_hump_match_rate,
            )
    print(
        json.dumps(
            {
                "environment_id": result.manifest.environment_id,
                "manifest_uri": result.manifest_uri,
                "active_pointer_uri": result.active_pointer_uri,
                "quality": result.manifest.quality,
            },
            indent=2,
            sort_keys=True,
        )
    )
