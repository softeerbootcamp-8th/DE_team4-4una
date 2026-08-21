"""Command-line interface for NYC sample acquisition and sensor replay."""

from __future__ import annotations

import argparse
import json
import os
from datetime import date
from pathlib import Path

from sensor_producer.domain import VEHICLE_MIXES, VEHICLE_PROFILES, SimulationConfig
from sensor_producer.environment import RoadEnvironment
from sensor_producer.nyc_data import DEFAULT_HVFHV_URL, fetch_nyc_sample, load_trips
from sensor_producer.publisher import JsonlPublisher, KafkaPublisher
from sensor_producer.routing import RoadRouter
from sensor_producer.runtime_environment import RoadEnvironmentLoader
from sensor_producer.simulation import ReplayCoordinator, ReplayResult


def ratio(value: str) -> float:
    parsed = float(value)
    if not 0 <= parsed <= 1:
        raise argparse.ArgumentTypeError("ratio must be between 0 and 1")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay TLC trips as Kafka sensor events")
    subparsers = parser.add_subparsers(dest="command", required=True)

    fetch_parser = subparsers.add_parser("fetch-nyc-sample")
    fetch_parser.add_argument("--output-dir", type=Path, required=True)
    fetch_parser.add_argument("--zone-id", type=int, default=181)
    fetch_parser.add_argument("--source-date", type=date.fromisoformat, default=date(2024, 2, 1))
    fetch_parser.add_argument("--max-trips", type=int, default=1000)
    fetch_parser.add_argument("--hvfhv-url", default=DEFAULT_HVFHV_URL)

    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--input-dir", type=Path)
    run_parser.add_argument("--trips-path", type=Path)
    run_parser.add_argument(
        "--road-segment-path",
        type=Path,
        help="canonical road_segment 단일 snapshot_date Parquet 파일 (Transform 2와 같은 경로여야 함)",
    )
    run_parser.add_argument(
        "--environment-pointer-uri", default=os.getenv("SENSOR_ENVIRONMENT_POINTER_URI")
    )
    run_parser.add_argument(
        "--environment-manifest-uri", default=os.getenv("SENSOR_ENVIRONMENT_MANIFEST_URI")
    )
    run_parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path(os.getenv("SENSOR_CACHE_DIR", ".local/sensor-producer-cache")),
    )
    run_parser.add_argument("--summary-output", type=Path)
    run_parser.add_argument("--publisher", choices=("kafka", "jsonl"), default="kafka")
    run_parser.add_argument("--output", type=Path)
    run_parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    run_parser.add_argument("--topic", default=os.getenv("KAFKA_SENSOR_TOPIC", "sensor-events"))
    run_parser.add_argument("--run-id", default="nyc-smoke-v1")
    run_parser.add_argument("--sample-hz", type=int, default=10)
    run_parser.add_argument("--time-scale", type=float, default=1.0)
    # 배정 모드는 배타적이다. 아무것도 주지 않으면 기존과 같이 프로필 1로 고정한다.
    assignment = run_parser.add_mutually_exclusive_group()
    assignment.add_argument("--vehicle-profile-id", type=int)
    assignment.add_argument("--vehicle-mix", choices=sorted(VEHICLE_MIXES))
    run_parser.add_argument("--max-trips", type=int)
    run_parser.add_argument("--max-trip-skip-ratio", type=ratio)
    return parser


def main(argv: list[str] | None = None) -> None:
    arguments = build_parser().parse_args(argv)
    if arguments.command == "fetch-nyc-sample":
        manifest = fetch_nyc_sample(
            arguments.output_dir,
            arguments.zone_id,
            arguments.source_date,
            arguments.max_trips,
            arguments.hvfhv_url,
        )
        print(json.dumps(manifest, indent=2, sort_keys=True))
        return

    input_dir: Path | None = arguments.input_dir
    environment, sources, environment_summary = resolve_environment(arguments)
    trips_path = arguments.trips_path or (input_dir / "trips.json" if input_dir else None)
    if trips_path is None:
        raise SystemExit("--trips-path or --input-dir is required")
    trips = load_trips(trips_path)
    if arguments.max_trips is not None:
        trips = trips[: arguments.max_trips]
    use_mix = arguments.vehicle_mix is not None
    config = SimulationConfig(
        run_id=arguments.run_id,
        sample_hz=arguments.sample_hz,
        time_scale=arguments.time_scale,
        vehicle_profile_id=None if use_mix else (arguments.vehicle_profile_id or 1),
        vehicle_mix=arguments.vehicle_mix,
    )
    if arguments.publisher == "kafka":
        publisher = KafkaPublisher(arguments.bootstrap_servers.split(","), arguments.topic)
    else:
        output = arguments.output or (input_dir or arguments.cache_dir) / "sensor_events.jsonl"
        publisher = JsonlPublisher(output)
    coordinator = ReplayCoordinator(
        RoadRouter(environment.segments),
        environment.taxi_zones,
        publisher,
        config,
    )
    result = coordinator.replay(trips)
    summary = {
        "run_id": config.run_id,
        "road_segment_snapshot_date": environment.road_segment_snapshot_date.isoformat(),
        "publisher": arguments.publisher,
        "topic": arguments.topic if arguments.publisher == "kafka" else None,
        "sample_hz": config.sample_hz,
        "time_scale": config.time_scale,
        "vehicle_assignment": vehicle_assignment(config, result),
        "max_trip_skip_ratio": arguments.max_trip_skip_ratio,
        "trips_attempted": result.trips_attempted,
        "trips_planned": result.trips_planned,
        "trips_skipped": result.trips_skipped,
        "trip_skip_ratio": result.trip_skip_ratio,
        "trip_skip_reasons": result.skip_reason_counts,
        "events_published": result.events_published,
        "unique_segments": result.unique_segments,
        "rated_samples": result.rated_samples,
        "hump_samples": result.hump_samples,
        "sources": sources,
        **environment_summary,
    }
    summary_output = arguments.summary_output or (
        input_dir or arguments.cache_dir
    ) / "run_summary.json"
    summary_output.parent.mkdir(parents=True, exist_ok=True)
    summary_output.write_text(json.dumps(summary, indent=2, sort_keys=True))
    print(json.dumps(summary, indent=2, sort_keys=True))
    enforce_trip_skip_ratio(result, arguments.max_trip_skip_ratio)


def resolve_environment(
    arguments: argparse.Namespace,
) -> tuple[RoadEnvironment, list[object], dict[str, object]]:
    pointer_uri = arguments.environment_pointer_uri
    manifest_uri = arguments.environment_manifest_uri
    if pointer_uri and manifest_uri:
        raise SystemExit("choose only one environment pointer or manifest URI")
    if arguments.road_segment_path and (pointer_uri or manifest_uri):
        raise SystemExit("choose either a local road segment or a published environment")

    if pointer_uri or manifest_uri:
        loader = RoadEnvironmentLoader()
        loaded = (
            loader.from_pointer(pointer_uri, arguments.cache_dir)
            if pointer_uri
            else loader.from_manifest(manifest_uri, arguments.cache_dir)
        )
        manifest = loaded.manifest
        sources: list[object] = [
            {
                "source_id": source.source_id,
                "snapshot_id": source.snapshot_id,
                "object_uri": source.object_uri,
                "sha256": source.sha256,
            }
            for source in manifest.sources
        ]
        return loaded.environment, sources, {
            "environment_id": manifest.environment_id,
            "environment_manifest_uri": loaded.manifest_uri,
            "reference_date": manifest.reference_date.isoformat(),
        }

    input_dir: Path | None = arguments.input_dir
    road_segment_path: Path | None = arguments.road_segment_path
    if input_dir is None or road_segment_path is None:
        raise SystemExit(
            "--input-dir and --road-segment-path, or a published environment URI, "
            "are required"
        )
    environment = RoadEnvironment.from_files(
        road_segment_path,
        input_dir / "pavement.geojson",
        input_dir / "speed_humps.geojson",
        input_dir / "taxi_zones.zip",
    )
    manifest = json.loads((input_dir / "manifest.json").read_text())
    return environment, manifest["sources"], {}


def vehicle_assignment(config: SimulationConfig, result: ReplayResult) -> dict[str, object]:
    """Record how vehicle profiles were chosen so a run can be reproduced."""
    if config.vehicle_mix is not None:
        return {
            "mode": "mix",
            "mix_name": config.vehicle_mix,
            "mix_version": config.vehicle_mix_version,
            "seed": config.seed,
            "shares": {
                VEHICLE_PROFILES[profile_id].profile_name: share
                for profile_id, share in VEHICLE_MIXES[config.vehicle_mix]
            },
            "profile_trip_counts": result.profile_trip_counts,
        }
    assert config.vehicle_profile_id is not None
    profile = VEHICLE_PROFILES[config.vehicle_profile_id]
    return {
        "mode": "fixed",
        "vehicle_profile_id": profile.vehicle_profile_id,
        "profile_name": profile.profile_name,
        "profile_trip_counts": result.profile_trip_counts,
    }


def enforce_trip_skip_ratio(result: ReplayResult, maximum: float | None) -> None:
    if maximum is not None and result.trip_skip_ratio > maximum:
        raise SystemExit(
            f"trip skip ratio {result.trip_skip_ratio:.3f} exceeds maximum {maximum:.3f}"
        )
