"""Command-line interface for NYC sample acquisition and sensor replay."""

from __future__ import annotations

import argparse
import json
import os
from datetime import date
from pathlib import Path

from sensor_producer.domain import SimulationConfig
from sensor_producer.environment import RoadEnvironment
from sensor_producer.nyc_data import DEFAULT_HVFHV_URL, fetch_nyc_sample, load_trips
from sensor_producer.publisher import JsonlPublisher, KafkaPublisher
from sensor_producer.routing import RoadRouter
from sensor_producer.simulation import ReplayCoordinator


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
    run_parser.add_argument("--input-dir", type=Path, required=True)
    run_parser.add_argument("--publisher", choices=("kafka", "jsonl"), default="kafka")
    run_parser.add_argument("--output", type=Path)
    run_parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    run_parser.add_argument("--topic", default=os.getenv("KAFKA_SENSOR_TOPIC", "sensor-events"))
    run_parser.add_argument("--run-id", default="nyc-smoke-v1")
    run_parser.add_argument("--sample-hz", type=int, default=10)
    run_parser.add_argument("--time-scale", type=float, default=1.0)
    run_parser.add_argument("--vehicle-profile-id", type=int, default=1)
    run_parser.add_argument("--max-trips", type=int)
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

    input_dir: Path = arguments.input_dir
    environment = RoadEnvironment.from_files(
        input_dir / "lion.geojson",
        input_dir / "pavement.geojson",
        input_dir / "speed_humps.geojson",
        input_dir / "taxi_zones.zip",
    )
    trips = load_trips(input_dir / "trips.json")
    if arguments.max_trips is not None:
        trips = trips[: arguments.max_trips]
    config = SimulationConfig(
        run_id=arguments.run_id,
        sample_hz=arguments.sample_hz,
        time_scale=arguments.time_scale,
        vehicle_profile_id=arguments.vehicle_profile_id,
    )
    if arguments.publisher == "kafka":
        publisher = KafkaPublisher(arguments.bootstrap_servers.split(","), arguments.topic)
    else:
        output = arguments.output or input_dir / "sensor_events.jsonl"
        publisher = JsonlPublisher(output)
    coordinator = ReplayCoordinator(
        RoadRouter(environment.segments),
        environment.taxi_zones,
        publisher,
        config,
    )
    result = coordinator.replay(trips)
    manifest = json.loads((input_dir / "manifest.json").read_text())
    summary = {
        "run_id": config.run_id,
        "publisher": arguments.publisher,
        "topic": arguments.topic if arguments.publisher == "kafka" else None,
        "sample_hz": config.sample_hz,
        "time_scale": config.time_scale,
        "vehicle_profile_id": config.vehicle_profile_id,
        "trips_planned": result.trips_planned,
        "events_published": result.events_published,
        "unique_segments": result.unique_segments,
        "rated_samples": result.rated_samples,
        "hump_samples": result.hump_samples,
        "sources": manifest["sources"],
    }
    (input_dir / "run_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True))
    print(json.dumps(summary, indent=2, sort_keys=True))
