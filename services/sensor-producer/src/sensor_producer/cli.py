"""Command-line interface for NYC sample acquisition and sensor replay."""

from __future__ import annotations

import argparse
import json
import logging
import os
from collections.abc import Iterable
from datetime import UTC, date, datetime
from pathlib import Path

from sensor_producer.domain import (
    VEHICLE_MIXES,
    VEHICLE_PROFILES,
    PreparedTrip,
    SimulationConfig,
    TripRecord,
)
from sensor_producer.environment import RoadEnvironment
from sensor_producer.nyc_data import (
    DEFAULT_HVFHV_URL,
    TLC_TRIP_READER_VERSION,
    fetch_nyc_sample,
    iter_hvfhv_parquet_trips,
)
from sensor_producer.parallel_replay import ParallelReplaySpec, run_parallel_replay
from sensor_producer.prepared_replay import (
    PREPARED_REPLAY_VERSION,
    iter_prepared_replay,
    prepare_replay_bundle,
    read_replay_bounds,
)
from sensor_producer.publisher import JsonlPublisher, KafkaPublisher
from sensor_producer.routing import RoadRouter
from sensor_producer.simulation import (
    TIMESTAMP_POLICY,
    ReplayClock,
    ReplayCoordinator,
    ReplayResult,
    ReplayTimeline,
)


def ratio(value: str) -> float:
    parsed = float(value)
    if not 0 <= parsed <= 1:
        raise argparse.ArgumentTypeError("ratio must be between 0 and 1")
    return parsed


def local_parquet(value: str) -> Path:
    path = Path(value).expanduser()
    if path.suffix.lower() != ".parquet":
        raise argparse.ArgumentTypeError("value must be a local Parquet file")
    if not path.is_file():
        raise argparse.ArgumentTypeError(f"local Parquet file not found: {path}")
    return path.resolve()


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
    trip_input = run_parser.add_mutually_exclusive_group(required=True)
    trip_input.add_argument("--trips-path", type=local_parquet)
    trip_input.add_argument("--prepared-replay-dir", type=Path)
    run_parser.add_argument(
        "--road-environment-path",
        type=local_parquet,
        required=True,
        help="batch-jobs가 준비한 simulation_road_environment Parquet 파일",
    )
    run_parser.add_argument(
        "--taxi-zone-path",
        type=local_parquet,
        required=True,
        help="batch-jobs가 준비한 taxi_zone Parquet 파일",
    )
    run_parser.add_argument("--summary-output", type=Path)
    run_parser.add_argument("--publisher", choices=("kafka", "jsonl"), default="kafka")
    run_parser.add_argument("--output", type=Path)
    run_parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    run_parser.add_argument(
        "--topic", default=os.getenv("KAFKA_SENSOR_TOPIC", "sensor-events")
    )
    run_parser.add_argument("--run-id", default="nyc-smoke-v1")
    run_parser.add_argument("--sample-hz", type=int, default=10)
    run_parser.add_argument("--time-scale", type=float, default=1.0)
    run_parser.add_argument(
        "--max-replay-lag-seconds",
        type=float,
        default=5.0,
        help="실제 시간축보다 허용값 이상 늦어지면 실행을 중단한다",
    )
    run_parser.add_argument(
        "--workers",
        type=int,
        default=int(os.getenv("SENSOR_PRODUCER_WORKERS", "1")),
    )
    # 배정 모드는 배타적이다. 아무것도 주지 않으면 기존과 같이 프로필 1로 고정한다.
    assignment = run_parser.add_mutually_exclusive_group()
    assignment.add_argument("--vehicle-profile-id", type=int)
    assignment.add_argument("--vehicle-mix", choices=sorted(VEHICLE_MIXES))
    run_parser.add_argument("--max-trip-skip-ratio", type=ratio)

    prepare_parser = subparsers.add_parser("prepare-replay")
    prepare_parser.add_argument("--trips-path", type=local_parquet, required=True)
    prepare_parser.add_argument(
        "--road-environment-path", type=local_parquet, required=True
    )
    prepare_parser.add_argument("--taxi-zone-path", type=local_parquet, required=True)
    prepare_parser.add_argument("--output-dir", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
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

    if arguments.command == "prepare-replay":
        environment, _, _ = resolve_environment(arguments)
        manifest = prepare_replay_bundle(
            arguments.trips_path,
            RoadRouter(environment.segments),
            environment.taxi_zones,
            arguments.output_dir,
            environment.road_segment_snapshot_date,
            road_environment_path=arguments.road_environment_path,
            taxi_zone_path=arguments.taxi_zone_path,
        )
        print(json.dumps(manifest, indent=2, sort_keys=True))
        return

    execution_started_at = datetime.now(UTC)
    if arguments.workers <= 0 or arguments.max_replay_lag_seconds < 0:
        raise ValueError("workers must be positive and replay lag must be non-negative")
    environment, sources, environment_summary = resolve_environment(arguments)
    router = RoadRouter(environment.segments)
    trips, trip_source_summary = resolve_trips(arguments, router, environment)
    use_mix = arguments.vehicle_mix is not None
    config = SimulationConfig(
        run_id=arguments.run_id,
        sample_hz=arguments.sample_hz,
        time_scale=arguments.time_scale,
        vehicle_profile_id=None if use_mix else (arguments.vehicle_profile_id or 1),
        vehicle_mix=arguments.vehicle_mix,
    )
    wall_anchor: datetime | None = None
    if arguments.workers > 1:
        if arguments.prepared_replay_dir is None:
            raise ValueError("parallel replay requires --prepared-replay-dir")
        if arguments.publisher != "kafka":
            raise ValueError("parallel replay requires --publisher kafka")
        bounds = read_replay_bounds(arguments.prepared_replay_dir)
        result, wall_anchor = run_parallel_replay(
            ParallelReplaySpec(
                bundle_dir=arguments.prepared_replay_dir,
                road_environment_path=arguments.road_environment_path,
                taxi_zone_path=arguments.taxi_zone_path,
                road_snapshot_date=environment.road_segment_snapshot_date,
                source_anchor=bounds.first_request_datetime,
                simulation=config,
                worker_count=arguments.workers,
                bootstrap_servers=tuple(arguments.bootstrap_servers.split(",")),
                topic=arguments.topic,
                max_replay_lag_seconds=arguments.max_replay_lag_seconds,
            )
        )
    else:
        if arguments.publisher == "kafka":
            publisher = KafkaPublisher(
                arguments.bootstrap_servers.split(","), arguments.topic
            )
        else:
            output = arguments.output or Path("sensor_events.jsonl")
            publisher = JsonlPublisher(output)
        timeline: ReplayTimeline | None = None
        clock: ReplayClock | None = None
        if arguments.prepared_replay_dir is not None:
            bounds = read_replay_bounds(arguments.prepared_replay_dir)
            wall_anchor = datetime.now(UTC)
            timeline = ReplayTimeline(bounds.first_request_datetime, wall_anchor)
            clock = ReplayClock(
                config.time_scale,
                source_anchor=bounds.first_request_datetime,
                wall_anchor=wall_anchor,
                max_lag_seconds=arguments.max_replay_lag_seconds,
            )
        elif config.time_scale != 0:
            clock = ReplayClock(
                config.time_scale,
                max_lag_seconds=arguments.max_replay_lag_seconds,
            )
        coordinator = ReplayCoordinator(
            router,
            environment.taxi_zones,
            publisher,
            config,
            clock=clock,
            timeline=timeline,
        )
        result = coordinator.replay(trips)
    summary = {
        "run_id": config.run_id,
        "execution_started_at": execution_started_at.isoformat(),
        "timestamp_policy": TIMESTAMP_POLICY,
        "road_segment_snapshot_date": environment.road_segment_snapshot_date.isoformat(),
        "publisher": arguments.publisher,
        "topic": arguments.topic if arguments.publisher == "kafka" else None,
        "sample_hz": config.sample_hz,
        "time_scale": config.time_scale,
        "worker_count": arguments.workers,
        "source_timeline_anchor": (
            bounds.first_request_datetime.isoformat()
            if arguments.prepared_replay_dir is not None
            else None
        ),
        "wall_timeline_anchor": wall_anchor.isoformat() if wall_anchor else None,
        "vehicle_assignment": vehicle_assignment(config, result),
        "max_trip_skip_ratio": arguments.max_trip_skip_ratio,
        "trips_attempted": result.trips_attempted,
        "trips_planned": result.trips_planned,
        "trips_skipped": result.trips_skipped,
        "trip_skip_ratio": result.trip_skip_ratio,
        "trip_skip_reasons": result.skip_reason_counts,
        "events_published": result.events_published,
        "final_replay_lag_seconds": result.final_replay_lag_seconds,
        "max_replay_lag_seconds": result.max_replay_lag_seconds,
        "replay_lag_limit_seconds": arguments.max_replay_lag_seconds,
        "unique_segments": result.unique_segments,
        "rated_samples": result.rated_samples,
        "hump_samples": result.hump_samples,
        "trip_source": trip_source_summary,
        "sources": sources,
        **environment_summary,
    }
    summary_output = arguments.summary_output or Path("run_summary.json")
    summary_output.parent.mkdir(parents=True, exist_ok=True)
    summary_output.write_text(json.dumps(summary, indent=2, sort_keys=True))
    print(json.dumps(summary, indent=2, sort_keys=True))
    enforce_trip_skip_ratio(result, arguments.max_trip_skip_ratio)


def resolve_trips(
    arguments: argparse.Namespace,
    router: RoadRouter | None = None,
    environment: RoadEnvironment | None = None,
) -> tuple[Iterable[TripRecord | PreparedTrip], dict[str, object]]:
    if arguments.prepared_replay_dir is not None:
        if router is None or environment is None:
            raise ValueError("prepared replay requires the runtime road environment")
        return (
            iter_prepared_replay(
                arguments.prepared_replay_dir,
                router,
                environment.road_segment_snapshot_date,
            ),
            {
                "format": "prepared_od_replay",
                "path": str(arguments.prepared_replay_dir),
                "reader_version": PREPARED_REPLAY_VERSION,
                "selection": "all_prepared_rows",
            },
        )
    assert arguments.trips_path is not None
    return (
        iter_hvfhv_parquet_trips(
            arguments.trips_path,
        ),
        {
            "format": "parquet",
            "path": str(arguments.trips_path),
            "reader_version": TLC_TRIP_READER_VERSION,
            "selection": "all_valid_rows",
        },
    )


def resolve_environment(
    arguments: argparse.Namespace,
) -> tuple[RoadEnvironment, list[object], dict[str, object]]:
    try:
        environment = RoadEnvironment.from_parquet(
            arguments.road_environment_path,
            arguments.taxi_zone_path,
        )
    except (KeyError, OSError, TypeError, ValueError) as error:
        raise SystemExit(f"failed to load local simulation environment: {error}") from error
    return environment, [], {
        "road_environment_path": str(arguments.road_environment_path),
        "taxi_zone_path": str(arguments.taxi_zone_path),
    }


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
