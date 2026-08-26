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
    read_hvfhv_request_bounds,
)
from sensor_producer.parallel_replay import ParallelReplaySpec, run_parallel_replay
from sensor_producer.prepared_replay import (
    PREPARED_REPLAY_VERSION,
    TRIPS_FILE_NAME,
    ReplayBounds,
    iter_prepared_replay,
    prepare_replay_bundle,
    read_replay_bounds,
    source_anchor_for_wall_time,
)
from sensor_producer.publisher import JsonlPublisher, KafkaPublisher
from sensor_producer.routing import RoadRouter
from sensor_producer.sampling import (
    DEFAULT_HOURLY_EVENT_TARGET,
    HourlySamplingPlan,
    build_hourly_sampling_plan,
)
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


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
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
    run_parser.add_argument(
        "--hourly-event-target",
        type=positive_int,
        default=DEFAULT_HOURLY_EVENT_TARGET,
        help="시간당 예상 센서 이벤트 상한",
    )
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
    # 배정 모드는 배타적이고 필수다. 예전에는 아무것도 주지 않으면 프로필 1로
    # 고정됐는데, 그러면 전 구간이 한 프로필 데이터가 되어 차량별 점수를 만들 수
    # 없다. 실제로 이 기본값 때문에 이틀치가 프로필 1만으로 적재됐다.
    assignment = run_parser.add_mutually_exclusive_group(required=True)
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

    if arguments.workers <= 0 or arguments.max_replay_lag_seconds < 0:
        raise ValueError("workers must be positive and replay lag must be non-negative")
    environment, sources, environment_summary = resolve_environment(arguments)
    router = RoadRouter(environment.segments)
    use_mix = arguments.vehicle_mix is not None
    config = SimulationConfig(
        run_id=arguments.run_id,
        sample_hz=arguments.sample_hz,
        time_scale=arguments.time_scale,
        vehicle_profile_id=None if use_mix else arguments.vehicle_profile_id,
        vehicle_mix=arguments.vehicle_mix,
    )
    bounds, sampling_path, prepared = resolve_replay_input(arguments)
    sampling_plan = build_hourly_sampling_plan(
        sampling_path,
        sample_hz=config.sample_hz,
        target_events_per_hour=arguments.hourly_event_target,
        seed=config.seed,
        cycle_hours=bounds.cycle_hours,
        prepared=prepared,
    )
    execution_started_at = datetime.now(UTC)
    wall_anchor: datetime | None = None
    source_anchor: datetime | None = None
    trip_source_summary: dict[str, object]
    if arguments.workers > 1:
        if arguments.prepared_replay_dir is None:
            raise ValueError("parallel replay requires --prepared-replay-dir")
        if arguments.publisher != "kafka":
            raise ValueError("parallel replay requires --publisher kafka")
        result, wall_anchor, source_anchor = run_parallel_replay(
            ParallelReplaySpec(
                bundle_dir=arguments.prepared_replay_dir,
                road_environment_path=arguments.road_environment_path,
                taxi_zone_path=arguments.taxi_zone_path,
                road_snapshot_date=environment.road_segment_snapshot_date,
                source_bounds=bounds,
                sampling_plan=sampling_plan,
                simulation=config,
                worker_count=arguments.workers,
                bootstrap_servers=tuple(arguments.bootstrap_servers.split(",")),
                topic=arguments.topic,
                max_replay_lag_seconds=arguments.max_replay_lag_seconds,
            )
        )
        _, trip_source_summary = resolve_trips(
            arguments,
            router,
            environment,
            source_anchor=source_anchor,
            bounds=bounds,
            sampling_plan=sampling_plan,
            create_iterator=False,
        )
    else:
        if arguments.publisher == "kafka":
            publisher = KafkaPublisher(
                arguments.bootstrap_servers.split(","), arguments.topic
            )
        else:
            output = arguments.output or Path("sensor_events.jsonl")
            publisher = JsonlPublisher(output)
        wall_anchor = datetime.now(UTC)
        source_anchor = source_anchor_for_wall_time(bounds, wall_anchor)
        trips, trip_source_summary = resolve_trips(
            arguments,
            router,
            environment,
            source_anchor=source_anchor,
            bounds=bounds,
            sampling_plan=sampling_plan,
        )
        timeline = ReplayTimeline(source_anchor, wall_anchor)
        clock = ReplayClock(
            config.time_scale,
            source_anchor=source_anchor,
            wall_anchor=wall_anchor,
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
        "source_timeline_anchor": source_anchor.isoformat(),
        "wall_timeline_anchor": wall_anchor.isoformat() if wall_anchor else None,
        "trip_sampling": sampling_plan.summary(),
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
    *,
    source_anchor: datetime | None = None,
    bounds: ReplayBounds | None = None,
    sampling_plan: HourlySamplingPlan | None = None,
    create_iterator: bool = True,
) -> tuple[Iterable[TripRecord | PreparedTrip], dict[str, object]]:
    if source_anchor is None or bounds is None or sampling_plan is None:
        raise ValueError("replay selection requires bounds, anchor, and sampling plan")
    if arguments.prepared_replay_dir is not None:
        if router is None or environment is None:
            raise ValueError("prepared replay requires the runtime road environment")
        iterator: Iterable[TripRecord | PreparedTrip] = ()
        if create_iterator:
            iterator = iter_prepared_replay(
                arguments.prepared_replay_dir,
                router,
                environment.road_segment_snapshot_date,
                start_at=source_anchor,
                sampling_plan=sampling_plan,
            )
        return (
            iterator,
            {
                "format": "prepared_od_replay",
                "path": str(arguments.prepared_replay_dir),
                "reader_version": PREPARED_REPLAY_VERSION,
                "selection": "hourly_budgeted_trip_sample",
                "source_anchor": source_anchor.isoformat(),
            },
        )
    assert arguments.trips_path is not None
    return (
        iter_hvfhv_parquet_trips(
            arguments.trips_path,
            start_at=source_anchor,
            cycle_duration=bounds.cycle_duration,
            sampling_plan=sampling_plan,
        ),
        {
            "format": "parquet",
            "path": str(arguments.trips_path),
            "reader_version": TLC_TRIP_READER_VERSION,
            "selection": "hourly_budgeted_trip_sample",
            "source_anchor": source_anchor.isoformat(),
        },
    )


def resolve_replay_input(
    arguments: argparse.Namespace,
) -> tuple[ReplayBounds, Path, bool]:
    if arguments.prepared_replay_dir is not None:
        bounds = read_replay_bounds(arguments.prepared_replay_dir)
        return bounds, arguments.prepared_replay_dir / TRIPS_FILE_NAME, True
    assert arguments.trips_path is not None
    first_request, last_request = read_hvfhv_request_bounds(arguments.trips_path)
    return ReplayBounds(first_request, last_request), arguments.trips_path, False


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
