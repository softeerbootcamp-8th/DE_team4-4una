"""Run deterministic replay shards in independent CPU and Kafka processes."""

from __future__ import annotations

import logging
import multiprocessing
import traceback
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from queue import Empty

from sensor_producer.domain import SimulationConfig
from sensor_producer.environment import RoadEnvironment
from sensor_producer.prepared_replay import iter_prepared_replay
from sensor_producer.publisher import KafkaPublisher
from sensor_producer.routing import RoadRouter
from sensor_producer.simulation import (
    ReplayClock,
    ReplayCoordinator,
    ReplayResult,
    ReplayTimeline,
)

STARTUP_DELAY_SECONDS = 2.0


@dataclass(frozen=True, slots=True)
class ParallelReplaySpec:
    bundle_dir: Path
    road_environment_path: Path
    taxi_zone_path: Path
    road_snapshot_date: date
    source_anchor: datetime
    simulation: SimulationConfig
    worker_count: int
    bootstrap_servers: tuple[str, ...]
    topic: str
    max_replay_lag_seconds: float

    def __post_init__(self) -> None:
        if self.worker_count <= 1:
            raise ValueError("parallel replay requires at least two workers")


def run_parallel_replay(spec: ParallelReplaySpec) -> tuple[ReplayResult, datetime]:
    """Load workers first, then release all of them onto one shared timeline."""
    context = multiprocessing.get_context("spawn")
    status_queue = context.Queue()
    start_queue = context.Queue()
    processes = [
        context.Process(
            target=_run_worker,
            args=(worker_index, spec, start_queue, status_queue),
            name=f"sensor-producer-{worker_index:02d}",
        )
        for worker_index in range(spec.worker_count)
    ]
    for process in processes:
        process.start()

    worker_results: list[ReplayResult] = []
    try:
        for _ in processes:
            kind, _, value = _next_message(status_queue, processes)
            if kind == "error":
                raise RuntimeError(str(value))
            if kind != "ready":
                raise RuntimeError(f"unexpected worker message: {kind}")

        wall_anchor = datetime.now(UTC) + timedelta(seconds=STARTUP_DELAY_SECONDS)
        for _ in processes:
            start_queue.put(wall_anchor)

        for _ in processes:
            kind, _, result = _next_message(status_queue, processes)
            if kind == "error":
                raise RuntimeError(str(result))
            if kind != "result":
                raise RuntimeError(f"unexpected worker message: {kind}")
            if not isinstance(result, ReplayResult):
                raise TypeError("parallel replay worker returned an invalid result")
            worker_results.append(result)
    except BaseException:
        for process in processes:
            if process.is_alive():
                process.terminate()
        raise
    finally:
        for process in processes:
            process.join(timeout=10)

    failed = [process for process in processes if process.exitcode != 0]
    if failed:
        names = ", ".join(process.name for process in failed)
        raise RuntimeError(f"parallel replay workers failed: {names}")
    return _merge_results(worker_results), wall_anchor


def _run_worker(
    worker_index: int,
    spec: ParallelReplaySpec,
    start_queue: object,
    status_queue: object,
) -> None:
    try:
        logging.basicConfig(
            level=logging.INFO,
            format=f"%(asctime)s worker={worker_index} %(name)s %(message)s",
        )
        environment = RoadEnvironment.from_parquet(
            spec.road_environment_path,
            spec.taxi_zone_path,
        )
        if environment.road_segment_snapshot_date != spec.road_snapshot_date:
            raise ValueError("worker road snapshot differs from prepared replay")
        router = RoadRouter(environment.segments)
        trips = iter_prepared_replay(
            spec.bundle_dir,
            router,
            spec.road_snapshot_date,
            worker_index=worker_index,
            worker_count=spec.worker_count,
        )
        publisher = KafkaPublisher(spec.bootstrap_servers, spec.topic)
        status_queue.put(("ready", worker_index, None))
        wall_anchor = start_queue.get()
        if not isinstance(wall_anchor, datetime):
            raise TypeError("parallel replay start signal must be a datetime")
        timeline = ReplayTimeline(spec.source_anchor, wall_anchor)
        clock = ReplayClock(
            spec.simulation.time_scale,
            source_anchor=spec.source_anchor,
            wall_anchor=wall_anchor,
            max_lag_seconds=spec.max_replay_lag_seconds,
        )
        result = ReplayCoordinator(
            router,
            environment.taxi_zones,
            publisher,
            spec.simulation,
            clock=clock,
            timeline=timeline,
        ).replay(trips)
        status_queue.put(("result", worker_index, result))
    except BaseException:
        status_queue.put(("error", worker_index, traceback.format_exc()))
        raise


def _next_message(
    status_queue: object,
    processes: list[object],
) -> tuple[str, int, object]:
    while True:
        try:
            return status_queue.get(timeout=1)
        except Empty:
            if any(process.is_alive() for process in processes):
                continue
            raise RuntimeError("parallel replay workers exited without a result")


def _merge_results(results: list[ReplayResult]) -> ReplayResult:
    skip_reasons: Counter[str] = Counter()
    profile_counts: Counter[str] = Counter()
    segments: set[str] = set()
    for result in results:
        skip_reasons.update(result.skip_reason_counts)
        profile_counts.update(result.profile_trip_counts)
        segments.update(result.observed_segment_ids)
    return ReplayResult(
        trips_attempted=sum(result.trips_attempted for result in results),
        trips_planned=sum(result.trips_planned for result in results),
        trips_skipped=sum(result.trips_skipped for result in results),
        skip_reason_counts=dict(sorted(skip_reasons.items())),
        events_published=sum(result.events_published for result in results),
        unique_segments=len(segments),
        rated_samples=sum(result.rated_samples for result in results),
        hump_samples=sum(result.hump_samples for result in results),
        profile_trip_counts=dict(sorted(profile_counts.items())),
        final_replay_lag_seconds=max(result.final_replay_lag_seconds for result in results),
        max_replay_lag_seconds=max(result.max_replay_lag_seconds for result in results),
        observed_segment_ids=frozenset(segments),
    )
