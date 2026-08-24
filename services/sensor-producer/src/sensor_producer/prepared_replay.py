"""Prepare compact OD route templates and stream sorted TLC replay rows."""

from __future__ import annotations

import hashlib
import json
import logging
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from sensor_producer.domain import PreparedTrip, TripRecord
from sensor_producer.errors import TripSkipReason
from sensor_producer.nyc_data import parse_nyc_datetime, stable_trip_id
from sensor_producer.routing import METERS_PER_MILE, GraphEdge, RoadRouter

PREPARED_REPLAY_VERSION = "od-route-replay-v1"
PREPARED_REPLAY_BATCH_SIZE = 1000
TRIPS_FILE_NAME = "trips.parquet"
ROUTES_FILE_NAME = "route_templates.parquet"
MANIFEST_FILE_NAME = "manifest.json"
logger = logging.getLogger(__name__)

_TRIP_COLUMNS = (
    "source_file_row_number", "request_datetime", "pickup_datetime",
    "dropoff_datetime", "pu_location_id", "do_location_id", "trip_miles",
)

_ROUTE_SCHEMA = pa.schema(
    [
        ("pu_location_id", pa.int32()),
        ("do_location_id", pa.int32()),
        ("skip_reason", pa.string()),
        ("start_node_id", pa.string()),
        ("end_node_id", pa.string()),
        ("route_segment_ids", pa.list_(pa.string())),
        ("route_reverse_flags", pa.list_(pa.bool_())),
    ]
)


@dataclass(frozen=True, slots=True)
class OdStat:
    pu_location_id: int
    do_location_id: int
    trip_count: int
    representative_trip_miles: float


@dataclass(frozen=True, slots=True)
class ReplayBounds:
    first_request_datetime: datetime
    last_request_datetime: datetime


def prepare_replay_bundle(
    source_path: Path,
    router: RoadRouter,
    taxi_zones: dict[int, object],
    output_dir: Path,
    road_snapshot_date: date,
    *,
    road_environment_path: Path,
    taxi_zone_path: Path,
) -> dict[str, object]:
    """Write sorted Trips once and one reusable route template per observed OD."""
    output_dir.mkdir(parents=True, exist_ok=True)
    trips_path = output_dir / TRIPS_FILE_NAME
    routes_path = output_dir / ROUTES_FILE_NAME
    manifest_path = output_dir / MANIFEST_FILE_NAME
    existing = [
        path for path in (trips_path, routes_path, manifest_path) if path.exists()
    ]
    if existing:
        raise FileExistsError(f"replay output already exists: {existing[0]}")

    trips_temporary = trips_path.with_suffix(".parquet.tmp")
    routes_temporary = routes_path.with_suffix(".parquet.tmp")
    try:
        trip_count, replay_bounds = _write_sorted_trips(source_path, trips_temporary)
        od_stats = _read_od_stats(trips_temporary)
        route_summary = _write_route_templates(
            od_stats,
            router,
            taxi_zones,
            routes_temporary,
        )
        trips_temporary.replace(trips_path)
        routes_temporary.replace(routes_path)
    except BaseException:
        trips_temporary.unlink(missing_ok=True)
        routes_temporary.unlink(missing_ok=True)
        raise

    manifest = {
        "prepared_replay_version": PREPARED_REPLAY_VERSION,
        "created_at": datetime.now(UTC).isoformat(),
        "source": _file_identity(source_path),
        "road_environment": _file_identity(road_environment_path),
        "taxi_zones": _file_identity(taxi_zone_path),
        "road_snapshot_date": road_snapshot_date.isoformat(),
        "selection": "all_valid_rows",
        "ordering": ["request_datetime", "source_file_row_number"],
        "trip_count": trip_count,
        "first_request_datetime": replay_bounds.first_request_datetime.isoformat(),
        "last_request_datetime": replay_bounds.last_request_datetime.isoformat(),
        "route_template_count": len(od_stats),
        **route_summary,
        "artifacts": {
            "trips": _file_identity(trips_path),
            "route_templates": _file_identity(routes_path),
        },
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))
    return manifest


def iter_prepared_replay(
    bundle_dir: Path,
    router: RoadRouter,
    road_snapshot_date: date,
    *,
    batch_size: int = PREPARED_REPLAY_BATCH_SIZE,
    worker_index: int = 0,
    worker_count: int = 1,
):
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if worker_count <= 0:
        raise ValueError("worker_count must be positive")
    if not 0 <= worker_index < worker_count:
        raise ValueError("worker_index must be in [0, worker_count)")
    manifest = _read_manifest(bundle_dir, road_snapshot_date)
    trips_path = bundle_dir / TRIPS_FILE_NAME
    routes_path = bundle_dir / ROUTES_FILE_NAME
    templates = _read_route_templates(routes_path)
    parquet = pq.ParquetFile(trips_path)
    _validate_columns(parquet.schema_arrow, _TRIP_COLUMNS, "prepared Trips")

    expected_templates = int(manifest["route_template_count"])
    if len(templates) != expected_templates:
        raise ValueError("route template count differs from manifest")

    previous_order: tuple[datetime, int] | None = None
    for batch in parquet.iter_batches(batch_size=batch_size):
        for row in batch.to_pylist():
            trip = _trip_from_row(row)
            order = (trip.request_datetime, int(row["source_file_row_number"]))
            if previous_order is not None and order < previous_order:
                raise ValueError("prepared replay is not ordered by request_datetime")
            previous_order = order
            if stable_worker_index(trip.trip_id, worker_count) != worker_index:
                continue

            key = (trip.pu_location_id, trip.do_location_id)
            try:
                template = templates[key]
            except KeyError as error:
                raise ValueError(
                    f"prepared replay is missing OD template {key}"
                ) from error
            if template["skip_reason"] is not None:
                yield PreparedTrip(
                    trip, None, TripSkipReason(str(template["skip_reason"]))
                )
                continue
            route = router.plan_from_segments(
                trip.trip_id,
                trip.request_datetime,
                str(template["start_node_id"]),
                str(template["end_node_id"]),
                tuple(template["route_segment_ids"]),
                tuple(template["route_reverse_flags"]),
            )
            yield PreparedTrip(trip, route)


def stable_worker_index(trip_id: str, worker_count: int) -> int:
    if worker_count <= 0:
        raise ValueError("worker_count must be positive")
    digest = hashlib.blake2b(trip_id.encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big") % worker_count


def read_replay_bounds(bundle_dir: Path) -> ReplayBounds:
    manifest = json.loads((bundle_dir / MANIFEST_FILE_NAME).read_text())
    return ReplayBounds(
        parse_nyc_datetime(manifest["first_request_datetime"]),
        parse_nyc_datetime(manifest["last_request_datetime"]),
    )


def _write_sorted_trips(
    source_path: Path,
    output_path: Path,
) -> tuple[int, ReplayBounds]:
    connection = duckdb.connect()
    escaped_source = str(source_path).replace("'", "''")
    try:
        connection.execute(
            f"""
            CREATE TEMP VIEW valid_trips AS
            SELECT
                CAST(file_row_number AS BIGINT) AS source_file_row_number,
                request_datetime,
                pickup_datetime,
                dropoff_datetime,
                CAST(PULocationID AS INTEGER) AS pu_location_id,
                CAST(DOLocationID AS INTEGER) AS do_location_id,
                CAST(trip_miles AS DOUBLE) AS trip_miles
            FROM read_parquet('{escaped_source}', file_row_number = true)
            WHERE request_datetime IS NOT NULL
              AND pickup_datetime IS NOT NULL
              AND dropoff_datetime IS NOT NULL
              AND PULocationID IS NOT NULL
              AND DOLocationID IS NOT NULL
              AND trip_miles IS NOT NULL
              AND request_datetime <= pickup_datetime
              AND pickup_datetime < dropoff_datetime
              AND trip_miles > 0
              AND isfinite(trip_miles)
            """
        )
        connection.execute(
            """
            COPY (
                SELECT * FROM valid_trips
                ORDER BY request_datetime, source_file_row_number
            ) TO ? (
                FORMAT PARQUET,
                COMPRESSION ZSTD,
                ROW_GROUP_SIZE 100000
            )
            """,
            [str(output_path)],
        )
        trip_count, first_request, last_request = connection.execute(
            """SELECT count(*), min(request_datetime), max(request_datetime)
            FROM read_parquet(?)""",
            [str(output_path)],
        ).fetchone()
    finally:
        connection.close()
    if not trip_count:
        raise ValueError("cannot prepare an empty TLC replay")
    return int(trip_count), ReplayBounds(
        parse_nyc_datetime(first_request), parse_nyc_datetime(last_request)
    )


def _read_od_stats(trips_path: Path) -> list[OdStat]:
    connection = duckdb.connect()
    try:
        rows = connection.execute(
            """
            SELECT
                pu_location_id,
                do_location_id,
                count(*) AS trip_count,
                median(trip_miles) AS representative_trip_miles
            FROM read_parquet(?)
            GROUP BY pu_location_id, do_location_id
            ORDER BY pu_location_id, do_location_id
            """,
            [str(trips_path)],
        ).fetchall()
    finally:
        connection.close()
    return [
        OdStat(int(pu), int(do), int(count), float(miles))
        for pu, do, count, miles in rows
    ]


def _write_route_templates(
    od_stats: list[OdStat],
    router: RoadRouter,
    taxi_zones: dict[int, object],
    output_path: Path,
) -> dict[str, object]:
    observed_zone_ids = sorted(
        {stat.pu_location_id for stat in od_stats}
        | {stat.do_location_id for stat in od_stats}
    )
    zone_anchors = {
        zone_id: router.zone_anchor_nodes(taxi_zones[zone_id])
        for zone_id in observed_zone_ids
        if zone_id in taxi_zones
    }
    stats_by_pickup: dict[int, list[OdStat]] = defaultdict(list)
    for stat in od_stats:
        stats_by_pickup[stat.pu_location_id].append(stat)

    writer = pq.ParquetWriter(output_path, _ROUTE_SCHEMA, compression="zstd")
    buffer: list[dict[str, object]] = []
    planned_count = 0
    planned_trip_count = 0
    skip_counts: Counter[str] = Counter()
    skipped_trip_counts: Counter[str] = Counter()
    try:
        for pickup_index, pickup_id in enumerate(sorted(stats_by_pickup), start=1):
            pickup_stats = stats_by_pickup[pickup_id]
            paths_by_start = _paths_for_pickup(
                pickup_id,
                pickup_stats,
                router,
                taxi_zones,
                zone_anchors,
            )
            for stat in pickup_stats:
                row = _route_template_row(
                    stat,
                    paths_by_start,
                    taxi_zones,
                    zone_anchors,
                )
                buffer.append(row)
                if row["skip_reason"] is None:
                    planned_count += 1
                    planned_trip_count += stat.trip_count
                else:
                    reason = str(row["skip_reason"])
                    skip_counts[reason] += 1
                    skipped_trip_counts[reason] += stat.trip_count
                if len(buffer) >= PREPARED_REPLAY_BATCH_SIZE:
                    writer.write_table(
                        pa.Table.from_pylist(buffer, schema=_ROUTE_SCHEMA)
                    )
                    buffer.clear()
            if pickup_index % 20 == 0 or pickup_index == len(stats_by_pickup):
                logger.info(
                    "prepared OD routes pickup_zones=%s/%s templates=%s/%s",
                    pickup_index,
                    len(stats_by_pickup),
                    planned_count + sum(skip_counts.values()),
                    len(od_stats),
                )
        if buffer:
            writer.write_table(pa.Table.from_pylist(buffer, schema=_ROUTE_SCHEMA))
    except BaseException:
        writer.close()
        raise
    writer.close()
    return {
        "planned_route_template_count": planned_count,
        "skipped_route_template_count": len(od_stats) - planned_count,
        "planned_trip_count": planned_trip_count,
        "skipped_trip_count": sum(skipped_trip_counts.values()),
        "route_skip_reason_counts": dict(sorted(skip_counts.items())),
        "trip_skip_reason_counts": dict(sorted(skipped_trip_counts.items())),
    }


def _paths_for_pickup(
    pickup_id: int,
    od_stats: list[OdStat],
    router: RoadRouter,
    taxi_zones: dict[int, object],
    zone_anchors: dict[int, tuple[str, ...]],
) -> dict[str, dict[str, tuple[GraphEdge, ...]]]:
    if pickup_id not in taxi_zones or not zone_anchors.get(pickup_id):
        return {}
    target_nodes = {
        node for stat in od_stats for node in zone_anchors.get(stat.do_location_id, ())
    }
    return {
        start_node: router.shortest_paths(start_node, target_nodes - {start_node})
        for start_node in zone_anchors[pickup_id]
    }


def _route_template_row(
    stat: OdStat,
    paths_by_start: dict[str, dict[str, tuple[GraphEdge, ...]]],
    taxi_zones: dict[int, object],
    zone_anchors: dict[int, tuple[str, ...]],
) -> dict[str, object]:
    base = {
        "pu_location_id": stat.pu_location_id,
        "do_location_id": stat.do_location_id,
    }
    if stat.pu_location_id not in taxi_zones:
        return _skipped_route_row(base, TripSkipReason.PICKUP_ZONE_NOT_FOUND)
    if stat.do_location_id not in taxi_zones:
        return _skipped_route_row(base, TripSkipReason.DROPOFF_ZONE_NOT_FOUND)
    if not zone_anchors.get(stat.pu_location_id):
        return _skipped_route_row(base, TripSkipReason.PICKUP_ZONE_NO_ROUTABLE_NODES)
    if not zone_anchors.get(stat.do_location_id):
        return _skipped_route_row(base, TripSkipReason.DROPOFF_ZONE_NO_ROUTABLE_NODES)

    target_distance_m = stat.representative_trip_miles * METERS_PER_MILE
    best: tuple[float, str, str, tuple[GraphEdge, ...]] | None = None
    for start_node, paths in paths_by_start.items():
        for end_node in zone_anchors[stat.do_location_id]:
            if start_node == end_node:
                continue
            edges = paths.get(end_node, ())
            if not edges:
                continue
            length_m = sum(edge.segment.length_m for edge in edges)
            candidate = (abs(length_m - target_distance_m), start_node, end_node, edges)
            if best is None or candidate[:3] < best[:3]:
                best = candidate
    if best is None:
        return _skipped_route_row(base, TripSkipReason.NO_DIRECTED_ROUTE)

    _, start_node, end_node, edges = best
    return {
        **base,
        "skip_reason": None,
        "start_node_id": start_node,
        "end_node_id": end_node,
        "route_segment_ids": [edge.segment.segment_id for edge in edges],
        "route_reverse_flags": [edge.reverse for edge in edges],
    }


def _skipped_route_row(
    base: dict[str, object],
    reason: TripSkipReason,
) -> dict[str, object]:
    return {
        **base,
        "skip_reason": reason.value,
        "start_node_id": None,
        "end_node_id": None,
        "route_segment_ids": [],
        "route_reverse_flags": [],
    }


def _read_route_templates(path: Path) -> dict[tuple[int, int], dict[str, object]]:
    parquet = pq.ParquetFile(path)
    _validate_columns(parquet.schema_arrow, _ROUTE_SCHEMA, "route templates")

    templates = {
        (int(row["pu_location_id"]), int(row["do_location_id"])): row
        for row in parquet.read().to_pylist()
    }
    if len(templates) != parquet.metadata.num_rows:
        raise ValueError("route templates contain a duplicate OD")
    return templates


def _trip_from_row(row: dict[str, object]) -> TripRecord:
    request_time = parse_nyc_datetime(row["request_datetime"])
    pickup_time = parse_nyc_datetime(row["pickup_datetime"])
    dropoff_time = parse_nyc_datetime(row["dropoff_datetime"])
    source_row = int(row["source_file_row_number"])
    pu_location_id = int(row["pu_location_id"])
    do_location_id = int(row["do_location_id"])
    trip_miles = float(row["trip_miles"])
    return TripRecord(
        trip_id=stable_trip_id(
            source_row,
            request_time,
            pickup_time,
            dropoff_time,
            pu_location_id,
            do_location_id,
            trip_miles,
        ),
        request_datetime=request_time,
        pickup_datetime=pickup_time,
        dropoff_datetime=dropoff_time,
        pu_location_id=pu_location_id,
        do_location_id=do_location_id,
        trip_miles=trip_miles,
    )


def _read_manifest(bundle_dir: Path, road_snapshot_date: date) -> dict[str, object]:
    manifest = json.loads((bundle_dir / MANIFEST_FILE_NAME).read_text())
    if manifest.get("prepared_replay_version") != PREPARED_REPLAY_VERSION:
        raise ValueError("unsupported prepared replay version")
    if manifest.get("road_snapshot_date") != road_snapshot_date.isoformat():
        raise ValueError(
            "prepared replay road snapshot does not match runtime environment"
        )
    return manifest


def _validate_columns(
    actual: pa.Schema,
    expected: pa.Schema | tuple[str, ...],
    label: str,
) -> None:
    expected_names = expected.names if isinstance(expected, pa.Schema) else expected
    missing = sorted(set(expected_names).difference(actual.names))
    if missing:
        raise ValueError(f"{label} missing columns: {', '.join(missing)}")


def _file_identity(path: Path) -> dict[str, object]:
    resolved = path.resolve()
    return {"path": str(resolved), "size_bytes": resolved.stat().st_size}
