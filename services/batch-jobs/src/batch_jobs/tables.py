# road_segment 자체는 road_segment.persist가 쓴다 — 여기는 그로부터 파생된 테이블만 쓴다.

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import duckdb

from batch_jobs.environment import PreparedEnvironment, pavement_condition
from batch_jobs.road_segment.geometry import geometry_from_wkb, reproject_to_source_crs


def write_environment_tables(
    prepared: PreparedEnvironment,
    output_dir: Path,
    reference_date: date,
    road_snapshot_date: date,
    processed_at: datetime,
) -> dict[str, tuple[Path, int]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    enriched_path = output_dir / "enriched_segment_reference.parquet"
    runtime_path = output_dir / "simulation_road_environment.parquet"
    taxi_zone_path = output_dir / "taxi_zone.parquet"
    connection = duckdb.connect()
    try:
        write_enriched_segments(
            connection,
            prepared,
            enriched_path,
            reference_date,
            road_snapshot_date,
            processed_at,
        )
        write_runtime_environment(
            connection,
            prepared,
            runtime_path,
            reference_date,
            road_snapshot_date,
        )
        write_taxi_zones(connection, prepared, taxi_zone_path)
    finally:
        connection.close()
    return {
        "enriched_segment_reference": (enriched_path, len(prepared.segments)),
        "simulation_road_environment": (runtime_path, len(prepared.segments)),
        "taxi_zone": (taxi_zone_path, len(prepared.taxi_zones)),
    }


def write_enriched_segments(
    connection: duckdb.DuckDBPyConnection,
    prepared: PreparedEnvironment,
    path: Path,
    reference_date: date,
    road_snapshot_date: date,
    processed_at: datetime,
) -> None:
    connection.execute("DROP TABLE IF EXISTS enriched_segment_reference")
    connection.execute(
        """
        CREATE TABLE enriched_segment_reference (
            segment_id VARCHAR NOT NULL,
            reference_date DATE NOT NULL,
            road_snapshot_date DATE NOT NULL,
            pavement_rating DOUBLE,
            pavement_condition VARCHAR,
            pavement_rating_date DATE,
            speed_hump_count INTEGER NOT NULL,
            traffic_signal_count INTEGER NOT NULL,
            curve_flag VARCHAR,
            curve_radius DOUBLE,
            posted_speed_mph INTEGER,
            length_m DOUBLE NOT NULL,
            pavement_quality_flag VARCHAR NOT NULL,
            hump_quality_flag VARCHAR NOT NULL,
            signal_quality_flag VARCHAR NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
        """
    )
    connection.executemany(
        "INSERT INTO enriched_segment_reference VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                segment.road.segment_id,
                reference_date,
                road_snapshot_date,
                segment.pavement_rating,
                pavement_condition(segment.pavement_rating),
                segment.pavement_rating_date,
                segment.hump_count,
                0,
                segment.road.curve_flag,
                segment.road.curve_radius_m,
                segment.road.posted_speed_mph,
                segment.road.length_m,
                segment.pavement_quality_flag,
                segment.hump_quality_flag,
                "NOT_INCLUDED",
                processed_at,
            )
            for segment in prepared.segments
        ],
    )
    copy_to_parquet(connection, "enriched_segment_reference", path)


def write_runtime_environment(
    connection: duckdb.DuckDBPyConnection,
    prepared: PreparedEnvironment,
    path: Path,
    reference_date: date,
    road_snapshot_date: date,
) -> None:
    connection.execute("DROP TABLE IF EXISTS simulation_road_environment")
    connection.execute(
        """
        CREATE TABLE simulation_road_environment (
            segment_id VARCHAR NOT NULL,
            reference_date DATE NOT NULL,
            road_snapshot_date DATE NOT NULL,
            from_node_id BIGINT NOT NULL,
            to_node_id BIGINT NOT NULL,
            traffic_direction VARCHAR NOT NULL,
            street_name VARCHAR NOT NULL,
            geometry_wkt VARCHAR NOT NULL,
            length_m DOUBLE NOT NULL,
            posted_speed_mph INTEGER,
            curve_radius_m DOUBLE,
            pavement_rating DOUBLE,
            hump_fractions_json VARCHAR NOT NULL
        )
        """
    )
    connection.executemany(
        "INSERT INTO simulation_road_environment VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                segment.road.segment_id,
                reference_date,
                road_snapshot_date,
                int(segment.road.from_node_id),
                int(segment.road.to_node_id),
                segment.road.traffic_direction,
                segment.road.street_name,
                # 시뮬레이터는 EPSG:4326(도)을 기대하므로 미터 기준 geometry_wkb를 여기서만 역투영한다.
                reproject_to_source_crs(geometry_from_wkb(segment.road.geometry_wkb)).wkt,
                segment.road.length_m,
                segment.road.posted_speed_mph,
                segment.road.curve_radius_m,
                segment.pavement_rating,
                json.dumps(segment.hump_fractions, separators=(",", ":")),
            )
            for segment in prepared.segments
        ],
    )
    copy_to_parquet(connection, "simulation_road_environment", path)


def write_taxi_zones(
    connection: duckdb.DuckDBPyConnection,
    prepared: PreparedEnvironment,
    path: Path,
) -> None:
    connection.execute("DROP TABLE IF EXISTS taxi_zone")
    connection.execute(
        """
        CREATE TABLE taxi_zone (
            location_id INTEGER NOT NULL,
            geometry_wkt VARCHAR NOT NULL
        )
        """
    )
    connection.executemany(
        "INSERT INTO taxi_zone VALUES (?, ?)",
        [
            # Zone은 Segment 결합용 EPSG:32118이므로 시뮬레이터가 기대하는 EPSG:4326으로 역투영한다.
            (location_id, reproject_to_source_crs(geometry).wkt)
            for location_id, geometry in sorted(prepared.taxi_zones.items())
        ],
    )
    copy_to_parquet(connection, "taxi_zone", path)


def copy_to_parquet(
    connection: duckdb.DuckDBPyConnection, table: str, path: Path
) -> None:
    escaped_path = str(path).replace("'", "''")
    connection.execute(
        f"COPY {table} TO '{escaped_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
