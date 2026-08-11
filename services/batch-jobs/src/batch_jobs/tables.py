"""Write normalized, enriched, and runtime road tables as Parquet."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import duckdb

from batch_jobs.environment import PreparedEnvironment, pavement_condition


def write_environment_tables(
    prepared: PreparedEnvironment,
    output_dir: Path,
    reference_date: date,
    road_snapshot_date: date,
    processed_at: datetime,
) -> dict[str, tuple[Path, int]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    road_segment_path = output_dir / "road_segment.parquet"
    enriched_path = output_dir / "enriched_segment_reference.parquet"
    runtime_path = output_dir / "simulation_road_environment.parquet"
    taxi_zone_path = output_dir / "taxi_zone.parquet"
    connection = duckdb.connect()
    try:
        write_road_segment(
            connection,
            prepared,
            road_segment_path,
            road_snapshot_date,
            processed_at,
        )
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
        "road_segment": (road_segment_path, len(prepared.segments)),
        "enriched_segment_reference": (enriched_path, len(prepared.segments)),
        "simulation_road_environment": (runtime_path, len(prepared.segments)),
        "taxi_zone": (taxi_zone_path, len(prepared.taxi_zones)),
    }


def write_road_segment(
    connection: duckdb.DuckDBPyConnection,
    prepared: PreparedEnvironment,
    path: Path,
    snapshot_date: date,
    processed_at: datetime,
) -> None:
    connection.execute("DROP TABLE IF EXISTS road_segment")
    connection.execute(
        """
        CREATE TABLE road_segment (
            segment_id VARCHAR NOT NULL,
            snapshot_date DATE NOT NULL,
            street_name VARCHAR NOT NULL,
            from_node_id BIGINT NOT NULL,
            to_node_id BIGINT NOT NULL,
            traffic_direction VARCHAR,
            segment_type VARCHAR NOT NULL,
            feature_type VARCHAR NOT NULL,
            roadbed_layer VARCHAR NOT NULL,
            from_node_level VARCHAR NOT NULL,
            to_node_level VARCHAR NOT NULL,
            posted_speed_mph INTEGER,
            curve_flag VARCHAR,
            curve_radius DOUBLE,
            length_m DOUBLE NOT NULL,
            location_id INTEGER,
            geometry VARCHAR NOT NULL,
            _ingested_at TIMESTAMP NOT NULL
        )
        """
    )
    connection.executemany(
        "INSERT INTO road_segment VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                segment.segment_id,
                snapshot_date,
                segment.street_name,
                segment.from_node_id,
                segment.to_node_id,
                segment.traffic_direction,
                segment.segment_type,
                segment.feature_type,
                segment.roadbed_layer,
                segment.from_node_level,
                segment.to_node_level,
                segment.posted_speed_mph,
                segment.curve_flag,
                segment.curve_radius_m,
                segment.length_m,
                segment.location_id,
                segment.geometry.wkt,
                processed_at,
            )
            for segment in prepared.segments
        ],
    )
    copy_to_parquet(connection, "road_segment", path)


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
                segment.segment_id,
                reference_date,
                road_snapshot_date,
                segment.pavement_rating,
                pavement_condition(segment.pavement_rating),
                segment.pavement_rating_date,
                segment.hump_count,
                0,
                segment.curve_flag,
                segment.curve_radius_m,
                segment.posted_speed_mph,
                segment.length_m,
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
                segment.segment_id,
                reference_date,
                road_snapshot_date,
                segment.from_node_id,
                segment.to_node_id,
                segment.traffic_direction,
                segment.street_name,
                segment.geometry.wkt,
                segment.length_m,
                segment.posted_speed_mph,
                segment.curve_radius_m,
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
            (location_id, geometry.wkt)
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
