"""Write validated road_segment records to Parquet and verify the result.

ingested_at은 항상 UTC(timezone-aware)라는 계약이며, DuckDB의 plain
TIMESTAMP는 세션 타임존으로 재해석해 값을 조용히 바꿔버리므로
TIMESTAMPTZ를 사용해 실제 시각(instant)이 보존되게 한다.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import duckdb

from road_segment.validate import RoadSegmentRecord

CREATE_TABLE_SQL = """
CREATE TABLE road_segment (
    segment_id VARCHAR NOT NULL,
    snapshot_date DATE NOT NULL,
    street_name VARCHAR,
    from_node_id VARCHAR NOT NULL,
    to_node_id VARCHAR NOT NULL,
    traffic_direction VARCHAR,
    segment_type VARCHAR,
    feature_type VARCHAR,
    roadway_type VARCHAR,
    roadbed_layer VARCHAR,
    from_node_level VARCHAR,
    to_node_level VARCHAR,
    posted_speed_mph INTEGER,
    curve_flag VARCHAR,
    curve_radius_m DOUBLE,
    length_m DOUBLE NOT NULL,
    geometry_wkb BLOB NOT NULL,
    source_version VARCHAR NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL
)
"""

INSERT_SQL = "INSERT INTO road_segment VALUES ({})".format(", ".join(["?"] * 19))


@dataclass(frozen=True, slots=True)
class ParquetSummary:
    row_count: int
    columns: tuple[tuple[str, str], ...]


def write_road_segment_parquet(records: list[RoadSegmentRecord], path: Path) -> None:
    for record in records:
        require_utc(record)
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    try:
        connection.execute(CREATE_TABLE_SQL)
        connection.executemany(INSERT_SQL, [_as_row(record) for record in records])
        copy_to_parquet(connection, path)
    finally:
        connection.close()


def require_utc(record: RoadSegmentRecord) -> None:
    # naive/비-UTC datetime을 TIMESTAMPTZ에 넣으면 DuckDB가 세션 타임존
    # 기준으로 재해석해버리므로, 쓰기 전에 UTC인지 명시적으로 검증한다.
    offset = record.ingested_at.utcoffset()
    if offset is None or offset != timedelta(0):
        raise ValueError(
            f"ingested_at must be UTC timezone-aware, got {record.ingested_at!r} "
            f"for segment_id={record.segment_id!r}"
        )


def copy_to_parquet(connection: duckdb.DuckDBPyConnection, path: Path) -> None:
    escaped_path = str(path).replace("'", "''")
    connection.execute(
        f"COPY road_segment TO '{escaped_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )


def _as_row(record: RoadSegmentRecord) -> tuple[object, ...]:
    return (
        record.segment_id,
        record.snapshot_date,
        record.street_name,
        record.from_node_id,
        record.to_node_id,
        record.traffic_direction,
        record.segment_type,
        record.feature_type,
        record.roadway_type,
        record.roadbed_layer,
        record.from_node_level,
        record.to_node_level,
        record.posted_speed_mph,
        record.curve_flag,
        record.curve_radius_m,
        record.length_m,
        record.geometry_wkb,
        record.source_version,
        record.ingested_at,
    )


def read_road_segment_parquet(path: Path) -> ParquetSummary:
    connection = duckdb.connect()
    try:
        escaped_path = str(path).replace("'", "''")
        row_count = connection.execute(
            f"SELECT COUNT(*) FROM read_parquet('{escaped_path}')"
        ).fetchone()[0]
        columns = connection.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{escaped_path}')"
        ).fetchall()
    finally:
        connection.close()
    return ParquetSummary(
        row_count=row_count,
        columns=tuple((name, column_type) for name, column_type, *_ in columns),
    )
