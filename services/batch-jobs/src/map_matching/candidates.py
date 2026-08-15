"""Find LION road_segment candidates near processed_sensor_event GPS points."""

from __future__ import annotations

import logging
import math
import pickle
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd
import shapely
from pyproj import Transformer
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    BinaryType,
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)
from shapely import STRtree

logger = logging.getLogger(__name__)

SOURCE_CRS = "EPSG:4326"
TARGET_CRS = "EPSG:32118"

ROAD_SEGMENT_COLUMNS = (
    "segment_id",
    "snapshot_date",
    "geometry_wkb",
    "traffic_direction",
    "from_node_id",
    "to_node_id",
)

OUTPUT_COLUMNS = (
    "event_id",
    "candidate_segment_id",
    "road_snapshot_date",
    "distance_m",
    "candidate_geometry_wkb",
    "candidate_traffic_direction",
    "candidate_from_node_id",
    "candidate_to_node_id",
)

CANDIDATE_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("candidate_segment_id", StringType(), nullable=True),
        StructField("road_snapshot_date", DateType(), nullable=True),
        StructField("distance_m", DoubleType(), nullable=True),
        StructField("candidate_geometry_wkb", BinaryType(), nullable=True),
        StructField("candidate_traffic_direction", StringType(), nullable=True),
        StructField("candidate_from_node_id", StringType(), nullable=True),
        StructField("candidate_to_node_id", StringType(), nullable=True),
    ]
)


@dataclass(frozen=True, slots=True)
class RoadSegmentCandidate:
    segment_id: str
    snapshot_date: date
    geometry_wkb: bytes
    traffic_direction: str | None
    from_node_id: str
    to_node_id: str


def find_segment_candidates(
    sensor_df: DataFrame,
    road_segment_df: DataFrame,
    search_radius_m: float,
) -> DataFrame:
    """센서 GPS별 주변 LION Segment 후보와 거리를 반환한다."""
    if not math.isfinite(search_radius_m) or search_radius_m <= 0:
        raise ValueError("search_radius_m must be finite and greater than 0")

    road_records = _collect_road_segment_candidates(road_segment_df)

    spark = sensor_df.sparkSession
    road_records_broadcast = spark.sparkContext.broadcast(road_records)

    def find_candidates(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        # 파티션 시작 시 STRtree/좌표 변환기를 한 번만 만들고 모든 배치에서 재사용한다
        records = road_records_broadcast.value
        geometries = shapely.from_wkb([record.geometry_wkb for record in records])
        tree = STRtree(geometries)
        transformer = Transformer.from_crs(SOURCE_CRS, TARGET_CRS, always_xy=True)

        for batch in batches:
            yield process_batch(batch, tree, geometries, records, transformer, search_radius_m)

    return sensor_df.select("event_id", "latitude", "longitude").mapInPandas(
        find_candidates, schema=CANDIDATE_SCHEMA
    )


# road_segment_df에서 필요한 컬럼만 뽑아 driver로 모으고 broadcast에 적합한지 검증한다
def _collect_road_segment_candidates(road_segment_df: DataFrame) -> list[RoadSegmentCandidate]:
    missing_columns = [c for c in ROAD_SEGMENT_COLUMNS if c not in road_segment_df.columns]
    if missing_columns:
        raise ValueError(f"road_segment_df is missing required columns: {missing_columns}")

    rows = road_segment_df.select(*ROAD_SEGMENT_COLUMNS).collect()
    if not rows:
        raise ValueError("road_segment_df must not be empty")

    snapshot_dates = {row["snapshot_date"] for row in rows}
    if None in snapshot_dates:
        raise ValueError("road_segment_df must not contain a null snapshot_date")
    if len(snapshot_dates) > 1:
        raise ValueError(
            f"road_segment_df spans multiple snapshot_date values: {sorted(snapshot_dates)}"
        )

    null_segment_ids = sum(1 for row in rows if row["segment_id"] is None)
    null_geometries = sum(1 for row in rows if row["geometry_wkb"] is None)
    if null_segment_ids or null_geometries:
        raise ValueError(
            "road_segment_df must not contain null segment_id or geometry_wkb "
            f"(found {null_segment_ids} null segment_id, {null_geometries} null geometry_wkb)"
        )

    records = [
        RoadSegmentCandidate(
            segment_id=row["segment_id"],
            snapshot_date=row["snapshot_date"],
            geometry_wkb=bytes(row["geometry_wkb"]),
            traffic_direction=row["traffic_direction"],
            from_node_id=row["from_node_id"],
            to_node_id=row["to_node_id"],
        )
        for row in rows
    ]

    # broadcast 적합성(행 수·직렬화 크기)을 판단할 수 있도록 기록한다
    logger.info(
        "collected %d road_segment candidates for broadcast (%d bytes serialized)",
        len(records),
        len(pickle.dumps(records)),
    )
    return records


def process_batch(
    batch: pd.DataFrame,
    tree: STRtree,
    geometries: np.ndarray,
    road_records: list[RoadSegmentCandidate],
    transformer: Transformer,
    search_radius_m: float,
) -> pd.DataFrame:
    """배치 전체를 벡터화해 이벤트별 후보 행을 만든다(NULL/비유한/범위 밖 좌표는 후보 없음)."""
    event_ids = batch["event_id"].to_numpy()
    latitudes = batch["latitude"].to_numpy(dtype="float64", na_value=np.nan)
    longitudes = batch["longitude"].to_numpy(dtype="float64", na_value=np.nan)
    snapshot_date = road_records[0].snapshot_date

    valid = (
        np.isfinite(latitudes)
        & np.isfinite(longitudes)
        & (latitudes >= -90)
        & (latitudes <= 90)
        & (longitudes >= -180)
        & (longitudes <= 180)
    )
    valid_positions = np.flatnonzero(valid)

    rows: list[dict[str, object]] = []
    matched_positions: set[int] = set()

    if valid_positions.size:
        x, y = transformer.transform(longitudes[valid_positions], latitudes[valid_positions])
        points = shapely.points(x, y)

        # dwithin은 GEOS가 정확한 거리로 직접 필터링하므로, buffer() 생성 후
        # 거리를 다시 계산해 걸러내는 것보다 훨씬 저렴하다.
        local_point_index, segment_index = tree.query(
            points, predicate="dwithin", distance=search_radius_m
        )
        distances = shapely.distance(points[local_point_index], geometries[segment_index])

        for local_index, seg_index, distance_m in zip(
            local_point_index, segment_index, distances, strict=True
        ):
            position = int(valid_positions[local_index])
            record = road_records[int(seg_index)]
            row = _empty_candidate_row(event_ids[position], snapshot_date)
            row.update(
                candidate_segment_id=record.segment_id,
                distance_m=float(distance_m),
                candidate_geometry_wkb=record.geometry_wkb,
                candidate_traffic_direction=record.traffic_direction,
                candidate_from_node_id=record.from_node_id,
                candidate_to_node_id=record.to_node_id,
            )
            rows.append(row)
            matched_positions.add(position)

    rows.extend(
        _empty_candidate_row(event_ids[position], snapshot_date)
        for position in range(len(batch))
        if position not in matched_positions
    )

    return pd.DataFrame(rows, columns=list(OUTPUT_COLUMNS))


def _empty_candidate_row(event_id: str, snapshot_date: date) -> dict[str, object]:
    # 후보가 없어도 어떤 road_segment snapshot으로 매칭을 시도했는지는 남긴다
    return {
        "event_id": event_id,
        "candidate_segment_id": None,
        "road_snapshot_date": snapshot_date,
        "distance_m": None,
        "candidate_geometry_wkb": None,
        "candidate_traffic_direction": None,
        "candidate_from_node_id": None,
        "candidate_to_node_id": None,
    }
