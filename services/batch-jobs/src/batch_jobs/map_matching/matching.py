"""Map Matching: candidate 검색부터 scoring, best-candidate 선택까지 mapInPandas 한 번에 수행한다(#479)."""

from __future__ import annotations

import logging
from collections.abc import Iterator

import numpy as np
import pandas as pd
import shapely
from pyproj import Transformer
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from shapely import STRtree

from batch_jobs.map_matching.candidates import (
    SOURCE_CRS,
    TARGET_CRS,
    RoadSegmentCandidate,
    collect_road_segment_candidates,
    validate_search_radius,
)
from batch_jobs.map_matching.scoring import (
    circular_heading_diff,
    compute_match_scores,
    compute_road_bearing,
    validate_score_weights,
)

logger = logging.getLogger(__name__)

MATCH_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("segment_id", StringType(), nullable=True),
        StructField("road_snapshot_date", DateType(), nullable=True),
        StructField("map_match_distance_m", DoubleType(), nullable=True),
        StructField("map_match_heading_diff_deg", DoubleType(), nullable=True),
        StructField("map_match_score", DoubleType(), nullable=True),
        StructField("candidate_count", LongType(), nullable=False),
        StructField("map_match_status", StringType(), nullable=False),
    ]
)

# select_best_segment()과 동일한 우선순위: match_score DESC, distance_m/heading_diff_deg/segment_id ASC, 모두 NULLS LAST.
_TIE_BREAK_COLUMNS = ["match_score", "distance_m", "heading_diff_deg", "segment_id"]
_TIE_BREAK_ASCENDING = [False, True, True, True]


def match_segment_candidates(
    sensor_df: DataFrame,
    road_segment_df: DataFrame,
    search_radius_m: float,
    distance_weight: float,
    heading_weight: float,
) -> DataFrame:
    """find_segment_candidates -> score_segment_candidates -> select_best_segment과 동일한 결과를 event당 1행으로 반환한다."""
    validate_search_radius(search_radius_m)
    validate_score_weights(distance_weight, heading_weight)

    road_records = collect_road_segment_candidates(road_segment_df)

    spark = sensor_df.sparkSession
    road_records_broadcast = spark.sparkContext.broadcast(road_records)

    def match_events(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        # 파티션 시작 시 STRtree/좌표 변환기를 한 번만 만들고 모든 배치에서 재사용한다
        records = road_records_broadcast.value
        geometries = shapely.from_wkb([record.geometry_wkb for record in records])
        tree = STRtree(geometries)
        transformer = Transformer.from_crs(SOURCE_CRS, TARGET_CRS, always_xy=True)

        for batch in batches:
            yield match_batch(
                batch,
                tree,
                geometries,
                records,
                transformer,
                search_radius_m,
                distance_weight,
                heading_weight,
            )

    return sensor_df.select("event_id", "latitude", "longitude", "heading").mapInPandas(
        match_events, schema=MATCH_SCHEMA
    )


def select_best_candidates(candidates_df: pd.DataFrame) -> pd.DataFrame:
    """position(그룹 키)별로 select_best_segment()과 동일한 tie-break로 최선의 1행을 고른다."""
    return (
        candidates_df.sort_values(
            by=_TIE_BREAK_COLUMNS,
            ascending=_TIE_BREAK_ASCENDING,
            na_position="last",
            kind="mergesort",
        )
        .drop_duplicates(subset="position", keep="first")
    )


def match_batch(
    batch: pd.DataFrame,
    tree: STRtree,
    geometries: np.ndarray,
    road_records: list[RoadSegmentCandidate],
    transformer: Transformer,
    search_radius_m: float,
    distance_weight: float,
    heading_weight: float,
) -> pd.DataFrame:
    """배치 전체를 벡터화해 이벤트별 candidate 검색/scoring/선택을 한 번에 수행한다."""
    event_ids = batch["event_id"].to_numpy()
    latitudes = batch["latitude"].to_numpy(dtype="float64", na_value=np.nan)
    longitudes = batch["longitude"].to_numpy(dtype="float64", na_value=np.nan)
    headings = batch["heading"].to_numpy(dtype="float64", na_value=np.nan)
    snapshot_date = road_records[0].snapshot_date
    n = len(batch)

    segment_id_out = np.full(n, None, dtype=object)
    distance_out = np.full(n, np.nan, dtype="float64")
    heading_diff_out = np.full(n, np.nan, dtype="float64")
    score_out = np.full(n, np.nan, dtype="float64")
    candidate_count = np.zeros(n, dtype="int64")

    valid = (
        np.isfinite(latitudes)
        & np.isfinite(longitudes)
        & (latitudes >= -90)
        & (latitudes <= 90)
        & (longitudes >= -180)
        & (longitudes <= 180)
    )
    valid_positions = np.flatnonzero(valid)

    if valid_positions.size:
        x, y = transformer.transform(longitudes[valid_positions], latitudes[valid_positions])
        points = shapely.points(x, y)

        # dwithin은 GEOS가 정확한 거리로 직접 필터링해 buffer() 후 재계산보다 저렴하다(find_segment_candidates와 동일).
        local_point_index, seg_index = tree.query(
            points, predicate="dwithin", distance=search_radius_m
        )

        if local_point_index.size:
            positions = valid_positions[local_point_index]
            distances = shapely.distance(points[local_point_index], geometries[seg_index])
            candidate_headings = headings[positions]
            candidate_geometry_wkb = np.array(
                [road_records[i].geometry_wkb for i in seg_index], dtype=object
            )
            candidate_traffic_direction = np.array(
                [road_records[i].traffic_direction for i in seg_index], dtype=object
            )
            candidate_segment_ids = np.array(
                [road_records[i].segment_id for i in seg_index], dtype=object
            )

            road_bearing = compute_road_bearing(
                pd.Series(candidate_geometry_wkb),
                pd.Series(candidate_traffic_direction),
                pd.Series(candidate_headings),
            ).to_numpy()
            heading_diff = circular_heading_diff(candidate_headings, road_bearing)
            _, _, match_score = compute_match_scores(
                distances, heading_diff, search_radius_m, distance_weight, heading_weight
            )

            candidates_df = pd.DataFrame(
                {
                    "position": positions,
                    "segment_id": candidate_segment_ids,
                    "distance_m": distances,
                    "heading_diff_deg": heading_diff,
                    "match_score": match_score,
                }
            )

            counts = candidates_df.groupby("position").size()
            candidate_count[counts.index.to_numpy()] = counts.to_numpy()

            best = select_best_candidates(candidates_df)

            idx = best["position"].to_numpy()
            segment_id_out[idx] = best["segment_id"].to_numpy()
            distance_out[idx] = best["distance_m"].to_numpy()
            heading_diff_out[idx] = best["heading_diff_deg"].to_numpy()
            score_out[idx] = best["match_score"].to_numpy()

    status = np.where(candidate_count > 0, "matched", "unmatched")

    return pd.DataFrame(
        {
            "event_id": event_ids,
            "segment_id": segment_id_out,
            "road_snapshot_date": snapshot_date,
            "map_match_distance_m": distance_out,
            "map_match_heading_diff_deg": heading_diff_out,
            "map_match_score": score_out,
            "candidate_count": candidate_count,
            "map_match_status": status,
        }
    )
