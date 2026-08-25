"""Score LION road_segment candidates by GPS distance and vehicle heading."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import shapely
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

from batch_jobs.map_matching.candidates import OUTPUT_COLUMNS, validate_search_radius


def validate_score_weights(distance_weight: float, heading_weight: float) -> None:
    if not all(
        math.isfinite(w) and 0.0 <= w <= 1.0 for w in (distance_weight, heading_weight)
    ):
        raise ValueError("distance_weight and heading_weight must be between 0.0 and 1.0")
    if not math.isclose(distance_weight + heading_weight, 1.0):
        raise ValueError("distance_weight + heading_weight must sum to 1.0")


def score_segment_candidates(
    candidate_df: DataFrame,
    sensor_df: DataFrame,
    search_radius_m: float,
    distance_weight: float,
    heading_weight: float,
) -> DataFrame:
    """candidate_df에 road_bearing_deg, heading_diff_deg, match_score를 추가한다."""
    validate_search_radius(search_radius_m)
    validate_score_weights(distance_weight, heading_weight)

    joined = candidate_df.join(
        sensor_df.select("event_id", "heading"), on="event_id", how="left"
    ).withColumn(
        "road_bearing_deg",
        _road_bearing_udf(
            F.col("candidate_geometry_wkb"),
            F.col("candidate_traffic_direction"),
            F.col("heading"),
        ),
    )

    # heading, road_bearing_deg 중 하나라도 NULL이면 산술 연산이 그대로 NULL로 전파된다.
    heading_diff_deg = F.abs(
        F.pmod(F.col("heading") - F.col("road_bearing_deg") + 180.0, F.lit(360.0)) - 180.0
    )
    with_diff = joined.withColumn("heading_diff_deg", heading_diff_deg)

    distance_score = _clamp_score(1.0 - F.col("distance_m") / search_radius_m, F.col("distance_m"))
    heading_score = _clamp_score(
        1.0 - F.col("heading_diff_deg") / 180.0, F.col("heading_diff_deg")
    )
    with_scores = with_diff.withColumn("distance_score", distance_score).withColumn(
        "heading_score", heading_score
    )

    match_score = (
        F.when(F.col("candidate_segment_id").isNull(), F.lit(None).cast(DoubleType()))
        .when(F.col("heading_score").isNull(), F.col("distance_score"))
        .otherwise(
            distance_weight * F.col("distance_score") + heading_weight * F.col("heading_score")
        )
    )

    return with_scores.withColumn("match_score", match_score).select(
        *OUTPUT_COLUMNS, "road_bearing_deg", "heading_diff_deg", "match_score"
    )


def _clamp_score(raw: Column, null_when: Column) -> Column:
    # F.greatest/least는 NULL을 건너뛰므로 clamp 전에 NULL 여부를 먼저 걸러낸다.
    return (
        F.when(null_when.isNull(), F.lit(None).cast(DoubleType()))
        .when(raw < 0.0, F.lit(0.0))
        .otherwise(raw)
    )


@pandas_udf(DoubleType())
def _road_bearing_udf(
    geometry_wkb: pd.Series, traffic_direction: pd.Series, heading: pd.Series
) -> pd.Series:
    return compute_road_bearing(geometry_wkb, traffic_direction, heading)


def compute_road_bearing(
    geometry_wkb: pd.Series, traffic_direction: pd.Series, heading: pd.Series
) -> pd.Series:
    """geometry 진행 방향(첫->마지막 좌표)을 traffic_direction(W/A/T)별로 해석한다."""
    has_geometry = geometry_wkb.notna().to_numpy()
    road_bearing = np.full(len(geometry_wkb), np.nan, dtype="float64")
    if not has_geometry.any():
        return pd.Series(road_bearing, index=geometry_wkb.index)

    lines = shapely.from_wkb(geometry_wkb.to_numpy()[has_geometry])
    forward_bearing, reverse_bearing = compute_forward_reverse_bearing(lines)

    directions = traffic_direction.to_numpy()[has_geometry]
    headings = heading.to_numpy(dtype="float64", na_value=np.nan)[has_geometry]

    road_bearing[has_geometry] = resolve_prematched_road_bearing(
        forward_bearing, reverse_bearing, directions, headings
    )
    return pd.Series(road_bearing, index=geometry_wkb.index)


def compute_forward_reverse_bearing(geometries: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """geometry(첫->마지막 좌표) 기준 forward/reverse bearing(0~360도)을 geometries와 동일 순서로 구한다."""
    starts = shapely.get_point(geometries, 0)
    ends = shapely.get_point(geometries, -1)
    dx = shapely.get_x(ends) - shapely.get_x(starts)
    dy = shapely.get_y(ends) - shapely.get_y(starts)
    forward_bearing = np.degrees(np.arctan2(dx, dy)) % 360
    reverse_bearing = (forward_bearing + 180.0) % 360
    return forward_bearing, reverse_bearing


def resolve_prematched_road_bearing(
    forward_bearing_deg: np.ndarray,
    reverse_bearing_deg: np.ndarray,
    traffic_direction: np.ndarray,
    heading: np.ndarray,
) -> np.ndarray:
    """precomputed forward/reverse bearing으로 traffic_direction(W/A/T) 규칙에 따라 road_bearing_deg를 구한다(compute_road_bearing()과 동일 계산식)."""
    diff_forward = circular_heading_diff(heading, forward_bearing_deg)
    diff_reverse = circular_heading_diff(heading, reverse_bearing_deg)
    two_way_bearing = np.where(diff_forward <= diff_reverse, forward_bearing_deg, reverse_bearing_deg)
    two_way_bearing = np.where(np.isnan(heading), np.nan, two_way_bearing)

    return np.select(
        [traffic_direction == "W", traffic_direction == "A", traffic_direction == "T"],
        [forward_bearing_deg, reverse_bearing_deg, two_way_bearing],
        default=np.nan,
    )


def circular_heading_diff(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """두 방위각(deg) 간 원형 차이. NaN이 섞이면 NaN으로 전파된다."""
    return np.abs((a - b + 180.0) % 360.0 - 180.0)


def compute_match_scores(
    distance_m: np.ndarray,
    heading_diff_deg: np.ndarray,
    search_radius_m: float,
    distance_weight: float,
    heading_weight: float,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """distance_score, heading_score, match_score를 score_segment_candidates()와 동일한 공식으로 NumPy로 계산한다."""
    distance_score = _clamp_score_np(1.0 - distance_m / search_radius_m, distance_m)
    heading_score = _clamp_score_np(1.0 - heading_diff_deg / 180.0, heading_diff_deg)
    match_score = np.where(
        np.isnan(heading_score),
        distance_score,
        distance_weight * distance_score + heading_weight * heading_score,
    )
    return distance_score, heading_score, match_score


def _clamp_score_np(raw: np.ndarray, null_when: np.ndarray) -> np.ndarray:
    clamped = np.where(raw < 0.0, 0.0, raw)
    return np.where(np.isnan(null_when), np.nan, clamped)
