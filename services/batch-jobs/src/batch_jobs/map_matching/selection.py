"""Select the best LION road segment candidate for each sensor event."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

REQUIRED_COLUMNS = (
    "event_id",
    "candidate_segment_id",
    "road_snapshot_date",
    "distance_m",
    "heading_diff_deg",
    "match_score",
)


def select_best_segment(scored_candidate_df: DataFrame) -> DataFrame:
    """이벤트별 최고 점수의 도로 Segment 후보를 하나만 선택한다."""
    missing_columns = [
        column for column in REQUIRED_COLUMNS if column not in scored_candidate_df.columns
    ]
    if missing_columns:
        raise ValueError(f"scored_candidate_df is missing required columns: {missing_columns}")

    # match_score만으로 정렬하면 동점일 때 실행마다 결과가 달라질 수 있어, 거리·방향·
    # segment_id 순으로 완전히 결정적인 순서를 만든다.
    event_window = Window.partitionBy("event_id")
    selection_window = event_window.orderBy(
        F.col("match_score").desc_nulls_last(),
        F.col("distance_m").asc_nulls_last(),
        F.col("heading_diff_deg").asc_nulls_last(),
        F.col("candidate_segment_id").asc_nulls_last(),
    )

    selected = (
        scored_candidate_df.withColumn(
            "candidate_count",
            F.count(F.when(F.col("candidate_segment_id").isNotNull(), 1)).over(event_window),
        )
        .withColumn("_candidate_rank", F.row_number().over(selection_window))
        .filter(F.col("_candidate_rank") == 1)
        .drop("_candidate_rank")
    )

    return selected.select(
        "event_id",
        F.col("candidate_segment_id").alias("segment_id"),
        "road_snapshot_date",
        F.col("distance_m").alias("map_match_distance_m"),
        F.col("heading_diff_deg").alias("map_match_heading_diff_deg"),
        F.col("match_score").alias("map_match_score"),
        "candidate_count",
        F.when(F.col("candidate_segment_id").isNotNull(), F.lit("matched"))
        .otherwise(F.lit("unmatched"))
        .alias("map_match_status"),
    )
