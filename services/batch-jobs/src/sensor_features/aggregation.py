"""Build Hourly aggregation keys for map-matched sensor events."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

_REQUIRED_COLUMNS = {
    "event_time",
    "segment_id",
    "vehicle_profile_id",
}


def add_hourly_aggregation_keys(df: DataFrame) -> DataFrame:
    """Add hourly aggregation keys and exclude unmatched sensor events."""
    missing_columns = _REQUIRED_COLUMNS - set(df.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"Missing required columns: {missing}")

    data_period_start = F.date_trunc("hour", F.col("event_time"))

    return (
        df.filter(F.col("segment_id").isNotNull())
        .withColumn("data_period_start", data_period_start)
        .withColumn("data_period_end", F.col("data_period_start") + F.expr("INTERVAL 1 HOUR"))
    )
