"""Convert cleansed Bronze rows for in-memory feature processing."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from batch_jobs.schemas import PROCESSED_SENSOR_EVENT_SCHEMA


def to_processed_sensor_events(
    passed: DataFrame,
    run_id: str,
    processed_at: datetime,
) -> DataFrame:
    """Convert cleansed rows into the typed DataFrame consumed by features."""
    event_time = F.to_timestamp("event_time")
    overrides: dict[str, Column] = {
        "event_time": event_time,
        "_processed_at": F.lit(processed_at),
        "_run_id": F.lit(run_id),
    }
    return passed.select(
        *[
            overrides.get(field.name, F.col(field.name)).alias(field.name)
            for field in PROCESSED_SENSOR_EVENT_SCHEMA.fields
        ]
    )
