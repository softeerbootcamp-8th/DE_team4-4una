"""Shared trip-ordering Window contract for steering/event feature calculations."""

from __future__ import annotations

from pyspark.sql.window import Window


def trip_window(*extra_partition_columns: str) -> Window:
    """같은 trip(및 추가 파티션 컬럼) 내부에서만 이전 행을 찾는 결정적 정렬 Window(trip_id[+extra], trip_seq/event_time/event_id 순)."""
    return Window.partitionBy("trip_id", *extra_partition_columns).orderBy(
        "trip_seq", "event_time", "event_id"
    )
