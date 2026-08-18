"""Read deterministic TLC replay inputs from local or S3 Parquet datasets."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from datetime import date, datetime, time, timedelta

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

from sensor_producer.domain import TripRecord
from sensor_producer.nyc_data import NYC_TIMEZONE, parse_nyc_datetime

TRIP_COLUMN_ALIASES = {
    "trip_id": ("trip_id",),
    "request_datetime": ("request_datetime",),
    "pickup_datetime": ("pickup_datetime",),
    "dropoff_datetime": ("dropoff_datetime",),
    "pu_location_id": ("pu_location_id", "PULocationID"),
    "do_location_id": ("do_location_id", "DOLocationID"),
    "trip_miles": ("trip_miles",),
    "dispatching_base_num": ("dispatching_base_num",),
    "hvfhs_license_num": ("hvfhs_license_num",),
}
REQUIRED_COLUMNS = (
    "request_datetime",
    "pickup_datetime",
    "dropoff_datetime",
    "pu_location_id",
    "do_location_id",
    "trip_miles",
)
IDENTITY_COLUMNS = (
    "hvfhs_license_num",
    "dispatching_base_num",
    *REQUIRED_COLUMNS,
)


def iter_parquet_trips(
    uri: str,
    replay_date: date | None,
    max_trips: int | None = None,
) -> Iterator[TripRecord]:
    """Yield a stable request-time ordering from a local or S3 Parquet dataset."""
    if uri.startswith("s3://") and replay_date is None:
        raise ValueError("replay_date is required for an S3 TLC Parquet input")
    if max_trips is not None and max_trips < 1:
        raise ValueError("max_trips must be positive")

    dataset = ds.dataset(
        uri,
        format="parquet",
        partitioning="hive",
        exclude_invalid_files=True,
    )
    resolved = resolve_trip_columns(dataset.schema.names)
    request_column = resolved["request_datetime"]
    source_filter = replay_date_filter(
        request_column,
        dataset.schema.field(request_column).type,
        replay_date,
    )
    selected_columns = list(dict.fromkeys(resolved.values()))
    source_table = dataset.to_table(columns=selected_columns, filter=source_filter)
    table = normalize_trip_table(source_table, resolved)
    table = table.filter(valid_trip_mask(table))
    if table.num_rows == 0:
        raise ValueError("TLC Parquet input contains no valid trips for the replay date")

    sort_keys = [
        (name, "ascending")
        for name in (
            "request_datetime",
            "pickup_datetime",
            "dispatching_base_num",
            "dropoff_datetime",
            "pu_location_id",
            "do_location_id",
            "trip_miles",
            "trip_id",
        )
        if name in table.column_names
    ]
    table = table.sort_by(sort_keys)
    if max_trips is not None:
        table = table.slice(0, max_trips)

    return trip_records(table)


def trip_records(table: pa.Table) -> Iterator[TripRecord]:
    """Convert bounded Arrow batches without expanding the full day into dicts."""

    row_index = 0
    for batch in table.to_batches(max_chunksize=65_536):
        for row in batch.to_pylist():
            trip_id = row.get("trip_id") or stable_trip_id(row, row_index)
            yield TripRecord(
                trip_id=str(trip_id),
                request_datetime=parse_nyc_datetime(row["request_datetime"]),
                pickup_datetime=parse_nyc_datetime(row["pickup_datetime"]),
                dropoff_datetime=parse_nyc_datetime(row["dropoff_datetime"]),
                pu_location_id=int(row["pu_location_id"]),
                do_location_id=int(row["do_location_id"]),
                trip_miles=float(row["trip_miles"]),
            )
            row_index += 1


def resolve_trip_columns(schema_names: list[str]) -> dict[str, str]:
    available = set(schema_names)
    resolved = {
        canonical: next(
            (candidate for candidate in aliases if candidate in available),
            "",
        )
        for canonical, aliases in TRIP_COLUMN_ALIASES.items()
    }
    missing = [name for name in REQUIRED_COLUMNS if not resolved[name]]
    if missing:
        raise ValueError(
            "TLC Parquet input is missing required columns: " + ", ".join(missing)
        )
    return {name: source for name, source in resolved.items() if source}


def replay_date_filter(
    request_column: str,
    request_type: pa.DataType,
    replay_date: date | None,
) -> ds.Expression | None:
    if replay_date is None:
        return None
    if not pa.types.is_timestamp(request_type):
        raise TypeError("request_datetime must be a Parquet timestamp")

    timezone = getattr(request_type, "tz", None)
    start = datetime.combine(replay_date, time.min)
    if timezone:
        start = start.replace(tzinfo=NYC_TIMEZONE)
    end = start + timedelta(days=1)
    field = ds.field(request_column)
    return (field >= start) & (field < end)


def normalize_trip_table(
    source_table: pa.Table,
    resolved: dict[str, str],
) -> pa.Table:
    return pa.table(
        {canonical: source_table[source] for canonical, source in resolved.items()}
    )


def valid_trip_mask(table: pa.Table) -> pa.Array | pa.ChunkedArray:
    mask = pc.less_equal(table["request_datetime"], table["pickup_datetime"])
    mask = pc.and_kleene(
        mask,
        pc.less(table["pickup_datetime"], table["dropoff_datetime"]),
    )
    mask = pc.and_kleene(mask, pc.greater(table["trip_miles"], 0))
    for name in REQUIRED_COLUMNS:
        mask = pc.and_kleene(mask, pc.is_valid(table[name]))
    return pc.fill_null(mask, False)


def stable_trip_id(row: dict[str, object], row_index: int) -> str:
    values = [stable_value(row.get(name)) for name in IDENTITY_COLUMNS]
    values.append(str(row_index))
    return hashlib.sha256("|".join(values).encode()).hexdigest()[:24]


def stable_value(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return "" if value is None else str(value)
