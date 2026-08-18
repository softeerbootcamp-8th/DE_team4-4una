from datetime import date, datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sensor_producer.nyc_data import NYC_TIMEZONE
from sensor_producer.trip_input import iter_parquet_trips


def source_time(day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(2024, 2, day, hour, minute, tzinfo=NYC_TIMEZONE).replace(
        tzinfo=None
    )


def test_parquet_trip_input_filters_day_and_orders_raw_tlc_columns(tmp_path) -> None:
    path = tmp_path / "fhvhv.parquet"
    table = pa.table(
        {
            "hvfhs_license_num": ["HV0003", "HV0003", "HV0005", "HV0003"],
            "dispatching_base_num": ["B2", "B3", "B1", "B4"],
            "request_datetime": [
                source_time(1, 10),
                source_time(2, 9),
                source_time(1, 9),
                source_time(1, 11),
            ],
            "pickup_datetime": [
                source_time(1, 10, 5),
                source_time(2, 9, 5),
                source_time(1, 9, 5),
                source_time(1, 11, 5),
            ],
            "dropoff_datetime": [
                source_time(1, 10, 15),
                source_time(2, 9, 15),
                source_time(1, 9, 15),
                source_time(1, 11, 15),
            ],
            "PULocationID": [181, 181, 181, 181],
            "DOLocationID": [181, 181, 181, 181],
            "trip_miles": [2.0, 3.0, 1.0, -1.0],
        }
    )
    pq.write_table(table, path)

    first = list(iter_parquet_trips(str(path), date(2024, 2, 1)))
    second = list(iter_parquet_trips(str(path), date(2024, 2, 1)))

    assert [trip.request_datetime.hour for trip in first] == [9, 10]
    assert [trip.trip_id for trip in first] == [trip.trip_id for trip in second]
    assert all(trip.request_datetime.tzinfo is not None for trip in first)


def test_parquet_trip_input_preserves_prepared_trip_id_and_limit(tmp_path) -> None:
    path = tmp_path / "prepared.parquet"
    pq.write_table(
        pa.table(
            {
                "trip_id": ["trip-2", "trip-1"],
                "request_datetime": [
                    source_time(1, 10),
                    source_time(1, 9),
                ],
                "pickup_datetime": [
                    source_time(1, 10, 5),
                    source_time(1, 9, 5),
                ],
                "dropoff_datetime": [
                    source_time(1, 10, 15),
                    source_time(1, 9, 15),
                ],
                "pu_location_id": [181, 181],
                "do_location_id": [181, 181],
                "trip_miles": [2.0, 1.0],
            }
        ),
        path,
    )

    trips = list(iter_parquet_trips(str(path), date(2024, 2, 1), max_trips=1))

    assert [trip.trip_id for trip in trips] == ["trip-1"]


def test_s3_trip_input_requires_a_bounded_replay_date() -> None:
    with pytest.raises(ValueError, match="replay_date"):
        iter_parquet_trips("s3://test-bucket/source.parquet", None)


def test_parquet_trip_input_reports_missing_required_columns(tmp_path) -> None:
    path = tmp_path / "missing.parquet"
    pq.write_table(pa.table({"request_datetime": [source_time(1, 9)]}), path)

    with pytest.raises(ValueError, match="missing required columns"):
        iter_parquet_trips(str(path), date(2024, 2, 1))
