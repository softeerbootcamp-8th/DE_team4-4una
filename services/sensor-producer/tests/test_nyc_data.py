from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from sensor_producer.nyc_data import iter_hvfhv_parquet_trips


def source_time(day: int, hour: int, minute: int = 0) -> datetime:
    # TLC 원천 Parquet의 시각은 timezone이 없는 뉴욕 현지 시각이다
    return datetime(2024, 2, day, hour, minute)  # noqa: DTZ001


def write_trip_parquet(path: Path) -> None:
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "request_datetime": source_time(1, 10, 5),
                    "pickup_datetime": source_time(1, 10, 6),
                    "dropoff_datetime": source_time(1, 10, 8),
                    "PULocationID": 181,
                    "DOLocationID": 182,
                    "trip_miles": 1.5,
                },
                {
                    "request_datetime": source_time(1, 10),
                    "pickup_datetime": source_time(1, 10, 1),
                    "dropoff_datetime": source_time(1, 10, 3),
                    "PULocationID": 183,
                    "DOLocationID": 184,
                    "trip_miles": 2.0,
                },
                {
                    "request_datetime": source_time(1, 11),
                    "pickup_datetime": source_time(1, 10, 59),
                    "dropoff_datetime": source_time(1, 11, 5),
                    "PULocationID": 181,
                    "DOLocationID": 182,
                    "trip_miles": 1.0,
                },
                {
                    "request_datetime": source_time(2, 9),
                    "pickup_datetime": source_time(2, 9, 1),
                    "dropoff_datetime": source_time(2, 9, 2),
                    "PULocationID": 181,
                    "DOLocationID": 182,
                    "trip_miles": 1.0,
                },
            ]
        ),
        path,
    )


def test_parquet_trips_filter_order_and_limit_in_batches(tmp_path: Path) -> None:
    path = tmp_path / "fhvhv.parquet"
    write_trip_parquet(path)

    first = list(
        iter_hvfhv_parquet_trips(
            path,
            date(2024, 2, 1),
            maximum=2,
            batch_size=1,
        )
    )
    second = list(iter_hvfhv_parquet_trips(path, date(2024, 2, 1), maximum=2))

    assert [trip.pu_location_id for trip in first] == [183, 181]
    assert [trip.request_datetime for trip in first] == sorted(
        trip.request_datetime for trip in first
    )
    assert [trip.trip_id for trip in first] == [trip.trip_id for trip in second]
