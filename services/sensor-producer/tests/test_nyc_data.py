import json
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from de4_core import ObjectStore
from sensor_producer.nyc_data import (
    iter_hvfhv_parquet_trips,
    load_trips,
    materialize_trip_parquet,
)


class FakeS3Client:
    def __init__(self, value: bytes) -> None:
        self.value = value
        self.download_count = 0

    def download_file(self, bucket: str, key: str, filename: str) -> None:
        assert (bucket, key) == ("trip-bucket", "raw/fhvhv.parquet")
        self.download_count += 1
        destination = Path(filename)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(self.value)


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


def test_load_trips_interprets_naive_tlc_times_as_new_york(tmp_path) -> None:
    path = tmp_path / "trips.json"
    path.write_text(
        json.dumps(
            [
                {
                    "trip_id": "trip-1",
                    "request_datetime": "2024-02-01T10:00:00",
                    "pickup_datetime": "2024-02-01T10:05:00",
                    "dropoff_datetime": "2024-02-01T10:06:00",
                    "pu_location_id": 181,
                    "do_location_id": 181,
                    "trip_miles": 1.25,
                }
            ]
        )
    )

    loaded = load_trips(path)

    assert loaded[0].request_datetime.tzinfo is not None
    assert loaded[0].request_datetime.utcoffset().total_seconds() == -5 * 3600
    assert loaded[0].trip_miles == 1.25


def test_load_trips_orders_dispatches_deterministically(tmp_path) -> None:
    path = tmp_path / "trips.json"
    path.write_text(
        json.dumps(
            [
                {
                    "trip_id": trip_id,
                    "request_datetime": request_time,
                    "pickup_datetime": "2024-02-01T10:05:00",
                    "dropoff_datetime": "2024-02-01T10:06:00",
                    "pu_location_id": 181,
                    "do_location_id": 181,
                    "trip_miles": 1.25,
                }
                for trip_id, request_time in (
                    ("trip-b", "2024-02-01T10:01:00"),
                    ("trip-c", "2024-02-01T10:00:00"),
                    ("trip-a", "2024-02-01T10:00:00"),
                )
            ]
        )
    )

    loaded = load_trips(path)

    assert [trip.trip_id for trip in loaded] == ["trip-a", "trip-c", "trip-b"]


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


def test_local_and_s3_parquet_inputs_produce_same_trips(tmp_path: Path) -> None:
    source = tmp_path / "fhvhv.parquet"
    write_trip_parquet(source)
    local_path = materialize_trip_parquet(source.as_uri(), tmp_path / "local-cache")

    client = FakeS3Client(source.read_bytes())
    store = ObjectStore(client)  # type: ignore[arg-type]
    uri = "s3://trip-bucket/raw/fhvhv.parquet"
    s3_path = materialize_trip_parquet(uri, tmp_path / "s3-cache", store)
    cached_path = materialize_trip_parquet(uri, tmp_path / "s3-cache", store)

    local = list(iter_hvfhv_parquet_trips(local_path, date(2024, 2, 1)))
    remote = list(iter_hvfhv_parquet_trips(s3_path, date(2024, 2, 1)))
    assert local == remote
    assert s3_path == cached_path
    assert client.download_count == 1


@pytest.mark.parametrize(
    "uri",
    ["s3://trip-bucket/raw/", "s3://trip-bucket/raw/trips.json"],
)
def test_materialize_trip_parquet_requires_one_parquet_object(
    tmp_path: Path, uri: str
) -> None:
    with pytest.raises(ValueError, match="one Parquet object"):
        materialize_trip_parquet(uri, tmp_path)
