import json

from sensor_producer.nyc_data import load_trips


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
                }
            ]
        )
    )

    loaded = load_trips(path)

    assert loaded[0].request_datetime.tzinfo is not None
    assert loaded[0].request_datetime.utcoffset().total_seconds() == -5 * 3600
