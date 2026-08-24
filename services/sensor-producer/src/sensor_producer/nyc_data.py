"""Acquire small reproducible samples from official NYC data sources."""

from __future__ import annotations

import hashlib
import json
import urllib.parse
import urllib.request
from collections.abc import Iterator
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import duckdb

from sensor_producer.domain import TripRecord
from sensor_producer.environment import load_taxi_zones

if TYPE_CHECKING:
    from sensor_producer.sampling import HourlySamplingPlan

NYC_TIMEZONE = ZoneInfo("America/New_York")
TAXI_ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
DEFAULT_HVFHV_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-02.parquet"
)
LION_QUERY_URL = (
    "https://services5.arcgis.com/GfwWNkhOj9bNBqoJ/arcgis/rest/services/"
    "LION/FeatureServer/0/query"
)
PAVEMENT_DATASET_ID = "6yyb-pb25"
HUMP_DATASET_ID = "jknp-skuy"
USER_AGENT = "DE4-sensor-producer/0.1 (+https://github.com/softeerbootcamp-8th/DE_team4-4una)"
TLC_TRIP_FETCH_BATCH_SIZE = 1000
TLC_TRIP_READER_VERSION = "tlc-hvfhv-parquet-v2"


def fetch_nyc_sample(
    output_dir: Path,
    zone_id: int = 181,
    source_date: date = date(2024, 2, 1),
    max_trips: int = 1,
    hvfhv_url: str = DEFAULT_HVFHV_URL,
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    taxi_path = output_dir / "taxi_zones.zip"
    download_file(TAXI_ZONE_URL, taxi_path)
    taxi_zones = load_taxi_zones(taxi_path)
    if zone_id not in taxi_zones:
        raise ValueError(f"taxi zone {zone_id} is not present in the official shapefile")
    west, south, east, north = taxi_zones[zone_id].bounds
    margin = 0.004
    bbox = (west - margin, south - margin, east + margin, north + margin)

    lion_path = output_dir / "lion.geojson"
    pavement_path = output_dir / "pavement.geojson"
    hump_path = output_dir / "speed_humps.geojson"
    trip_path = output_dir / "trips.json"
    fetch_lion(bbox, lion_path)
    fetch_socrata(PAVEMENT_DATASET_ID, bbox, pavement_path)
    fetch_socrata(HUMP_DATASET_ID, bbox, hump_path)
    trip_rows = fetch_hvfhv_rows(hvfhv_url, source_date, zone_id, max_trips)
    trip_path.write_text(json.dumps(trip_rows, indent=2, sort_keys=True))

    source_items = [
        source_item("tlc_hvfhv_trip_records", hvfhv_url, trip_path, len(trip_rows)),
        source_item("tlc_taxi_zones", TAXI_ZONE_URL, taxi_path, len(taxi_zones)),
        source_item(
            "nyc_lion_26b",
            LION_QUERY_URL,
            lion_path,
            geojson_count(lion_path),
        ),
        source_item(
            "nyc_street_pavement_ratings",
            f"https://data.cityofnewyork.us/resource/{PAVEMENT_DATASET_ID}.geojson",
            pavement_path,
            geojson_count(pavement_path),
        ),
        source_item(
            "nyc_vzv_speed_humps",
            f"https://data.cityofnewyork.us/resource/{HUMP_DATASET_ID}.geojson",
            hump_path,
            geojson_count(hump_path),
        ),
    ]
    manifest: dict[str, object] = {
        "created_at": datetime.now().astimezone().isoformat(),
        "source_date": source_date.isoformat(),
        "taxi_zone_id": zone_id,
        "bbox_wgs84": list(bbox),
        "sources": source_items,
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True))
    return manifest


def fetch_hvfhv_rows(
    source_url: str,
    source_date: date,
    zone_id: int,
    max_trips: int,
) -> list[dict[str, object]]:
    escaped_url = source_url.replace("'", "''")
    query = f"""
        SELECT
            hvfhs_license_num,
            dispatching_base_num,
            originating_base_num,
            request_datetime,
            on_scene_datetime,
            pickup_datetime,
            dropoff_datetime,
            PULocationID AS pu_location_id,
            DOLocationID AS do_location_id,
            trip_miles,
            trip_time
        FROM read_parquet('{escaped_url}')
        WHERE CAST(request_datetime AS DATE) = ?
          AND PULocationID = ?
          AND DOLocationID = ?
          AND request_datetime <= pickup_datetime
          AND pickup_datetime < dropoff_datetime
          AND trip_time > 0
        ORDER BY request_datetime, pickup_datetime, dispatching_base_num
        LIMIT ?
    """
    connection = duckdb.connect()
    rows = connection.execute(
        query,
        [source_date.isoformat(), zone_id, zone_id, max_trips],
    ).fetchall()
    columns = [description[0] for description in connection.description]
    connection.close()
    if not rows:
        raise ValueError(
            f"no valid HVFHV trips found for {source_date} within taxi zone {zone_id}"
        )

    result: list[dict[str, object]] = []
    for index, row in enumerate(rows):
        values = dict(zip(columns, row, strict=True))
        stable_text = "|".join(str(values[column]) for column in columns)
        values["trip_id"] = hashlib.sha256(f"{stable_text}|{index}".encode()).hexdigest()[:24]
        for key, value in list(values.items()):
            if isinstance(value, datetime):
                values[key] = value.isoformat()
        result.append(values)
    return result


def iter_hvfhv_parquet_trips(
    path: Path,
    *,
    batch_size: int = TLC_TRIP_FETCH_BATCH_SIZE,
    start_at: datetime | None = None,
    cycle_duration: timedelta | None = None,
    sampling_plan: HourlySamplingPlan | None = None,
) -> Iterator[TripRecord]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if (start_at is None) != (cycle_duration is None):
        raise ValueError("start_at and cycle_duration must be set together")
    if start_at is not None and start_at.utcoffset() is None:
        raise ValueError("start_at must be timezone-aware")

    query = """
        WITH replay_config AS (
            SELECT CAST(? AS TIMESTAMP) AS start_at, ?::DOUBLE AS cycle_seconds
        ), valid AS (
        SELECT
            request_datetime,
            pickup_datetime,
            dropoff_datetime,
            PULocationID AS pu_location_id,
            DOLocationID AS do_location_id,
            trip_miles,
            file_row_number
        FROM read_parquet(?, file_row_number = true)
        WHERE request_datetime IS NOT NULL
          AND pickup_datetime IS NOT NULL
          AND dropoff_datetime IS NOT NULL
          AND PULocationID IS NOT NULL
          AND DOLocationID IS NOT NULL
          AND trip_miles IS NOT NULL
          AND request_datetime <= pickup_datetime
          AND pickup_datetime < dropoff_datetime
          AND trip_miles > 0
          AND isfinite(trip_miles)
        )
        SELECT
            request_datetime,
            pickup_datetime,
            dropoff_datetime,
            pu_location_id,
            do_location_id,
            trip_miles,
            file_row_number,
            CASE WHEN request_datetime < start_at
                THEN request_datetime + cycle_seconds * INTERVAL '1 second'
                ELSE request_datetime END AS replay_request_datetime,
            CASE WHEN request_datetime < start_at
                THEN pickup_datetime + cycle_seconds * INTERVAL '1 second'
                ELSE pickup_datetime END AS replay_pickup_datetime,
            CASE WHEN request_datetime < start_at
                THEN dropoff_datetime + cycle_seconds * INTERVAL '1 second'
                ELSE dropoff_datetime END AS replay_dropoff_datetime
        FROM valid, replay_config
        ORDER BY
            replay_request_datetime,
            file_row_number
    """

    connection = duckdb.connect()
    yielded = False
    try:
        source_start = (
            start_at.astimezone(NYC_TIMEZONE).replace(tzinfo=None)
            if start_at is not None
            else datetime.min.replace(tzinfo=NYC_TIMEZONE).replace(tzinfo=None)
        )
        cycle_seconds = cycle_duration.total_seconds() if cycle_duration else 0
        cursor = connection.execute(
            query,
            [source_start, cycle_seconds, str(path)],
        )
        # Python 메모리에 전체 원천을 올리지 않고 정렬된 결과를 일정 건수씩 읽는다
        while rows := cursor.fetchmany(batch_size):
            for row in rows:
                request_time = parse_nyc_datetime(row[0])
                pickup_time = parse_nyc_datetime(row[1])
                dropoff_time = parse_nyc_datetime(row[2])
                source_trip = TripRecord(
                    trip_id=stable_trip_id(
                        int(row[6]),
                        request_time,
                        pickup_time,
                        dropoff_time,
                        int(row[3]),
                        int(row[4]),
                        float(row[5]),
                    ),
                    request_datetime=request_time,
                    pickup_datetime=pickup_time,
                    dropoff_datetime=dropoff_time,
                    pu_location_id=int(row[3]),
                    do_location_id=int(row[4]),
                    trip_miles=float(row[5]),
                )
                if sampling_plan is not None and not sampling_plan.includes(source_trip):
                    continue
                trip = TripRecord(
                    trip_id=source_trip.trip_id,
                    request_datetime=parse_nyc_datetime(row[7]),
                    pickup_datetime=parse_nyc_datetime(row[8]),
                    dropoff_datetime=parse_nyc_datetime(row[9]),
                    pu_location_id=source_trip.pu_location_id,
                    do_location_id=source_trip.do_location_id,
                    trip_miles=source_trip.trip_miles,
                )
                yielded = True
                yield trip
        if not yielded:
            raise ValueError(f"no valid HVFHV trips found in {path}")
    finally:
        connection.close()


def read_hvfhv_request_bounds(path: Path) -> tuple[datetime, datetime]:
    connection = duckdb.connect()
    try:
        first_request, last_request = connection.execute(
            """
            SELECT min(request_datetime), max(request_datetime)
            FROM read_parquet(?)
            WHERE request_datetime IS NOT NULL
              AND pickup_datetime IS NOT NULL
              AND dropoff_datetime IS NOT NULL
              AND PULocationID IS NOT NULL
              AND DOLocationID IS NOT NULL
              AND trip_miles IS NOT NULL
              AND request_datetime <= pickup_datetime
              AND pickup_datetime < dropoff_datetime
              AND trip_miles > 0
              AND isfinite(trip_miles)
            """,
            [str(path)],
        ).fetchone()
    finally:
        connection.close()
    if first_request is None or last_request is None:
        raise ValueError(f"no valid HVFHV trips found in {path}")
    return parse_nyc_datetime(first_request), parse_nyc_datetime(last_request)


def stable_trip_id(
    file_row_number: int,
    request_datetime: datetime,
    pickup_datetime: datetime,
    dropoff_datetime: datetime,
    pu_location_id: int,
    do_location_id: int,
    trip_miles: float,
) -> str:
    values = (
        TLC_TRIP_READER_VERSION,
        str(file_row_number),
        request_datetime.isoformat(),
        pickup_datetime.isoformat(),
        dropoff_datetime.isoformat(),
        str(pu_location_id),
        str(do_location_id),
        format(trip_miles, ".17g"),
    )
    return hashlib.sha256("|".join(values).encode()).hexdigest()[:24]


def parse_nyc_datetime(value: str | datetime) -> datetime:
    parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
    return parsed.replace(tzinfo=NYC_TIMEZONE) if parsed.tzinfo is None else parsed


def fetch_lion(bbox: tuple[float, float, float, float], output_path: Path) -> None:
    features: list[dict[str, object]] = []
    offset = 0
    while True:
        parameters = {
            "where": "1=1",
            "geometry": ",".join(str(value) for value in bbox),
            "geometryType": "esriGeometryEnvelope",
            "inSR": "4326",
            "spatialRel": "esriSpatialRelIntersects",
            "outFields": (
                "SegmentID,NodeIDFrom,NodeIDTo,TrafDir,SegmentTyp,FeatureTyp,"
                "RB_Layer,NodeLevelF,NodeLevelT,POSTED_SPEED,CurveFlag,Radius,Street"
            ),
            "returnGeometry": "true",
            "outSR": "4326",
            "f": "geojson",
            "resultRecordCount": "2000",
            "resultOffset": str(offset),
        }
        document = request_json(f"{LION_QUERY_URL}?{urllib.parse.urlencode(parameters)}")
        page = document.get("features", [])
        features.extend(page)
        if not document.get("properties", {}).get("exceededTransferLimit") or not page:
            break
        offset += len(page)
    output_path.write_text(
        json.dumps(
            {"type": "FeatureCollection", "features": features},
            separators=(",", ":"),
        )
    )


def fetch_socrata(
    dataset_id: str,
    bbox: tuple[float, float, float, float],
    output_path: Path,
) -> None:
    west, south, east, north = bbox
    parameters = {
        "$limit": "50000",
        "$where": f"within_box(the_geom, {north}, {west}, {south}, {east})",
    }
    url = (
        f"https://data.cityofnewyork.us/resource/{dataset_id}.geojson?"
        f"{urllib.parse.urlencode(parameters)}"
    )
    document = request_json(url)
    output_path.write_text(json.dumps(document, separators=(",", ":")))


def request_json(url: str) -> dict[str, object]:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=120) as response:
        return json.load(response)


def download_file(url: str, output_path: Path) -> None:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with (
        urllib.request.urlopen(request, timeout=120) as response,
        output_path.open("wb") as destination,
    ):
        while chunk := response.read(1024 * 1024):
            destination.write(chunk)


def source_item(
    source_id: str,
    source_url: str,
    local_path: Path,
    selected_records: int,
) -> dict[str, object]:
    return {
        "source_id": source_id,
        "source_url": source_url,
        "local_file": local_path.name,
        "selected_records": selected_records,
        "sha256": file_sha256(local_path),
        "bytes": local_path.stat().st_size,
    }


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def geojson_count(path: Path) -> int:
    return len(json.loads(path.read_text()).get("features", []))
