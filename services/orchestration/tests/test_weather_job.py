# jobs/weather.py 테스트 (#207; batch-jobs의 weather_snapshot_job.py에서 이식, #209).

from __future__ import annotations

import io
import os
import sys
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import ClassVar
from urllib.parse import unquote

import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests
from botocore.exceptions import ClientError
from de4_core import ObjectStore

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from jobs import weather as weather_module
from jobs.weather import (
    _SNAPSHOT_SCHEMA,
    FOG_WEATHER_CODES,
    HIGH_WIND_GUST_THRESHOLD_MPS,
    HTTP_RETRY_STATUS_FORCELIST,
    HTTP_RETRY_TOTAL,
    LOW_VISIBILITY_THRESHOLD_M,
    RAIN_WEATHER_CODES,
    SNOW_WEATHER_CODES,
    LatestZoneWeatherJobConfig,
    WeatherRegionCoordinate,
    _build_default_session,
    _validate_target_time,
    classify_weather_state,
    fetch_open_meteo,
    load_weather_regions,
    load_zone_weather_region_map,
    run_latest_zone_weather_job,
    write_zone_weather_snapshot,
)

RUN_INTEGRATION = os.environ.get("RUN_INTEGRATION") == "1"

TARGET_TIME = datetime(2026, 8, 19, 10, 15, tzinfo=UTC)
TARGET_KEY = "2026-08-19T10:15"


# jobs.weather가 읽는 reference 파일 2개를 만든다. regions는 {권역 id: (lat, lon)},
# mapping은 {location_id: 권역 id}. 기본값은 zone 하나가 권역 하나인 최소 구성이다.
def write_weather_region_files(
    directory: Path,
    *,
    regions: dict[int, tuple[float, float]] | None = None,
    mapping: dict[int, int] | None = None,
) -> tuple[Path, Path]:
    regions = regions if regions is not None else {1: (40.7, -73.9)}
    mapping = mapping if mapping is not None else {181: 1}

    master_path = directory / "weather_region_master.parquet"
    pq.write_table(
        pa.table(
            {
                "weather_region_id": list(regions),
                "representative_latitude": [latitude for latitude, _ in regions.values()],
                "representative_longitude": [longitude for _, longitude in regions.values()],
            }
        ),
        master_path,
    )

    map_path = directory / "zone_weather_region_map.parquet"
    pq.write_table(
        pa.table(
            {"location_id": list(mapping), "weather_region_id": list(mapping.values())}
        ),
        map_path,
    )
    return master_path, map_path


class FakeResponse:
    def __init__(self, payload, status_code: int = 200, headers: dict | None = None):
        self._payload = payload
        self.status_code = status_code
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        pass

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, payloads: list) -> None:
        self._payloads = list(payloads)
        self.calls: list[dict] = []

    def get(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        return FakeResponse(self._payloads[len(self.calls) - 1])


class SequencedSession:
    # 호출 순서대로 응답을 돌려준다. Exception이면 그대로 raise한다(batch 실패
    # 시뮬레이션용). 이미 만들어진 FakeResponse(예: 429 상태코드/헤더가 필요한
    # 경우)는 그대로 반환하고, 그 외 raw payload는 FakeResponse로 감싼다.
    def __init__(self, responses: list) -> None:
        self._responses = list(responses)
        self.calls = 0

    def get(self, url, params=None, timeout=None):
        item = self._responses[self.calls]
        self.calls += 1
        if isinstance(item, Exception):
            raise item
        if isinstance(item, FakeResponse):
            return item
        return FakeResponse(item)


def reading(**overrides) -> dict:
    values = {
        "temperature_2m": 20.0,
        "precipitation": 0.0,
        "rain": 0.0,
        "snowfall": 0.0,
        "visibility": 10000.0,
        "wind_speed_10m": 3.0,
        "wind_gusts_10m": 5.0,
        "weather_code": 0,
    }
    values.update(overrides)
    return values


def location_payload(times: list[str], **series_overrides) -> dict:
    series = {
        "temperature_2m": [20.0] * len(times),
        "precipitation": [0.0] * len(times),
        "rain": [0.0] * len(times),
        "snowfall": [0.0] * len(times),
        "visibility": [10000.0] * len(times),
        "wind_speed_10m": [3.0] * len(times),
        "wind_gusts_10m": [5.0] * len(times),
        "weather_code": [0] * len(times),
    }
    series.update(series_overrides)
    return {"latitude": 40.7, "longitude": -73.9, "minutely_15": {"time": times, **series}}


class TestValidateTargetTime:
    def test_rejects_a_naive_datetime(self):
        with pytest.raises(ValueError, match="timezone-aware"):
            _validate_target_time(datetime(2026, 8, 19, 10, 15))  # noqa: DTZ001

    def test_rejects_a_time_off_the_15_minute_boundary(self):
        with pytest.raises(ValueError, match="15-minute boundary"):
            _validate_target_time(datetime(2026, 8, 19, 10, 5, tzinfo=UTC))

    def test_accepts_every_valid_boundary_minute(self):
        for minute in (0, 15, 30, 45):
            _validate_target_time(datetime(2026, 8, 19, 10, minute, tzinfo=UTC))  # must not raise

    def test_normalizes_a_non_utc_timezone_to_utc(self):
        kst = timezone(timedelta(hours=9))
        result = _validate_target_time(datetime(2026, 8, 19, 19, 15, tzinfo=kst))

        assert result == datetime(2026, 8, 19, 10, 15, tzinfo=UTC)
        assert result.utcoffset() == timedelta(0)


class TestBuildDefaultSession:
    def test_configures_retries_for_get_requests(self):
        session = _build_default_session()

        adapter = session.get_adapter("https://api.open-meteo.com/v1/forecast")

        assert adapter.max_retries.total == HTTP_RETRY_TOTAL
        assert adapter.max_retries.status_forcelist == HTTP_RETRY_STATUS_FORCELIST
        assert adapter.max_retries.allowed_methods == frozenset({"GET"})

    def test_5xx_is_still_retried_at_the_http_layer(self):
        # 429 처리 방식을 바꾸면서 5xx retry는 그대로 유지되는지 확인한다(#444).
        assert {500, 502, 503, 504} <= set(HTTP_RETRY_STATUS_FORCELIST)

    def test_429_is_excluded_from_http_layer_retry(self):
        # 429는 fetch_open_meteo()가 직접 처리한다 — HTTP adapter가 조용히 재시도하면
        # 안 된다(#444).
        assert 429 not in HTTP_RETRY_STATUS_FORCELIST


class TestClassifyWeatherState:
    def test_dry_when_nothing_notable(self):
        assert classify_weather_state(reading()) == "dry"

    def test_rain_from_measured_rain_mm(self):
        assert classify_weather_state(reading(rain=0.2)) == "rain"

    def test_rain_from_weather_code_even_when_measured_rain_is_zero(self):
        code = next(iter(RAIN_WEATHER_CODES))
        assert classify_weather_state(reading(weather_code=code)) == "rain"

    def test_snow_from_measured_snowfall_cm(self):
        assert classify_weather_state(reading(snowfall=0.1)) == "snow"

    def test_snow_from_weather_code_even_when_measured_snowfall_is_zero(self):
        code = next(iter(SNOW_WEATHER_CODES))
        assert classify_weather_state(reading(weather_code=code)) == "snow"

    def test_snow_takes_priority_over_rain_when_both_present(self):
        assert classify_weather_state(reading(rain=0.2, snowfall=0.1)) == "snow"

    def test_fog_from_weather_code_even_when_visibility_is_high(self):
        code = next(iter(FOG_WEATHER_CODES))
        assert classify_weather_state(reading(weather_code=code, visibility=10000.0)) == "fog"

    def test_fog_from_low_visibility_just_below_the_threshold(self):
        assert classify_weather_state(reading(visibility=LOW_VISIBILITY_THRESHOLD_M - 1)) == "fog"

    def test_rain_takes_priority_over_fog_when_both_present(self):
        assert (
            classify_weather_state(reading(rain=0.2, visibility=LOW_VISIBILITY_THRESHOLD_M - 1))
            == "rain"
        )

    def test_high_wind_at_the_gust_threshold(self):
        assert (
            classify_weather_state(reading(wind_gusts_10m=HIGH_WIND_GUST_THRESHOLD_MPS))
            == "high_wind"
        )

    def test_missing_value_is_treated_as_absent_not_an_error(self):
        assert classify_weather_state({}) == "dry"


class FakeS3Client:
    """put_object/get_object만 갖춘 최소 in-memory S3 — zone_weather_compaction 테스트와 같은 패턴."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def put_object(self, **kwargs: object) -> None:
        self.objects[(str(kwargs["Bucket"]), str(kwargs["Key"]))] = kwargs["Body"]  # type: ignore[assignment]

    def get_object(self, **kwargs: object) -> dict[str, object]:
        key = (str(kwargs["Bucket"]), str(kwargs["Key"]))
        if key not in self.objects:
            raise ClientError(
                {
                    "Error": {"Code": "NoSuchKey", "Message": "not found"},
                    "ResponseMetadata": {"HTTPStatusCode": 404},
                },
                "GetObject",
            )
        return {"Body": io.BytesIO(self.objects[key])}


class TestLoadWeatherRegions:
    def test_reads_the_region_query_coordinates(self, tmp_path):
        master_path, _ = write_weather_region_files(
            tmp_path, regions={1: (40.7, -73.9), 2: (40.8, -74.0)}, mapping={181: 1, 182: 2}
        )

        regions = load_weather_regions(master_path)

        assert regions == [
            WeatherRegionCoordinate(1, 40.7, -73.9),
            WeatherRegionCoordinate(2, 40.8, -74.0),
        ]

    def test_does_not_read_the_geometry_column(self, tmp_path):
        # geometry는 시각화/검증용이고 파일 용량의 대부분이다. 런타임은 컬럼 프로젝션으로
        # 아예 읽지 않으므로, 폴리곤으로 해석할 수 없는 값이 들어 있어도 로딩은 성공한다.
        table = pa.table(
            {
                "weather_region_id": [1],
                "geometry": [b"not-a-polygon"],
                "representative_latitude": [40.7],
                "representative_longitude": [-73.9],
            }
        )
        path = tmp_path / "weather_region_master.parquet"
        pq.write_table(table, path)

        assert load_weather_regions(path) == [WeatherRegionCoordinate(1, 40.7, -73.9)]

    def test_drops_a_region_with_missing_coordinates(self, tmp_path):
        table = pa.table(
            {
                "weather_region_id": [1, 2],
                "representative_latitude": [40.7, None],
                "representative_longitude": [-73.9, None],
            }
        )
        path = tmp_path / "weather_region_master.parquet"
        pq.write_table(table, path)

        assert load_weather_regions(path) == [WeatherRegionCoordinate(1, 40.7, -73.9)]

    def test_reads_from_an_s3_uri(self):
        table = pa.table(
            {
                "weather_region_id": [1],
                "representative_latitude": [40.7],
                "representative_longitude": [-73.9],
            }
        )
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        client = FakeS3Client()
        client.put_object(
            Bucket="de4-reference",
            Key="normalized/weather_region/weather_region_master.parquet",
            Body=buffer.getvalue(),
        )
        store = ObjectStore(client)  # type: ignore[arg-type]

        regions = load_weather_regions(
            "s3://de4-reference/normalized/weather_region/weather_region_master.parquet",
            store=store,
        )

        assert regions == [WeatherRegionCoordinate(1, 40.7, -73.9)]

    def test_raises_when_the_s3_object_is_missing(self):
        store = ObjectStore(FakeS3Client())  # type: ignore[arg-type]

        with pytest.raises(ClientError, match="NoSuchKey"):
            load_weather_regions(
                "s3://de4-reference/normalized/weather_region/weather_region_master.parquet",
                store=store,
            )


class TestLoadZoneWeatherRegionMap:
    def test_maps_every_zone_to_its_region(self, tmp_path):
        _, map_path = write_weather_region_files(
            tmp_path, regions={1: (40.7, -73.9), 2: (40.8, -74.0)},
            mapping={181: 1, 182: 1, 183: 2},
        )

        assert load_zone_weather_region_map(map_path) == {181: 1, 182: 1, 183: 2}

    def test_keeps_the_file_order_because_it_decides_snapshot_row_order(self, tmp_path):
        _, map_path = write_weather_region_files(
            tmp_path, regions={1: (40.7, -73.9)}, mapping={183: 1, 181: 1, 182: 1}
        )

        assert list(load_zone_weather_region_map(map_path)) == [183, 181, 182]

    def test_reads_from_an_s3_uri(self):
        buffer = io.BytesIO()
        pq.write_table(pa.table({"location_id": [181], "weather_region_id": [1]}), buffer)
        client = FakeS3Client()
        client.put_object(
            Bucket="de4-reference",
            Key="normalized/weather_region/zone_weather_region_map.parquet",
            Body=buffer.getvalue(),
        )
        store = ObjectStore(client)  # type: ignore[arg-type]

        mapping = load_zone_weather_region_map(
            "s3://de4-reference/normalized/weather_region/zone_weather_region_map.parquet",
            store=store,
        )

        assert mapping == {181: 1}


class TestLatestZoneWeatherJobConfigFromEnv:
    BASE_ENV: ClassVar[dict[str, str]] = {
        "WEATHER_REGION_MASTER_URI": "data/reference/weather_region/weather_region_master.parquet",
        "ZONE_WEATHER_REGION_MAP_URI": (
            "data/reference/weather_region/zone_weather_region_map.parquet"
        ),
        "ZONE_WEATHER_SNAPSHOT_DATA_LAKE_URI": "data/local-lake/bronze/zone_weather_snapshot",
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "de4",
        "POSTGRES_USER": "de4",
        "POSTGRES_PASSWORD": "de4",
    }

    # 모듈 기본값을 두지 않으므로 URI는 전부 환경변수로 들어와야 한다 — 빠뜨리면
    # 엉뚱한 경로를 조용히 읽는 대신 설정 단계에서 바로 실패한다.
    @pytest.mark.parametrize(
        "missing",
        [
            "WEATHER_REGION_MASTER_URI",
            "ZONE_WEATHER_REGION_MAP_URI",
            "ZONE_WEATHER_SNAPSHOT_DATA_LAKE_URI",
        ],
    )
    def test_raises_when_a_uri_is_missing(self, missing):
        env = {key: value for key, value in self.BASE_ENV.items() if key != missing}

        with pytest.raises(ValueError, match=f"{missing} must be set"):
            LatestZoneWeatherJobConfig.from_env(env)

    def test_reads_s3_uris_from_the_environment(self):
        root = "s3://de4-reference/normalized/weather_region"
        config = LatestZoneWeatherJobConfig.from_env(
            {
                **self.BASE_ENV,
                "WEATHER_REGION_MASTER_URI": f"{root}/weather_region_master.parquet",
                "ZONE_WEATHER_REGION_MAP_URI": f"{root}/zone_weather_region_map.parquet",
            }
        )

        assert config.weather_region_master_uri == f"{root}/weather_region_master.parquet"
        assert config.zone_weather_region_map_uri == f"{root}/zone_weather_region_map.parquet"


class TestFetchOpenMeteo:
    REGIONS = (
        WeatherRegionCoordinate(1, 40.7, -73.9),
        WeatherRegionCoordinate(2, 40.8, -74.0),
        WeatherRegionCoordinate(3, 40.9, -74.1),
    )

    def test_sends_only_the_region_coordinates_in_one_request(self):
        # 호출 좌표가 zone 263개가 아니라 권역 수만큼만 나가는지 — 이 변경의 핵심이다.
        regions = self.REGIONS[:2]
        session = FakeSession([[location_payload([TARGET_KEY]), location_payload([TARGET_KEY])]])

        fetch_open_meteo(regions, TARGET_TIME, session=session)

        assert len(session.calls) == 1
        params = session.calls[0]["params"]
        assert params["latitude"] == "40.7,40.8"
        assert params["longitude"] == "-73.9,-74.0"

    def test_maps_readings_back_to_the_requesting_region(self):
        regions = self.REGIONS[:2]
        session = FakeSession(
            [
                [
                    location_payload([TARGET_KEY], rain=[0.5]),
                    location_payload([TARGET_KEY], rain=[0.0]),
                ]
            ]
        )

        readings, failures = fetch_open_meteo(regions, TARGET_TIME, session=session)

        assert readings[1]["rain"] == 0.5
        assert readings[2]["rain"] == 0.0
        assert failures == {}

    def test_skips_a_region_whose_response_has_no_matching_target_time(self):
        regions = self.REGIONS[:2]
        session = FakeSession(
            [
                [
                    location_payload([TARGET_KEY]),
                    location_payload(["2026-08-19T09:00"]),  # target_time 없음
                ]
            ]
        )

        readings, failures = fetch_open_meteo(regions, TARGET_TIME, session=session)

        assert set(readings) == {1}
        assert failures == {2: "missing target_time in Open-Meteo response"}

    def test_a_failed_batch_does_not_stop_other_batches(self):
        session = SequencedSession(
            [
                [location_payload([TARGET_KEY], rain=[1.0])],
                requests.ConnectionError("boom"),
                [location_payload([TARGET_KEY], rain=[2.0])],
            ]
        )

        readings, failures = fetch_open_meteo(
            self.REGIONS, TARGET_TIME, session=session, batch_size=1
        )

        assert set(readings) == {1, 3}
        assert "boom" in failures[2]

    def test_a_batch_with_a_mismatched_location_count_fails_only_that_batch(self):
        session = SequencedSession(
            [
                [location_payload([TARGET_KEY])],  # 권역 2개 요청인데 응답은 1개뿐
                [location_payload([TARGET_KEY], rain=[3.0])],
            ]
        )

        readings, failures = fetch_open_meteo(
            self.REGIONS, TARGET_TIME, session=session, batch_size=2
        )

        assert set(readings) == {3}
        assert 1 in failures
        assert 2 in failures

    def test_a_429_on_the_first_batch_stops_after_a_single_call(self):
        # HTTP 내부 retry가 아니라 fetch_open_meteo가 직접 429를 감지해서, 이후
        # batch 요청 자체를 만들지 않아야 한다(#444).
        session = SequencedSession(
            [FakeResponse(None, status_code=429, headers={"Retry-After": "30"})]
        )

        readings, failures = fetch_open_meteo(
            self.REGIONS, TARGET_TIME, session=session, batch_size=1
        )

        assert session.calls == 1
        assert readings == {}
        assert set(failures) == {1, 2, 3}
        assert all("429" in reason for reason in failures.values())

    def test_a_429_after_a_successful_batch_keeps_the_earlier_batch_results(self):
        session = SequencedSession(
            [
                [location_payload([TARGET_KEY], rain=[1.0])],
                FakeResponse(None, status_code=429),
            ]
        )

        readings, failures = fetch_open_meteo(
            self.REGIONS, TARGET_TIME, session=session, batch_size=1
        )

        # 세 번째 batch(권역 3)는 요청조차 만들어지지 않는다.
        assert session.calls == 2
        assert set(readings) == {1}
        assert readings[1]["rain"] == 1.0
        assert set(failures) == {2, 3}


def snapshot_row(**overrides) -> dict:
    values = {
        "location_id": 181,
        "weather_time": TARGET_TIME,
        "latitude": 40.7,
        "longitude": -73.9,
        "temperature_2m_c": 20.0,
        "precipitation_mm": 0.0,
        "rain_mm": 0.0,
        "snowfall_cm": 0.0,
        "visibility_m": 10000.0,
        "wind_speed_10m_mps": 3.0,
        "wind_gusts_10m_mps": 5.0,
        "weather_code": 0,
        "weather_state": "dry",
        "impact_signature": "1.0.0|clear",
        "fetched_at": TARGET_TIME,
        "fetch_status": "success",
        "error_reason": None,
    }
    values.update(overrides)
    return values


def _read_snapshot_table(store: ObjectStore, uri: str) -> pa.Table:
    return pq.read_table(io.BytesIO(store.read_bytes(uri)))


class TestWriteZoneWeatherSnapshot:
    def test_writes_one_parquet_file_keyed_by_weather_date_and_time(self, tmp_path):
        root = tmp_path / "zone_weather_snapshot"
        store = ObjectStore()

        uri = write_zone_weather_snapshot(str(root), TARGET_TIME, [snapshot_row()], store=store)

        written = list(root.rglob("*.parquet"))
        assert len(written) == 1
        # weather_date=.../weather_time=... 파티션 이름을 그대로 유지한다(file:// URI라
        # '='이 %3D로 percent-encode되므로 unquote해서 비교한다).
        assert "weather_date=2026-08-19" in unquote(uri)
        assert "weather_time=2026-08-19T10-15-00Z.parquet" in unquote(uri)
        table = _read_snapshot_table(store, uri)
        assert table.schema == _SNAPSHOT_SCHEMA
        assert table.num_rows == 1
        assert table.column("location_id").to_pylist() == [181]
        assert table.column("rain_mm").to_pylist() == [0.0]
        assert table.column("fetch_status").to_pylist() == ["success"]

    def test_a_failed_zone_writes_null_measurements_with_fetch_status(self, tmp_path):
        root = tmp_path / "zone_weather_snapshot"
        store = ObjectStore()
        failed_row = snapshot_row(
            rain_mm=None,
            weather_state=None,
            impact_signature=None,
            fetch_status="failed",
            error_reason="missing target_time in Open-Meteo response",
        )

        uri = write_zone_weather_snapshot(str(root), TARGET_TIME, [failed_row], store=store)

        table = _read_snapshot_table(store, uri)
        assert table.column("rain_mm").to_pylist() == [None]
        assert table.column("fetch_status").to_pylist() == ["failed"]
        assert table.column("error_reason").to_pylist() == [
            "missing target_time in Open-Meteo response"
        ]

    def test_rerunning_the_same_weather_time_overwrites_instead_of_duplicating(self, tmp_path):
        root = tmp_path / "zone_weather_snapshot"
        store = ObjectStore()

        first_uri = write_zone_weather_snapshot(
            str(root), TARGET_TIME, [snapshot_row(rain_mm=0.0)], store=store
        )
        second_uri = write_zone_weather_snapshot(
            str(root), TARGET_TIME, [snapshot_row(rain_mm=5.0)], store=store
        )

        assert first_uri == second_uri
        written = list(root.rglob("*.parquet"))
        assert len(written) == 1
        table = _read_snapshot_table(store, second_uri)
        assert table.column("rain_mm").to_pylist() == [5.0]

    def test_a_different_weather_time_writes_a_separate_file(self, tmp_path):
        root = tmp_path / "zone_weather_snapshot"
        later_target_time = TARGET_TIME + timedelta(minutes=15)
        store = ObjectStore()

        write_zone_weather_snapshot(str(root), TARGET_TIME, [snapshot_row()], store=store)
        write_zone_weather_snapshot(str(root), later_target_time, [snapshot_row()], store=store)

        assert len(list(root.rglob("*.parquet"))) == 2

    def test_writes_to_an_s3_uri(self):
        store = ObjectStore(FakeS3Client())  # type: ignore[arg-type]
        root = "s3://de4-data-lake/bronze/weather-snapshots"

        uri = write_zone_weather_snapshot(root, TARGET_TIME, [snapshot_row()], store=store)

        assert uri == (
            "s3://de4-data-lake/bronze/weather-snapshots/"
            "weather_date=2026-08-19/weather_time=2026-08-19T10-15-00Z.parquet"
        )
        table = _read_snapshot_table(store, uri)
        assert table.schema == _SNAPSHOT_SCHEMA
        assert table.column("location_id").to_pylist() == [181]

    def test_rerunning_the_same_weather_time_overwrites_the_same_s3_key(self):
        store = ObjectStore(FakeS3Client())  # type: ignore[arg-type]
        root = "s3://de4-data-lake/bronze/weather-snapshots"

        first_uri = write_zone_weather_snapshot(
            root, TARGET_TIME, [snapshot_row(rain_mm=0.0)], store=store
        )
        second_uri = write_zone_weather_snapshot(
            root, TARGET_TIME, [snapshot_row(rain_mm=5.0)], store=store
        )

        assert first_uri == second_uri
        table = _read_snapshot_table(store, second_uri)
        assert table.column("rain_mm").to_pylist() == [5.0]

    def test_propagates_the_error_when_the_object_store_write_fails(self, tmp_path):
        class FailingObjectStore(ObjectStore):
            def write_bytes(self, uri: str, value: bytes) -> None:
                raise RuntimeError("write failed")

        with pytest.raises(RuntimeError, match="write failed"):
            write_zone_weather_snapshot(
                str(tmp_path / "zone_weather_snapshot"),
                TARGET_TIME,
                [snapshot_row()],
                store=FailingObjectStore(),
            )

    def test_zone_weather_compaction_finds_the_snapshot_under_the_same_root(self, tmp_path):
        # weather.py가 쓰는 root와 zone_weather_compaction이 읽는 root가 같은 값을 가리키면
        # 그대로 발견돼야 한다 — 두 job이 같은 S3 root를 바라보는 계약을 지킨다(#400).
        from jobs.zone_weather_compaction import compact_bronze_prefix

        root = str(tmp_path / "zone_weather_snapshot")
        store = ObjectStore()
        write_zone_weather_snapshot(root, TARGET_TIME, [snapshot_row()], store=store)

        objects = store.list_objects(root)

        assert len(objects) == 1
        assert objects[0].uri.endswith(".parquet")
        # compact_bronze_prefix 자체는 이번 이슈에서 수정하지 않았다 — 같은 root를
        # 그대로 넘겨도 대상 없이(그룹당 1개뿐이라 skip) 정상 동작하는지만 확인한다.
        summary = compact_bronze_prefix(store, root, now=datetime(2099, 1, 1, tzinfo=UTC))
        assert summary.skipped_group_count == 1
        assert summary.compacted_groups == ()


# Postgres 없이 "권역 관측 1건 -> 소속 zone 여러 행" 펼치기만 검증한다. snapshot은
# UPSERT보다 먼저 쓰이므로 upsert를 스텁으로 바꿔도 행 단위 결과를 그대로 볼 수 있다.
class TestRegionFanOut:
    @staticmethod
    @pytest.fixture(autouse=True)
    def stub_upsert(monkeypatch):
        monkeypatch.setattr(
            weather_module, "upsert_latest_zone_weather", lambda connection, rows: len(rows)
        )

    @staticmethod
    def make_config(tmp_path, *, regions=None, mapping=None) -> LatestZoneWeatherJobConfig:
        master_path, map_path = write_weather_region_files(
            tmp_path, regions=regions, mapping=mapping
        )
        return LatestZoneWeatherJobConfig(
            weather_region_master_uri=str(master_path),
            zone_weather_region_map_uri=str(map_path),
            zone_weather_snapshot_uri=str(tmp_path / "zone_weather_snapshot"),
            postgres_host="localhost",
            postgres_port=5432,
            postgres_db="de4",
            postgres_user="de4",
            postgres_password="de4",
        )

    @staticmethod
    def read_snapshot(summary) -> dict[int, dict]:
        table = _read_snapshot_table(ObjectStore(), summary.snapshot_uri)
        return {row["location_id"]: row for row in table.to_pylist()}

    def test_one_region_reading_is_spread_over_every_zone_in_it(self, tmp_path):
        config = self.make_config(
            tmp_path,
            regions={1: (40.7, -73.9), 2: (40.8, -74.0)},
            mapping={181: 1, 182: 1, 183: 2},
        )
        session = FakeSession(
            [
                [
                    location_payload([TARGET_KEY], rain=[1.5]),
                    location_payload([TARGET_KEY], rain=[0.0]),
                ]
            ]
        )

        summary = run_latest_zone_weather_job(config, TARGET_TIME, None, session=session)
        rows = self.read_snapshot(summary)

        # 권역 1의 두 zone은 같은 관측값과 같은 조회 좌표를 갖는다.
        assert rows[181]["rain_mm"] == 1.5
        assert rows[182]["rain_mm"] == 1.5
        assert rows[183]["rain_mm"] == 0.0
        assert (rows[181]["latitude"], rows[181]["longitude"]) == (40.7, -73.9)
        assert (rows[182]["latitude"], rows[182]["longitude"]) == (40.7, -73.9)
        assert (rows[183]["latitude"], rows[183]["longitude"]) == (40.8, -74.0)

    def test_row_count_follows_the_zone_map_not_the_region_count(self, tmp_path):
        config = self.make_config(
            tmp_path, regions={1: (40.7, -73.9)}, mapping={181: 1, 182: 1, 183: 1}
        )
        session = FakeSession([[location_payload([TARGET_KEY])]])

        summary = run_latest_zone_weather_job(config, TARGET_TIME, None, session=session)

        assert summary.requested_zone_count == 3
        assert summary.requested_region_count == 1
        assert summary.collected_count == 3
        assert len(session.calls) == 1

    def test_a_failed_region_fails_exactly_its_own_zones(self, tmp_path):
        config = self.make_config(
            tmp_path,
            regions={1: (40.7, -73.9), 2: (40.8, -74.0)},
            mapping={181: 1, 182: 1, 183: 2},
        )
        session = FakeSession(
            [
                [
                    location_payload(["2026-08-19T09:00"]),  # 권역 1 실패
                    location_payload([TARGET_KEY], rain=[2.0]),
                ]
            ]
        )

        summary = run_latest_zone_weather_job(config, TARGET_TIME, None, session=session)
        rows = self.read_snapshot(summary)

        assert summary.failed_zone_count == 2
        assert rows[181]["fetch_status"] == "failed"
        assert rows[182]["fetch_status"] == "failed"
        assert rows[183]["fetch_status"] == "success"
        # 실패한 zone도 조회 좌표는 남아서 어느 권역에서 실패했는지 추적할 수 있다.
        assert (rows[181]["latitude"], rows[181]["longitude"]) == (40.7, -73.9)
        assert rows[181]["error_reason"] == "missing target_time in Open-Meteo response"

    def test_raises_when_the_map_references_a_region_the_master_does_not_define(self, tmp_path):
        # 두 reference 파일이 어긋난 채로 진행하면 좌표 없는 행이 조용히 생기므로
        # 요청을 보내기 전에 멈춰야 한다.
        config = self.make_config(
            tmp_path, regions={1: (40.7, -73.9)}, mapping={181: 1, 182: 7}
        )
        session = FakeSession([])

        with pytest.raises(ValueError, match=r"weather_region_id \[7\]"):
            run_latest_zone_weather_job(config, TARGET_TIME, None, session=session)

        assert session.calls == []


def _connect():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


@pytest.mark.skipif(
    not RUN_INTEGRATION, reason="set RUN_INTEGRATION=1 to run against a real Postgres"
)
class TestWeatherJobIntegration:
    # latest_zone_weather는 batch-jobs의 마이그레이션(0007)이 만든다 — orchestration은
    # 서빙 DB를 그대로 쓰는 쪽이라 여기서 마이그레이션을 실행하지 않는다. 대상 DB에
    # `make migrate`가 먼저 적용돼 있어야 한다.
    @staticmethod
    @pytest.fixture(autouse=True)
    def clean_table():
        connection = _connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute("TRUNCATE latest_zone_weather")
            connection.commit()
        finally:
            connection.close()
        yield

    @staticmethod
    def make_config(region_files, snapshot_uri) -> LatestZoneWeatherJobConfig:
        master_path, map_path = region_files
        env = os.environ
        return LatestZoneWeatherJobConfig(
            weather_region_master_uri=str(master_path),
            zone_weather_region_map_uri=str(map_path),
            zone_weather_snapshot_uri=snapshot_uri,
            postgres_host=env["POSTGRES_HOST"],
            postgres_port=int(env["POSTGRES_PORT"]),
            postgres_db=env["POSTGRES_DB"],
            postgres_user=env["POSTGRES_USER"],
            postgres_password=env["POSTGRES_PASSWORD"],
        )

    def test_a_later_target_time_updates_the_same_row_instead_of_inserting(self, tmp_path):
        region_files = write_weather_region_files(tmp_path)
        config = self.make_config(region_files, str(tmp_path / "zone_weather_snapshot"))
        connection = _connect()
        try:
            session = FakeSession([[location_payload([TARGET_KEY], rain=[0.0])]])
            run_latest_zone_weather_job(config, TARGET_TIME, connection, session=session)

            later_target_time = TARGET_TIME + timedelta(minutes=15)
            later_target_key = "2026-08-19T10:30"
            session = FakeSession([[location_payload([later_target_key], rain=[5.0])]])
            run_latest_zone_weather_job(config, later_target_time, connection, session=session)

            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT weather_time, rain_mm, weather_state FROM latest_zone_weather "
                    "WHERE location_id = 181"
                )
                rows = cursor.fetchall()
            assert rows == [(later_target_time, 5.0, "rain")]
            # 서로 다른 weather_time이니 이력 Parquet는 2개 남아야 한다.
            snapshot_files = list((tmp_path / "zone_weather_snapshot").rglob("*.parquet"))
            assert len(snapshot_files) == 2
        finally:
            connection.close()

    def test_an_older_target_time_does_not_overwrite_a_newer_row(self, tmp_path):
        # 10:30이 먼저 끝나고 재시도 등으로 늦게 끝난 10:15가 그 뒤에 와도 10:30 값을 지키는지 확인.
        region_files = write_weather_region_files(tmp_path)
        config = self.make_config(region_files, str(tmp_path / "zone_weather_snapshot"))
        connection = _connect()
        try:
            newer_target_time = TARGET_TIME + timedelta(minutes=15)
            newer_target_key = "2026-08-19T10:30"
            session = FakeSession([[location_payload([newer_target_key], rain=[5.0])]])
            run_latest_zone_weather_job(config, newer_target_time, connection, session=session)

            session = FakeSession([[location_payload([TARGET_KEY], rain=[0.0])]])
            run_latest_zone_weather_job(config, TARGET_TIME, connection, session=session)

            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT weather_time, rain_mm, weather_state FROM latest_zone_weather "
                    "WHERE location_id = 181"
                )
                rows = cursor.fetchall()
            assert rows == [(newer_target_time, 5.0, "rain")]
        finally:
            connection.close()

    # zone 하나가 권역 하나인 구성 — 기존 두-zone 시나리오(각 zone이 독립적으로
    # 성공/실패)를 권역 기반에서도 그대로 재현한다.
    @staticmethod
    def write_two_regions(directory) -> tuple[Path, Path]:
        return write_weather_region_files(
            directory,
            regions={1: (40.7, -73.9), 2: (40.8, -74.0)},
            mapping={181: 1, 182: 2},
        )

    def test_a_failed_zone_is_snapshotted_but_not_upserted(self, tmp_path):
        region_files = self.write_two_regions(tmp_path)
        snapshot_root = tmp_path / "zone_weather_snapshot"
        config = self.make_config(region_files, str(snapshot_root))
        connection = _connect()
        try:
            session = FakeSession(
                [
                    [
                        location_payload([TARGET_KEY], rain=[1.2]),
                        location_payload(["2026-08-19T09:00"]),  # 182는 target_time 없음
                    ]
                ]
            )

            summary = run_latest_zone_weather_job(config, TARGET_TIME, connection, session=session)

            assert summary.collected_count == 1
            assert summary.failed_zone_count == 1

            with connection.cursor() as cursor:
                cursor.execute("SELECT location_id, rain_mm FROM latest_zone_weather")
                rows = cursor.fetchall()
            assert rows == [(181, 1.2)]

            table = pq.read_table(next(iter(snapshot_root.rglob("*.parquet"))))
            by_zone = dict(zip(table.column("location_id").to_pylist(), range(2), strict=True))
            statuses = table.column("fetch_status").to_pylist()
            assert statuses[by_zone[181]] == "success"
            assert statuses[by_zone[182]] == "failed"
            assert table.column("rain_mm").to_pylist()[by_zone[182]] is None
        finally:
            connection.rollback()
            connection.close()

    def test_a_zone_that_fails_this_run_keeps_its_previous_latest_row(self, tmp_path):
        region_files = self.write_two_regions(tmp_path)
        config = self.make_config(region_files, str(tmp_path / "zone_weather_snapshot"))
        connection = _connect()
        try:
            session = FakeSession(
                [[location_payload([TARGET_KEY], rain=[0.0]), location_payload([TARGET_KEY])]]
            )
            run_latest_zone_weather_job(config, TARGET_TIME, connection, session=session)

            later_target_time = TARGET_TIME + timedelta(minutes=15)
            later_target_key = "2026-08-19T10:30"
            session = FakeSession(
                [
                    [
                        location_payload([later_target_key], rain=[5.0]),
                        location_payload(["2026-08-19T09:00"]),  # 182는 이번에도 실패
                    ]
                ]
            )
            run_latest_zone_weather_job(config, later_target_time, connection, session=session)

            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT location_id, weather_time FROM latest_zone_weather ORDER BY location_id"
                )
                rows = cursor.fetchall()
            # 182는 두 실행 모두 실패했으니 첫 실행에서 남은 TARGET_TIME 행 그대로다.
            assert rows == [(181, later_target_time), (182, TARGET_TIME)]
        finally:
            connection.close()

    def test_all_zones_failing_raises_but_still_writes_the_snapshot(self, tmp_path):
        region_files = self.write_two_regions(tmp_path)
        snapshot_root = tmp_path / "zone_weather_snapshot"
        config = self.make_config(region_files, str(snapshot_root))
        connection = _connect()
        try:
            session = FakeSession(
                [
                    [
                        location_payload(["2026-08-19T09:00"]),
                        location_payload(["2026-08-19T09:00"]),
                    ]
                ]
            )

            with pytest.raises(RuntimeError, match="all 2 zones failed"):
                run_latest_zone_weather_job(config, TARGET_TIME, connection, session=session)

            with connection.cursor() as cursor:
                cursor.execute("SELECT count(*) FROM latest_zone_weather")
                (count,) = cursor.fetchone()
            assert count == 0

            table = pq.read_table(next(iter(snapshot_root.rglob("*.parquet"))))
            assert table.column("fetch_status").to_pylist() == ["failed", "failed"]
        finally:
            connection.rollback()
            connection.close()
