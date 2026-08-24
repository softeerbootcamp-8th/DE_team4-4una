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

from jobs.weather import (
    _SNAPSHOT_SCHEMA,
    DEFAULT_ZONE_MASTER_URI,
    FOG_WEATHER_CODES,
    HIGH_WIND_GUST_THRESHOLD_MPS,
    HTTP_RETRY_STATUS_FORCELIST,
    HTTP_RETRY_TOTAL,
    LOW_VISIBILITY_THRESHOLD_M,
    RAIN_WEATHER_CODES,
    SNOW_WEATHER_CODES,
    LatestZoneWeatherJobConfig,
    ZoneCoordinate,
    _build_default_session,
    _validate_target_time,
    classify_weather_state,
    fetch_open_meteo,
    load_zone_coordinates,
    run_latest_zone_weather_job,
    write_zone_weather_snapshot,
)

RUN_INTEGRATION = os.environ.get("RUN_INTEGRATION") == "1"

TARGET_TIME = datetime(2026, 8, 19, 10, 15, tzinfo=UTC)
TARGET_KEY = "2026-08-19T10:15"


def write_zone_master(path: Path, *, location_id=181, latitude=40.7, longitude=-73.9) -> Path:
    table = pa.table(
        {
            "location_id": [location_id],
            "representative_latitude": [latitude],
            "representative_longitude": [longitude],
        }
    )
    pq.write_table(table, path)
    return path


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
    """put_object/get_object만 갖춘 최소 in-memory S3 — bronze_compaction 테스트와 같은 패턴."""

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


class TestLoadZoneCoordinates:
    def test_drops_zones_with_missing_coordinates(self, tmp_path):
        table = pa.table(
            {
                "location_id": [181, 264, 265],
                "representative_latitude": [40.7, None, None],
                "representative_longitude": [-73.9, None, None],
            }
        )
        path = tmp_path / "zone_master.parquet"
        pq.write_table(table, path)

        zones = load_zone_coordinates(path)

        assert [zone.location_id for zone in zones] == [181]
        assert zones[0] == ZoneCoordinate(181, 40.7, -73.9)

    def test_reads_from_an_s3_uri(self):
        table = pa.table(
            {
                "location_id": [181],
                "representative_latitude": [40.7],
                "representative_longitude": [-73.9],
            }
        )
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        client = FakeS3Client()
        client.put_object(Bucket="de4-reference", Key="normalized/zone_master/zone_master.parquet", Body=buffer.getvalue())
        store = ObjectStore(client)  # type: ignore[arg-type]

        zones = load_zone_coordinates(
            "s3://de4-reference/normalized/zone_master/zone_master.parquet", store=store
        )

        assert zones == [ZoneCoordinate(181, 40.7, -73.9)]

    def test_raises_when_the_s3_object_is_missing(self):
        store = ObjectStore(FakeS3Client())  # type: ignore[arg-type]

        with pytest.raises(ClientError, match="NoSuchKey"):
            load_zone_coordinates(
                "s3://de4-reference/normalized/zone_master/zone_master.parquet", store=store
            )


class TestLatestZoneWeatherJobConfigFromEnv:
    BASE_ENV: ClassVar[dict[str, str]] = {
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "de4",
        "POSTGRES_USER": "de4",
        "POSTGRES_PASSWORD": "de4",
    }

    def test_falls_back_to_the_local_default_uri(self):
        config = LatestZoneWeatherJobConfig.from_env(self.BASE_ENV)

        assert config.zone_master_uri == DEFAULT_ZONE_MASTER_URI

    def test_reads_an_s3_uri_from_the_environment(self):
        config = LatestZoneWeatherJobConfig.from_env(
            {**self.BASE_ENV, "ZONE_MASTER_URI": "s3://de4-reference/normalized/zone_master/zone_master.parquet"}
        )

        assert config.zone_master_uri == "s3://de4-reference/normalized/zone_master/zone_master.parquet"


class TestFetchOpenMeteo:
    def test_maps_readings_back_to_the_requesting_zone(self):
        zones = [ZoneCoordinate(181, 40.7, -73.9), ZoneCoordinate(182, 40.8, -74.0)]
        session = FakeSession(
            [
                [
                    location_payload([TARGET_KEY], rain=[0.5]),
                    location_payload([TARGET_KEY], rain=[0.0]),
                ]
            ]
        )

        readings, failures = fetch_open_meteo(zones, TARGET_TIME, session=session)

        assert readings[181]["rain"] == 0.5
        assert readings[182]["rain"] == 0.0
        assert failures == {}

    def test_skips_a_zone_whose_response_has_no_matching_target_time(self):
        zones = [ZoneCoordinate(181, 40.7, -73.9), ZoneCoordinate(182, 40.8, -74.0)]
        session = FakeSession(
            [
                [
                    location_payload([TARGET_KEY]),
                    location_payload(["2026-08-19T09:00"]),  # target_time 없음
                ]
            ]
        )

        readings, failures = fetch_open_meteo(zones, TARGET_TIME, session=session)

        assert set(readings) == {181}
        assert failures == {182: "missing target_time in Open-Meteo response"}

    def test_a_failed_batch_does_not_stop_other_batches(self):
        zones = [
            ZoneCoordinate(181, 40.7, -73.9),
            ZoneCoordinate(182, 40.8, -74.0),
            ZoneCoordinate(183, 40.9, -74.1),
        ]
        session = SequencedSession(
            [
                [location_payload([TARGET_KEY], rain=[1.0])],
                requests.ConnectionError("boom"),
                [location_payload([TARGET_KEY], rain=[2.0])],
            ]
        )

        readings, failures = fetch_open_meteo(zones, TARGET_TIME, session=session, batch_size=1)

        assert set(readings) == {181, 183}
        assert "boom" in failures[182]

    def test_a_batch_with_a_mismatched_location_count_fails_only_that_batch(self):
        zones = [
            ZoneCoordinate(181, 40.7, -73.9),
            ZoneCoordinate(182, 40.8, -74.0),
            ZoneCoordinate(183, 40.9, -74.1),
        ]
        session = SequencedSession(
            [
                [location_payload([TARGET_KEY])],  # 2개 zone 요청인데 응답은 1개뿐
                [location_payload([TARGET_KEY], rain=[3.0])],
            ]
        )

        readings, failures = fetch_open_meteo(zones, TARGET_TIME, session=session, batch_size=2)

        assert set(readings) == {183}
        assert 181 in failures
        assert 182 in failures

    def test_a_429_on_the_first_batch_stops_after_a_single_call(self):
        # HTTP 내부 retry가 아니라 fetch_open_meteo가 직접 429를 감지해서, 이후
        # batch 요청 자체를 만들지 않아야 한다(#444).
        zones = [
            ZoneCoordinate(181, 40.7, -73.9),
            ZoneCoordinate(182, 40.8, -74.0),
            ZoneCoordinate(183, 40.9, -74.1),
        ]
        session = SequencedSession(
            [FakeResponse(None, status_code=429, headers={"Retry-After": "30"})]
        )

        readings, failures = fetch_open_meteo(zones, TARGET_TIME, session=session, batch_size=1)

        assert session.calls == 1
        assert readings == {}
        assert set(failures) == {181, 182, 183}
        assert all("429" in reason for reason in failures.values())

    def test_a_429_after_a_successful_batch_keeps_the_earlier_batch_results(self):
        zones = [
            ZoneCoordinate(181, 40.7, -73.9),
            ZoneCoordinate(182, 40.8, -74.0),
            ZoneCoordinate(183, 40.9, -74.1),
        ]
        session = SequencedSession(
            [
                [location_payload([TARGET_KEY], rain=[1.0])],
                FakeResponse(None, status_code=429),
            ]
        )

        readings, failures = fetch_open_meteo(zones, TARGET_TIME, session=session, batch_size=1)

        # 세 번째 batch(183)는 요청조차 만들어지지 않는다.
        assert session.calls == 2
        assert set(readings) == {181}
        assert readings[181]["rain"] == 1.0
        assert set(failures) == {182, 183}


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

    def test_bronze_compaction_finds_the_snapshot_under_the_same_root(self, tmp_path):
        # weather.py가 쓰는 root와 bronze_compaction이 읽는 root가 같은 값을 가리키면
        # 그대로 발견돼야 한다 — 두 job이 같은 S3 root를 바라보는 계약을 지킨다(#400).
        from jobs.bronze_compaction import compact_bronze_prefix

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
    def make_config(zone_master_uri, snapshot_uri) -> LatestZoneWeatherJobConfig:
        env = os.environ
        return LatestZoneWeatherJobConfig(
            zone_master_uri=str(zone_master_uri),
            zone_weather_snapshot_uri=snapshot_uri,
            postgres_host=env["POSTGRES_HOST"],
            postgres_port=int(env["POSTGRES_PORT"]),
            postgres_db=env["POSTGRES_DB"],
            postgres_user=env["POSTGRES_USER"],
            postgres_password=env["POSTGRES_PASSWORD"],
        )

    def test_a_later_target_time_updates_the_same_row_instead_of_inserting(self, tmp_path):
        zone_master_path = write_zone_master(tmp_path / "zone_master.parquet")
        config = self.make_config(zone_master_path, str(tmp_path / "zone_weather_snapshot"))
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
        zone_master_path = write_zone_master(tmp_path / "zone_master.parquet")
        config = self.make_config(zone_master_path, str(tmp_path / "zone_weather_snapshot"))
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

    @staticmethod
    def write_two_zone_master(path) -> Path:
        table = pa.table(
            {
                "location_id": [181, 182],
                "representative_latitude": [40.7, 40.8],
                "representative_longitude": [-73.9, -74.0],
            }
        )
        pq.write_table(table, path)
        return path

    def test_a_failed_zone_is_snapshotted_but_not_upserted(self, tmp_path):
        zone_master_path = self.write_two_zone_master(tmp_path / "zone_master.parquet")
        snapshot_root = tmp_path / "zone_weather_snapshot"
        config = self.make_config(zone_master_path, str(snapshot_root))
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
        zone_master_path = self.write_two_zone_master(tmp_path / "zone_master.parquet")
        config = self.make_config(zone_master_path, str(tmp_path / "zone_weather_snapshot"))
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
        zone_master_path = self.write_two_zone_master(tmp_path / "zone_master.parquet")
        snapshot_root = tmp_path / "zone_weather_snapshot"
        config = self.make_config(zone_master_path, str(snapshot_root))
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
