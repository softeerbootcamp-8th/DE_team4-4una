# jobs/weather.py 테스트 (#207; batch-jobs의 weather_snapshot_job.py에서 이식, #209).

from __future__ import annotations

import os
import sys
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from jobs.weather import (
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
    def __init__(self, payload):
        self._payload = payload

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

        readings = fetch_open_meteo(zones, TARGET_TIME, session=session)

        assert readings[181]["rain"] == 0.5
        assert readings[182]["rain"] == 0.0

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

        readings = fetch_open_meteo(zones, TARGET_TIME, session=session)

        assert set(readings) == {181}


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
    def make_config(zone_master_path) -> LatestZoneWeatherJobConfig:
        env = os.environ
        return LatestZoneWeatherJobConfig(
            zone_master_path=zone_master_path,
            postgres_host=env["POSTGRES_HOST"],
            postgres_port=int(env["POSTGRES_PORT"]),
            postgres_db=env["POSTGRES_DB"],
            postgres_user=env["POSTGRES_USER"],
            postgres_password=env["POSTGRES_PASSWORD"],
        )

    def test_a_later_target_time_updates_the_same_row_instead_of_inserting(self, tmp_path):
        zone_master_path = write_zone_master(tmp_path / "zone_master.parquet")
        config = self.make_config(zone_master_path)
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
        finally:
            connection.close()

    def test_an_older_target_time_does_not_overwrite_a_newer_row(self, tmp_path):
        # 10:30이 먼저 끝나고 재시도 등으로 늦게 끝난 10:15가 그 뒤에 와도 10:30 값을 지키는지 확인.
        zone_master_path = write_zone_master(tmp_path / "zone_master.parquet")
        config = self.make_config(zone_master_path)
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

    def test_a_missing_zone_fails_the_whole_run_and_writes_nothing(self, tmp_path):
        zone_master_path = tmp_path / "zone_master.parquet"
        table = pa.table(
            {
                "location_id": [181, 182],
                "representative_latitude": [40.7, 40.8],
                "representative_longitude": [-73.9, -74.0],
            }
        )
        pq.write_table(table, zone_master_path)
        config = self.make_config(zone_master_path)
        connection = _connect()
        try:
            session = FakeSession(
                [
                    [
                        location_payload([TARGET_KEY]),
                        location_payload(["2026-08-19T09:00"]),  # 182는 target_time 없음
                    ]
                ]
            )

            with pytest.raises(RuntimeError, match="182"):
                run_latest_zone_weather_job(config, TARGET_TIME, connection, session=session)

            with connection.cursor() as cursor:
                cursor.execute("SELECT count(*) FROM latest_zone_weather")
                (count,) = cursor.fetchone()
            assert count == 0
        finally:
            connection.rollback()
            connection.close()
