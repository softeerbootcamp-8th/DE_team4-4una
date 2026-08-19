"""Tests for weather_snapshot_job.py (#199)."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta, timezone

import pandas as pd
import psycopg2
import pytest
from batch_jobs.migrate import MigrationConfig, run_migrations
from batch_jobs.weather_snapshot_job import (
    FOG_WEATHER_CODES,
    HIGH_WIND_GUST_THRESHOLD_MPS,
    HTTP_RETRY_STATUS_FORCELIST,
    HTTP_RETRY_TOTAL,
    LOW_VISIBILITY_THRESHOLD_M,
    RAIN_WEATHER_CODES,
    SNOW_WEATHER_CODES,
    WeatherSnapshotJobConfig,
    ZoneCoordinate,
    _build_default_session,
    _validate_target_time,
    build_impact_signature,
    classify_weather_state,
    fetch_open_meteo,
    load_zone_coordinates,
    run_weather_snapshot_job,
)

RUN_INTEGRATION = os.environ.get("RUN_INTEGRATION") == "1"

TARGET_TIME = datetime(2026, 8, 19, 10, 15, tzinfo=UTC)
TARGET_KEY = "2026-08-19T10:15"


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

    def test_rejects_a_non_zero_second(self):
        with pytest.raises(ValueError, match="15-minute boundary"):
            _validate_target_time(datetime(2026, 8, 19, 10, 15, 30, tzinfo=UTC))

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

    def test_not_fog_at_the_visibility_threshold(self):
        assert classify_weather_state(reading(visibility=LOW_VISIBILITY_THRESHOLD_M)) == "dry"

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

    def test_not_high_wind_just_below_the_gust_threshold(self):
        assert (
            classify_weather_state(reading(wind_gusts_10m=HIGH_WIND_GUST_THRESHOLD_MPS - 0.1))
            == "dry"
        )

    def test_fog_takes_priority_over_high_wind_when_both_present(self):
        assert (
            classify_weather_state(
                reading(visibility=LOW_VISIBILITY_THRESHOLD_M - 1, wind_gusts_10m=20.0)
            )
            == "fog"
        )

    def test_missing_value_is_treated_as_absent_not_an_error(self):
        assert classify_weather_state({}) == "dry"


class TestBuildImpactSignature:
    def test_same_reading_produces_the_same_signature(self):
        assert build_impact_signature(reading()) == build_impact_signature(reading())

    def test_a_changed_field_produces_a_different_signature(self):
        assert build_impact_signature(reading()) != build_impact_signature(reading(rain=0.2))

    def test_field_order_does_not_matter(self):
        a = {"temperature_2m": 1, "rain": 2}
        b = {"rain": 2, "temperature_2m": 1}
        assert build_impact_signature(a) == build_impact_signature(b)


class TestLoadZoneCoordinates:
    def test_drops_zones_with_missing_coordinates(self, tmp_path):
        path = tmp_path / "zone_master.parquet"
        pd.DataFrame(
            {
                "location_id": [181, 264, 265],
                "representative_latitude": [40.7, None, None],
                "representative_longitude": [-73.9, None, None],
            }
        ).to_parquet(path)

        zones = load_zone_coordinates(path)

        assert [zone.location_id for zone in zones] == [181]
        assert zones[0] == ZoneCoordinate(181, 40.7, -73.9)


class TestFetchOpenMeteo:
    def test_maps_readings_back_to_the_requesting_zone(self):
        zones = [ZoneCoordinate(181, 40.7, -73.9), ZoneCoordinate(182, 40.8, -74.0)]
        # Open-Meteo가 여러 좌표를 요청하면 위치별 구조체 리스트를 돌려준다.
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

    def test_sends_batch_size_requests_when_zone_count_exceeds_batch_size(self):
        zones = [ZoneCoordinate(i, 40.0 + i, -73.0 - i) for i in range(1, 4)]
        session = FakeSession(
            [
                [location_payload([TARGET_KEY]), location_payload([TARGET_KEY])],
                [location_payload([TARGET_KEY])],
            ]
        )

        readings = fetch_open_meteo(zones, TARGET_TIME, session=session, batch_size=2)

        assert len(session.calls) == 2
        assert set(readings) == {1, 2, 3}
        assert session.calls[0]["params"]["latitude"] == "41.0,42.0"
        assert session.calls[1]["params"]["latitude"] == "43.0"

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

    def test_raises_when_response_location_count_does_not_match_request(self):
        zones = [ZoneCoordinate(181, 40.7, -73.9), ZoneCoordinate(182, 40.8, -74.0)]
        session = FakeSession([[location_payload([TARGET_KEY])]])

        with pytest.raises(ValueError, match="cannot match by position"):
            fetch_open_meteo(zones, TARGET_TIME, session=session)


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
class TestWeatherSnapshotJobIntegration:
    @staticmethod
    @pytest.fixture(scope="class", autouse=True)
    def migrated():
        connection = _connect()
        try:
            run_migrations(MigrationConfig.from_env().migrations_dir, connection)
        finally:
            connection.close()

    @staticmethod
    def _truncate() -> None:
        # #209: current_segment_comfort_score의 weather FK가 없어져 latest_zone_weather만 비우면 된다.
        connection = _connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute("TRUNCATE latest_zone_weather")
            connection.commit()
        finally:
            connection.close()

    @staticmethod
    @pytest.fixture(autouse=True)
    def clean_tables():
        TestWeatherSnapshotJobIntegration._truncate()
        yield
        TestWeatherSnapshotJobIntegration._truncate()

    @staticmethod
    def make_config(zone_master_path) -> WeatherSnapshotJobConfig:
        env = os.environ
        return WeatherSnapshotJobConfig(
            zone_master_path=zone_master_path,
            postgres_host=env["POSTGRES_HOST"],
            postgres_port=int(env["POSTGRES_PORT"]),
            postgres_db=env["POSTGRES_DB"],
            postgres_user=env["POSTGRES_USER"],
            postgres_password=env["POSTGRES_PASSWORD"],
        )

    @staticmethod
    def write_zone_master(tmp_path, *, location_id=181, latitude=40.7, longitude=-73.9):
        path = tmp_path / "zone_master.parquet"
        pd.DataFrame(
            {
                "location_id": [location_id],
                "representative_latitude": [latitude],
                "representative_longitude": [longitude],
            }
        ).to_parquet(path)
        return path

    def test_a_rerun_at_the_same_target_time_updates_the_row(self, tmp_path):
        zone_master_path = self.write_zone_master(tmp_path)
        config = self.make_config(zone_master_path)
        connection = _connect()
        try:
            session = FakeSession([[location_payload([TARGET_KEY], rain=[0.0])]])
            first = run_weather_snapshot_job(config, TARGET_TIME, connection, session=session)
            assert first.collected_count == 1

            session = FakeSession([[location_payload([TARGET_KEY], rain=[5.0])]])
            second = run_weather_snapshot_job(config, TARGET_TIME, connection, session=session)
            assert second.collected_count == 1

            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT rain_mm, weather_state FROM latest_zone_weather "
                    "WHERE location_id = 181"
                )
                rows = cursor.fetchall()
            assert rows == [(5.0, "rain")]
        finally:
            connection.close()

    def test_a_later_target_time_updates_the_same_row_instead_of_inserting(self, tmp_path):
        # #209: PK가 location_id뿐이라, 새 weather_time이 와도 존당 행은 하나로 유지돼야 한다.
        zone_master_path = self.write_zone_master(tmp_path)
        config = self.make_config(zone_master_path)
        connection = _connect()
        try:
            session = FakeSession([[location_payload([TARGET_KEY], rain=[0.0])]])
            run_weather_snapshot_job(config, TARGET_TIME, connection, session=session)

            later_target_time = TARGET_TIME + timedelta(minutes=15)
            later_target_key = "2026-08-19T10:30"
            session = FakeSession([[location_payload([later_target_key], rain=[5.0])]])
            run_weather_snapshot_job(config, later_target_time, connection, session=session)

            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT weather_time, rain_mm, weather_state FROM latest_zone_weather "
                    "WHERE location_id = 181"
                )
                rows = cursor.fetchall()
            assert rows == [(later_target_time, 5.0, "rain")]
        finally:
            connection.close()

    def test_upsert_refreshes_coordinates_on_rerun(self, tmp_path):
        zone_master_path = self.write_zone_master(
            tmp_path, latitude=40.7, longitude=-73.9
        )
        config = self.make_config(zone_master_path)
        connection = _connect()
        try:
            session = FakeSession([[location_payload([TARGET_KEY])]])
            run_weather_snapshot_job(config, TARGET_TIME, connection, session=session)

            # zone_master의 대표좌표가 갱신됐다고 가정하고 새 zone_master로 재실행.
            moved_zone_master_path = self.write_zone_master(
                tmp_path, latitude=41.0, longitude=-74.5
            )
            moved_config = self.make_config(moved_zone_master_path)
            session = FakeSession([[location_payload([TARGET_KEY])]])
            run_weather_snapshot_job(moved_config, TARGET_TIME, connection, session=session)

            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT latitude, longitude FROM latest_zone_weather "
                    "WHERE location_id = 181"
                )
                rows = cursor.fetchall()
            assert rows == [(41.0, -74.5)]
        finally:
            connection.close()

    def test_a_missing_zone_fails_the_whole_run_and_writes_nothing(self, tmp_path):
        zone_master_path = tmp_path / "zone_master.parquet"
        pd.DataFrame(
            {
                "location_id": [181, 182],
                "representative_latitude": [40.7, 40.8],
                "representative_longitude": [-73.9, -74.0],
            }
        ).to_parquet(zone_master_path)
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
                run_weather_snapshot_job(config, TARGET_TIME, connection, session=session)

            with connection.cursor() as cursor:
                cursor.execute("SELECT count(*) FROM latest_zone_weather")
                (count,) = cursor.fetchone()
            assert count == 0
        finally:
            connection.rollback()
            connection.close()
