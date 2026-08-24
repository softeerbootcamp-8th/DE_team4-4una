# Open-Meteo 15분 날씨를 latest_zone_weather(존당 최신 1행)에 수집하고, 같은 관측을
# zone_weather_snapshot Parquet에 이력으로도 남긴다 (#199, #209, #207, #222).
# batch-jobs(EMR/Spark 전용)가 아니라 orchestration의 lightweight Python job으로 둔다 — Spark가 필요 없다.

from __future__ import annotations

import io
import logging
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from de4_core import ObjectStore, join_uri
from psycopg2.extras import execute_values
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .weather_rules import (
    WeatherRuleConfig,
    build_impact_signature,
    load_weather_rule_config,
)

logger = logging.getLogger(__name__)

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# 한 번에 너무 많은 좌표를 보내지 않도록 zone을 나눠서 호출한다.
ZONE_BATCH_SIZE = 50

# minutely_15 요청 변수 — latest_zone_weather 컬럼과 1:1 대응, 단위는 ms/mm로 맞춤.
MINUTELY_15_VARIABLES = (
    "temperature_2m",
    "precipitation",
    "rain",
    "snowfall",
    "visibility",
    "wind_speed_10m",
    "wind_gusts_10m",
    "weather_code",
)

# target_time이 응답 범위에 들어오도록 앞뒤로 여유를 둔다(15분 스텝 수, 2시간).
PAST_MINUTELY_15_STEPS = 8
FORECAST_MINUTELY_15_STEPS = 8

# target_time은 15분 스냅샷 경계여야 한다 (내부에서는 항상 UTC로 통일한다).
VALID_TARGET_MINUTES = frozenset({0, 15, 30, 45})

# 429/5xx/네트워크 오류에 대해 재시도한다.
HTTP_RETRY_TOTAL = 3
HTTP_RETRY_BACKOFF_FACTOR = 1.0
HTTP_RETRY_STATUS_FORCELIST = (429, 500, 502, 503, 504)

DEFAULT_ZONE_MASTER_URI = "data/reference/tlc_zone/zone_master.parquet"
DEFAULT_ZONE_WEATHER_SNAPSHOT_URI = "data/local-lake/bronze/zone_weather_snapshot"

TABLE = "latest_zone_weather"

_ROW_COLUMNS = (
    "location_id",
    "weather_time",
    "latitude",
    "longitude",
    "temperature_2m_c",
    "precipitation_mm",
    "rain_mm",
    "snowfall_cm",
    "visibility_m",
    "wind_speed_10m_mps",
    "wind_gusts_10m_mps",
    "weather_code",
    "weather_state",
    "impact_signature",
    "fetched_at",
)

_UPSERT_SQL = f"""
INSERT INTO {TABLE} ({", ".join(_ROW_COLUMNS)})
VALUES %s
ON CONFLICT (location_id) DO UPDATE SET
    weather_time = EXCLUDED.weather_time,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    temperature_2m_c = EXCLUDED.temperature_2m_c,
    precipitation_mm = EXCLUDED.precipitation_mm,
    rain_mm = EXCLUDED.rain_mm,
    snowfall_cm = EXCLUDED.snowfall_cm,
    visibility_m = EXCLUDED.visibility_m,
    wind_speed_10m_mps = EXCLUDED.wind_speed_10m_mps,
    wind_gusts_10m_mps = EXCLUDED.wind_gusts_10m_mps,
    weather_code = EXCLUDED.weather_code,
    weather_state = EXCLUDED.weather_state,
    impact_signature = EXCLUDED.impact_signature,
    fetched_at = EXCLUDED.fetched_at
WHERE {TABLE}.weather_time <= EXCLUDED.weather_time
"""
# WHERE 없으면 늦게 끝난 옛 target_time 실행(재시도 등)이 더 최신 행을 덮어쓸 수 있다.

# zone_weather_snapshot은 latest_zone_weather와 달리 실패한 zone도 행으로 남긴다(fetch_status).
_SNAPSHOT_ROW_COLUMNS = (*_ROW_COLUMNS, "fetch_status", "error_reason")

# zone_weather_snapshot Parquet 컬럼 타입 — PK 개념은 (location_id, weather_time).
_SNAPSHOT_SCHEMA = pa.schema(
    [
        ("location_id", pa.int64()),
        ("weather_time", pa.timestamp("us", tz="UTC")),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("temperature_2m_c", pa.float64()),
        ("precipitation_mm", pa.float64()),
        ("rain_mm", pa.float64()),
        ("snowfall_cm", pa.float64()),
        ("visibility_m", pa.float64()),
        ("wind_speed_10m_mps", pa.float64()),
        ("wind_gusts_10m_mps", pa.float64()),
        ("weather_code", pa.int64()),
        ("weather_state", pa.string()),
        ("impact_signature", pa.string()),
        ("fetched_at", pa.timestamp("us", tz="UTC")),
        ("fetch_status", pa.string()),
        ("error_reason", pa.string()),
    ]
)

# 날씨 상태 분류 임계값/코드(우선값, 최종 확정은 후속 이슈) — WMO weather_code 우선, 실측값 보완.
HIGH_WIND_GUST_THRESHOLD_MPS = 15.0
LOW_VISIBILITY_THRESHOLD_M = 1000.0
SNOW_WEATHER_CODES = frozenset({71, 73, 75, 77, 85, 86})
RAIN_WEATHER_CODES = frozenset({51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82, 95, 96, 99})
FOG_WEATHER_CODES = frozenset({45, 48})


@dataclass(frozen=True, slots=True)
class ZoneCoordinate:
    location_id: int
    latitude: float
    longitude: float


@dataclass(frozen=True, slots=True)
class LatestZoneWeatherJobConfig:
    zone_master_uri: str
    zone_weather_snapshot_uri: str
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> LatestZoneWeatherJobConfig:
        source = env if env is not None else os.environ
        return cls(
            zone_master_uri=source.get("ZONE_MASTER_URI") or DEFAULT_ZONE_MASTER_URI,
            zone_weather_snapshot_uri=(
                source.get("ZONE_WEATHER_SNAPSHOT_DATA_LAKE_URI")
                or DEFAULT_ZONE_WEATHER_SNAPSHOT_URI
            ),
            postgres_host=_require(source, "POSTGRES_HOST"),
            postgres_port=int(_require(source, "POSTGRES_PORT")),
            postgres_db=_require(source, "POSTGRES_DB"),
            postgres_user=_require(source, "POSTGRES_USER"),
            postgres_password=_require(source, "POSTGRES_PASSWORD"),
        )


def _require(source: Mapping[str, str], key: str) -> str:
    value = source.get(key)
    if not value:
        raise ValueError(f"{key} must be set")
    return value


@dataclass(frozen=True, slots=True)
class LatestZoneWeatherJobSummary:
    requested_zone_count: int
    collected_count: int
    failed_zone_count: int
    snapshot_uri: str


# target_time을 UTC로 통일하고, tz 정보와 15분 경계(00/15/30/45)를 검증한다.
def _validate_target_time(target_time: datetime) -> datetime:
    if target_time.utcoffset() is None:
        raise ValueError("target_time must be timezone-aware")
    normalized = target_time.astimezone(UTC)
    if (
        normalized.minute not in VALID_TARGET_MINUTES
        or normalized.second != 0
        or normalized.microsecond != 0
    ):
        raise ValueError(
            "target_time must fall on a 15-minute boundary (:00/:15/:30/:45), "
            f"got {target_time.isoformat()}"
        )
    return normalized


# 429/5xx/네트워크 오류에 재시도하는 기본 세션을 만든다.
def _build_default_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=HTTP_RETRY_TOTAL,
        backoff_factor=HTTP_RETRY_BACKOFF_FACTOR,
        status_forcelist=HTTP_RETRY_STATUS_FORCELIST,
        allowed_methods=frozenset({"GET"}),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# zone_master.parquet에서 location_id/대표좌표를 읽는다 — 좌표 없는 zone(264, 265)은 제외.
# zone_master_uri는 local path/file:// URI/s3:// URI를 모두 받는다(#400) — 실제 접근은
# de4_core.ObjectStore에 위임해 S3 접근 로직을 중복 구현하지 않는다.
def load_zone_coordinates(
    zone_master_uri: str | Path, *, store: ObjectStore | None = None
) -> list[ZoneCoordinate]:
    active_store = store if store is not None else ObjectStore()
    table = pq.read_table(
        io.BytesIO(active_store.read_bytes(str(zone_master_uri))),
        columns=["location_id", "representative_latitude", "representative_longitude"],
    )
    location_ids = table.column("location_id").to_pylist()
    latitudes = table.column("representative_latitude").to_pylist()
    longitudes = table.column("representative_longitude").to_pylist()
    return [
        ZoneCoordinate(location_id=int(location_id), latitude=float(latitude), longitude=float(longitude))
        for location_id, latitude, longitude in zip(location_ids, latitudes, longitudes, strict=True)
        if latitude is not None and longitude is not None
    ]


# Open-Meteo batch 요청이 실패해도 잡아서 그 zone들만 실패 처리하고 다른 batch는 계속 조회한다.
_BATCH_REQUEST_ERRORS = (requests.RequestException, ValueError, KeyError)


def fetch_open_meteo(
    zones: Sequence[ZoneCoordinate],
    target_time: datetime,
    *,
    session: requests.Session | None = None,
    batch_size: int = ZONE_BATCH_SIZE,
) -> tuple[dict[int, dict[str, float | int | None]], dict[int, str]]:
    # target_time 관측값을 zone별로 반환. 실패한 zone은 readings에서 빠지고 failure_reasons에 이유가 남는다.
    session = session or _build_default_session()
    target_key = target_time.astimezone(UTC).strftime("%Y-%m-%dT%H:%M")
    readings: dict[int, dict[str, float | int | None]] = {}
    failure_reasons: dict[int, str] = {}

    for start in range(0, len(zones), batch_size):
        batch = zones[start : start + batch_size]
        try:
            response = session.get(
                OPEN_METEO_URL,
                params={
                    "latitude": ",".join(str(zone.latitude) for zone in batch),
                    "longitude": ",".join(str(zone.longitude) for zone in batch),
                    "minutely_15": ",".join(MINUTELY_15_VARIABLES),
                    "wind_speed_unit": "ms",
                    "precipitation_unit": "mm",
                    "timezone": "UTC",
                    "past_minutely_15": PAST_MINUTELY_15_STEPS,
                    "forecast_minutely_15": FORECAST_MINUTELY_15_STEPS,
                },
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
            locations = payload if isinstance(payload, list) else [payload]
            if len(locations) != len(batch):
                raise ValueError(
                    f"Open-Meteo returned {len(locations)} locations for a "
                    f"{len(batch)}-zone request — cannot match by position"
                )
        except _BATCH_REQUEST_ERRORS as error:
            # 이 batch가 통째로 실패해도 다른 batch는 계속 조회한다 — 이 zone들만 나중에 failed로 남는다.
            logger.warning(
                "Open-Meteo request failed for zones %s: %s",
                [zone.location_id for zone in batch],
                error,
            )
            for zone in batch:
                failure_reasons[zone.location_id] = f"Open-Meteo request failed: {error}"
            continue

        for zone, location in zip(batch, locations, strict=True):
            series = location["minutely_15"]
            try:
                index = series["time"].index(target_key)
            except ValueError:
                logger.warning(
                    "target_time %s not found in Open-Meteo response for zone %s",
                    target_key,
                    zone.location_id,
                )
                failure_reasons[zone.location_id] = "missing target_time in Open-Meteo response"
                continue
            readings[zone.location_id] = {
                variable: series[variable][index] for variable in MINUTELY_15_VARIABLES
            }

    return readings, failure_reasons


# snow -> rain -> fog -> high_wind -> dry 우선순위로 분류한다(실측값 0이어도 weather_code로 잡음).
def classify_weather_state(reading: Mapping[str, float | int | None]) -> str:
    weather_code = reading.get("weather_code")
    visibility = reading.get("visibility")

    if (reading.get("snowfall") or 0) > 0 or weather_code in SNOW_WEATHER_CODES:
        return "snow"
    if (
        (reading.get("rain") or 0) > 0
        or (reading.get("precipitation") or 0) > 0
        or weather_code in RAIN_WEATHER_CODES
    ):
        return "rain"
    if weather_code in FOG_WEATHER_CODES or (
        visibility is not None and visibility < LOW_VISIBILITY_THRESHOLD_M
    ):
        return "fog"
    if (reading.get("wind_gusts_10m") or 0) >= HIGH_WIND_GUST_THRESHOLD_MPS:
        return "high_wind"
    return "dry"


# weather_time을 키로 삼아 zone_weather_snapshot(Bronze)에 15분 관측 전체를 Parquet
# 1개로 남긴다. 같은 weather_time으로 재실행되면 같은 object URI를 덮어써서 중복
# snapshot이 생기지 않는다. snapshot_root는 local path/file:// URI/s3:// URI를 모두
# 받는다(#400) — 운영에서는 bronze/weather-snapshots를 가리키는 s3:// URI가 들어오고,
# bronze_compaction(#271)이 이 root를 그대로 압축 대상으로 재사용한다. 실제 저장은
# de4_core.ObjectStore에 위임해 S3 접근 로직을 중복 구현하지 않는다.
def write_zone_weather_snapshot(
    snapshot_root: str,
    target_time: datetime,
    rows: Sequence[Mapping[str, object]],
    *,
    store: ObjectStore | None = None,
) -> str:
    active_store = store if store is not None else ObjectStore()
    uri = join_uri(
        str(snapshot_root),
        f"weather_date={target_time.strftime('%Y-%m-%d')}",
        f"weather_time={target_time.strftime('%Y-%m-%dT%H-%M-%SZ')}.parquet",
    )
    table = pa.Table.from_pylist(
        [{column: row[column] for column in _SNAPSHOT_ROW_COLUMNS} for row in rows],
        schema=_SNAPSHOT_SCHEMA,
    )
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    active_store.write_bytes(uri, buffer.getvalue())
    return uri


# latest_zone_weather에 UPSERT — location_id당 최신 관측 1행만 유지된다.
def upsert_latest_zone_weather(connection, rows: Sequence[Mapping[str, object]]) -> int:
    if not rows:
        return 0
    with connection.cursor() as cursor:
        execute_values(
            cursor,
            _UPSERT_SQL,
            [tuple(row[column] for column in _ROW_COLUMNS) for row in rows],
        )
    connection.commit()
    return len(rows)


def run_latest_zone_weather_job(
    config: LatestZoneWeatherJobConfig,
    target_time: datetime,
    connection,
    *,
    session: requests.Session | None = None,
    rule_config: WeatherRuleConfig | None = None,
) -> LatestZoneWeatherJobSummary:
    target_time = _validate_target_time(target_time)
    # zone마다 YAML을 다시 읽지 않도록 한 번만 로드해 돌려 쓴다.
    rule_config = rule_config if rule_config is not None else load_weather_rule_config()
    zones = load_zone_coordinates(config.zone_master_uri)
    readings, failure_reasons = fetch_open_meteo(zones, target_time, session=session)
    fetched_at = datetime.now(UTC)

    def _row_for_zone(zone: ZoneCoordinate) -> dict[str, object]:
        reading = readings.get(zone.location_id)
        if reading is None:
            return {
                "location_id": zone.location_id,
                "weather_time": target_time,
                "latitude": zone.latitude,
                "longitude": zone.longitude,
                "temperature_2m_c": None,
                "precipitation_mm": None,
                "rain_mm": None,
                "snowfall_cm": None,
                "visibility_m": None,
                "wind_speed_10m_mps": None,
                "wind_gusts_10m_mps": None,
                "weather_code": None,
                "weather_state": None,
                "impact_signature": None,
                "fetched_at": fetched_at,
                "fetch_status": "failed",
                "error_reason": failure_reasons.get(zone.location_id, "unknown fetch failure"),
            }
        return {
            "location_id": zone.location_id,
            "weather_time": target_time,
            "latitude": zone.latitude,
            "longitude": zone.longitude,
            "temperature_2m_c": reading["temperature_2m"],
            "precipitation_mm": reading["precipitation"],
            "rain_mm": reading["rain"],
            "snowfall_cm": reading["snowfall"],
            "visibility_m": reading["visibility"],
            "wind_speed_10m_mps": reading["wind_speed_10m"],
            "wind_gusts_10m_mps": reading["wind_gusts_10m"],
            "weather_code": (
                int(reading["weather_code"]) if reading["weather_code"] is not None else None
            ),
            "weather_state": classify_weather_state(reading),
            "impact_signature": build_impact_signature(reading, rule_config),
            "fetched_at": fetched_at,
            "fetch_status": "success",
            "error_reason": None,
        }

    snapshot_rows = [_row_for_zone(zone) for zone in zones]
    success_rows = [row for row in snapshot_rows if row["fetch_status"] == "success"]
    failed_zone_count = len(snapshot_rows) - len(success_rows)

    # 이력(Parquet)을 먼저 남기고 latest를 갱신한다 — 실패한 zone은 UPSERT 대상에서 빠져 기존
    # latest_zone_weather 행이 그대로 남는다.
    snapshot_uri = write_zone_weather_snapshot(
        config.zone_weather_snapshot_uri, target_time, snapshot_rows
    )
    collected_count = upsert_latest_zone_weather(connection, success_rows)

    # 전 zone 실패는 latest_zone_weather를 손대지 않았어도 task를 실패시켜 Airflow가 재시도하게 한다.
    if zones and not success_rows:
        raise RuntimeError(f"all {len(zones)} zones failed for target_time={target_time.isoformat()}")

    return LatestZoneWeatherJobSummary(
        requested_zone_count=len(zones),
        collected_count=collected_count,
        failed_zone_count=failed_zone_count,
        snapshot_uri=snapshot_uri,
    )
