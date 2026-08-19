# Open-Meteo 15분 날씨를 zone_weather_snapshot에 수집한다 (#199).

from __future__ import annotations

import hashlib
import json
import logging
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import requests
from psycopg2.extras import execute_values
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# 한 번에 너무 많은 좌표를 보내지 않도록 zone을 나눠서 호출한다.
ZONE_BATCH_SIZE = 50

# minutely_15 요청 변수 — zone_weather_snapshot 컬럼과 1:1 대응, 단위는 ms/mm로 맞춤.
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

DEFAULT_ZONE_MASTER_PATH = Path("data/reference/tlc_zone/zone_master.parquet")

TABLE = "zone_weather_snapshot"

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
ON CONFLICT (location_id, weather_time) DO UPDATE SET
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
"""

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
class WeatherSnapshotJobConfig:
    zone_master_path: Path
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> WeatherSnapshotJobConfig:
        source = env if env is not None else os.environ
        return cls(
            zone_master_path=Path(
                source.get("ZONE_MASTER_PATH") or DEFAULT_ZONE_MASTER_PATH
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
class WeatherSnapshotJobSummary:
    requested_zone_count: int
    collected_count: int


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
def load_zone_coordinates(zone_master_path: Path) -> list[ZoneCoordinate]:
    frame = pd.read_parquet(
        zone_master_path,
        columns=["location_id", "representative_latitude", "representative_longitude"],
    )
    frame = frame.dropna(subset=["representative_latitude", "representative_longitude"])
    return [
        ZoneCoordinate(
            location_id=int(row.location_id),
            latitude=float(row.representative_latitude),
            longitude=float(row.representative_longitude),
        )
        for row in frame.itertuples()
    ]


def fetch_open_meteo(
    zones: Sequence[ZoneCoordinate],
    target_time: datetime,
    *,
    session: requests.Session | None = None,
    batch_size: int = ZONE_BATCH_SIZE,
) -> dict[int, dict[str, float | int | None]]:
    # target_time 관측값을 zone별로 반환 — HTTP 실패는 재시도 후 예외, target_time 없는 zone만 skip.
    session = session or _build_default_session()
    target_key = target_time.astimezone(UTC).strftime("%Y-%m-%dT%H:%M")
    readings: dict[int, dict[str, float | int | None]] = {}

    for start in range(0, len(zones), batch_size):
        batch = zones[start : start + batch_size]
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
                continue
            readings[zone.location_id] = {
                variable: series[variable][index] for variable in MINUTELY_15_VARIABLES
            }

    return readings


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


# 관측값을 정렬된 JSON으로 SHA-256 해시화 — 값이 바뀌면 해시도 바뀌어 변경 감지에 쓴다.
def build_impact_signature(reading: Mapping[str, float | int | None]) -> str:
    canonical = json.dumps(
        {variable: reading.get(variable) for variable in MINUTELY_15_VARIABLES},
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# zone_weather_snapshot에 UPSERT — 같은 (location_id, weather_time) 재실행해도 중복 안 생김.
def upsert_weather_snapshots(connection, rows: Sequence[Mapping[str, object]]) -> int:
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


def run_weather_snapshot_job(
    config: WeatherSnapshotJobConfig,
    target_time: datetime,
    connection,
    *,
    session: requests.Session | None = None,
) -> WeatherSnapshotJobSummary:
    target_time = _validate_target_time(target_time)
    zones = load_zone_coordinates(config.zone_master_path)
    zones_by_id = {zone.location_id: zone for zone in zones}
    readings = fetch_open_meteo(zones, target_time, session=session)

    # 일부 zone만 누락돼도 전체를 쓰지 않는다 — Airflow가 이 target_time을 통째로 재시도한다.
    skipped_zone_ids = tuple(sorted(set(zones_by_id) - set(readings)))
    if skipped_zone_ids:
        raise RuntimeError(
            f"missing target_time={target_time.isoformat()} for zones "
            f"{skipped_zone_ids} — refusing to write a partial snapshot"
        )

    fetched_at = datetime.now(UTC)

    rows = [
        {
            "location_id": location_id,
            "weather_time": target_time,
            "latitude": zones_by_id[location_id].latitude,
            "longitude": zones_by_id[location_id].longitude,
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
            "impact_signature": build_impact_signature(reading),
            "fetched_at": fetched_at,
        }
        for location_id, reading in readings.items()
    ]

    collected_count = upsert_weather_snapshots(connection, rows)
    return WeatherSnapshotJobSummary(
        requested_zone_count=len(zones),
        collected_count=collected_count,
    )
