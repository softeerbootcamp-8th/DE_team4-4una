# Open-Meteo 15분 날씨를 latest_zone_weather(존당 최신 1행)에 수집하고, 같은 관측을
# zone_weather_snapshot Parquet에 이력으로도 남긴다 (#199, #209, #207, #222).
# Open-Meteo 호출은 zone 263개가 아니라 날씨 권역(weather region) 20개 좌표로만 나가고,
# 그 결과를 zone으로 펼쳐서 기존과 같은 263행을 만든다 — Open-Meteo가 요청 1건을 좌표
# 수로 가중해 API call을 세기 때문에, 263 좌표로는 일일 가중 call이 무료 한도(10,000)를
# 넘긴다. 263 -> 20 매핑은 15분마다 다시 계산하지 않고 reference 데이터로 고정해 둔다
# (zone_profile/build_weather_region.py가 오프라인 1회 생성).
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

# 한 번에 너무 많은 좌표를 보내지 않도록 나눠서 호출한다. 권역 20개는 이 크기 안에
# 들어가므로 실제로는 tick당 요청 1번이지만, 권역 수를 늘려도 동작하도록 batch 루프는
# 그대로 둔다(#444에서 50->100으로 키운 값을 유지).
REGION_BATCH_SIZE = 100

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

# 5xx/네트워크 오류에 대해 재시도한다. 429는 여기 넣지 않는다 — HTTP 내부 retry가
# backoff 동안 조용히 재시도하면, 그 사이 다음 batch까지 계속 요청이 나가 이미 rate
# limit에 걸린 상태에서 전체 zone이 연쇄로 실패할 수 있다. 429는 fetch_open_meteo()가
# 직접 감지해서 남은 batch 요청 자체를 중단한다(#444).
HTTP_RETRY_TOTAL = 3
HTTP_RETRY_BACKOFF_FACTOR = 1.0
HTTP_RETRY_STATUS_FORCELIST = (500, 502, 503, 504)

# reference/lake URI에는 모듈 기본값을 두지 않는다. 상대경로 기본값은 프로세스 CWD에
# 따라 다른 곳을 가리키는데 Airflow task의 CWD는 보장되지 않고, 운영에서 설정을
# 빼먹었을 때 "설정 없음"으로 실패하는 대신 엉뚱한 로컬 경로를 조용히 시도하게 된다.
# 로컬 개발용 기본값은 infra/compose/airflow.yaml이 ${VAR:-${AIRFLOW_HOME}/...} 형태로
# 제공한다 — 기본값은 배포 설정의 몫이고 이 모듈의 몫이 아니다.

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
class WeatherRegionCoordinate:
    weather_region_id: int
    latitude: float
    longitude: float


@dataclass(frozen=True, slots=True)
class LatestZoneWeatherJobConfig:
    weather_region_master_uri: str
    zone_weather_region_map_uri: str
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
            weather_region_master_uri=_require(source, "WEATHER_REGION_MASTER_URI"),
            zone_weather_region_map_uri=_require(source, "ZONE_WEATHER_REGION_MAP_URI"),
            zone_weather_snapshot_uri=_require(source, "ZONE_WEATHER_SNAPSHOT_DATA_LAKE_URI"),
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
    requested_region_count: int
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


# 5xx/네트워크 오류에 재시도하는 기본 세션을 만든다(429는 별도 처리, 위 설명 참고).
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


# weather_region_master.parquet에서 Open-Meteo에 보낼 권역 대표좌표를 읽는다. geometry
# 컬럼은 columns=로 제외해 아예 읽지 않는다 — 시각화용으로 같은 파일에 들어 있지만
# 런타임에는 필요 없고 폴리곤이 파일 용량의 대부분이다.
# URI는 local path/file:// URI/s3:// URI를 모두 받는다(#400) — 실제 접근은
# de4_core.ObjectStore에 위임해 S3 접근 로직을 중복 구현하지 않는다.
def load_weather_regions(
    weather_region_master_uri: str | Path, *, store: ObjectStore | None = None
) -> list[WeatherRegionCoordinate]:
    active_store = store if store is not None else ObjectStore()
    table = pq.read_table(
        io.BytesIO(active_store.read_bytes(str(weather_region_master_uri))),
        columns=["weather_region_id", "representative_latitude", "representative_longitude"],
    )
    region_ids = table.column("weather_region_id").to_pylist()
    latitudes = table.column("representative_latitude").to_pylist()
    longitudes = table.column("representative_longitude").to_pylist()
    return [
        WeatherRegionCoordinate(
            weather_region_id=int(region_id),
            latitude=float(latitude),
            longitude=float(longitude),
        )
        for region_id, latitude, longitude in zip(region_ids, latitudes, longitudes, strict=True)
        if latitude is not None and longitude is not None
    ]


# zone_weather_region_map.parquet에서 location_id -> weather_region_id 매핑을 읽는다.
# 반환 순서가 곧 snapshot 행 순서이며, 생성 스크립트가 location_id 오름차순으로 쓴다.
def load_zone_weather_region_map(
    zone_weather_region_map_uri: str | Path, *, store: ObjectStore | None = None
) -> dict[int, int]:
    active_store = store if store is not None else ObjectStore()
    table = pq.read_table(
        io.BytesIO(active_store.read_bytes(str(zone_weather_region_map_uri))),
        columns=["location_id", "weather_region_id"],
    )
    location_ids = table.column("location_id").to_pylist()
    region_ids = table.column("weather_region_id").to_pylist()
    return {
        int(location_id): int(region_id)
        for location_id, region_id in zip(location_ids, region_ids, strict=True)
    }


# Open-Meteo batch 요청이 실패해도 잡아서 그 권역들만 실패 처리하고 다른 batch는 계속 조회한다.
_BATCH_REQUEST_ERRORS = (requests.RequestException, ValueError, KeyError)


def fetch_open_meteo(
    regions: Sequence[WeatherRegionCoordinate],
    target_time: datetime,
    *,
    session: requests.Session | None = None,
    batch_size: int = REGION_BATCH_SIZE,
) -> tuple[dict[int, dict[str, float | int | None]], dict[int, str]]:
    # target_time 관측값을 weather_region_id별로 반환. 실패한 권역은 readings에서 빠지고
    # failure_reasons에 이유가 남는다 — 그 권역에 속한 zone 전체가 실패로 기록된다.
    session = session or _build_default_session()
    target_key = target_time.astimezone(UTC).strftime("%Y-%m-%dT%H:%M")
    readings: dict[int, dict[str, float | int | None]] = {}
    failure_reasons: dict[int, str] = {}

    for start in range(0, len(regions), batch_size):
        batch = regions[start : start + batch_size]
        try:
            response = session.get(
                OPEN_METEO_URL,
                params={
                    "latitude": ",".join(str(region.latitude) for region in batch),
                    "longitude": ",".join(str(region.longitude) for region in batch),
                    "minutely_15": ",".join(MINUTELY_15_VARIABLES),
                    "wind_speed_unit": "ms",
                    "precipitation_unit": "mm",
                    "timezone": "UTC",
                    "past_minutely_15": PAST_MINUTELY_15_STEPS,
                    "forecast_minutely_15": FORECAST_MINUTELY_15_STEPS,
                },
                timeout=30,
            )
            # rate limit에 걸리면 이 batch뿐 아니라 아직 요청 안 한 나머지 batch도
            # 전부 실패로 남기고 멈춘다 — 계속 요청하면 이미 걸린 rate limit 때문에
            # 나머지 zone도 연쇄로 실패할 뿐이다(#444).
            if response.status_code == 429:
                remaining_regions = regions[start:]
                logger.warning(
                    "Open-Meteo rate limited (429) at weather regions %s (Retry-After=%s) — "
                    "stopping remaining Open-Meteo requests, %d regions left unrequested",
                    [region.weather_region_id for region in batch],
                    response.headers.get("Retry-After"),
                    len(remaining_regions),
                )
                for region in remaining_regions:
                    failure_reasons[region.weather_region_id] = "Open-Meteo rate limited (429)"
                break
            response.raise_for_status()
            payload = response.json()
            locations = payload if isinstance(payload, list) else [payload]
            if len(locations) != len(batch):
                raise ValueError(
                    f"Open-Meteo returned {len(locations)} locations for a "
                    f"{len(batch)}-region request — cannot match by position"
                )
        except _BATCH_REQUEST_ERRORS as error:
            # 이 batch가 통째로 실패해도 다른 batch는 계속 조회한다 — 이 권역들만 나중에 failed로 남는다.
            logger.warning(
                "Open-Meteo request failed for weather regions %s: %s",
                [region.weather_region_id for region in batch],
                error,
            )
            for region in batch:
                failure_reasons[region.weather_region_id] = f"Open-Meteo request failed: {error}"
            continue

        for region, location in zip(batch, locations, strict=True):
            series = location["minutely_15"]
            try:
                index = series["time"].index(target_key)
            except ValueError:
                logger.warning(
                    "target_time %s not found in Open-Meteo response for weather region %s",
                    target_key,
                    region.weather_region_id,
                )
                failure_reasons[region.weather_region_id] = (
                    "missing target_time in Open-Meteo response"
                )
                continue
            readings[region.weather_region_id] = {
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
# zone_weather_compaction(#271)이 이 root를 그대로 압축 대상으로 재사용한다. 실제 저장은
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
    regions = load_weather_regions(config.weather_region_master_uri)
    zone_regions = load_zone_weather_region_map(config.zone_weather_region_map_uri)
    coordinates = {region.weather_region_id: region for region in regions}

    # 두 reference 파일은 함께 생성되므로 어긋날 일이 없어야 한다. 어긋난 채로 진행하면
    # 해당 zone이 좌표 없는 행이 되거나 조용히 실패로 남으므로 먼저 멈춘다.
    unknown_region_ids = sorted(set(zone_regions.values()) - coordinates.keys())
    if unknown_region_ids:
        raise ValueError(
            f"zone_weather_region_map references weather_region_id {unknown_region_ids} "
            "that weather_region_master does not define"
        )

    readings, failure_reasons = fetch_open_meteo(regions, target_time, session=session)
    fetched_at = datetime.now(UTC)

    # 권역 관측 1건을 그 권역에 속한 zone 전체로 펼친다 — 출력은 예전처럼 zone당 1행이다.
    # latitude/longitude는 Open-Meteo에 실제로 보낸 좌표라는 정의를 유지하므로, zone
    # 대표좌표가 아니라 권역 대표좌표가 들어간다(zone 대표좌표는 zone_master에 그대로 있다).
    def _row_for_zone(location_id: int, region_id: int) -> dict[str, object]:
        region = coordinates[region_id]
        reading = readings.get(region_id)
        if reading is None:
            return {
                "location_id": location_id,
                "weather_time": target_time,
                "latitude": region.latitude,
                "longitude": region.longitude,
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
                "error_reason": failure_reasons.get(region_id, "unknown fetch failure"),
            }
        return {
            "location_id": location_id,
            "weather_time": target_time,
            "latitude": region.latitude,
            "longitude": region.longitude,
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

    snapshot_rows = [
        _row_for_zone(location_id, region_id) for location_id, region_id in zone_regions.items()
    ]
    success_rows = [row for row in snapshot_rows if row["fetch_status"] == "success"]
    failed_zone_count = len(snapshot_rows) - len(success_rows)

    # 이력(Parquet)을 먼저 남기고 latest를 갱신한다 — 실패한 zone은 UPSERT 대상에서 빠져 기존
    # latest_zone_weather 행이 그대로 남는다.
    snapshot_uri = write_zone_weather_snapshot(
        config.zone_weather_snapshot_uri, target_time, snapshot_rows
    )
    collected_count = upsert_latest_zone_weather(connection, success_rows)

    # 전 zone 실패는 latest_zone_weather를 손대지 않았어도 task를 실패시켜 Airflow가 재시도하게 한다.
    if zone_regions and not success_rows:
        raise RuntimeError(
            f"all {len(zone_regions)} zones failed for target_time={target_time.isoformat()}"
        )

    return LatestZoneWeatherJobSummary(
        requested_zone_count=len(zone_regions),
        requested_region_count=len(regions),
        collected_count=collected_count,
        failed_zone_count=failed_zone_count,
        snapshot_uri=snapshot_uri,
    )
