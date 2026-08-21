# latest_zone_weather in-flight 검증 (#250, ADR-0004 예외).
#
# ADR-0004는 저장소가 Postgres인 규칙을 GX(SqlAlchemyExecutionEngine)로 검증하기로
# 했지만, 실측 결과 great-expectations는 엔진 선택과 무관하게 pandas/numpy/scipy/
# altair 등 무거운 스택을 통째로 끌고 온다(postgresql extra는 psycopg2/sqlalchemy만
# 추가). zone_weather_pipeline은 Spark/docker 없이 airflow-scheduler 컨테이너
# 안에서 직접 도는 경량 PythonOperator라는 설계를 유지하려고, 이 파이프라인만
# 예외적으로 GX 없이 인라인 Python/SQL로 검증한다. 규칙이 6개뿐이고
# latest_zone_weather는 이력을 남기지 않아(#209) 재사용할 at-rest 감사 대상도
# 없어, GX의 두 이점(규칙 증가 대비, in-flight/at-rest 공유)이 여기서는 약하다.

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime

TABLE = "latest_zone_weather"

# 물리적으로 음수일 수 없는 관측값(강수/적설/시야/풍속) — 결측(NULL)은 검사하지 않는다.
_NON_NEGATIVE_COLUMNS = (
    "precipitation_mm",
    "rain_mm",
    "snowfall_cm",
    "visibility_m",
    "wind_speed_10m_mps",
    "wind_gusts_10m_mps",
)

TEMPERATURE_MIN_C = -60.0
TEMPERATURE_MAX_C = 60.0

# WMO 코드는 0~99 범위로 정의된다(Open-Meteo weather_code).
WEATHER_CODE_MIN = 0
WEATHER_CODE_MAX = 99

# jobs.weather.classify_weather_state의 출력 집합.
VALID_WEATHER_STATES = frozenset({"snow", "rain", "fog", "high_wind", "dry"})

# jobs.weather_rules.format_impact_signature와 같은 형식: "{semver}|{정렬된 조건 목록 or clear}".
_IMPACT_SIGNATURE_HEADER_PATTERN = re.compile(r"^\d+\.\d+\.\d+\|(.*)$")
_VALID_IMPACT_CONDITIONS = frozenset({"rain", "ice", "snow", "wind", "low_visibility"})


def _is_valid_impact_signature(value: str) -> bool:
    match = _IMPACT_SIGNATURE_HEADER_PATTERN.match(value)
    if not match:
        return False
    body = match.group(1)
    if body == "clear":
        return True
    tokens = body.split(",")
    if any(token not in _VALID_IMPACT_CONDITIONS for token in tokens):
        return False
    # format_impact_signature는 frozenset을 sorted()로 적기 때문에 중복이 없고 항상
    # 오름차순이다 — 정렬 여부와 중복 여부를 한 번에 검사한다.
    return tokens == sorted(set(tokens))

# fetched_at은 weather_time(스케줄 경계) 이후 잠깐 뒤에 기록된다. 재시도(2회 x 2분) +
# 스케줄러 지연을 감안한 여유(provisional). 약간의 시계 오차를 허용해 음수 쪽도 소폭 열어둔다.
FRESHNESS_MIN_SECONDS = -60.0
FRESHNESS_MAX_SECONDS = 1800.0

_SCOPE_QUERY_COLUMNS = (
    "location_id",
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
    "freshness_seconds",
)

# weather_time = target_time인 행만 본다. UPSERT의 역전 방지 WHERE(weather.py) 때문에
# 이 조건에 걸리는 행 = 이번 실행이 실제로 값을 쓴(또는 유지한) 성공 zone뿐이다 — 오래된
# 재시도로 스킵된 행이나 실패한 zone(원래 UPSERT 대상이 아님)은 자연히 빠진다.
_SCOPE_QUERY = f"""
    SELECT
        location_id,
        temperature_2m_c,
        precipitation_mm,
        rain_mm,
        snowfall_cm,
        visibility_m,
        wind_speed_10m_mps,
        wind_gusts_10m_mps,
        weather_code,
        weather_state,
        impact_signature,
        EXTRACT(EPOCH FROM (fetched_at - weather_time)) AS freshness_seconds
    FROM {TABLE}
    WHERE weather_time = %s
"""


class WeatherValidationFailed(Exception):
    """검증 실패 시 발생시켜 Airflow task를 hard fail시킨다(ADR-0004: in-flight는 hard fail)."""


@dataclass(frozen=True, slots=True)
class WeatherValidationSummary:
    row_count: int
    violations: tuple[str, ...]

    @property
    def success(self) -> bool:
        return not self.violations


def find_row_violations(row: Mapping[str, object]) -> list[str]:
    """한 zone 행에서 규칙을 어긴 항목만 문자열로 뽑는다. None(결측)은 검사하지 않는다 —
    latest_zone_weather는 성공한 zone만 UPSERT되므로 실무적으로 항상 값이 있지만,
    방어적으로 결측을 위반으로 취급하지 않는다."""
    location_id = row["location_id"]
    violations: list[str] = []

    temperature = row.get("temperature_2m_c")
    if temperature is not None and not (TEMPERATURE_MIN_C <= temperature <= TEMPERATURE_MAX_C):
        violations.append(
            f"location_id={location_id}: temperature_2m_c={temperature} out of range "
            f"[{TEMPERATURE_MIN_C}, {TEMPERATURE_MAX_C}]"
        )

    for column in _NON_NEGATIVE_COLUMNS:
        value = row.get(column)
        if value is not None and value < 0:
            violations.append(f"location_id={location_id}: {column}={value} is negative")

    weather_code = row.get("weather_code")
    if weather_code is not None and not (WEATHER_CODE_MIN <= weather_code <= WEATHER_CODE_MAX):
        violations.append(
            f"location_id={location_id}: weather_code={weather_code} out of range "
            f"[{WEATHER_CODE_MIN}, {WEATHER_CODE_MAX}]"
        )

    weather_state = row.get("weather_state")
    if weather_state is not None and weather_state not in VALID_WEATHER_STATES:
        violations.append(
            f"location_id={location_id}: weather_state={weather_state!r} not in "
            f"{sorted(VALID_WEATHER_STATES)}"
        )

    impact_signature = row.get("impact_signature")
    if impact_signature is not None and not _is_valid_impact_signature(impact_signature):
        violations.append(
            f"location_id={location_id}: impact_signature={impact_signature!r} does not match "
            f"the expected '<semver>|<sorted conditions or clear>' format"
        )

    freshness = row.get("freshness_seconds")
    if freshness is not None and not (
        FRESHNESS_MIN_SECONDS <= freshness <= FRESHNESS_MAX_SECONDS
    ):
        violations.append(
            f"location_id={location_id}: freshness_seconds={freshness} out of range "
            f"[{FRESHNESS_MIN_SECONDS}, {FRESHNESS_MAX_SECONDS}]"
        )

    return violations


def fetch_scope_rows(connection, target_time: datetime) -> list[dict[str, object]]:
    """weather_time=target_time인 행만 dict로 반환한다."""
    if target_time.utcoffset() is None:
        raise ValueError("target_time must be timezone-aware")
    normalized = target_time.astimezone(UTC)

    with connection.cursor() as cursor:
        cursor.execute(_SCOPE_QUERY, (normalized,))
        rows = cursor.fetchall()
    return [dict(zip(_SCOPE_QUERY_COLUMNS, row, strict=True)) for row in rows]


def run_weather_collection_validation(
    connection,
    target_time: datetime,
) -> WeatherValidationSummary:
    rows = fetch_scope_rows(connection, target_time)
    if not rows:
        raise WeatherValidationFailed(
            f"no {TABLE} rows were actually upserted for weather_time={target_time.isoformat()}"
        )

    violations = [violation for row in rows for violation in find_row_violations(row)]
    summary = WeatherValidationSummary(row_count=len(rows), violations=tuple(violations))
    if not summary.success:
        raise WeatherValidationFailed(
            f"{TABLE} validation failed for weather_time={target_time.isoformat()}: "
            f"{len(violations)} violation(s): {violations}"
        )
    return summary
