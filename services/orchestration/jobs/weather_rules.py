# 날씨 보정 규칙 (#215). context/comfort-score.md "Weather-adjusted current score" Step B/C.
# 관측을 조건 집합으로 정규화하고(impact_signature), 그 조건에 따라 방향별 점수를 깎는다.

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import yaml

# 규칙이 바뀌면 올린다. signature에 들어가므로, 올리는 순간 모든 zone이 한 번 재계산된다.
WEATHER_RULE_VERSION = "1.0.0"

DEFAULT_WEATHER_RULES_CONFIG_PATH = Path(__file__).parent / "resources" / "weather_rules.yaml"

# Open-Meteo minutely_15는 직전 15분 합이라 시간당으로 환산해서 비교한다.
STEPS_PER_HOUR = 4

RAIN = "rain"
ICE = "ice"
SNOW = "snow"
WIND = "wind"
LOW_VISIBILITY = "low_visibility"

# 조건이 하나도 없을 때 signature에 넣을 값. 빈 문자열은 파싱에서 구분이 안 된다.
CLEAR = "clear"

# 실측이 0으로 와도 WMO 코드가 강수를 말하면 인정한다.
RAIN_WEATHER_CODES = frozenset({51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82, 95, 96, 99})
SNOW_WEATHER_CODES = frozenset({71, 73, 75, 77, 85, 86})
FREEZING_WEATHER_CODES = frozenset({56, 57, 66, 67})
# 안개 코드는 실측 visibility가 높게 와도 시야 불량으로 본다.
FOG_WEATHER_CODES = frozenset({45, 48})

SCORE_MIN = 0.0
SCORE_MAX = 100.0


@dataclass(frozen=True, slots=True)
class ProvisionalThreshold:
    value: float
    # 나중에 값이 바뀔 수 있으면 True
    provisional: bool


@dataclass(frozen=True, slots=True)
class WeatherRuleConfig:
    rain_mm_per_hour: ProvisionalThreshold
    snow_cm_per_hour: ProvisionalThreshold
    freezing_temperature_c: ProvisionalThreshold
    wind_gust_mps: ProvisionalThreshold
    low_visibility_m: ProvisionalThreshold
    rain_longitudinal_deduction: ProvisionalThreshold
    ice_longitudinal_deduction: ProvisionalThreshold
    snow_vertical_deduction: ProvisionalThreshold
    snow_longitudinal_deduction: ProvisionalThreshold
    wind_lateral_deduction: ProvisionalThreshold
    low_visibility_final_deduction: ProvisionalThreshold
    vertical_weight: ProvisionalThreshold
    longitudinal_weight: ProvisionalThreshold
    lateral_weight: ProvisionalThreshold


@dataclass(frozen=True, slots=True)
class WeatherDeduction:
    vertical: float
    longitudinal: float
    lateral: float
    # 방향 점수가 아니라 결합된 최종 점수에서만 깎는다 (시야)
    final: float


@dataclass(frozen=True, slots=True)
class AdjustedScores:
    vertical_score: float
    longitudinal_score: float
    lateral_score: float
    comfort_score: float


def load_weather_rule_config(
    path: Path = DEFAULT_WEATHER_RULES_CONFIG_PATH,
) -> WeatherRuleConfig:
    document = yaml.safe_load(path.read_text())
    if not isinstance(document, dict):
        raise TypeError(f"{path}: top-level YAML document must be a mapping")

    config = WeatherRuleConfig(
        **{
            field: _parse_threshold(document, field, path)
            for field in WeatherRuleConfig.__dataclass_fields__
        }
    )
    # 합이 1이 아니면 Step C 결합에서 0~100 스케일이 깨진다.
    weight_sum = (
        config.vertical_weight.value
        + config.longitudinal_weight.value
        + config.lateral_weight.value
    )
    if abs(weight_sum - 1.0) > 1e-9:
        raise ValueError(f"{path}: directional weights must sum to 1.0, got {weight_sum}")
    return config


def _parse_threshold(document: dict, key: str, path: Path) -> ProvisionalThreshold:
    if key not in document:
        raise ValueError(f"{path}: missing required key '{key}'")
    raw = document[key]
    if not isinstance(raw, dict):
        raise TypeError(f"{path}: '{key}' must be a mapping")

    value = raw.get("value")
    is_number = isinstance(value, int | float) and not isinstance(value, bool)
    if not is_number:
        raise ValueError(f"{path}: '{key}.value' must be numeric")
    provisional = raw.get("provisional")
    if not isinstance(provisional, bool):
        raise TypeError(f"{path}: '{key}.provisional' must be a boolean")
    return ProvisionalThreshold(value=float(value), provisional=provisional)


def classify_weather(
    reading: Mapping[str, float | int | None],
    config: WeatherRuleConfig,
) -> frozenset[str]:
    """관측 한 건에서 점수에 영향을 주는 조건만 뽑는다. 키는 weather.py의 MINUTELY_15_VARIABLES."""
    weather_code = reading.get("weather_code")
    rain_per_hour = _positive(reading.get("rain")) * STEPS_PER_HOUR
    snow_per_hour = _positive(reading.get("snowfall")) * STEPS_PER_HOUR
    precipitation_per_hour = _positive(reading.get("precipitation")) * STEPS_PER_HOUR
    visibility = reading.get("visibility")
    temperature = reading.get("temperature_2m")

    conditions = set()
    if rain_per_hour >= config.rain_mm_per_hour.value or weather_code in RAIN_WEATHER_CODES:
        conditions.add(RAIN)
    if snow_per_hour >= config.snow_cm_per_hour.value or weather_code in SNOW_WEATHER_CODES:
        conditions.add(SNOW)
    # 기온만 낮아도 마른 노면은 미끄럽지 않다. 강수가 같이 있어야 결빙으로 본다.
    # 기온 결측은 다른 필드와 달리 0으로 치환하면 안 된다 — 전부 결빙이 돼버린다.
    is_freezing = (
        temperature is not None and float(temperature) <= config.freezing_temperature_c.value
    )
    has_precipitation = rain_per_hour > 0 or precipitation_per_hour > 0
    if weather_code in FREEZING_WEATHER_CODES or (is_freezing and has_precipitation):
        conditions.add(ICE)
    if _positive(reading.get("wind_gusts_10m")) >= config.wind_gust_mps.value:
        conditions.add(WIND)
    if weather_code in FOG_WEATHER_CODES or (
        visibility is not None and float(visibility) <= config.low_visibility_m.value
    ):
        conditions.add(LOW_VISIBILITY)
    return frozenset(conditions)


def _positive(value: float | None) -> float:
    # 결측과 음수(관측 오류)는 둘 다 "없음"으로 본다.
    if value is None:
        return 0.0
    number = float(value)
    return number if number > 0 else 0.0


def build_impact_signature(
    reading: Mapping[str, float | int | None],
    config: WeatherRuleConfig,
) -> str:
    return format_impact_signature(classify_weather(reading, config))


# 조건 집합을 그대로 문자열로 적는다 (예: `1.0.0|ice,snow`). 해시가 아니라 정규화된
# 조건이라, 기온이 0.1도 흔들리는 정도로는 값이 바뀌지 않는다 — 존당 최신 1행만 남기는
# latest_zone_weather에서 재계산 트리거로 쓰려면 이 성질이 필요하다.
def format_impact_signature(conditions: frozenset[str]) -> str:
    body = ",".join(sorted(conditions)) if conditions else CLEAR
    return f"{WEATHER_RULE_VERSION}|{body}"


# 재계산 job은 DB 컬럼을 다시 분류하지 않고 이 값을 되돌려 쓴다 — 변경 감지에 쓴 조건과
# 실제로 적용한 조건이 어긋날 수 없다.
def parse_impact_signature(signature: str) -> frozenset[str]:
    version, separator, body = signature.partition("|")
    if not separator or version != WEATHER_RULE_VERSION:
        raise ValueError(
            f"impact_signature {signature!r} was not written by rule version "
            f"{WEATHER_RULE_VERSION!r} — recompute every zone"
        )
    if body == CLEAR:
        return frozenset()

    conditions = frozenset(body.split(","))
    unknown = conditions - {RAIN, ICE, SNOW, WIND, LOW_VISIBILITY}
    if unknown:
        raise ValueError(f"unknown condition {sorted(unknown)} in {signature!r}")
    return conditions


def weather_deduction(conditions: frozenset[str], config: WeatherRuleConfig) -> WeatherDeduction:
    """방향별 감점. 방향 매핑은 Step B에서 확정된 것이고 크기만 설정에서 온다.

    같은 방향에 여러 조건이 걸리면 합이 아니라 최댓값을 쓴다 — 결빙성 비(rain+ice)나
    진눈깨비(rain+snow)에서 종방향을 두 번 깎지 않기 위한 것이다.
    """
    vertical = config.snow_vertical_deduction.value if SNOW in conditions else 0.0
    longitudinal = max(
        config.rain_longitudinal_deduction.value if RAIN in conditions else 0.0,
        config.ice_longitudinal_deduction.value if ICE in conditions else 0.0,
        config.snow_longitudinal_deduction.value if SNOW in conditions else 0.0,
    )
    lateral = config.wind_lateral_deduction.value if WIND in conditions else 0.0
    final = config.low_visibility_final_deduction.value if LOW_VISIBILITY in conditions else 0.0
    return WeatherDeduction(
        vertical=vertical, longitudinal=longitudinal, lateral=lateral, final=final
    )


def adjust_comfort_scores(
    vertical_score: float,
    longitudinal_score: float,
    lateral_score: float,
    conditions: frozenset[str],
    config: WeatherRuleConfig,
) -> AdjustedScores:
    """standard 방향 점수에 감점을 적용하고 최종 점수까지 다시 만든다 (Step B -> C).

    시야 감점은 결합된 값에만 걸리므로, 그 조건이 켜지면 comfort_score는 세 방향 점수의
    가중합과 일치하지 않는다. standard 테이블과 달리 여기서는 의도된 동작이다.
    """
    deduction = weather_deduction(conditions, config)
    adjusted_vertical = _clamp(vertical_score - deduction.vertical)
    adjusted_longitudinal = _clamp(longitudinal_score - deduction.longitudinal)
    adjusted_lateral = _clamp(lateral_score - deduction.lateral)

    combined = (
        config.vertical_weight.value * adjusted_vertical
        + config.longitudinal_weight.value * adjusted_longitudinal
        + config.lateral_weight.value * adjusted_lateral
    )
    return AdjustedScores(
        vertical_score=adjusted_vertical,
        longitudinal_score=adjusted_longitudinal,
        lateral_score=adjusted_lateral,
        comfort_score=_clamp(combined - deduction.final),
    )


def _clamp(value: float) -> float:
    return min(max(value, SCORE_MIN), SCORE_MAX)
