"""Load and validate sensor-event cleansing thresholds from YAML."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import yaml

DEFAULT_CLEANSING_CONFIG_PATH = (
    Path(__file__).resolve().parents[2] / "config" / "cleansing_rules.yaml"
)


@dataclass(frozen=True, slots=True)
class ValueRange:
    """OUT_OF_RANGE 판정에 쓰는 컬럼별 유효 범위. min/max는 없으면 None."""

    minimum: float | None
    maximum: float | None
    # heading의 360처럼 상한을 포함하지 않는 범위(예: [0, 360))를 표현하기 위한 플래그
    max_exclusive: bool = False


@dataclass(frozen=True, slots=True)
class EventTimeBounds:
    """event_time의 절대 상하한. 정밀한 배치창 계산이 아니라 명백히 잘못된 값만 거른다."""

    minimum: datetime
    maximum: datetime
    # 실측 근거 없이 잠정적으로 정한 값이면 True
    provisional: bool


@dataclass(frozen=True, slots=True)
class DeduplicationRule:
    """중복 제거 키와 우선순위."""

    key: tuple[str, ...]
    # 동일 key를 가진 행이 여럿일 때 무엇을 남길지 나타내는 식별자(예: "latest_ingested_at")
    priority: str


@dataclass(frozen=True, slots=True)
class CleansingConfig:
    """cleansing_rules.yaml 한 파일의 파싱 결과 전체."""

    required_columns: tuple[str, ...]
    value_ranges: dict[str, ValueRange]
    event_time_bounds: EventTimeBounds
    deduplication: DeduplicationRule


def load_cleansing_config(path: Path = DEFAULT_CLEANSING_CONFIG_PATH) -> CleansingConfig:
    """Parse and validate a cleansing_rules.yaml file into a CleansingConfig."""

    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise TypeError(f"{path}: top-level YAML document must be a mapping")

    return CleansingConfig(
        required_columns=tuple(_require_list_of_str(document, "required_columns", path)),
        value_ranges={
            column: _parse_value_range(column, raw, path)
            for column, raw in _require_mapping(document, "value_ranges", path).items()
        },
        event_time_bounds=_parse_event_time_bounds(
            _require_mapping(document, "event_time_bounds", path), path
        ),
        deduplication=_parse_deduplication(
            _require_mapping(document, "deduplication", path), path
        ),
    )


# document[key]가 있고 dict 타입인지 검사한다(value_ranges/event_time_bounds/deduplication에 재사용)
def _require_mapping(document: dict, key: str, path: Path) -> dict:
    if key not in document:
        raise ValueError(f"{path}: missing required key '{key}'")
    value = document[key]
    if not isinstance(value, dict):
        raise TypeError(f"{path}: '{key}' must be a mapping")
    return value


# document[key]가 있고 문자열 리스트인지 검사한다(required_columns에 사용)
def _require_list_of_str(document: dict, key: str, path: Path) -> list[str]:
    if key not in document:
        raise ValueError(f"{path}: missing required key '{key}'")
    value = document[key]
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{path}: '{key}' must be a list of strings")
    return value


# 값이 bool 타입인지 검사한다(max_exclusive, provisional에 재사용)
def _require_bool(value: object, field_name: str, path: Path) -> bool:
    if not isinstance(value, bool):
        raise TypeError(f"{path}: '{field_name}' must be a boolean")
    return value


# min/max처럼 null이 허용되는 숫자 필드를 검사한다
def _require_number_or_none(value: object, field_name: str, path: Path) -> float | None:
    if value is not None and not isinstance(value, int | float):
        raise ValueError(f"{path}: '{field_name}' must be numeric or null")
    return value


# value_ranges.<column> 한 항목을 ValueRange로 변환한다
def _parse_value_range(column: str, raw: object, path: Path) -> ValueRange:
    if not isinstance(raw, dict):
        raise TypeError(f"{path}: value_ranges.{column} must be a mapping")
    if "min" not in raw or "max" not in raw:
        raise ValueError(f"{path}: value_ranges.{column} must define 'min' and 'max'")
    max_exclusive = _require_bool(
        raw.get("max_exclusive", False), f"value_ranges.{column}.max_exclusive", path
    )
    return ValueRange(
        minimum=_require_number_or_none(raw["min"], f"value_ranges.{column}.min", path),
        maximum=_require_number_or_none(raw["max"], f"value_ranges.{column}.max", path),
        max_exclusive=max_exclusive,
    )


# event_time_bounds.min/max 문자열을 datetime으로 파싱한다
def _parse_event_time_bounds(raw: dict, path: Path) -> EventTimeBounds:
    minimum = raw.get("min")
    maximum = raw.get("max")
    if not isinstance(minimum, str) or not isinstance(maximum, str):
        raise TypeError(f"{path}: event_time_bounds.min/max must be ISO 8601 strings")
    provisional = _require_bool(
        raw.get("provisional", False), "event_time_bounds.provisional", path
    )
    try:
        return EventTimeBounds(
            minimum=datetime.fromisoformat(minimum),
            maximum=datetime.fromisoformat(maximum),
            provisional=provisional,
        )
    except ValueError as error:
        raise ValueError(f"{path}: event_time_bounds.min/max must be ISO 8601") from error


# deduplication.key/priority를 DeduplicationRule로 변환한다
def _parse_deduplication(raw: dict, path: Path) -> DeduplicationRule:
    key = raw.get("key")
    if not isinstance(key, list) or not key or not all(isinstance(item, str) for item in key):
        raise ValueError(f"{path}: deduplication.key must be a non-empty list of strings")
    priority = raw.get("priority")
    if not isinstance(priority, str) or not priority:
        raise ValueError(f"{path}: deduplication.priority must be a non-empty string")
    return DeduplicationRule(key=tuple(key), priority=priority)
