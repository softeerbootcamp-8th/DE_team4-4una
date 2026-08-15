"""Load and validate sensor-feature thresholds from YAML."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

DEFAULT_STEERING_FEATURE_CONFIG_PATH = (
    Path(__file__).resolve().parents[2] / "config" / "steering_features.yaml"
)
DEFAULT_EVENT_FEATURE_CONFIG_PATH = (
    Path(__file__).resolve().parents[2] / "config" / "event_features.yaml"
)


# value와 provisional 플래그를 함께 담는 단일 설정값
@dataclass(frozen=True, slots=True)
class ProvisionalThreshold:
    value: float
    # 나중에 값이 바뀔 수 있으면 True
    provisional: bool


# steering_features.yaml 한 파일의 파싱 결과 전체
@dataclass(frozen=True, slots=True)
class SteeringFeatureConfig:
    max_gap_seconds: ProvisionalThreshold
    steering_rate_deadband_deg_per_sec: ProvisionalThreshold


# event_features.yaml 한 파일의 파싱 결과 전체
@dataclass(frozen=True, slots=True)
class EventFeatureConfig:
    max_gap_seconds: ProvisionalThreshold
    hard_accel_threshold_mps2: ProvisionalThreshold
    hard_brake_threshold_mps2: ProvisionalThreshold
    min_event_duration_seconds: ProvisionalThreshold
    sharp_steer_threshold_deg_per_sec: ProvisionalThreshold
    sharp_steer_min_duration_seconds: ProvisionalThreshold


# steering_features.yaml을 읽어 SteeringFeatureConfig로 검증한다
def load_steering_feature_config(
    path: Path = DEFAULT_STEERING_FEATURE_CONFIG_PATH,
) -> SteeringFeatureConfig:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise TypeError(f"{path}: top-level YAML document must be a mapping")

    return SteeringFeatureConfig(
        max_gap_seconds=_parse_threshold(document, "max_gap_seconds", path),
        steering_rate_deadband_deg_per_sec=_parse_threshold(
            document, "steering_rate_deadband_deg_per_sec", path
        ),
    )


# event_features.yaml을 읽어 EventFeatureConfig로 검증한다
def load_event_feature_config(
    path: Path = DEFAULT_EVENT_FEATURE_CONFIG_PATH,
) -> EventFeatureConfig:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise TypeError(f"{path}: top-level YAML document must be a mapping")

    return EventFeatureConfig(
        max_gap_seconds=_parse_threshold(document, "max_gap_seconds", path),
        hard_accel_threshold_mps2=_parse_threshold(document, "hard_accel_threshold_mps2", path),
        hard_brake_threshold_mps2=_parse_threshold(document, "hard_brake_threshold_mps2", path),
        min_event_duration_seconds=_parse_threshold(
            document, "min_event_duration_seconds", path
        ),
        sharp_steer_threshold_deg_per_sec=_parse_threshold(
            document, "sharp_steer_threshold_deg_per_sec", path
        ),
        sharp_steer_min_duration_seconds=_parse_threshold(
            document, "sharp_steer_min_duration_seconds", path
        ),
    )


# document[key]가 있고 dict 타입인지 검사한다
def _require_mapping(document: dict, key: str, path: Path) -> dict:
    if key not in document:
        raise ValueError(f"{path}: missing required key '{key}'")
    value = document[key]
    if not isinstance(value, dict):
        raise TypeError(f"{path}: '{key}' must be a mapping")
    return value


# <key>.value/<key>.provisional 한 항목을 ProvisionalThreshold로 변환한다
def _parse_threshold(document: dict, key: str, path: Path) -> ProvisionalThreshold:
    raw = _require_mapping(document, key, path)
    value = raw.get("value")
    is_number = isinstance(value, int | float) and not isinstance(value, bool)
    if not is_number:
        raise ValueError(f"{path}: '{key}.value' must be numeric")
    provisional = raw.get("provisional")
    if not isinstance(provisional, bool):
        raise TypeError(f"{path}: '{key}.provisional' must be a boolean")
    return ProvisionalThreshold(value=float(value), provisional=provisional)
