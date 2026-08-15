"""Load and validate steering-feature thresholds from YAML."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

DEFAULT_STEERING_FEATURE_CONFIG_PATH = (
    Path(__file__).resolve().parents[2] / "config" / "steering_features.yaml"
)


@dataclass(frozen=True, slots=True)
class ProvisionalThreshold:
    """근거가 아직 확정되지 않았을 수 있는 단일 설정값."""

    value: float
    # 실측 근거 없이 잠정적으로 정한 값이면 True
    provisional: bool


@dataclass(frozen=True, slots=True)
class SteeringFeatureConfig:
    """steering_features.yaml 한 파일의 파싱 결과 전체."""

    max_gap_seconds: ProvisionalThreshold
    steering_rate_deadband_deg_per_sec: ProvisionalThreshold


def load_steering_feature_config(
    path: Path = DEFAULT_STEERING_FEATURE_CONFIG_PATH,
) -> SteeringFeatureConfig:
    """Parse and validate a steering_features.yaml file into a SteeringFeatureConfig."""
    document = yaml.safe_load(path.read_text())
    if not isinstance(document, dict):
        raise TypeError(f"{path}: top-level YAML document must be a mapping")

    return SteeringFeatureConfig(
        max_gap_seconds=_parse_threshold(document, "max_gap_seconds", path),
        steering_rate_deadband_deg_per_sec=_parse_threshold(
            document, "steering_rate_deadband_deg_per_sec", path
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
