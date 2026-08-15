"""Load and validate map-matching thresholds from YAML."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path

import yaml

DEFAULT_MAP_MATCHING_CONFIG_PATH = (
    Path(__file__).resolve().parents[2] / "config" / "map_matching.yaml"
)


# value와 provisional 플래그를 함께 담는 단일 설정값
@dataclass(frozen=True, slots=True)
class ProvisionalThreshold:
    value: float
    # 나중에 값이 바뀔 수 있으면 True
    provisional: bool


# map_matching.yaml 한 파일의 파싱 결과 전체
@dataclass(frozen=True, slots=True)
class MapMatchingConfig:
    candidate_search_radius_m: ProvisionalThreshold
    distance_weight: ProvisionalThreshold
    heading_weight: ProvisionalThreshold


# map_matching.yaml을 읽어 MapMatchingConfig로 검증한다
def load_map_matching_config(path: Path = DEFAULT_MAP_MATCHING_CONFIG_PATH) -> MapMatchingConfig:
    document = yaml.safe_load(path.read_text())
    if not isinstance(document, dict):
        raise TypeError(f"{path}: top-level YAML document must be a mapping")

    radius = _parse_threshold(document, "candidate_search_radius_m", path)
    distance_weight = _parse_threshold(document, "distance_weight", path)
    heading_weight = _parse_threshold(document, "heading_weight", path)

    if not math.isfinite(radius.value) or radius.value <= 0:
        raise ValueError(f"{path}: 'candidate_search_radius_m.value' must be finite and > 0")
    if not all(
        math.isfinite(w.value) and 0.0 <= w.value <= 1.0 for w in (distance_weight, heading_weight)
    ):
        raise ValueError(f"{path}: 'distance_weight'/'heading_weight' values must be in [0, 1]")
    if not math.isclose(distance_weight.value + heading_weight.value, 1.0):
        raise ValueError(f"{path}: 'distance_weight' + 'heading_weight' must sum to 1.0")

    return MapMatchingConfig(
        candidate_search_radius_m=radius,
        distance_weight=distance_weight,
        heading_weight=heading_weight,
    )


# <key>.value/<key>.provisional 한 항목을 ProvisionalThreshold로 변환한다
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
