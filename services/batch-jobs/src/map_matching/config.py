"""Load and validate map-matching thresholds from YAML."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml
from sensor_features.config import ProvisionalThreshold

DEFAULT_MAP_MATCHING_CONFIG_PATH = (
    Path(__file__).resolve().parents[2] / "config" / "map_matching.yaml"
)


# map_matching.yaml 한 파일의 파싱 결과 전체
@dataclass(frozen=True, slots=True)
class MapMatchingConfig:
    candidate_search_radius_m: ProvisionalThreshold


# map_matching.yaml을 읽어 MapMatchingConfig로 검증한다
def load_map_matching_config(path: Path = DEFAULT_MAP_MATCHING_CONFIG_PATH) -> MapMatchingConfig:
    document = yaml.safe_load(path.read_text())
    if not isinstance(document, dict):
        raise TypeError(f"{path}: top-level YAML document must be a mapping")

    return MapMatchingConfig(
        candidate_search_radius_m=_parse_threshold(document, "candidate_search_radius_m", path)
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
