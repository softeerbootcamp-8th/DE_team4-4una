"""Load and validate Gold comfort-score formula parameters from YAML."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml
from sensor_features.config import ProvisionalThreshold

DEFAULT_COMFORT_SCORE_CONFIG_PATH = (
    Path(__file__).resolve().parents[2] / "config" / "comfort_score.yaml"
)


# comfort_score.yaml 한 파일의 파싱 결과 전체
@dataclass(frozen=True, slots=True)
class ComfortScoreConfig:
    vertical_weight: ProvisionalThreshold
    longitudinal_weight: ProvisionalThreshold
    lateral_weight: ProvisionalThreshold
    min_traffic_threshold: ProvisionalThreshold
    shrinkage_k: ProvisionalThreshold


# comfort_score.yaml을 읽어 ComfortScoreConfig로 검증한다
def load_comfort_score_config(
    path: Path = DEFAULT_COMFORT_SCORE_CONFIG_PATH,
) -> ComfortScoreConfig:
    document = yaml.safe_load(path.read_text())
    if not isinstance(document, dict):
        raise TypeError(f"{path}: top-level YAML document must be a mapping")

    return ComfortScoreConfig(
        vertical_weight=_parse_threshold(document, "vertical_weight", path),
        longitudinal_weight=_parse_threshold(document, "longitudinal_weight", path),
        lateral_weight=_parse_threshold(document, "lateral_weight", path),
        min_traffic_threshold=_parse_threshold(document, "min_traffic_threshold", path),
        shrinkage_k=_parse_threshold(document, "shrinkage_k", path),
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
