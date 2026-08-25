"""Load and validate Gold comfort-score formula parameters from YAML."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from batch_jobs.resources import RESOURCE_DIR
from batch_jobs.sensor_features.config import ProvisionalThreshold

DEFAULT_COMFORT_SCORE_CONFIG_PATH = RESOURCE_DIR / "comfort_score.yaml"


# comfort_score.yaml 한 파일의 파싱 결과 전체
@dataclass(frozen=True, slots=True)
class ComfortScoreConfig:
    vertical_weight: ProvisionalThreshold
    longitudinal_weight: ProvisionalThreshold
    lateral_weight: ProvisionalThreshold
    min_traffic_threshold: ProvisionalThreshold
    shrinkage_k: ProvisionalThreshold

    def __post_init__(self) -> None:
        # 점수 0~100은 방향 가중치가 비음수이고 합이 1이며 shrinkage_k가 비음수일
        # 때만 성립한다(ADR-0012). 어긋나면 comfort_score가 범위를 벗어나는데,
        # 방향 점수는 각각 범위 안이라 GX도 통과하고 Postgres CHECK 제약에서야
        # 드러난다 — 계산을 다 마친 뒤다.
        weights = {
            "vertical_weight": self.vertical_weight.value,
            "longitudinal_weight": self.longitudinal_weight.value,
            "lateral_weight": self.lateral_weight.value,
        }
        for name, value in weights.items():
            if value < 0:
                raise ValueError(f"{name} must not be negative")
        if abs(sum(weights.values()) - 1.0) > 1e-9:
            raise ValueError("direction weights must sum to 1")
        if self.shrinkage_k.value < 0:
            raise ValueError("shrinkage_k must not be negative")


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
