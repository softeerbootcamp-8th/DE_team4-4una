"""Versioned parameters for hourly directional comfort scoring."""

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True, slots=True)
class NormalizationRange:
    comfortable: float
    uncomfortable: float


@dataclass(frozen=True, slots=True)
class SpeedBand:
    upper_mps: float | None
    anchor_scale: float


@dataclass(frozen=True, slots=True)
class ComponentRule:
    output_column: str
    weights: tuple[tuple[str, float], ...]


@dataclass(frozen=True, slots=True)
class HourlyScoringConfig:
    scoring_version: str
    compatible_feature_versions: frozenset[str]
    minimum_valid_weight: float
    speed_bands: tuple[SpeedBand, ...]
    normalizers: tuple[tuple[str, NormalizationRange], ...]
    components: tuple[ComponentRule, ...]

    def __post_init__(self) -> None:
        normalizer_names = {name for name, _ in self.normalizers}
        if not self.scoring_version or not self.compatible_feature_versions:
            raise ValueError("scoring versions must be configured")
        if not 0 < self.minimum_valid_weight <= 1:
            raise ValueError("minimum_valid_weight must be in (0, 1]")
        if not self.speed_bands or self.speed_bands[-1].upper_mps is not None:
            raise ValueError("the final speed band must have no upper bound")
        if any(band.anchor_scale <= 0 for band in self.speed_bands):
            raise ValueError("speed-band scales must be positive")
        if any(
            anchors.comfortable >= anchors.uncomfortable
            for _, anchors in self.normalizers
        ):
            raise ValueError("comfortable anchors must be below uncomfortable anchors")
        for component in self.components:
            if abs(sum(weight for _, weight in component.weights) - 1.0) > 1e-9:
                raise ValueError(f"{component.output_column} weights must sum to 1")
            if missing := {name for name, _ in component.weights} - normalizer_names:
                raise ValueError(f"normalizers are missing for {sorted(missing)}")


DEFAULT_HOURLY_SCORING_CONFIG_PATH = Path(__file__).with_name("hourly_comfort.yaml")


def load_hourly_scoring_config(
    path: Path = DEFAULT_HOURLY_SCORING_CONFIG_PATH,
) -> HourlyScoringConfig:
    """Load and validate versioned hourly scoring parameters."""
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise TypeError("hourly scoring config must be a mapping")

    try:
        normalizers = tuple(
            (name, NormalizationRange(**anchors))
            for name, anchors in document["normalizers"].items()
        )
        components = tuple(
            ComponentRule(name, tuple(weights.items()))
            for name, weights in document["components"].items()
        )
        return HourlyScoringConfig(
            scoring_version=document["scoring_version"],
            compatible_feature_versions=frozenset(
                document["compatible_feature_versions"]
            ),
            minimum_valid_weight=document["minimum_valid_weight"],
            speed_bands=tuple(
                SpeedBand(**speed_band) for speed_band in document["speed_bands"]
            ),
            normalizers=normalizers,
            components=components,
        )
    except (KeyError, TypeError) as error:
        raise ValueError(f"invalid hourly scoring config: {error}") from error


# 설정 파일과 계산 코드가 같은 기본 규칙을 사용하도록 import 시 한 번 검증한다
DEFAULT_HOURLY_SCORING_CONFIG = load_hourly_scoring_config()
