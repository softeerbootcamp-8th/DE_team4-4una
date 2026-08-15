"""Versioned parameters for hourly directional comfort scoring."""

from dataclasses import dataclass


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


# 실제 Silver2 분포가 나오면 기준값과 scoring_version을 함께 갱신한다
DEFAULT_HOURLY_SCORING_CONFIG = HourlyScoringConfig(
    scoring_version="hourly-comfort-v1",
    compatible_feature_versions=frozenset({"hourly-features-v1"}),
    minimum_valid_weight=0.8,
    # 같은 움직임도 속도대별 정상 범위가 달라질 수 있어 앵커만 완만하게 보정한다
    speed_bands=(SpeedBand(5.0, 0.9), SpeedBand(10.0, 1.0), SpeedBand(None, 1.1)),
    normalizers=(
        ("rms_accel_x", NormalizationRange(0.15, 1.5)),
        ("rms_accel_y", NormalizationRange(0.12, 1.2)),
        ("rms_accel_z", NormalizationRange(0.10, 1.0)),
        ("p95_abs_accel_x", NormalizationRange(0.40, 3.0)),
        ("p95_abs_accel_y", NormalizationRange(0.35, 2.5)),
        ("p95_abs_accel_z", NormalizationRange(0.30, 2.0)),
        ("rms_jerk_x", NormalizationRange(0.50, 6.0)),
        ("rms_jerk_y", NormalizationRange(0.50, 6.0)),
        ("rms_jerk_z", NormalizationRange(0.50, 8.0)),
        ("p95_abs_jerk_x", NormalizationRange(1.50, 15.0)),
        ("p95_abs_jerk_y", NormalizationRange(1.50, 15.0)),
        ("p95_abs_jerk_z", NormalizationRange(2.00, 20.0)),
        ("hard_brake_rate", NormalizationRange(0.0, 2.0)),
        ("hard_accel_rate", NormalizationRange(0.0, 2.0)),
        ("sharp_steer_rate", NormalizationRange(0.0, 2.0)),
        ("steer_reversal_rate", NormalizationRange(0.5, 5.0)),
        ("rms_steering_rate", NormalizationRange(2.0, 30.0)),
        ("rms_steering_vibration", NormalizationRange(0.05, 1.0)),
    ),
    components=(
        ComponentRule(
            "vertical_score",
            (
                ("rms_accel_z", 0.35),
                ("p95_abs_accel_z", 0.20),
                ("rms_jerk_z", 0.20),
                ("p95_abs_jerk_z", 0.15),
                ("rms_steering_vibration", 0.10),
            ),
        ),
        ComponentRule(
            "longitudinal_score",
            (
                ("rms_accel_x", 0.25),
                ("p95_abs_accel_x", 0.15),
                ("rms_jerk_x", 0.20),
                ("p95_abs_jerk_x", 0.15),
                ("hard_brake_rate", 0.15),
                ("hard_accel_rate", 0.10),
            ),
        ),
        ComponentRule(
            "lateral_score",
            (
                ("rms_accel_y", 0.25),
                ("p95_abs_accel_y", 0.15),
                ("rms_jerk_y", 0.15),
                ("p95_abs_jerk_y", 0.10),
                ("sharp_steer_rate", 0.15),
                ("rms_steering_rate", 0.15),
                ("steer_reversal_rate", 0.05),
            ),
        ),
    ),
)
