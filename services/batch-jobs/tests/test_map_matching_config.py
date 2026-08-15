import pytest
from map_matching.config import load_map_matching_config


def test_load_map_matching_config_reads_provisional_threshold() -> None:
    config = load_map_matching_config()

    assert config.candidate_search_radius_m.value == 30.0
    assert config.candidate_search_radius_m.provisional is True
    assert config.distance_weight.value == 0.7
    assert config.distance_weight.provisional is True
    assert config.heading_weight.value == 0.3
    assert config.heading_weight.provisional is True


def _write_config(tmp_path, radius: float, distance_weight: float, heading_weight: float):
    path = tmp_path / "map_matching.yaml"
    path.write_text(
        f"""
candidate_search_radius_m:
  value: {radius}
  provisional: true
distance_weight:
  value: {distance_weight}
  provisional: true
heading_weight:
  value: {heading_weight}
  provisional: true
"""
    )
    return path


@pytest.mark.parametrize(
    "radius, distance_weight, heading_weight",
    [
        (0.0, 0.7, 0.3),  # 반경이 0
        (-1.0, 0.7, 0.3),  # 반경이 음수
        (30.0, -0.2, 1.2),  # 가중치가 범위 밖(합은 1.0)
        (30.0, 1.2, -0.2),  # 가중치가 범위 밖(합은 1.0)
        (30.0, 0.5, 0.6),  # 가중치 합이 1.0이 아님
    ],
)
def test_load_map_matching_config_rejects_invalid_values(
    tmp_path, radius: float, distance_weight: float, heading_weight: float
) -> None:
    path = _write_config(tmp_path, radius, distance_weight, heading_weight)

    with pytest.raises(ValueError):
        load_map_matching_config(path)
