from batch_jobs.comfort_score.config import load_comfort_score_config


def test_load_comfort_score_config_reads_provisional_thresholds() -> None:
    config = load_comfort_score_config()

    assert config.vertical_weight.value == 0.5
    assert config.vertical_weight.provisional is True
    assert config.longitudinal_weight.value == 0.3
    assert config.longitudinal_weight.provisional is True
    assert config.lateral_weight.value == 0.2
    assert config.lateral_weight.provisional is True
    assert config.min_traffic_threshold.value == 5.0
    assert config.min_traffic_threshold.provisional is True
    assert config.shrinkage_k.value == 10.0
    assert config.shrinkage_k.provisional is True
