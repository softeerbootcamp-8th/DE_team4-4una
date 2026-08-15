from sensor_features.config import load_event_feature_config


def test_load_event_feature_config_reads_provisional_thresholds() -> None:
    config = load_event_feature_config()

    assert config.max_gap_seconds.value == 0.5
    assert config.max_gap_seconds.provisional is True
    assert config.hard_accel_threshold_mps2.value == 3.0
    assert config.hard_accel_threshold_mps2.provisional is True
    assert config.hard_brake_threshold_mps2.value == -3.0
    assert config.hard_brake_threshold_mps2.provisional is True
    assert config.min_event_duration_seconds.value == 0.3
    assert config.min_event_duration_seconds.provisional is True
