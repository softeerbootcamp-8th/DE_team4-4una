from batch_jobs.sensor_features.config import load_steering_feature_config


def test_load_steering_feature_config_reads_provisional_thresholds() -> None:
    config = load_steering_feature_config()

    assert config.max_gap_seconds.value == 0.5
    assert config.max_gap_seconds.provisional is True
    assert config.steering_rate_deadband_deg_per_sec.value == 10.0
    assert config.steering_rate_deadband_deg_per_sec.provisional is True
