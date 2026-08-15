from map_matching.config import load_map_matching_config


def test_load_map_matching_config_reads_provisional_threshold() -> None:
    config = load_map_matching_config()

    assert config.candidate_search_radius_m.value == 30.0
    assert config.candidate_search_radius_m.provisional is True
