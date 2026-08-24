import pytest
from dashboard.config import (
    DEFAULT_BATCH_CHUNK_SIZE,
    DEFAULT_VEHICLE_PROFILE_ID,
    DashboardConfig,
)


def test_config_uses_vehicle_agnostic_profile_and_api_batch_contract_defaults() -> None:
    config = DashboardConfig.from_env(
        {
            "DASHBOARD_ROAD_SEGMENT_S3_URI": (
                "s3://bucket/road_segment/snapshot_date=2026-08-24/data.parquet"
            )
        }
    )

    assert config.vehicle_profile_id == DEFAULT_VEHICLE_PROFILE_ID
    assert config.batch_chunk_size == DEFAULT_BATCH_CHUNK_SIZE
    assert DEFAULT_BATCH_CHUNK_SIZE == 1000
    assert config.batch_endpoint == (
        "http://localhost:8000/api/v1/comfort-scores/batch"
    )


def test_config_rejects_non_s3_road_segment_location() -> None:
    with pytest.raises(ValueError, match="must start with s3://"):
        DashboardConfig.from_env(
            {"DASHBOARD_ROAD_SEGMENT_S3_URI": "/tmp/road_segment/data.parquet"}
        )
