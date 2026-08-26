from importlib import import_module

import pytest


@pytest.mark.parametrize(
    "package_name",
    [
        "de4_core",
        "sensor_producer",
        "stream_processor",
        "batch_jobs",
        "orchestration",
        "serving_api",
        "dashboard",
        "ops_agent",
        "pipeline_perf",
    ],
)
def test_workspace_package_is_importable(package_name: str) -> None:
    assert import_module(package_name)
