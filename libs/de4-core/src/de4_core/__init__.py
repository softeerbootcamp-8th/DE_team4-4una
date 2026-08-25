"""Shared contracts and domain primitives for DE4 services."""

from de4_core.environment import DataArtifact, RoadEnvironmentManifest, SourceSnapshot
from de4_core.perf import PERF_LOG_PREFIX, perf_phase
from de4_core.sensor import SensorEvent
from de4_core.storage import ObjectStore, join_uri

__version__ = "0.1.0"

__all__ = [
    "PERF_LOG_PREFIX",
    "DataArtifact",
    "ObjectStore",
    "RoadEnvironmentManifest",
    "SensorEvent",
    "SourceSnapshot",
    "join_uri",
    "perf_phase",
]
