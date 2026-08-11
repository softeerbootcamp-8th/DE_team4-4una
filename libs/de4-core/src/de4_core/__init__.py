"""Shared contracts and domain primitives for DE4 services."""

from de4_core.environment import DataArtifact, RoadEnvironmentManifest, SourceSnapshot
from de4_core.storage import ObjectStore, join_uri

__version__ = "0.1.0"

__all__ = [
    "DataArtifact",
    "ObjectStore",
    "RoadEnvironmentManifest",
    "SourceSnapshot",
    "join_uri",
]
