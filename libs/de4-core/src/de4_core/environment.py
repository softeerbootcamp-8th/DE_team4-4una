"""Versioned contracts for prepared road-simulation environments."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Self


@dataclass(frozen=True, slots=True)
class DataArtifact:
    """One immutable object published by a reference-data build."""

    role: str
    uri: str
    media_type: str
    sha256: str
    size_bytes: int
    row_count: int

    def __post_init__(self) -> None:
        if not self.role or not self.uri or not self.media_type:
            raise ValueError("artifact role, URI, and media type must be non-empty")
        if len(self.sha256) != 64:
            raise ValueError("artifact sha256 must contain 64 hexadecimal characters")
        try:
            int(self.sha256, 16)
        except ValueError as error:
            raise ValueError("artifact sha256 must be hexadecimal") from error
        if self.size_bytes < 0 or self.row_count < 0:
            raise ValueError("artifact size and row count must be non-negative")


@dataclass(frozen=True, slots=True)
class SourceSnapshot:
    """Lineage for one unmodified source object retained in the data lake."""

    source_id: str
    snapshot_id: str
    source_uri: str
    object_uri: str
    retrieved_at: datetime
    source_period_or_version: str
    file_format: str
    sha256: str
    schema_fingerprint: str
    size_bytes: int
    row_count: int
    ingestion_run_id: str

    def __post_init__(self) -> None:
        if not all(
            (
                self.source_id,
                self.snapshot_id,
                self.source_uri,
                self.object_uri,
                self.source_period_or_version,
                self.file_format,
                self.schema_fingerprint,
                self.ingestion_run_id,
            )
        ):
            raise ValueError("source snapshot identifiers and URIs must be non-empty")
        if self.retrieved_at.utcoffset() is None:
            raise ValueError("retrieved_at must be timezone-aware")
        if len(self.sha256) != 64:
            raise ValueError("source sha256 must contain 64 hexadecimal characters")
        try:
            int(self.sha256, 16)
        except ValueError as error:
            raise ValueError("source sha256 must be hexadecimal") from error
        if self.size_bytes < 0 or self.row_count < 0:
            raise ValueError("source size and row count must be non-negative")


@dataclass(frozen=True, slots=True)
class RoadEnvironmentManifest:
    """Published contract consumed by the sensor simulation at startup."""

    schema_version: str
    environment_id: str
    reference_date: date
    road_snapshot_date: date
    build_id: str
    created_at: datetime
    mapping_version: str
    status: str
    artifacts: tuple[DataArtifact, ...]
    sources: tuple[SourceSnapshot, ...]
    quality: dict[str, int | float | str]

    REQUIRED_RUNTIME_ARTIFACTS = frozenset(
        {"simulation_road_environment", "taxi_zone"}
    )

    def __post_init__(self) -> None:
        if not all(
            (
                self.schema_version,
                self.environment_id,
                self.build_id,
                self.mapping_version,
            )
        ):
            raise ValueError("environment manifest identifiers must be non-empty")
        if any(separator in self.environment_id for separator in ("/", "\\")):
            raise ValueError("environment_id must be path-safe")
        if self.created_at.utcoffset() is None:
            raise ValueError("created_at must be timezone-aware")
        if self.status != "READY":
            raise ValueError("only READY environments may be consumed")
        roles = [artifact.role for artifact in self.artifacts]
        if len(roles) != len(set(roles)):
            raise ValueError("environment artifact roles must be unique")
        missing = self.REQUIRED_RUNTIME_ARTIFACTS.difference(roles)
        if missing:
            raise ValueError(
                f"environment is missing runtime artifacts: {', '.join(sorted(missing))}"
            )
        if not self.sources:
            raise ValueError("environment must retain at least one source snapshot")

    def artifact(self, role: str) -> DataArtifact:
        for artifact in self.artifacts:
            if artifact.role == role:
                return artifact
        raise KeyError(f"environment artifact role not found: {role}")

    def to_dict(self) -> dict[str, object]:
        value = asdict(self)
        value["reference_date"] = self.reference_date.isoformat()
        value["road_snapshot_date"] = self.road_snapshot_date.isoformat()
        value["created_at"] = self.created_at.isoformat()
        value["artifacts"] = [asdict(item) for item in self.artifacts]
        value["sources"] = [
            {
                **asdict(item),
                "retrieved_at": item.retrieved_at.isoformat(),
            }
            for item in self.sources
        ]
        return value

    def to_json(self) -> bytes:
        return json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ).encode()

    @classmethod
    def from_dict(cls, value: dict[str, object]) -> Self:
        artifact_values = value.get("artifacts")
        source_values = value.get("sources")
        quality = value.get("quality")
        if not isinstance(artifact_values, list) or not isinstance(source_values, list):
            raise TypeError("manifest artifacts and sources must be lists")
        if not isinstance(quality, dict):
            raise TypeError("manifest quality must be an object")
        return cls(
            schema_version=str(value["schema_version"]),
            environment_id=str(value["environment_id"]),
            reference_date=date.fromisoformat(str(value["reference_date"])),
            road_snapshot_date=date.fromisoformat(str(value["road_snapshot_date"])),
            build_id=str(value["build_id"]),
            created_at=datetime.fromisoformat(str(value["created_at"])),
            mapping_version=str(value["mapping_version"]),
            status=str(value["status"]),
            artifacts=tuple(DataArtifact(**item) for item in artifact_values),
            sources=tuple(
                SourceSnapshot(
                    **{
                        **item,
                        "retrieved_at": datetime.fromisoformat(str(item["retrieved_at"])),
                    }
                )
                for item in source_values
            ),
            quality={str(key): item for key, item in quality.items()},
        )

    @classmethod
    def from_json(cls, value: bytes | str) -> Self:
        document = json.loads(value)
        if not isinstance(document, dict):
            raise TypeError("environment manifest must be a JSON object")
        return cls.from_dict(document)
