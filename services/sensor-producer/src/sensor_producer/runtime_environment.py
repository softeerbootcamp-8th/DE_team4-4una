"""Resolve a published road environment into verified local runtime files."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

from de4_core import DataArtifact, ObjectStore, RoadEnvironmentManifest

from sensor_producer.environment import RoadEnvironment


@dataclass(frozen=True, slots=True)
class LoadedRoadEnvironment:
    environment: RoadEnvironment
    manifest: RoadEnvironmentManifest
    manifest_uri: str


class RoadEnvironmentLoader:
    def __init__(self, store: ObjectStore | None = None):
        self.store = store or ObjectStore()

    def from_pointer(self, pointer_uri: str, cache_dir: Path) -> LoadedRoadEnvironment:
        pointer = parse_active_pointer(self.store.read_bytes(pointer_uri))
        manifest_bytes = self.store.read_bytes(pointer["manifest_uri"])
        verify_bytes(manifest_bytes, pointer["manifest_sha256"], "environment manifest")
        manifest = RoadEnvironmentManifest.from_json(manifest_bytes)
        if manifest.environment_id != pointer["environment_id"]:
            raise ValueError("active pointer environment_id does not match its manifest")
        return self._load(manifest, pointer["manifest_uri"], cache_dir)

    def from_manifest(self, manifest_uri: str, cache_dir: Path) -> LoadedRoadEnvironment:
        manifest = RoadEnvironmentManifest.from_json(self.store.read_bytes(manifest_uri))
        return self._load(manifest, manifest_uri, cache_dir)

    def _load(
        self,
        manifest: RoadEnvironmentManifest,
        manifest_uri: str,
        cache_dir: Path,
    ) -> LoadedRoadEnvironment:
        environment_dir = cache_dir / manifest.environment_id
        road_path = self._materialize(
            manifest.artifact("simulation_road_environment"), environment_dir
        )
        taxi_zone_path = self._materialize(
            manifest.artifact("taxi_zone"), environment_dir
        )
        environment = RoadEnvironment.from_parquet(road_path, taxi_zone_path)
        if environment.road_segment_snapshot_date != manifest.road_snapshot_date:
            raise ValueError("runtime road snapshot does not match its manifest")
        return LoadedRoadEnvironment(environment, manifest, manifest_uri)

    def _materialize(self, artifact: DataArtifact, environment_dir: Path) -> Path:
        if artifact.media_type != "application/vnd.apache.parquet":
            raise ValueError(f"unsupported runtime artifact media type: {artifact.media_type}")
        destination = environment_dir / f"{artifact.role}.parquet"
        if destination.is_file() and file_matches(destination, artifact):
            return destination

        temporary = destination.with_suffix(".parquet.part")
        temporary.unlink(missing_ok=True)
        self.store.download_file(artifact.uri, temporary)
        try:
            if not file_matches(temporary, artifact):
                raise ValueError(
                    f"runtime artifact failed size/checksum validation: {artifact.role}"
                )
            # 검증이 끝난 파일만 교체해 중단된 다운로드가 다음 실행에 사용되지 않게 한다
            temporary.replace(destination)
        finally:
            temporary.unlink(missing_ok=True)
        return destination


def parse_active_pointer(value: bytes) -> dict[str, str]:
    document = json.loads(value)
    if not isinstance(document, dict):
        raise TypeError("active environment pointer must be a JSON object")
    required = ("environment_id", "manifest_uri", "manifest_sha256")
    result = {key: document.get(key) for key in required}
    if any(not isinstance(item, str) or not item for item in result.values()):
        raise ValueError("active environment pointer has missing or invalid fields")
    checksum = result["manifest_sha256"]
    assert isinstance(checksum, str)
    try:
        int(checksum, 16)
    except ValueError as error:
        raise ValueError("active pointer manifest_sha256 must be hexadecimal") from error
    if len(checksum) != 64:
        raise ValueError("active pointer manifest_sha256 must contain 64 characters")
    return result  # type: ignore[return-value]


def verify_bytes(value: bytes, expected_sha256: str, label: str) -> None:
    if hashlib.sha256(value).hexdigest() != expected_sha256:
        raise ValueError(f"{label} failed checksum validation")


def file_matches(path: Path, artifact: DataArtifact) -> bool:
    if path.stat().st_size != artifact.size_bytes:
        return False
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest() == artifact.sha256
