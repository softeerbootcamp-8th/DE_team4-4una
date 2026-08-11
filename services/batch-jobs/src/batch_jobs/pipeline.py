"""Publish a complete monthly road environment into a file/S3 data lake."""

from __future__ import annotations

import hashlib
import json
import re
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

from de4_core import (
    DataArtifact,
    ObjectStore,
    RoadEnvironmentManifest,
    SourceSnapshot,
    join_uri,
)

from batch_jobs.environment import PreparedEnvironment, prepare_environment
from batch_jobs.sources import file_sha256, geojson_count
from batch_jobs.tables import write_environment_tables

SOURCE_FILES = {
    "nyc_lion": "lion.geojson",
    "nyc_street_pavement_ratings": "pavement.geojson",
    "nyc_speed_humps": "speed_humps.geojson",
    "tlc_taxi_zones": "taxi_zones.zip",
}

ARTIFACT_KEYS = {
    "road_segment": (
        "normalized/road_segment/snapshot_date={road_snapshot_date}/"
        "build_id={build_id}/part-00000.parquet"
    ),
    "enriched_segment_reference": (
        "prepared/enriched_segment_reference/reference_date={reference_date}/"
        "build_id={build_id}/part-00000.parquet"
    ),
    "simulation_road_environment": (
        "prepared/simulation_environment/reference_date={reference_date}/"
        "build_id={build_id}/road_environment.parquet"
    ),
    "taxi_zone": (
        "prepared/simulation_environment/reference_date={reference_date}/"
        "build_id={build_id}/taxi_zones.parquet"
    ),
}


@dataclass(frozen=True, slots=True)
class BuildResult:
    manifest_uri: str
    active_pointer_uri: str | None
    manifest: RoadEnvironmentManifest


def build_and_publish_environment(
    source_dir: Path,
    data_lake_uri: str,
    reference_date: date,
    road_snapshot_date: date,
    build_id: str,
    *,
    activate: bool = False,
    object_store: ObjectStore | None = None,
    minimum_pavement_segment_match_rate: float = 0.0,
    minimum_hump_source_match_rate: float = 0.0,
) -> BuildResult:
    store = object_store or ObjectStore()
    validate_build_id(build_id)
    manifest_key = (
        "prepared/simulation_environment/"
        f"reference_date={reference_date.isoformat()}/build_id={build_id}/manifest.json"
    )
    manifest_uri = join_uri(data_lake_uri, manifest_key)
    if store.exists(manifest_uri):
        raise FileExistsError(
            f"immutable road-environment build already exists: {manifest_uri}"
        )
    source_paths = {
        source_id: source_dir / filename
        for source_id, filename in SOURCE_FILES.items()
    }
    missing = [str(path) for path in source_paths.values() if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"missing reference source files: {', '.join(missing)}")

    prepared = prepare_environment(
        source_paths["nyc_lion"],
        source_paths["nyc_street_pavement_ratings"],
        source_paths["nyc_speed_humps"],
        source_paths["tlc_taxi_zones"],
        reference_date,
    )
    enforce_quality_thresholds(
        prepared,
        minimum_pavement_segment_match_rate,
        minimum_hump_source_match_rate,
    )
    created_at = datetime.now(UTC)
    environment_id = f"nyc-{reference_date:%Y%m%d}-{build_id}"

    with tempfile.TemporaryDirectory(prefix="de4-road-environment-") as temporary:
        table_files = write_environment_tables(
            prepared,
            Path(temporary),
            reference_date,
            road_snapshot_date,
            created_at,
        )
        sources = publish_sources(
            source_dir,
            source_paths,
            data_lake_uri,
            road_snapshot_date,
            build_id,
            prepared,
            store,
            created_at,
        )
        artifacts = publish_tables(
            table_files,
            data_lake_uri,
            reference_date,
            road_snapshot_date,
            build_id,
            store,
        )

    manifest = RoadEnvironmentManifest(
        schema_version="1",
        environment_id=environment_id,
        reference_date=reference_date,
        road_snapshot_date=road_snapshot_date,
        build_id=build_id,
        created_at=created_at,
        mapping_version="street-nearest-v1",
        status="READY",
        artifacts=tuple(artifacts),
        sources=tuple(sources),
        quality=prepared.quality.to_dict(),
    )
    store.write_bytes(manifest_uri, manifest.to_json())

    active_pointer_uri = None
    if activate:
        active_pointer_uri = join_uri(
            data_lake_uri, "prepared/simulation_environment/active.json"
        )
        pointer = {
            "environment_id": environment_id,
            "manifest_uri": manifest_uri,
            "manifest_sha256": hashlib.sha256(manifest.to_json()).hexdigest(),
            "updated_at": datetime.now(UTC).isoformat(),
        }
        store.write_bytes(
            active_pointer_uri,
            json.dumps(pointer, indent=2, sort_keys=True).encode(),
        )
    return BuildResult(manifest_uri, active_pointer_uri, manifest)


def publish_sources(
    source_dir: Path,
    source_paths: dict[str, Path],
    data_lake_uri: str,
    snapshot_date: date,
    build_id: str,
    prepared: PreparedEnvironment,
    store: ObjectStore,
    created_at: datetime,
) -> list[SourceSnapshot]:
    source_metadata = load_source_metadata(source_dir / "source_manifest.json")
    row_counts = {
        "nyc_lion": geojson_count(source_paths["nyc_lion"]),
        "nyc_street_pavement_ratings": geojson_count(
            source_paths["nyc_street_pavement_ratings"]
        ),
        "nyc_speed_humps": geojson_count(source_paths["nyc_speed_humps"]),
        "tlc_taxi_zones": len(prepared.taxi_zones),
    }
    snapshots: list[SourceSnapshot] = []
    for source_id, source_path in source_paths.items():
        key = (
            f"source/{source_id}/snapshot_date={snapshot_date.isoformat()}/"
            f"build_id={build_id}/{source_path.name}"
        )
        object_uri = join_uri(data_lake_uri, key)
        store.upload_file(source_path, object_uri)
        metadata = source_metadata.get(source_id, {})
        retrieved_value = metadata.get("retrieved_at")
        retrieved_at = (
            datetime.fromisoformat(str(retrieved_value))
            if retrieved_value
            else created_at
        )
        snapshots.append(
            SourceSnapshot(
                source_id=source_id,
                snapshot_id=f"{snapshot_date.isoformat()}-{build_id}",
                source_uri=str(
                    metadata.get("source_uri") or f"local-source:{source_path.name}"
                ),
                object_uri=object_uri,
                retrieved_at=retrieved_at,
                source_period_or_version=str(
                    metadata.get("source_period_or_version")
                    or snapshot_date.isoformat()
                ),
                file_format="shapefile_zip" if source_path.suffix == ".zip" else "geojson",
                sha256=file_sha256(source_path),
                schema_fingerprint=source_schema_fingerprint(source_path),
                size_bytes=source_path.stat().st_size,
                row_count=row_counts[source_id],
                ingestion_run_id=build_id,
            )
        )
    return snapshots


def publish_tables(
    table_files: dict[str, tuple[Path, int]],
    data_lake_uri: str,
    reference_date: date,
    road_snapshot_date: date,
    build_id: str,
    store: ObjectStore,
) -> list[DataArtifact]:
    artifacts: list[DataArtifact] = []
    values = {
        "reference_date": reference_date.isoformat(),
        "road_snapshot_date": road_snapshot_date.isoformat(),
        "build_id": build_id,
    }
    for role, (local_path, row_count) in table_files.items():
        object_uri = join_uri(data_lake_uri, ARTIFACT_KEYS[role].format(**values))
        store.upload_file(local_path, object_uri)
        artifacts.append(
            DataArtifact(
                role=role,
                uri=object_uri,
                media_type="application/vnd.apache.parquet",
                sha256=file_sha256(local_path),
                size_bytes=local_path.stat().st_size,
                row_count=row_count,
            )
        )
    return artifacts


def load_source_metadata(path: Path) -> dict[str, dict[str, object]]:
    if not path.is_file():
        return {}
    document = json.loads(path.read_text())
    retrieved_at = document.get("retrieved_at")
    result: dict[str, dict[str, object]] = {}
    for item in document.get("sources", []):
        if not isinstance(item, dict) or not item.get("source_id"):
            continue
        result[str(item["source_id"])] = {
            **item,
            "retrieved_at": item.get("retrieved_at") or retrieved_at,
        }
    return result


def enforce_quality_thresholds(
    prepared: PreparedEnvironment,
    minimum_pavement_segment_match_rate: float,
    minimum_hump_source_match_rate: float,
) -> None:
    if not 0 <= minimum_pavement_segment_match_rate <= 1:
        raise ValueError("pavement match threshold must be in [0, 1]")
    if not 0 <= minimum_hump_source_match_rate <= 1:
        raise ValueError("hump match threshold must be in [0, 1]")
    quality = prepared.quality
    pavement_rate = quality.pavement_matched_segment_count / quality.lion_segment_count
    hump_rate = quality.hump_mapped_source_count / max(1, quality.hump_source_count)
    if pavement_rate < minimum_pavement_segment_match_rate:
        raise ValueError(
            f"pavement segment match rate {pavement_rate:.3f} is below "
            f"{minimum_pavement_segment_match_rate:.3f}"
        )
    if hump_rate < minimum_hump_source_match_rate:
        raise ValueError(
            f"hump source match rate {hump_rate:.3f} is below "
            f"{minimum_hump_source_match_rate:.3f}"
        )


def validate_build_id(build_id: str) -> None:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", build_id):
        raise ValueError("build_id must be a non-empty path-safe value")


def source_schema_fingerprint(path: Path) -> str:
    if path.suffix != ".geojson":
        return hashlib.sha256(b"shapefile:LocationID,geometry").hexdigest()
    document = json.loads(path.read_text())
    property_names: set[str] = set()
    geometry_types: set[str] = set()
    for feature in document.get("features", []):
        if not isinstance(feature, dict):
            continue
        properties = feature.get("properties")
        geometry = feature.get("geometry")
        if isinstance(properties, dict):
            property_names.update(str(name) for name in properties)
        if isinstance(geometry, dict) and geometry.get("type"):
            geometry_types.add(str(geometry["type"]))
    schema = {
        "geometry_types": sorted(geometry_types),
        "property_names": sorted(property_names),
    }
    return hashlib.sha256(
        json.dumps(schema, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
