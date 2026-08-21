import hashlib
import io
import json
import logging
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from de4_core import DataArtifact, ObjectStore, RoadEnvironmentManifest, SourceSnapshot
from sensor_producer.runtime_environment import RoadEnvironmentLoader


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def get_object(self, **kwargs: object) -> dict[str, object]:
        value = self.objects[(str(kwargs["Bucket"]), str(kwargs["Key"]))]
        return {"Body": io.BytesIO(value)}

    def put_object(self, **kwargs: object) -> None:
        self.objects[(str(kwargs["Bucket"]), str(kwargs["Key"]))] = bytes(kwargs["Body"])

    def download_file(self, bucket: str, key: str, filename: str) -> None:
        destination = Path(filename)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(self.objects[(bucket, key)])


def write_runtime_tables(directory: Path) -> tuple[Path, Path]:
    directory.mkdir(parents=True, exist_ok=True)
    road_path = directory / "road.parquet"
    zone_path = directory / "zones.parquet"
    pq.write_table(
        pa.Table.from_pylist([{
            "segment_id": "1001",
            "reference_date": date(2026, 8, 1),
            "road_snapshot_date": date(2026, 7, 1),
            "from_node_id": 10,
            "to_node_id": 11,
            "traffic_direction": "W",
            "street_name": "Main St",
            "geometry_wkt": "LINESTRING (-73.99 40.73, -73.98 40.74)",
            "length_m": 120.0,
            "posted_speed_mph": 25,
            "curve_radius_m": None,
            "pavement_rating": 7.5,
            "hump_fractions_json": "[0.5]",
        }]),
        road_path,
    )
    pq.write_table(
        pa.Table.from_pylist([{
            "location_id": 181,
            "geometry_wkt": (
                "POLYGON ((-74 40.7, -73.9 40.7, -73.9 40.8, -74 40.8, -74 40.7))"
            ),
        }]),
        zone_path,
    )
    return road_path, zone_path


def publish_environment(
    store: ObjectStore,
    root_uri: str,
    source_dir: Path,
    *,
    bad_artifact_hash: bool = False,
) -> str:
    road_path, zone_path = write_runtime_tables(source_dir)
    artifacts: list[DataArtifact] = []
    for role, path in (
        ("simulation_road_environment", road_path),
        ("taxi_zone", zone_path),
    ):
        uri = f"{root_uri}/{role}.parquet"
        store.write_bytes(uri, path.read_bytes())
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        artifacts.append(
            DataArtifact(
                role,
                uri,
                "application/vnd.apache.parquet",
                "0" * 64 if bad_artifact_hash and role == "taxi_zone" else digest,
                path.stat().st_size,
                1,
            )
        )
    manifest = RoadEnvironmentManifest(
        "1",
        "env-20260801-test",
        date(2026, 8, 1),
        date(2026, 7, 1),
        "test-build",
        datetime(2026, 8, 1, tzinfo=UTC),
        "test-v1",
        "READY",
        tuple(artifacts),
        (
            SourceSnapshot(
                "nyc_lion",
                "snapshot-1",
                "https://example.test/lion",
                f"{root_uri}/source/lion.zip",
                datetime(2026, 8, 1, tzinfo=UTC),
                "2026-07",
                "zip",
                "1" * 64,
                "schema-v1",
                1,
                1,
                "test-build",
            ),
        ),
        {"segment_count": 1},
    )
    manifest_uri = f"{root_uri}/manifest.json"
    manifest_bytes = manifest.to_json()
    store.write_bytes(manifest_uri, manifest_bytes)
    pointer_uri = f"{root_uri}/active.json"
    store.write_bytes(
        pointer_uri,
        json.dumps(
            {
                "environment_id": manifest.environment_id,
                "manifest_uri": manifest_uri,
                "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
            }
        ).encode(),
    )
    return pointer_uri


@pytest.mark.parametrize("scheme", ["file", "s3"])
def test_loader_resolves_verified_environment(tmp_path: Path, scheme: str) -> None:
    store = ObjectStore(FakeS3Client())
    root_uri = (tmp_path / "lake").as_uri() if scheme == "file" else "s3://bucket/lake"
    pointer_uri = publish_environment(store, root_uri, tmp_path / "source")

    loaded = RoadEnvironmentLoader(store).from_pointer(pointer_uri, tmp_path / "cache")

    assert loaded.manifest.environment_id == "env-20260801-test"
    assert loaded.environment.road_segment_snapshot_date == date(2026, 7, 1)
    assert loaded.environment.segments[0].pavement_rating == 7.5
    assert loaded.environment.segments[0].hump_fractions == [0.5]
    assert set(loaded.environment.taxi_zones) == {181}


def test_loader_logs_cache_miss_then_hit(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    store = ObjectStore()
    pointer_uri = publish_environment(
        store, (tmp_path / "lake").as_uri(), tmp_path / "source"
    )
    loader = RoadEnvironmentLoader(store)
    caplog.set_level(logging.INFO, logger="sensor_producer.runtime_environment")

    loader.from_pointer(pointer_uri, tmp_path / "cache")
    loader.from_pointer(pointer_uri, tmp_path / "cache")

    messages = [record.getMessage() for record in caplog.records]
    assert any("cache miss" in message for message in messages)
    assert any("cache hit" in message for message in messages)


def test_loader_rejects_artifact_checksum_mismatch(tmp_path: Path) -> None:
    store = ObjectStore()
    pointer_uri = publish_environment(
        store,
        (tmp_path / "lake").as_uri(),
        tmp_path / "source",
        bad_artifact_hash=True,
    )

    with pytest.raises(ValueError, match="size/checksum"):
        RoadEnvironmentLoader(store).from_pointer(pointer_uri, tmp_path / "cache")


def test_loader_rejects_manifest_checksum_mismatch(tmp_path: Path) -> None:
    store = ObjectStore()
    pointer_uri = publish_environment(
        store, (tmp_path / "lake").as_uri(), tmp_path / "source"
    )
    pointer = json.loads(store.read_bytes(pointer_uri))
    pointer["manifest_sha256"] = "0" * 64
    store.write_bytes(pointer_uri, json.dumps(pointer).encode())

    with pytest.raises(ValueError, match="manifest failed checksum"):
        RoadEnvironmentLoader(store).from_pointer(pointer_uri, tmp_path / "cache")
