import io
from datetime import UTC, date, datetime

from de4_core import (
    DataArtifact,
    ObjectStore,
    RoadEnvironmentManifest,
    SourceSnapshot,
    join_uri,
)


def manifest() -> RoadEnvironmentManifest:
    checksum = "a" * 64
    return RoadEnvironmentManifest(
        schema_version="1",
        environment_id="nyc-20260801-build-1",
        reference_date=date(2026, 8, 1),
        road_snapshot_date=date(2026, 8, 1),
        build_id="build-1",
        created_at=datetime(2026, 8, 2, tzinfo=UTC),
        mapping_version="mapping-v1",
        status="READY",
        artifacts=(
            DataArtifact(
                "simulation_road_environment",
                "s3://bucket/environment.parquet",
                "application/vnd.apache.parquet",
                checksum,
                10,
                1,
            ),
            DataArtifact(
                "taxi_zone",
                "s3://bucket/taxi.parquet",
                "application/vnd.apache.parquet",
                checksum,
                10,
                1,
            ),
        ),
        sources=(
            SourceSnapshot(
                "nyc_lion",
                "snapshot-1",
                "https://example.test/lion",
                "s3://bucket/lion.geojson",
                datetime(2026, 8, 2, tzinfo=UTC),
                "2026-08-01",
                "geojson",
                checksum,
                checksum,
                10,
                1,
                "build-1",
            ),
        ),
        quality={"status": "PASSED"},
    )


def test_environment_manifest_round_trips_deterministically() -> None:
    source = manifest()

    restored = RoadEnvironmentManifest.from_json(source.to_json())

    assert restored == source
    assert restored.artifact("taxi_zone").row_count == 1


def test_file_object_store_uses_same_uri_contract_as_s3(tmp_path) -> None:
    root = tmp_path.as_uri()
    uri = join_uri(root, "prepared/environment/manifest.json")
    store = ObjectStore()

    store.write_bytes(uri, manifest().to_json())

    assert RoadEnvironmentManifest.from_json(store.read_bytes(uri)) == manifest()
    assert join_uri("s3://bucket", "prepared/manifest.json") == (
        "s3://bucket/prepared/manifest.json"
    )


def test_s3_object_store_accepts_an_injected_client() -> None:
    class FakeS3:
        def __init__(self) -> None:
            self.objects: dict[tuple[str, str], bytes] = {}

        def put_object(self, **kwargs: object) -> None:
            self.objects[(str(kwargs["Bucket"]), str(kwargs["Key"]))] = kwargs["Body"]  # type: ignore[assignment]

        def get_object(self, **kwargs: object) -> dict[str, object]:
            value = self.objects[(str(kwargs["Bucket"]), str(kwargs["Key"]))]
            return {"Body": io.BytesIO(value)}

    client = FakeS3()
    store = ObjectStore(client)  # type: ignore[arg-type]

    store.write_bytes("s3://test-bucket/manifests/environment.json", b"value")

    assert store.read_bytes("s3://test-bucket/manifests/environment.json") == b"value"
