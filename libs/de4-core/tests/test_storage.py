"""Tests for de4_core.storage's list_objects()/delete_objects() (#271, ADR-0009)."""

from __future__ import annotations

import io
from datetime import UTC, datetime

from de4_core import ObjectStore, join_uri
from de4_core.storage import ObjectMetadata


def test_list_objects_returns_files_recursively_with_metadata(tmp_path) -> None:
    root = tmp_path.as_uri()
    store = ObjectStore()
    store.write_bytes(join_uri(root, "a.parquet"), b"aaa")
    store.write_bytes(join_uri(root, "weather_date=2026-08-20", "b.parquet"), b"bb")

    objects = store.list_objects(root)

    assert [obj.uri for obj in objects] == sorted(obj.uri for obj in objects)
    assert len(objects) == 2
    sizes = {obj.size for obj in objects}
    assert sizes == {3, 2}
    for obj in objects:
        assert isinstance(obj, ObjectMetadata)
        assert obj.last_modified.tzinfo is not None


def test_list_objects_returns_empty_list_for_missing_root(tmp_path) -> None:
    store = ObjectStore()

    assert store.list_objects((tmp_path / "does-not-exist").as_uri()) == []


def test_delete_objects_removes_local_files(tmp_path) -> None:
    root = tmp_path.as_uri()
    store = ObjectStore()
    uri_a = join_uri(root, "a.parquet")
    uri_b = join_uri(root, "b.parquet")
    store.write_bytes(uri_a, b"aaa")
    store.write_bytes(uri_b, b"bbb")

    store.delete_objects([uri_a])

    assert not store.exists(uri_a)
    assert store.exists(uri_b)


def test_delete_objects_is_a_no_op_for_an_empty_list(tmp_path) -> None:
    store = ObjectStore()

    store.delete_objects([])  # must not raise


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.fail_delete_keys: set[str] = set()

    def put_object(self, **kwargs: object) -> None:
        self.objects[(str(kwargs["Bucket"]), str(kwargs["Key"]))] = kwargs["Body"]  # type: ignore[assignment]

    def get_object(self, **kwargs: object) -> dict[str, object]:
        value = self.objects[(str(kwargs["Bucket"]), str(kwargs["Key"]))]
        return {"Body": io.BytesIO(value)}

    def head_object(self, **kwargs: object) -> dict[str, object]:
        key = (str(kwargs["Bucket"]), str(kwargs["Key"]))
        if key not in self.objects:
            raise KeyError(f"Object not found: {key}")
        return {}

    def list_objects_v2(self, **kwargs: object) -> dict[str, object]:
        bucket = str(kwargs["Bucket"])
        prefix = str(kwargs["Prefix"])
        contents = [
            {"Key": key, "LastModified": datetime(2026, 8, 22, tzinfo=UTC), "Size": len(body)}
            for (obj_bucket, key), body in self.objects.items()
            if obj_bucket == bucket and key.startswith(prefix)
        ]
        return {"Contents": contents, "IsTruncated": False}

    def delete_objects(self, **kwargs: object) -> dict[str, object]:
        bucket = str(kwargs["Bucket"])
        errors = []
        for entry in kwargs["Delete"]["Objects"]:  # type: ignore[index]
            key = entry["Key"]
            if key in self.fail_delete_keys:
                errors.append({"Key": key, "Code": "AccessDenied", "Message": "Access Denied"})
            else:
                self.objects.pop((bucket, key), None)
        return {"Errors": errors} if errors else {}


def test_list_objects_reads_from_s3_via_list_objects_v2() -> None:
    client = FakeS3Client()
    store = ObjectStore(client)  # type: ignore[arg-type]
    store.write_bytes("s3://test-bucket/bronze/sensor-events/part-1.parquet", b"aaa")
    store.write_bytes("s3://test-bucket/bronze/sensor-events/part-2.parquet", b"bb")
    store.write_bytes("s3://test-bucket/other/unrelated.parquet", b"z")

    objects = store.list_objects("s3://test-bucket/bronze/sensor-events")

    assert {obj.uri for obj in objects} == {
        "s3://test-bucket/bronze/sensor-events/part-1.parquet",
        "s3://test-bucket/bronze/sensor-events/part-2.parquet",
    }


def test_delete_objects_removes_from_s3_via_delete_objects() -> None:
    client = FakeS3Client()
    store = ObjectStore(client)  # type: ignore[arg-type]
    store.write_bytes("s3://test-bucket/a.parquet", b"aaa")
    store.write_bytes("s3://test-bucket/b.parquet", b"bbb")

    store.delete_objects(["s3://test-bucket/a.parquet"])

    assert not store.exists("s3://test-bucket/a.parquet")
    assert store.exists("s3://test-bucket/b.parquet")


def test_delete_objects_raises_on_partial_s3_failure() -> None:
    client = FakeS3Client()
    store = ObjectStore(client)  # type: ignore[arg-type]
    store.write_bytes("s3://test-bucket/a.parquet", b"aaa")
    store.write_bytes("s3://test-bucket/b.parquet", b"bbb")
    store.write_bytes("s3://test-bucket/c.parquet", b"ccc")
    # Simulate a deletion permission error on b.parquet
    client.fail_delete_keys.add("b.parquet")

    try:
        store.delete_objects(
            [
                "s3://test-bucket/a.parquet",
                "s3://test-bucket/b.parquet",
                "s3://test-bucket/c.parquet",
            ]
        )
        assert False, "Expected RuntimeError for partial deletion failure"
    except RuntimeError as e:
        assert "Failed to delete" in str(e)
        assert "b.parquet" in str(e)

    # Verify a.parquet was deleted despite the error
    assert not store.exists("s3://test-bucket/a.parquet")
    # b.parquet should still exist (deletion failed)
    assert store.exists("s3://test-bucket/b.parquet")
    # c.parquet should be deleted
    assert not store.exists("s3://test-bucket/c.parquet")
