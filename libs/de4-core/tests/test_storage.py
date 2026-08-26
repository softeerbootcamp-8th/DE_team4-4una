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
        self.requested_ranges: list[str] = []

    def put_object(self, **kwargs: object) -> None:
        self.objects[(str(kwargs["Bucket"]), str(kwargs["Key"]))] = kwargs["Body"]  # type: ignore[assignment]

    def get_object(self, **kwargs: object) -> dict[str, object]:
        value = self.objects[(str(kwargs["Bucket"]), str(kwargs["Key"]))]
        # 실제 S3처럼 Range 헤더를 존중해, 요청한 구간만 돌려준다. 어떤 구간을
        # 요청했는지 기록해 두면 테스트가 "전량을 받지 않았다"를 직접 검증할 수 있다.
        byte_range = kwargs.get("Range")
        if byte_range is not None:
            self.requested_ranges.append(str(byte_range))
            start, _, end = str(byte_range).removeprefix("bytes=").partition("-")
            value = value[int(start) : int(end) + 1]
        return {"Body": io.BytesIO(value)}

    def head_object(self, **kwargs: object) -> dict[str, object]:
        key = (str(kwargs["Bucket"]), str(kwargs["Key"]))
        if key not in self.objects:
            raise KeyError(f"Object not found: {key}")
        return {"ContentLength": len(self.objects[key])}

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
        raise AssertionError("Expected RuntimeError for partial deletion failure")
    except RuntimeError as e:
        assert "Failed to delete" in str(e)
        assert "b.parquet" in str(e)

    # Verify a.parquet was deleted despite the error
    assert not store.exists("s3://test-bucket/a.parquet")
    # b.parquet should still exist (deletion failed)
    assert store.exists("s3://test-bucket/b.parquet")
    # c.parquet should be deleted
    assert not store.exists("s3://test-bucket/c.parquet")


def test_open_reader_reads_a_local_file_seekably(tmp_path) -> None:
    store = ObjectStore()
    uri = join_uri(tmp_path.as_uri(), "a.parquet")
    store.write_bytes(uri, b"0123456789")

    with store.open_reader(uri) as reader:
        assert reader.seek(0, io.SEEK_END) == 10
        reader.seek(-3, io.SEEK_END)
        assert reader.read() == b"789"


def test_open_reader_requests_only_the_asked_byte_range_from_s3() -> None:
    client = FakeS3Client()
    store = ObjectStore(client)  # type: ignore[arg-type]
    store.write_bytes("s3://test-bucket/a.parquet", b"0123456789")

    with store.open_reader("s3://test-bucket/a.parquet") as reader:
        reader.seek(-3, io.SEEK_END)
        tail = reader.read(3)

    # 꼬리 3바이트만 필요했으므로 S3에도 그 구간만 요청해야 한다 — 이것이 이
    # 리더의 존재 이유다(전량 다운로드 회피).
    assert tail == b"789"
    assert client.requested_ranges == ["bytes=7-9"]


def test_open_reader_on_s3_reports_size_and_position() -> None:
    client = FakeS3Client()
    store = ObjectStore(client)  # type: ignore[arg-type]
    store.write_bytes("s3://test-bucket/a.parquet", b"0123456789")

    with store.open_reader("s3://test-bucket/a.parquet") as reader:
        assert reader.seek(0, io.SEEK_END) == 10
        assert reader.tell() == 10
        # 파일 끝을 넘어선 위치에서 읽으면 빈 바이트열이고 S3 요청도 없어야 한다.
        assert reader.read(5) == b""
        reader.seek(4)
        assert reader.read(2) == b"45"
        assert reader.tell() == 6

    assert client.requested_ranges == ["bytes=4-5"]
