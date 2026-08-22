"""Small file/S3 object-store boundary shared by batch and runtime services."""

from __future__ import annotations

import shutil
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Protocol
from urllib.parse import unquote, urlparse

from botocore.exceptions import ClientError


class S3Client(Protocol):
    def get_object(self, **kwargs: object) -> dict[str, object]: ...

    def put_object(self, **kwargs: object) -> object: ...

    def upload_file(self, filename: str, bucket: str, key: str) -> object: ...

    def download_file(self, bucket: str, key: str, filename: str) -> object: ...

    def head_object(self, **kwargs: object) -> object: ...

    def list_objects_v2(self, **kwargs: object) -> dict[str, object]: ...

    def delete_objects(self, **kwargs: object) -> object: ...


@dataclass(frozen=True, slots=True)
class ObjectMetadata:
    """One object under a `list_objects()` root — enough to filter without a re-read."""

    uri: str
    last_modified: datetime
    size: int


class ObjectStore:
    """Read and write complete objects through portable file and S3 URIs."""

    def __init__(self, s3_client: S3Client | None = None):
        self._s3_client = s3_client

    def read_bytes(self, uri: str) -> bytes:
        scheme, bucket, path = parse_uri(uri)
        if scheme == "file":
            return Path(path).read_bytes()
        require_s3_key(path)
        response = self._s3().get_object(Bucket=bucket, Key=path)
        body = response["Body"]
        return body.read()  # type: ignore[no-any-return, union-attr]

    def write_bytes(self, uri: str, value: bytes) -> None:
        scheme, bucket, path = parse_uri(uri)
        if scheme == "file":
            destination = Path(path)
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(value)
            return
        require_s3_key(path)
        self._s3().put_object(Bucket=bucket, Key=path, Body=value)

    def upload_file(self, source: Path, uri: str) -> None:
        scheme, bucket, path = parse_uri(uri)
        if scheme == "file":
            destination = Path(path)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
            return
        require_s3_key(path)
        self._s3().upload_file(str(source), bucket, path)

    def download_file(self, uri: str, destination: Path) -> None:
        scheme, bucket, path = parse_uri(uri)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if scheme == "file":
            shutil.copyfile(Path(path), destination)
            return
        require_s3_key(path)
        self._s3().download_file(bucket, path, str(destination))

    def exists(self, uri: str) -> bool:
        scheme, bucket, path = parse_uri(uri)
        if scheme == "file":
            return Path(path).is_file()
        require_s3_key(path)
        try:
            self._s3().head_object(Bucket=bucket, Key=path)
        except KeyError:
            return False
        except ClientError as error:
            status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == 404:
                return False
            raise
        return True

    def list_objects(self, uri: str) -> list[ObjectMetadata]:
        scheme, bucket, path = parse_uri(uri)
        if scheme == "file":
            root = Path(path)
            if not root.exists():
                return []
            objects = [
                ObjectMetadata(
                    uri=child.as_uri(),
                    last_modified=datetime.fromtimestamp(child.stat().st_mtime, tz=UTC),
                    size=child.stat().st_size,
                )
                for child in root.rglob("*")
                if child.is_file()
            ]
            return sorted(objects, key=lambda obj: obj.uri)
        require_s3_key(path)
        prefix = path if path.endswith("/") else f"{path}/"
        objects = []
        continuation_token: str | None = None
        while True:
            kwargs: dict[str, object] = {"Bucket": bucket, "Prefix": prefix}
            if continuation_token is not None:
                kwargs["ContinuationToken"] = continuation_token
            response = self._s3().list_objects_v2(**kwargs)
            for item in response.get("Contents", []):  # type: ignore[union-attr]
                objects.append(
                    ObjectMetadata(
                        uri=f"s3://{bucket}/{item['Key']}",  # type: ignore[index]
                        last_modified=item["LastModified"],  # type: ignore[index]
                        size=item["Size"],  # type: ignore[index]
                    )
                )
            if not response.get("IsTruncated"):  # type: ignore[union-attr]
                break
            continuation_token = response.get("NextContinuationToken")  # type: ignore[assignment]
        return sorted(objects, key=lambda obj: obj.uri)

    def delete_objects(self, uris: Sequence[str]) -> None:
        """Delete every URI. Assumes all URIs share one scheme (mixed file/s3 batches
        are not supported — callers always operate within one root/group)."""
        if not uris:
            return
        scheme, _, _ = parse_uri(uris[0])
        if scheme == "file":
            for uri in uris:
                _, _, path = parse_uri(uri)
                Path(path).unlink(missing_ok=True)
            return
        by_bucket: dict[str, list[str]] = {}
        for uri in uris:
            _, bucket, path = parse_uri(uri)
            require_s3_key(path)
            by_bucket.setdefault(bucket, []).append(path)
        all_errors: list[tuple[str, str]] = []
        for bucket, keys in by_bucket.items():
            for start in range(0, len(keys), 1000):  # S3 delete_objects의 배치 상한
                chunk = keys[start : start + 1000]
                response = self._s3().delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": [{"Key": key} for key in chunk]},
                )
                errors = response.get("Errors", [])  # type: ignore[union-attr]
                for error in errors:  # type: ignore[union-attr]
                    all_errors.append((bucket, error.get("Key")))  # type: ignore[index]
        if all_errors:
            failed = [f"s3://{bucket}/{key}" for bucket, key in all_errors]
            raise RuntimeError(
                f"Failed to delete {len(all_errors)} object(s): {failed}"
            )

    def _s3(self) -> S3Client:
        if self._s3_client is None:
            try:
                import boto3
            except ImportError as error:  # pragma: no cover - packaging guard
                raise RuntimeError("boto3 is required for s3:// artifact URIs") from error
            self._s3_client = boto3.client("s3")
        return self._s3_client


def join_uri(root_uri: str, *parts: str) -> str:
    scheme, bucket, root_path = parse_uri(root_uri)
    safe_parts = [safe_object_part(part) for part in parts]
    if scheme == "file":
        return (Path(root_path).joinpath(*safe_parts)).resolve().as_uri()
    key = str(PurePosixPath(root_path, *safe_parts))
    return f"s3://{bucket}/{key.lstrip('/')}"


def parse_uri(uri: str) -> tuple[str, str, str]:
    parsed = urlparse(uri)
    if parsed.scheme in {"", "file"}:
        path = unquote(parsed.path) if parsed.scheme else uri
        if not path:
            raise ValueError("file URI must contain a path")
        return "file", "", str(Path(path).expanduser().resolve())
    if parsed.scheme == "s3":
        if not parsed.netloc:
            raise ValueError("S3 URI must contain a bucket")
        return "s3", parsed.netloc, parsed.path.lstrip("/")
    raise ValueError(f"unsupported artifact URI scheme: {parsed.scheme}")


def safe_object_part(part: str) -> str:
    path = PurePosixPath(part)
    if path.is_absolute() or ".." in path.parts or not part:
        raise ValueError(f"unsafe data-lake object path: {part}")
    return str(path)


def require_s3_key(key: str) -> None:
    if not key:
        raise ValueError("S3 object operation requires an object key")
