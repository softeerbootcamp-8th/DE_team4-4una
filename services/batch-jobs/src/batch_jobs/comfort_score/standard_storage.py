"""Persist standard_segment_comfort_score snapshots to S3 Gold (#265, #343).

PostgreSQL에 바로 쓰지 않고 먼저 S3 Gold에 `score_as_of`별 snapshot을 저장한 뒤
그 결과를 다시 읽어 PostgreSQL writer(standard_writer.py)에 넘긴다 — S3 Gold가
기준 데이터셋, PostgreSQL은 서빙 스토어다(context/data/lineage.md).

경로 규칙과 manifest 스키마는 `de4_core.gold_snapshot`이 갖는다 — orchestration이
검증을 위해 같은 snapshot을 읽으므로 서비스 간 계약이다(ADR-0012). 이 모듈은 그
계약을 *안전하게 쓰는 절차*만 갖는다.

기존 활성 version을 절대 overwrite하지 않는다 — 매 실행은 uuid로 새 version 경로에
쓰고 read-back으로 검증한 뒤, 그 검증이 끝난 다음에만 manifest를 새 version으로
전환한다(#343). 그래서 write/검증 중 실패하면 manifest와 기존 활성 snapshot이
그대로 남고, PostgreSQL도 건드리지 않는다. manifest는 universe.py의
active pointer -> manifest 패턴과 같은 `de4_core.ObjectStore`로 읽고 쓴다.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime

from botocore.exceptions import ClientError
from de4_core import ObjectStore
from de4_core.gold_snapshot import (
    StandardGoldManifest,
    standard_manifest_uri,
    standard_snapshot_uri,
    standard_version_uri,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass(frozen=True, slots=True)
class StandardGoldWriteResult:
    version_id: str
    version_uri: str
    row_count: int


def write_standard_comfort_score_snapshot(
    spark: SparkSession,
    df: DataFrame,
    output_root: str,
    as_of: datetime,
    store: ObjectStore | None = None,
) -> StandardGoldWriteResult:
    """새 version을 unique 경로에 쓰고 read-back으로 검증한 뒤에만 manifest를 전환한다.

    write든 검증이든 하나라도 실패하면 예외가 그대로 올라가고, 이 함수는 manifest를
    건드리지 않는다 — 기존 활성 snapshot이 그대로 서빙 기준으로 남는다.
    """
    # 경로를 먼저 만든다 — naive as_of는 standard_snapshot_uri가 거부하므로 Spark
    # 작업을 시작하기 전에 실패한다.
    snapshot_root_uri = standard_snapshot_uri(output_root, as_of)
    _require_single_as_of(df, as_of)

    version_id = uuid.uuid4().hex
    version_uri = standard_version_uri(snapshot_root_uri, version_id)
    expected_count = df.count()

    # version_id가 매번 새 경로를 만들므로 overwrite가 필요 없다 — 대신 "error" 모드로
    # 기존 경로를 실수로 덮어쓰는 상황 자체를 명시적으로 막는다.
    df.write.mode("error").parquet(version_uri)

    stored = spark.read.parquet(version_uri)
    _validate_snapshot_schema(stored, df)
    row_count = stored.count()
    if row_count != expected_count:
        raise ValueError(
            f"{version_uri}: read-back row count {row_count} does not match "
            f"written row count {expected_count}"
        )

    _write_manifest(
        store if store is not None else ObjectStore(),
        snapshot_root_uri,
        StandardGoldManifest(
            score_as_of=as_of,
            version_id=version_id,
            snapshot_uri=version_uri,
            row_count=row_count,
        ),
    )

    return StandardGoldWriteResult(
        version_id=version_id, version_uri=version_uri, row_count=row_count
    )


def resolve_active_standard_snapshot_uri(
    output_root: str,
    as_of: datetime,
    store: ObjectStore | None = None,
) -> str:
    """manifest를 읽어 현재 활성 snapshot(version) URI를 반환한다 — source-of-truth pointer."""
    snapshot_root_uri = standard_snapshot_uri(output_root, as_of)
    manifest = _read_manifest(store if store is not None else ObjectStore(), snapshot_root_uri, as_of)
    return manifest.snapshot_uri


def read_active_standard_comfort_score_snapshot(
    spark: SparkSession,
    output_root: str,
    as_of: datetime,
    store: ObjectStore | None = None,
) -> DataFrame:
    """manifest가 가리키는 활성 snapshot을 읽는다.

    PostgreSQL publish는 이 함수의 결과를 써야 한다 — write_standard_comfort_score_snapshot()이
    방금 돌려준 version_uri를 직접 읽으면 manifest가 실제 source-of-truth pointer 역할을
    못 하게 된다.
    """
    active_uri = resolve_active_standard_snapshot_uri(output_root, as_of, store)
    return spark.read.parquet(active_uri)


def _write_manifest(
    store: ObjectStore, snapshot_root_uri: str, manifest: StandardGoldManifest
) -> None:
    store.write_bytes(standard_manifest_uri(snapshot_root_uri), manifest.to_json())


def _read_manifest(
    store: ObjectStore, snapshot_root_uri: str, as_of: datetime
) -> StandardGoldManifest:
    manifest_uri = standard_manifest_uri(snapshot_root_uri)
    try:
        raw = store.read_bytes(manifest_uri)
    except FileNotFoundError as error:
        raise ValueError(
            f"{manifest_uri}: no manifest found — write a snapshot first"
        ) from error
    except ClientError as error:
        status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 404:
            raise ValueError(
                f"{manifest_uri}: no manifest found — write a snapshot first"
            ) from error
        raise

    manifest = StandardGoldManifest.from_json(raw, snapshot_root_uri=snapshot_root_uri)
    # as_of 대조는 계약이 아니라 호출자의 요구다 — 이 루트를 요청한 as_of로 만들었으니
    # manifest도 같은 as_of여야 한다.
    if manifest.score_as_of != as_of:
        raise ValueError(
            f"{manifest_uri}: manifest score_as_of {manifest.score_as_of!r} "
            f"does not match requested as_of {as_of!r}"
        )
    return manifest


def _require_single_as_of(df: DataFrame, as_of: datetime) -> None:
    mismatched = df.filter(F.col("score_as_of") != F.lit(as_of))
    if mismatched.limit(1).count():
        raise ValueError("df contains rows whose score_as_of does not match as_of")


def _validate_snapshot_schema(stored: DataFrame, written: DataFrame) -> None:
    stored_fields = {field.name: field.dataType for field in stored.schema.fields}
    written_fields = {field.name: field.dataType for field in written.schema.fields}
    if stored_fields != written_fields:
        raise ValueError(
            "read-back schema does not match the written DataFrame: "
            f"stored={stored_fields} written={written_fields}"
        )
