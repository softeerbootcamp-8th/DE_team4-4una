"""Persist standard_segment_comfort_score snapshots to S3 Gold (#265).

PostgreSQL에 바로 쓰지 않고 먼저 S3 Gold에 `score_as_of`별 snapshot을 저장한 뒤
그 결과를 다시 읽어 PostgreSQL writer(standard_writer.py)에 넘긴다 — S3 Gold가
기준 데이터셋, PostgreSQL은 서빙 스토어다(context/data/lineage.md). 같은 as_of는
같은 디렉터리를 overwrite하므로(멱등) 로컬 파일시스템 전용인
hourly_segment_feature_storage.py의 staging/backup 교체는 필요 없다.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from de4_core import join_uri
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass(frozen=True, slots=True)
class StandardGoldWriteResult:
    output_uri: str
    row_count: int


def standard_snapshot_uri(output_root: str, as_of: datetime) -> str:
    """as_of 전용 snapshot 경로. 같은 as_of는 항상 같은 경로를 가리킨다.

    로컬 file:// 루트에서는 join_uri()가 '='를 %3D로 인코딩한다
    (universe.py._decode_local_file_uri와 같은 특성) — 쓰기/읽기 모두 이 함수를
    거치므로 자체 정합성엔 문제없고, 로컬 디스크의 디렉터리 이름만 그렇게 보인다.
    """
    return join_uri(
        output_root,
        f"score_as_of_date={as_of.date().isoformat()}",
        f"score_as_of={_as_of_path_segment(as_of)}",
    )


def _as_of_path_segment(as_of: datetime) -> str:
    # ':'는 S3 키/로컬 경로 양쪽에서 다루기 번거로우니 '-'로 치환한다.
    return as_of.strftime("%Y-%m-%dT%H-%M-%SZ")


def write_standard_comfort_score_snapshot(
    spark: SparkSession,
    df: DataFrame,
    output_root: str,
    as_of: datetime,
) -> StandardGoldWriteResult:
    """scored DataFrame을 as_of snapshot 경로에 overwrite하고 read-back으로 검증한다.

    실패하면 예외를 던진다 — 호출자는 이어서 PostgreSQL을 쓰면 안 된다.
    """
    _validate_as_of(as_of)
    _require_single_as_of(df, as_of)

    target_uri = standard_snapshot_uri(output_root, as_of)
    expected_count = df.count()

    df.write.mode("overwrite").parquet(target_uri)

    stored = spark.read.parquet(target_uri)
    _validate_snapshot_schema(stored, df)
    row_count = stored.count()
    if row_count != expected_count:
        raise ValueError(
            f"{target_uri}: read-back row count {row_count} does not match "
            f"written row count {expected_count}"
        )

    return StandardGoldWriteResult(output_uri=target_uri, row_count=row_count)


def _validate_as_of(as_of: datetime) -> None:
    if as_of.utcoffset() is None:
        raise ValueError("as_of must be timezone-aware")


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
