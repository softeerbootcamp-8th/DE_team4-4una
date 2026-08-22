"""Stage, validate, and replace one hourly cleansing quarantine partition."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from batch_jobs.schemas import SENSOR_EVENT_QUARANTINE_SCHEMA

_SAFE_RUN_ID = re.compile(r"^[A-Za-z0-9_.:+-]+$")
_STAGING_DIRNAME = "_staging"


@dataclass(frozen=True, slots=True)
class QuarantineWriteResult:
    output_path: str
    row_count: int


def quarantine_hour_path(output_root: str, target_hour: datetime) -> str:
    return (
        f"{output_root.rstrip('/')}/target_date={target_hour.date().isoformat()}"
        f"/target_hour={target_hour.hour:02d}"
    )


def write_hourly_quarantine(
    spark: SparkSession,
    quarantined: DataFrame,
    quarantine_output_root: str,
    target_hour: datetime,
    run_id: str,
) -> QuarantineWriteResult:
    """Replace only the requested UTC-hour quarantine after validation."""
    _require_safe_run_id(run_id)
    _require_utc_hour(target_hour)
    _require_schema(quarantined, SENSOR_EVENT_QUARANTINE_SCHEMA)

    staging_root = f"{quarantine_output_root.rstrip('/')}/{_STAGING_DIRNAME}/{run_id}"
    row_count = quarantined.count()

    try:
        staged_path = _stage_quarantine(
            spark,
            quarantined,
            staging_root,
            target_hour,
            row_count,
        )
        final_path = quarantine_hour_path(quarantine_output_root, target_hour)
        _replace_partition(spark, final_path, staged_path)
    finally:
        _delete_path(spark, staging_root)

    return QuarantineWriteResult(output_path=final_path, row_count=row_count)


def _stage_quarantine(
    spark: SparkSession,
    quarantined: DataFrame,
    staging_root: str,
    target_hour: datetime,
    expected_count: int,
) -> str | None:
    partitioned = quarantined.withColumn(
        "target_date", F.lit(target_hour.date())
    ).withColumn("target_hour", F.lit(target_hour.hour))
    (
        partitioned.write.mode("overwrite")
        .partitionBy("target_date", "target_hour")
        .parquet(staging_root)
    )
    if expected_count == 0:
        return None

    staged_path = (
        f"{staging_root}/target_date={target_hour.date().isoformat()}"
        f"/target_hour={target_hour.hour}"
    )
    staged = spark.read.schema(SENSOR_EVENT_QUARANTINE_SCHEMA).parquet(staged_path)
    _require_schema(staged, SENSOR_EVENT_QUARANTINE_SCHEMA)
    if staged.count() != expected_count:
        raise ValueError("staged quarantine row count does not match computed result")
    return staged_path


def _require_safe_run_id(run_id: str) -> None:
    if not _SAFE_RUN_ID.fullmatch(run_id):
        raise ValueError(f"run_id contains unsafe path characters: {run_id!r}")


def _require_utc_hour(target_hour: datetime) -> None:
    if target_hour.utcoffset() != timedelta(0):
        raise ValueError("target_hour must be UTC timezone-aware")
    if (target_hour.minute, target_hour.second, target_hour.microsecond) != (0, 0, 0):
        raise ValueError("target_hour must be truncated to the hour")


def _require_schema(df: DataFrame, expected: StructType) -> None:
    actual_fields = {field.name: field.dataType for field in df.schema.fields}
    expected_fields = {field.name: field.dataType for field in expected.fields}
    if actual_fields != expected_fields:
        raise ValueError(
            f"schema mismatch: expected {expected.simpleString()}, "
            f"got {df.schema.simpleString()}"
        )


def _replace_partition(
    spark: SparkSession,
    final_path: str,
    staged_path: str | None,
) -> None:
    """백업 후 rename으로 스왑하고, 실패 시 백업에서 되돌린다.

    **S3(EMRFS) 주의**: 로컬/HDFS의 rename은 메타데이터만 바꾸는 원자적
    연산이지만, EMRFS의 `FileSystem.rename()`은 내부적으로 디렉터리의 각
    객체를 copy 후 delete하는 방식이라 원자적이지 않다. 이 copy+delete
    도중 프로세스가 죽으면 `final_path` 아래에 일부 파일만 옮겨진 채로
    남을 수 있는데, `_path_exists()`는 디렉터리에 파일이 하나라도 있으면
    "존재함"으로 보므로 다음 실행의 `_recover_backup()`이 이 부분 상태를
    "정상적으로 완료된 상태"로 오판해 백업을 지워버릴 수 있다(row count는
    호출부에서 별도로 검증하지만, 그 검증은 staging 단계에서 끝나고 이
    스왑 단계에는 적용되지 않는다). 이 위험은 현재 감수하기로 했고(#290),
    필요해지면 별도 이슈에서 스왑 전략 자체를 재설계한다.
    """
    backup_path = _backup_path(final_path)
    had_existing = False
    promoted = False
    try:
        _recover_backup(spark, final_path, backup_path)
        if _path_exists(spark, final_path):
            _rename_path(spark, final_path, backup_path)
            had_existing = True

        if staged_path is not None:
            _make_parent_directory(spark, final_path)
            _rename_path(spark, staged_path, final_path)
            promoted = True
    except Exception:
        if promoted:
            _delete_path(spark, final_path)
        if had_existing:
            _rename_path(spark, backup_path, final_path)
        raise
    else:
        if had_existing:
            _delete_path(spark, backup_path)


def _backup_path(final_path: str) -> str:
    parent, name = final_path.rsplit("/", maxsplit=1)
    return f"{parent}/_backup_{name}"


def _recover_backup(spark: SparkSession, final_path: str, backup_path: str) -> None:
    if not _path_exists(spark, backup_path):
        return
    if _path_exists(spark, final_path):
        _delete_path(spark, backup_path)
    else:
        _rename_path(spark, backup_path, final_path)


def _hadoop_path(spark: SparkSession, path: str):
    return spark._jvm.org.apache.hadoop.fs.Path(path)


def _filesystem(spark: SparkSession, path: str):
    hadoop_path = _hadoop_path(spark, path)
    return hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())


def _path_exists(spark: SparkSession, path: str) -> bool:
    return bool(_filesystem(spark, path).exists(_hadoop_path(spark, path)))


def _delete_path(spark: SparkSession, path: str) -> None:
    filesystem = _filesystem(spark, path)
    hadoop_path = _hadoop_path(spark, path)
    if filesystem.exists(hadoop_path):
        filesystem.delete(hadoop_path, True)


def _rename_path(spark: SparkSession, source: str, destination: str) -> None:
    filesystem = _filesystem(spark, source)
    renamed = filesystem.rename(
        _hadoop_path(spark, source), _hadoop_path(spark, destination)
    )
    if not renamed:
        raise OSError(f"failed to rename {source!r} to {destination!r}")


def _make_parent_directory(spark: SparkSession, path: str) -> None:
    parent = _hadoop_path(spark, path).getParent()
    filesystem = parent.getFileSystem(spark._jsc.hadoopConfiguration())
    if not filesystem.mkdirs(parent) and not filesystem.exists(parent):
        raise OSError(f"failed to create output directory {parent}")
