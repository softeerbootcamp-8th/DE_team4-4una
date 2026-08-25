"""Idempotently write hourly_segment_features Parquet to a per-hour path."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from batch_jobs.sensor_features.aggregation import validate_hourly_segment_features

_STAGING_DIRNAME = "_staging"
_BACKUP_SUFFIX = ".bak"
_SAFE_RUN_ID = re.compile(r"^[A-Za-z0-9_.:+-]+$")


@dataclass(frozen=True, slots=True)
class HourlySegmentFeatureWriteResult:
    output_path: str
    row_count: int


# target_hour 전용 출력 경로(다른 Hour와 완전히 분리된 디렉터리)를 계산한다
def hour_output_path(output_root: str, target_hour: datetime) -> str:
    return (
        f"{output_root.rstrip('/')}/data_period_date={target_hour.date().isoformat()}"
        f"/hour={target_hour.hour:02d}"
    )


def write_hourly_segment_features(
    spark: SparkSession,
    result: DataFrame,
    output_root: str,
    target_hour: datetime,
    run_id: str,
    *,
    result_count: int | None = None,
) -> HourlySegmentFeatureWriteResult:
    # 검증된 result를 staging에 쓰고, read-back 확인 후에만 대상 Hour 경로에 반영한다
    if not _SAFE_RUN_ID.match(run_id):
        raise ValueError(f"run_id contains unsafe path characters: {run_id!r}")
    _require_utc_target_hour(target_hour)
    _require_single_target_hour(result, target_hour)

    final_path = hour_output_path(output_root, target_hour)
    staging_path = f"{output_root.rstrip('/')}/{_STAGING_DIRNAME}/{run_id}"

    _recover_stale_backup(spark, final_path)
    # 직전 실행이 죽어 staging_path에 잔여물이 남아있을 수 있다(#380) — 이 write는
    # mode("overwrite") 없이 하므로(#377) 미리 지워야 PATH_ALREADY_EXISTS로 막히지 않는다.
    _delete_path(spark, staging_path)
    try:
        result.write.parquet(staging_path)
        staged = spark.read.parquet(staging_path)
        validate_hourly_segment_features(staged)
        row_count = staged.count()
        if row_count == 0:
            raise ValueError("refusing to write an empty result over an existing hour")
        # 호출부가 이미 count를 알고 있으면 재사용한다 — 여기서 또 count()하면 상류 lineage가 재계산될 수 있다(#474).
        computed_count = result_count if result_count is not None else result.count()
        if row_count != computed_count:
            raise ValueError("staged row count does not match the computed result")

        _replace_hour_path(spark, final_path, staging_path)
    finally:
        _delete_path(spark, staging_path)

    return HourlySegmentFeatureWriteResult(output_path=final_path, row_count=row_count)


def _require_utc_target_hour(target_hour: datetime) -> None:
    # tzinfo를 떼면 호스트 OS 타임존에 따라 다른 시각으로 재해석되므로, UTC 정각인지 그대로 검증한다.
    if target_hour.utcoffset() != timedelta(0):
        raise ValueError("target_hour must be UTC timezone-aware")
    if (target_hour.minute, target_hour.second, target_hour.microsecond) != (0, 0, 0):
        raise ValueError("target_hour must be truncated to the hour")


def _require_single_target_hour(result: DataFrame, target_hour: datetime) -> None:
    # 엉뚱한 Hour 경로에 쓰지 않도록, 결과의 data_period_start가 전부 target_hour인지 확인한다.
    # target_hour는 UTC-aware 그대로 비교해야 호스트 OS 타임존과 무관하게 같은 결과가 나온다.
    mismatched = result.filter(F.col("data_period_start") != F.lit(target_hour))
    if mismatched.limit(1).count():
        raise ValueError("result contains rows outside the requested target_hour")


def _backup_path(final_path: str) -> str:
    parent, name = final_path.rsplit("/", maxsplit=1)
    return f"{parent}/{name}{_BACKUP_SUFFIX}"


def _recover_stale_backup(spark: SparkSession, final_path: str) -> None:
    # 직전 실행이 final -> backup 이동 직후 죽었다면, backup이 유일한 정상본이니 복구한다.
    backup_path = _backup_path(final_path)
    if not _path_exists(spark, backup_path):
        return
    if _path_exists(spark, final_path):
        _delete_path(spark, backup_path)
    else:
        _rename_path(spark, backup_path, final_path)


def _replace_hour_path(spark: SparkSession, final_path: str, staging_path: str) -> None:
    """백업 후 rename으로 스왑하고, 실패 시 백업에서 되돌린다.

    **S3(EMRFS) 주의**: 로컬/HDFS의 rename은 메타데이터만 바꾸는 원자적
    연산이지만, EMRFS의 `FileSystem.rename()`은 내부적으로 디렉터리의 각
    객체를 copy 후 delete하는 방식이라 원자적이지 않다(`cleansing/hourly_storage.py`의
    `_replace_partition`과 동일한 위험을 그대로 감수한다).
    """
    backup_path = _backup_path(final_path)
    had_existing = False
    promoted = False
    try:
        if _path_exists(spark, final_path):
            _rename_path(spark, final_path, backup_path)
            had_existing = True

        _make_parent_directory(spark, final_path)
        _rename_path(spark, staging_path, final_path)
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
