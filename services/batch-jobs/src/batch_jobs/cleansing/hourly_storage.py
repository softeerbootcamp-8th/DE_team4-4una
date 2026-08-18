"""Stage, validate, and replace one hourly cleansing output partition."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StructField, StructType

from batch_jobs.schemas import (
    PROCESSED_SENSOR_EVENT_SCHEMA,
    SENSOR_EVENT_QUARANTINE_SCHEMA,
)

_SAFE_RUN_ID = re.compile(r"^[A-Za-z0-9_.:+-]+$")
_STAGING_DIRNAME = "_staging"
PROCESSED_SENSOR_EVENT_PARTITIONED_SCHEMA = StructType(
    [
        *PROCESSED_SENSOR_EVENT_SCHEMA.fields,
        StructField("event_hour", IntegerType(), nullable=False),
    ]
)
PROCESSED_SENSOR_EVENT_FILE_SCHEMA = StructType(
    [
        field
        for field in PROCESSED_SENSOR_EVENT_SCHEMA.fields
        if field.name != "event_date"
    ]
)


@dataclass(frozen=True, slots=True)
class CleansingWriteResult:
    processed_output_path: str
    quarantine_output_path: str
    processed_count: int
    quarantined_count: int


@dataclass(frozen=True, slots=True)
class _PartitionReplacement:
    final_path: str
    staged_path: str | None


def processed_hour_path(output_root: str, target_hour: datetime) -> str:
    return (
        f"{output_root.rstrip('/')}/event_date={target_hour.date().isoformat()}"
        f"/event_hour={target_hour.hour:02d}"
    )


def quarantine_hour_path(output_root: str, target_hour: datetime) -> str:
    return (
        f"{output_root.rstrip('/')}/target_date={target_hour.date().isoformat()}"
        f"/target_hour={target_hour.hour:02d}"
    )


def write_hourly_cleansing_results(
    spark: SparkSession,
    processed: DataFrame,
    quarantined: DataFrame,
    processed_output_root: str,
    quarantine_output_root: str,
    target_hour: datetime,
    run_id: str,
) -> CleansingWriteResult:
    """Replace only the requested UTC hour after validating staged Parquet."""
    _require_safe_run_id(run_id)
    _require_utc_hour(target_hour)
    _require_schema(processed, PROCESSED_SENSOR_EVENT_PARTITIONED_SCHEMA)
    _require_schema(quarantined, SENSOR_EVENT_QUARANTINE_SCHEMA)
    _require_processed_target_hour(processed, target_hour)

    processed_staging_root = (
        f"{processed_output_root.rstrip('/')}/{_STAGING_DIRNAME}/{run_id}"
    )
    quarantine_staging_root = (
        f"{quarantine_output_root.rstrip('/')}/{_STAGING_DIRNAME}/{run_id}"
    )
    processed_count = processed.count()
    quarantined_count = quarantined.count()

    try:
        processed_staged_path = _stage_processed(
            spark,
            processed,
            processed_staging_root,
            target_hour,
            processed_count,
        )
        quarantine_staged_path = _stage_quarantine(
            spark,
            quarantined,
            quarantine_staging_root,
            target_hour,
            quarantined_count,
        )
        processed_final_path = processed_hour_path(processed_output_root, target_hour)
        quarantine_final_path = quarantine_hour_path(quarantine_output_root, target_hour)
        _replace_partitions(
            spark,
            (
                _PartitionReplacement(processed_final_path, processed_staged_path),
                _PartitionReplacement(quarantine_final_path, quarantine_staged_path),
            ),
        )
    finally:
        _delete_path(spark, processed_staging_root)
        _delete_path(spark, quarantine_staging_root)

    return CleansingWriteResult(
        processed_output_path=processed_final_path,
        quarantine_output_path=quarantine_final_path,
        processed_count=processed_count,
        quarantined_count=quarantined_count,
    )


def _stage_processed(
    spark: SparkSession,
    processed: DataFrame,
    staging_root: str,
    target_hour: datetime,
    expected_count: int,
) -> str | None:
    (
        processed.write.mode("overwrite")
        .partitionBy("event_date", "event_hour")
        .parquet(staging_root)
    )
    if expected_count == 0:
        return None

    staged = spark.read.parquet(staging_root)
    _require_schema(staged, PROCESSED_SENSOR_EVENT_PARTITIONED_SCHEMA)
    _require_processed_target_hour(staged, target_hour)
    if staged.count() != expected_count:
        raise ValueError("staged processed row count does not match computed result")

    return (
        f"{staging_root}/event_date={target_hour.date().isoformat()}"
        f"/event_hour={target_hour.hour}"
    )


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


def _require_processed_target_hour(processed: DataFrame, target_hour: datetime) -> None:
    target_hour_end = target_hour + timedelta(hours=1)
    invalid = processed.filter(
        F.col("event_time").isNull()
        | (F.col("event_time") < F.lit(target_hour))
        | (F.col("event_time") >= F.lit(target_hour_end))
        | (F.col("event_date") != F.lit(target_hour.date()))
        | (F.col("event_hour") != F.lit(target_hour.hour))
    )
    if invalid.limit(1).count():
        raise ValueError("processed result contains rows outside target_hour")


def _replace_partitions(
    spark: SparkSession, replacements: tuple[_PartitionReplacement, ...]
) -> None:
    backups: list[tuple[str, str]] = []
    promoted: list[str] = []
    try:
        for replacement in replacements:
            backup_path = _backup_path(replacement.final_path)
            _recover_backup(spark, replacement.final_path, backup_path)
            if _path_exists(spark, replacement.final_path):
                _rename_path(spark, replacement.final_path, backup_path)
                backups.append((replacement.final_path, backup_path))

        for replacement in replacements:
            if replacement.staged_path is None:
                continue
            _make_parent_directory(spark, replacement.final_path)
            _rename_path(spark, replacement.staged_path, replacement.final_path)
            promoted.append(replacement.final_path)
    except Exception:
        for final_path in reversed(promoted):
            _delete_path(spark, final_path)
        for final_path, backup_path in reversed(backups):
            _rename_path(spark, backup_path, final_path)
        raise
    else:
        for _, backup_path in backups:
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
