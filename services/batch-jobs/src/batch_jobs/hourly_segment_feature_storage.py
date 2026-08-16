"""Idempotently write hourly_segment_features Parquet to a per-hour path."""

from __future__ import annotations

import re
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from sensor_features.aggregation import validate_hourly_segment_features

_STAGING_DIRNAME = "_staging"
_BACKUP_SUFFIX = ".bak"
_SAFE_RUN_ID = re.compile(r"^[A-Za-z0-9_-]+$")


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
) -> HourlySegmentFeatureWriteResult:
    # 검증된 result를 staging에 쓰고, read-back 확인 후에만 대상 Hour 경로에 반영한다
    if not _SAFE_RUN_ID.match(run_id):
        raise ValueError(f"run_id contains unsafe path characters: {run_id!r}")
    _require_single_target_hour(result, target_hour)

    final_path = hour_output_path(output_root, target_hour)
    staging_path = f"{output_root.rstrip('/')}/{_STAGING_DIRNAME}/{run_id}"

    _recover_stale_backup(Path(final_path))
    try:
        result.write.mode("overwrite").parquet(staging_path)
        staged = spark.read.parquet(staging_path)
        validate_hourly_segment_features(staged)
        row_count = staged.count()
        if row_count == 0:
            raise ValueError("refusing to write an empty result over an existing hour")
        if row_count != result.count():
            raise ValueError("staged row count does not match the computed result")

        _replace_hour_path(final_path, staging_path)
    finally:
        shutil.rmtree(staging_path, ignore_errors=True)

    return HourlySegmentFeatureWriteResult(output_path=final_path, row_count=row_count)


def _require_single_target_hour(result: DataFrame, target_hour: datetime) -> None:
    # 엉뚱한 Hour 경로에 쓰지 않도록, 결과의 data_period_start가 전부 target_hour인지 확인한다.
    naive_target_hour = target_hour.replace(tzinfo=None)
    mismatched = result.filter(F.col("data_period_start") != F.lit(naive_target_hour))
    if mismatched.limit(1).count():
        raise ValueError("result contains rows outside the requested target_hour")


def _backup_path(final: Path) -> Path:
    return final.with_name(final.name + _BACKUP_SUFFIX)


def _recover_stale_backup(final: Path) -> None:
    # 직전 실행이 final -> backup 이동 직후 죽었다면, backup이 유일한 정상본이니 복구한다.
    backup = _backup_path(final)
    if not backup.exists():
        return
    if final.exists():
        shutil.rmtree(backup, ignore_errors=True)
    else:
        shutil.move(str(backup), str(final))


def _replace_hour_path(final_path: str, staging_path: str) -> None:
    final = Path(final_path)
    backup = _backup_path(final)

    had_existing = final.exists()
    if had_existing:
        shutil.move(str(final), str(backup))
    try:
        final.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(staging_path), str(final))
    except Exception:
        if had_existing:
            shutil.rmtree(final, ignore_errors=True)
            shutil.move(str(backup), str(final))
        raise
    else:
        if had_existing:
            shutil.rmtree(backup, ignore_errors=True)
