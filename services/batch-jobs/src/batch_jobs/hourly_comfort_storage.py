"""Stage, validate, and replace one hourly comfort-score partition (#469)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

_STAGING_DIRNAME = "_staging"
_SAFE_RUN_ID = re.compile(r"^[A-Za-z0-9_.:+-]+$")


@dataclass(frozen=True, slots=True)
class HourlyComfortWriteResult:
    output_path: str
    row_count: int


def hour_output_path(output_root: str, target_hour: datetime) -> str:
    """Silver2(`hourly_segment_feature_storage.hour_output_path`)와 같은 레이아웃을 쓴다."""
    return (
        f"{output_root.rstrip('/')}/data_period_date={target_hour.date().isoformat()}"
        f"/hour={target_hour.hour:02d}"
    )


def _backup_path(final_path: str) -> str:
    # 반드시 `_`로 시작해야 Spark 파티션 탐색이 무시한다. `hour=09.bak`으로 쓰면
    # hour="09.bak" 값으로 인식돼 컬럼 타입 추론이 int에서 string으로 바뀐다.
    parent, name = final_path.rsplit("/", maxsplit=1)
    return f"{parent}/_backup_{name}"


def write_hourly_comfort_partition(
    spark: SparkSession,
    frame: DataFrame,
    output_root: str,
    target_hour: datetime,
    run_id: str,
    expected_schema: StructType,
    *,
    allow_empty: bool = False,
    expected_count: int | None = None,
) -> HourlyComfortWriteResult:
    """Replace only the requested UTC-hour partition after a read-back check.

    `cleansing/hourly_storage.py`, `hourly_segment_feature_storage.py`와 같은 절차다 —
    staging에 쓰고, 다시 읽어 행 수를 확인한 뒤에만 대상 파티션과 교체한다.

    `allow_empty`는 0행 결과를 어떻게 볼지 정한다. 점수 출력이 비는 것은 정상 상황이
    아니므로 기본은 거부다(`hourly_segment_feature_storage`가 Silver2에 거는 가드와
    같다). quarantine처럼 비어 있는 것이 정상인 출력만 True로 열어 준다 — 그때는
    기존 파티션을 지우기만 하고 새로 쓰지 않는다.

    `expected_count`는 호출자가 이미 행 수를 알고 있을 때 넘긴다
    (`hourly_segment_feature_storage.write_hourly_segment_features`의 `result_count`와
    같은 규약이다). 넘기지 않으면 여기서 직접 센다. 이 값은 아래 read-back 대조의
    기준이 되므로, `frame`의 실제 행 수와 다른 값을 넘기면 교체 직전에 실패한다.
    """
    _require_safe_run_id(run_id)
    _require_utc_hour(target_hour)
    _require_schema(frame, expected_schema)

    final_path = hour_output_path(output_root, target_hour)
    staging_path = f"{output_root.rstrip('/')}/{_STAGING_DIRNAME}/{run_id}"
    if expected_count is None:
        expected_count = frame.count()
    if expected_count == 0 and not allow_empty:
        # 여기서 막지 않으면 아래 _replace_partition이 기존 정상 데이터를 지운다.
        raise ValueError("refusing to write an empty result over an existing hour")

    try:
        staged_path: str | None = None
        if expected_count:
            # 직전 실행이 죽어 staging에 잔여물이 남아 있을 수 있다(#380). mode("overwrite")
            # 없이 쓰므로 미리 지워야 PATH_ALREADY_EXISTS로 막히지 않는다.
            _delete_path(spark, staging_path)
            frame.write.parquet(staging_path)
            # Spark는 lazy라 쓰기가 부분적으로만 끝나도 예외가 안 날 수 있다. 실제로
            # 다시 읽어 행 수를 맞춰봐야 대상 파티션에 반영해도 되는지 알 수 있다.
            staged = spark.read.schema(expected_schema).parquet(staging_path)
            if staged.count() != expected_count:
                raise ValueError("staged row count does not match the computed result")
            staged_path = staging_path
        # expected_count가 0이면 staged_path를 None으로 둬, 기존 파티션을 지우기만 한다.
        # 빈 디렉터리를 남기면 읽는 쪽의 스키마 추론이 애매해진다.
        _replace_partition(spark, final_path, staged_path)
    finally:
        _delete_path(spark, staging_path)

    return HourlyComfortWriteResult(output_path=final_path, row_count=expected_count)


def _require_safe_run_id(run_id: str) -> None:
    # run_id가 그대로 staging 디렉터리 이름이 되므로 경로를 벗어나는 문자를 막는다.
    if not _SAFE_RUN_ID.fullmatch(run_id):
        raise ValueError(f"run_id contains unsafe path characters: {run_id!r}")


def _require_utc_hour(target_hour: datetime) -> None:
    # tzinfo를 떼면 호스트 OS 타임존에 따라 다른 시각으로 재해석된다.
    if target_hour.utcoffset() != timedelta(0):
        raise ValueError("target_hour must be UTC timezone-aware")
    if (target_hour.minute, target_hour.second, target_hour.microsecond) != (0, 0, 0):
        raise ValueError("target_hour must be truncated to the hour")


def _require_schema(frame: DataFrame, expected: StructType) -> None:
    # nullable은 비교하지 않는다 — Spark가 Parquet을 읽을 때 느슨하게 잡는 경우가 있어
    # 엄격히 비교하면 정상 데이터가 튕긴다(hourly_storage.py와 같은 방식).
    actual_fields = {field.name: field.dataType for field in frame.schema.fields}
    expected_fields = {field.name: field.dataType for field in expected.fields}
    if actual_fields != expected_fields:
        raise ValueError(
            f"schema mismatch: expected {expected.simpleString()}, "
            f"got {frame.schema.simpleString()}"
        )


def _replace_partition(
    spark: SparkSession, final_path: str, staged_path: str | None
) -> None:
    """백업 후 rename으로 스왑하고, 실패 시 백업에서 되돌린다.

    **S3(EMRFS) 주의**: EMRFS의 `FileSystem.rename()`은 디렉터리의 각 객체를 copy 후
    delete하는 방식이라 원자적이지 않다. `cleansing/hourly_storage.py::_replace_partition`과
    동일한 위험을 그대로 감수한다(ADR-0011).
    """
    backup_path = _backup_path(final_path)
    # 실패 지점에 따라 되돌릴 대상이 다르다. 아직 만들지 않은 backup을 rename하려 들면
    # 복구 도중 또 실패하므로, 어디까지 진행했는지 플래그로 들고 간다.
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


def _recover_backup(spark: SparkSession, final_path: str, backup_path: str) -> None:
    # 이전 실행의 잔해를 정리한다. final -> backup 직후에 죽었다면 backup이 유일한
    # 정상본이므로 되돌리고, 둘 다 있다면 스왑을 마치고 backup 삭제 직전에 죽은
    # 것이므로 backup을 버린다.
    if not _path_exists(spark, backup_path):
        return
    if _path_exists(spark, final_path):
        _delete_path(spark, backup_path)
    else:
        _rename_path(spark, backup_path, final_path)


def _hadoop_path(spark: SparkSession, path: str):
    # PySpark에는 디렉터리를 옮기고 지우는 API가 없어 JVM의 Hadoop FileSystem을 직접
    # 쓴다. 스킴(file://, s3://)에 맞는 구현체를 Hadoop이 알아서 고르므로 로컬 테스트와
    # EMR에서 같은 코드가 동작한다.
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
    # Hadoop의 rename은 실패해도 예외를 던지지 않고 false를 반환한다. 확인하지 않으면
    # 실패가 조용히 지나간다.
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
