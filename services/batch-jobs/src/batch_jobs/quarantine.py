"""Split Bronze rows that fail parsing or required-field checks into quarantine rows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from batch_jobs.cleansing_config import CleansingConfig
from batch_jobs.schemas import CORRUPT_RECORD_COLUMN

MALFORMED_JSON = "MALFORMED_JSON"
MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"


@dataclass(frozen=True, slots=True)
class CleansingResult:
    """통과 행과 격리 행. 둘을 합치면 입력 행 수와 같다."""

    passed: DataFrame
    quarantined: DataFrame


def split_required_field_failures(
    bronze_df: DataFrame,
    config: CleansingConfig,
    run_id: str,
    rejected_at: datetime,
) -> CleansingResult:
    """Quarantine unparseable rows and rows whose required columns are NULL."""
    is_malformed = F.col(CORRUPT_RECORD_COLUMN).isNotNull()
    null_columns = _null_required_columns(config.required_columns)

    parsed = bronze_df.filter(~is_malformed)
    malformed_rows = _quarantine_rows(
        bronze_df.filter(is_malformed),
        reject_reason=MALFORMED_JSON,
        reject_detail=F.lit(None).cast("string"),
        raw_record=F.col(CORRUPT_RECORD_COLUMN),
        run_id=run_id,
        rejected_at=rejected_at,
    )
    missing_rows = _quarantine_rows(
        parsed.filter(F.size(null_columns) > 0),
        reject_reason=MISSING_REQUIRED_FIELD,
        reject_detail=F.concat_ws(", ", null_columns),
        raw_record=_reserialized_record(bronze_df),
        run_id=run_id,
        rejected_at=rejected_at,
    )
    return CleansingResult(
        passed=parsed.filter(F.size(null_columns) == 0),
        quarantined=malformed_rows.unionByName(missing_rows),
    )


def _null_required_columns(required_columns: tuple[str, ...]) -> Column:
    """NULL인 필수 컬럼명만 담은 배열."""
    return F.array_compact(
        F.array(*[F.when(F.col(name).isNull(), F.lit(name)) for name in required_columns])
    )


def _reserialized_record(bronze_df: DataFrame) -> Column:
    """파싱에 성공한 행에는 원본 문자열이 없어 선언된 컬럼만으로 다시 직렬화한다."""
    columns = [name for name in bronze_df.columns if name != CORRUPT_RECORD_COLUMN]
    return F.to_json(F.struct(*columns), {"ignoreNullFields": "false"})


def _quarantine_rows(
    df: DataFrame,
    reject_reason: str,
    reject_detail: Column,
    raw_record: Column,
    run_id: str,
    rejected_at: datetime,
) -> DataFrame:
    return df.select(
        F.col("event_id"),
        F.col("trip_id"),
        # Bronze의 event_time은 STRING이라 DATE로 변환한다. 파싱 실패 행은 NULL이 된다.
        F.to_date(F.col("event_time")).alias("event_date"),
        F.lit(reject_reason).alias("reject_reason"),
        reject_detail.alias("reject_detail"),
        raw_record.alias("raw_record"),
        F.lit(run_id).alias("_run_id"),
        F.lit(rejected_at).alias("_rejected_at"),
    )
