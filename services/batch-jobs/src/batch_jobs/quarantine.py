"""Split Bronze rows that fail parsing, required-field, or value-range checks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from batch_jobs.cleansing_config import CleansingConfig, ValueRange
from batch_jobs.schemas import CORRUPT_RECORD_COLUMN

MALFORMED_JSON = "MALFORMED_JSON"
MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"
OUT_OF_RANGE = "OUT_OF_RANGE"
DUPLICATE_EVENT = "DUPLICATE_EVENT"

# 중복 판정을 위해 잠시 붙였다가 결과에서 다시 떼어내는 컬럼
_DUPLICATE_RANK = "_duplicate_rank"

# 설정의 deduplication.priority 값과 잔존 행을 고르는 정렬 기준의 대응
_DEDUPLICATION_ORDERS = {"latest_ingested_at": F.col("_ingested_at").desc()}


@dataclass(frozen=True, slots=True)
class CleansingResult:
    """통과 행과 격리 행. 둘을 합치면 입력 행 수와 같다."""

    passed: DataFrame
    quarantined: DataFrame


def cleanse_sensor_events(
    bronze_df: DataFrame,
    config: CleansingConfig,
    run_id: str,
    rejected_at: datetime,
) -> CleansingResult:
    """Run every cleansing stage in order and collect the quarantined rows.

    중복 판정은 키가 NULL인 행을 한 그룹으로 묶으므로 필수 컬럼 검증
    뒤에 와야 한다.
    """
    required = split_required_field_failures(bronze_df, config, run_id, rejected_at)
    ranges = split_out_of_range_values(required.passed, config, run_id, rejected_at)
    duplicates = split_duplicate_events(ranges.passed, config, run_id, rejected_at)
    return CleansingResult(
        passed=duplicates.passed,
        quarantined=required.quarantined.unionByName(ranges.quarantined).unionByName(
            duplicates.quarantined
        ),
    )


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


def split_out_of_range_values(
    df: DataFrame,
    config: CleansingConfig,
    run_id: str,
    rejected_at: datetime,
) -> CleansingResult:
    """Quarantine rows whose columns fall outside their configured range."""
    violations = _range_violations(config)
    return CleansingResult(
        passed=df.filter(F.size(violations) == 0),
        quarantined=_quarantine_rows(
            df.filter(F.size(violations) > 0),
            reject_reason=OUT_OF_RANGE,
            reject_detail=F.concat_ws(", ", violations),
            raw_record=_reserialized_record(df),
            run_id=run_id,
            rejected_at=rejected_at,
        ),
    )


def split_duplicate_events(
    df: DataFrame,
    config: CleansingConfig,
    run_id: str,
    rejected_at: datetime,
) -> CleansingResult:
    """Keep one row per deduplication key and quarantine the rest.

    키 컬럼이 NULL인 행은 서로 같은 그룹으로 묶이므로, 필수 컬럼 검증을
    통과한 행만 넘겨야 한다.
    """
    key = config.deduplication.key
    window = Window.partitionBy(*key).orderBy(_deduplication_order(config.deduplication.priority))
    ranked = df.withColumn(_DUPLICATE_RANK, F.row_number().over(window))
    return CleansingResult(
        passed=ranked.filter(F.col(_DUPLICATE_RANK) == 1).drop(_DUPLICATE_RANK),
        quarantined=_quarantine_rows(
            ranked.filter(F.col(_DUPLICATE_RANK) > 1),
            reject_reason=DUPLICATE_EVENT,
            reject_detail=F.concat_ws(
                ", ",
                *[F.concat(F.lit(f"{name}="), F.col(name).cast("string")) for name in key],
            ),
            raw_record=_reserialized_record(df),
            run_id=run_id,
            rejected_at=rejected_at,
        ),
    )


def _deduplication_order(priority: str) -> Column:
    if priority not in _DEDUPLICATION_ORDERS:
        raise ValueError(f"unsupported deduplication priority: {priority}")
    return _DEDUPLICATION_ORDERS[priority]


def _range_violations(config: CleansingConfig) -> Column:
    """범위를 벗어난 컬럼을 "이름=값" 형태로 담은 배열."""
    entries = [
        F.when(
            _violates_range(name, value_range, config.negative_allowed.get(name, True)),
            F.concat(F.lit(f"{name}="), F.col(name).cast("string")),
        )
        for name, value_range in config.value_ranges.items()
    ]
    return F.array_compact(F.array(*entries))


def _violates_range(name: str, value_range: ValueRange, negative_allowed: bool) -> Column:
    """컬럼 하나가 범위를 벗어났는지 판정하는 조건식."""
    violated = F.lit(False)
    if value_range.minimum is not None:
        violated = violated | (F.col(name) < value_range.minimum)
    if value_range.maximum is not None:
        violated = violated | (
            F.col(name) >= value_range.maximum
            if value_range.max_exclusive
            else F.col(name) > value_range.maximum
        )
    if not negative_allowed:
        violated = violated | (F.col(name) < 0)
    return violated


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
