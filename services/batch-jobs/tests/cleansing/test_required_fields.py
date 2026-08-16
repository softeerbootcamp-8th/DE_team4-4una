from datetime import UTC, datetime

from batch_jobs.schemas import SENSOR_EVENT_QUARANTINE_SCHEMA
from bronze_samples import MALFORMED_VALUE, valid_value, write_bronze_parquet
from cleansing.reader import read_bronze_sensor_events
from cleansing.rules import load_cleansing_config
from cleansing.validate import (
    MALFORMED_JSON,
    MISSING_REQUIRED_FIELD,
    split_required_field_failures,
)

RUN_ID = "cleansing-20260814-001"
REJECTED_AT = datetime(2026, 8, 14, 12, 0, 0, tzinfo=UTC)


def split(spark, path):
    bronze = read_bronze_sensor_events(spark, path)
    return split_required_field_failures(bronze, load_cleansing_config(), RUN_ID, REJECTED_AT)


def test_parse_failure_is_quarantined_with_its_reason(spark, tmp_path):
    # 잘린 줄이 MALFORMED_JSON 사유로 격리되고 원본 문자열이 그대로 담기는지 확인한다.
    path = write_bronze_parquet(spark, tmp_path, valid_value(), MALFORMED_VALUE)

    result = split(spark, path)

    rows = result.quarantined.collect()
    assert [row["reject_reason"] for row in rows] == [MALFORMED_JSON]
    assert rows[0]["raw_record"] == MALFORMED_VALUE
    assert result.passed.count() == 1


def test_null_required_column_is_quarantined_with_its_reason(spark, tmp_path):
    # 필수 컬럼이 NULL인 행이 MISSING_REQUIRED_FIELD 사유로 격리되는지 확인한다.
    path = write_bronze_parquet(spark, tmp_path, valid_value(), valid_value(event_id=None))

    result = split(spark, path)

    rows = result.quarantined.collect()
    assert [row["reject_reason"] for row in rows] == [MISSING_REQUIRED_FIELD]
    assert result.passed.count() == 1


def test_reject_detail_names_the_violating_columns(spark, tmp_path):
    # 판정 상세에 NULL이었던 컬럼명이 모두 들어가는지 확인한다.
    path = write_bronze_parquet(spark, tmp_path, valid_value(event_id=None, speed_mps=None))

    rows = split(spark, path).quarantined.collect()

    assert rows[0]["reject_detail"] == "event_id, speed_mps"


def test_no_row_is_lost_and_quarantine_matches_its_schema(spark, tmp_path):
    # 통과 행과 격리 행을 합치면 입력 행 수와 같고, 격리 행이 스키마와 일치하는지 확인한다.
    path = write_bronze_parquet(
        spark,
        tmp_path,
        valid_value(),
        MALFORMED_VALUE,
        valid_value(trip_id=None),
        valid_value(trip_seq=48),
    )

    result = split(spark, path)

    assert len(result.passed.collect()) == 2
    assert len(result.quarantined.collect()) == 2
    assert result.quarantined.columns == [
        field.name for field in SENSOR_EVENT_QUARANTINE_SCHEMA.fields
    ]
