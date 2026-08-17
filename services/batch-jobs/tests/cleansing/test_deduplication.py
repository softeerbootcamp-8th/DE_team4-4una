from datetime import UTC, datetime

from batch_jobs.cleansing.reader import read_bronze_sensor_events
from batch_jobs.cleansing.rules import load_cleansing_config
from batch_jobs.cleansing.validate import (
    DUPLICATE_EVENT,
    MISSING_REQUIRED_FIELD,
    cleanse_sensor_events,
)
from bronze_samples import VALID_EVENT, valid_value, write_bronze_parquet

RUN_ID = "cleansing-20260814-001"
REJECTED_AT = datetime(2026, 8, 14, 12, 0, 0, tzinfo=UTC)

EARLIER = "2026-08-13T10:23:24.000000+00:00"
LATER = "2026-08-13T10:23:25.000000+00:00"


def cleanse(spark, path):
    bronze = read_bronze_sensor_events(spark, path)
    return cleanse_sensor_events(bronze, load_cleansing_config(), RUN_ID, REJECTED_AT)


def test_only_the_latest_ingested_duplicate_survives(spark, tmp_path):
    # event_id가 같은 두 행 중 _ingested_at이 최신인 행(trip_seq=2)만 남는지 확인한다.
    path = write_bronze_parquet(
        spark,
        tmp_path,
        valid_value(_ingested_at=EARLIER, trip_seq=1),
        valid_value(_ingested_at=LATER, trip_seq=2),
    )

    result = cleanse(spark, path)

    passed = result.passed.collect()
    assert len(passed) == 1
    assert passed[0]["trip_seq"] == 2


def test_dropped_duplicate_is_quarantined_with_its_reason(spark, tmp_path):
    # 탈락한 행이 DUPLICATE_EVENT 사유로 격리되고 판정 상세에 키가 남는지 확인한다.
    path = write_bronze_parquet(
        spark, tmp_path, valid_value(_ingested_at=EARLIER), valid_value(_ingested_at=LATER)
    )

    rows = cleanse(spark, path).quarantined.collect()

    assert [row["reject_reason"] for row in rows] == [DUPLICATE_EVENT]
    assert rows[0]["reject_detail"] == f"event_id={VALID_EVENT['event_id']}"


def test_distinct_event_ids_are_all_kept(spark, tmp_path):
    # event_id가 다르면 중복이 아니므로 두 행 모두 남는다.
    path = write_bronze_parquet(
        spark, tmp_path, valid_value(), valid_value(event_id="other-event-id")
    )

    result = cleanse(spark, path)

    assert len(result.passed.collect()) == 2
    assert result.quarantined.collect() == []


def test_null_event_ids_are_not_treated_as_duplicates(spark, tmp_path):
    # event_id가 NULL인 두 행은 서로 중복이 아니라 각각 필수 컬럼 위반으로 격리된다.
    path = write_bronze_parquet(
        spark, tmp_path, valid_value(event_id=None), valid_value(event_id=None)
    )

    rows = cleanse(spark, path).quarantined.collect()

    assert [row["reject_reason"] for row in rows] == [MISSING_REQUIRED_FIELD] * 2
