from datetime import UTC, datetime, timedelta
from pathlib import Path

from batch_jobs.cleansing.config import CleansingJobConfig
from batch_jobs.cleansing.job import run_cleansing_job
from batch_jobs.cleansing.hourly_storage import processed_hour_path, quarantine_hour_path
from bronze_samples import MALFORMED_VALUE, valid_value, write_bronze_parquet

RUN_ID = "cleansing-20260815-001"
PROCESSED_AT = datetime(2026, 8, 15, 12, 0, 0, tzinfo=UTC)
TARGET_HOUR = datetime(2024, 2, 1, 5, 0, 0, tzinfo=UTC)


def job_config(directory: Path, bronze_path: Path) -> CleansingJobConfig:
    return CleansingJobConfig.from_env(
        {
            "CLEANSING_BRONZE_INPUT_PATH": str(bronze_path),
            "CLEANSING_SILVER_OUTPUT_PATH": str(directory / "processed"),
            "CLEANSING_QUARANTINE_OUTPUT_PATH": str(directory / "quarantine"),
        }
    )


def test_job_writes_both_outputs(spark, tmp_path):
    # 한 번 실행으로 통과 행과 격리 행이 각자의 경로에 저장되는지 확인한다.
    bronze = write_bronze_parquet(
        spark,
        tmp_path,
        valid_value(),
        valid_value(event_id="other"),
        MALFORMED_VALUE,
        valid_value(event_id="too-fast", speed_mps=999.0),
    )
    config = job_config(tmp_path, bronze)

    run_cleansing_job(spark, config, RUN_ID, TARGET_HOUR, PROCESSED_AT)

    assert spark.read.parquet(config.processed_output_path).count() == 2
    assert spark.read.parquet(config.quarantine_output_path).count() == 2


def test_job_loses_no_row(spark, tmp_path):
    # 저장된 통과 행과 격리 행을 합치면 입력 행 수와 같은지 확인한다.
    bronze = write_bronze_parquet(
        spark, tmp_path, valid_value(), MALFORMED_VALUE, valid_value(event_id="other")
    )
    config = job_config(tmp_path, bronze)

    run_cleansing_job(spark, config, RUN_ID, TARGET_HOUR, PROCESSED_AT)

    processed = spark.read.parquet(config.processed_output_path).count()
    quarantined = spark.read.parquet(config.quarantine_output_path).count()
    assert processed + quarantined == 3


def test_rerunning_same_hour_replaces_silver_and_quarantine(spark, tmp_path):
    bronze = write_bronze_parquet(
        spark, tmp_path, valid_value(event_id="first"), MALFORMED_VALUE
    )
    config = job_config(tmp_path, bronze)
    run_cleansing_job(spark, config, "run-1", TARGET_HOUR, PROCESSED_AT)

    write_bronze_parquet(spark, tmp_path, valid_value(event_id="second"), MALFORMED_VALUE)
    run_cleansing_job(spark, config, "run-2", TARGET_HOUR, PROCESSED_AT)

    processed = spark.read.parquet(
        processed_hour_path(config.processed_output_path, TARGET_HOUR)
    ).collect()
    quarantined = spark.read.parquet(
        quarantine_hour_path(config.quarantine_output_path, TARGET_HOUR)
    ).collect()
    assert [row["event_id"] for row in processed] == ["second"]
    assert len(quarantined) == 1
    assert quarantined[0]["_run_id"] == "run-2"


def test_replacing_one_hour_preserves_the_adjacent_hour(spark, tmp_path):
    next_hour = TARGET_HOUR + timedelta(hours=1)
    bronze = write_bronze_parquet(
        spark,
        tmp_path,
        valid_value(event_id="hour-5"),
        valid_value(event_id="hour-6", event_time=next_hour.isoformat()),
    )
    config = job_config(tmp_path, bronze)
    run_cleansing_job(spark, config, "run-5", TARGET_HOUR, PROCESSED_AT)
    run_cleansing_job(spark, config, "run-6", next_hour, PROCESSED_AT)

    write_bronze_parquet(spark, tmp_path, valid_value(event_id="hour-5-new"))
    run_cleansing_job(spark, config, "run-5-new", TARGET_HOUR, PROCESSED_AT)

    hour_5 = spark.read.parquet(
        processed_hour_path(config.processed_output_path, TARGET_HOUR)
    ).collect()
    hour_6 = spark.read.parquet(
        processed_hour_path(config.processed_output_path, next_hour)
    ).collect()
    assert [row["event_id"] for row in hour_5] == ["hour-5-new"]
    assert [row["event_id"] for row in hour_6] == ["hour-6"]


def test_midnight_uses_the_next_utc_date_partition(spark, tmp_path):
    midnight = datetime(2024, 2, 2, 0, 0, 0, tzinfo=UTC)
    bronze = write_bronze_parquet(
        spark,
        tmp_path,
        valid_value(event_id="midnight", event_time=midnight.isoformat()),
    )
    config = job_config(tmp_path, bronze)

    run_cleansing_job(spark, config, "midnight-run", midnight, PROCESSED_AT)

    output_path = processed_hour_path(config.processed_output_path, midnight)
    assert output_path.endswith("event_date=2024-02-02/event_hour=00")
    assert spark.read.parquet(output_path).count() == 1
