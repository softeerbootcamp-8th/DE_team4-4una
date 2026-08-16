from datetime import UTC, datetime
from pathlib import Path

from bronze_samples import MALFORMED_VALUE, valid_value, write_bronze_parquet
from cleansing.config import CleansingJobConfig
from cleansing.job import run_cleansing_job

RUN_ID = "cleansing-20260815-001"
PROCESSED_AT = datetime(2026, 8, 15, 12, 0, 0, tzinfo=UTC)


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

    run_cleansing_job(spark, config, RUN_ID, PROCESSED_AT)

    assert spark.read.parquet(config.processed_output_path).count() == 2
    assert spark.read.parquet(config.quarantine_output_path).count() == 2


def test_job_completes_when_event_time_is_unusable(spark, tmp_path):
    # event_time을 변환할 수 없는 행이 섞여 있어도 잡이 중단되지 않는지 확인한다.
    bronze = write_bronze_parquet(
        spark, tmp_path, valid_value(), valid_value(event_id="broken", event_time="unknown")
    )
    config = job_config(tmp_path, bronze)

    run_cleansing_job(spark, config, RUN_ID, PROCESSED_AT)

    processed = spark.read.parquet(config.processed_output_path)
    assert processed.count() == 1
    assert processed.filter("event_time is null").count() == 0
    assert spark.read.parquet(config.quarantine_output_path).count() == 1


def test_job_loses_no_row(spark, tmp_path):
    # 저장된 통과 행과 격리 행을 합치면 입력 행 수와 같은지 확인한다.
    bronze = write_bronze_parquet(
        spark, tmp_path, valid_value(), MALFORMED_VALUE, valid_value(event_id="other")
    )
    config = job_config(tmp_path, bronze)

    run_cleansing_job(spark, config, RUN_ID, PROCESSED_AT)

    processed = spark.read.parquet(config.processed_output_path).count()
    quarantined = spark.read.parquet(config.quarantine_output_path).count()
    assert processed + quarantined == 3
