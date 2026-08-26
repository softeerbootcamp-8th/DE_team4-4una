from datetime import UTC, datetime, timedelta
from pathlib import Path

from batch_jobs.cleansing.config import CleansingJobConfig
from batch_jobs.cleansing.hourly_storage import (
    quarantine_hour_path,
    write_hourly_quarantine,
)
from batch_jobs.cleansing.job import run_cleansing_job
from batch_jobs.hourly_segment_feature_job import (
    HourlySegmentFeatureJobConfig,
    HourlySegmentFeatureJobSummary,
)
from bronze_samples import MALFORMED_VALUE, valid_value, write_bronze_parquet

RUN_ID = "cleansing-20260815-001"
PROCESSED_AT = datetime(2026, 8, 15, 12, 0, 0, tzinfo=UTC)
TARGET_HOUR = datetime(2024, 2, 1, 5, 0, 0, tzinfo=UTC)
ROAD_SNAPSHOT_DATE = TARGET_HOUR.date()
FEATURE_VERSION = "v1"


def cleansing_config(directory: Path, bronze_path: Path) -> CleansingJobConfig:
    return CleansingJobConfig.from_env(
        {
            "CLEANSING_BRONZE_INPUT_PATH": str(bronze_path),
            "CLEANSING_QUARANTINE_OUTPUT_PATH": str(directory / "quarantine"),
        }
    )


def feature_config(directory: Path) -> HourlySegmentFeatureJobConfig:
    return HourlySegmentFeatureJobConfig.from_env(
        {
            "HOURLY_SEGMENT_FEATURE_ROAD_SEGMENT_PATH": str(directory / "road_segment"),
            "HOURLY_SEGMENT_FEATURE_OUTPUT_PATH": str(directory / "features"),
        }
    )


def stub_feature_job(monkeypatch, captured_rows: list[dict[str, object]]) -> None:
    def run_stub(
        spark,
        sensor_df,
        config,
        target_hour,
        snapshot,
        version,
        run_id,
        processed_at,
        *,
        cleansing_quarantine,
        cleansing_quarantined_count,
        raw_record_source,
        quarantine_output_path,
    ):
        captured_rows.extend(row.asDict() for row in sensor_df.collect())
        quarantine_write = write_hourly_quarantine(
            spark,
            cleansing_quarantine,
            quarantine_output_path,
            target_hour,
            run_id,
            expected_count=cleansing_quarantined_count,
        )
        return HourlySegmentFeatureJobSummary(
            result_count=1,
            accepted_count=sensor_df.count(),
            map_matching_quarantined_count=0,
            quarantined_count=quarantine_write.row_count,
            quarantine_output_path=quarantine_write.output_path,
            output_path=config.output_path,
            target_hour=target_hour,
            run_id=run_id,
        )

    monkeypatch.setattr(
        "batch_jobs.cleansing.job.run_hourly_segment_feature_job",
        run_stub,
    )


def run_job(spark, tmp_path, bronze_path, monkeypatch, run_id=RUN_ID):
    captured_rows: list[dict[str, object]] = []
    stub_feature_job(monkeypatch, captured_rows)
    summary = run_cleansing_job(
        spark,
        cleansing_config(tmp_path, bronze_path),
        feature_config(tmp_path),
        run_id,
        TARGET_HOUR,
        ROAD_SNAPSHOT_DATE,
        FEATURE_VERSION,
        PROCESSED_AT,
    )
    return summary, captured_rows


def test_job_passes_typed_rows_directly_to_features(spark, tmp_path, monkeypatch):
    bronze = write_bronze_parquet(
        spark,
        tmp_path,
        valid_value(event_id="first"),
        valid_value(event_id="second", trip_seq=48),
        MALFORMED_VALUE,
        valid_value(event_id="too-fast", speed_mps=999.0),
    )

    summary, rows = run_job(spark, tmp_path, bronze, monkeypatch)

    assert summary.processed_count == 2
    assert summary.accepted_count == 2
    assert summary.cleansing_quarantined_count == 2
    assert summary.map_matching_quarantined_count == 0
    assert summary.quarantined_count == 2
    assert {row["event_id"] for row in rows} == {"first", "second"}
    assert all("event_date" not in row for row in rows)
    assert not (tmp_path / "processed_sensor_event").exists()


def test_missing_bronze_partition_yields_zero_processed_and_quarantined(
    spark, tmp_path, monkeypatch
):
    # target_hour에 해당하는 Bronze 파티션 자체가 없는 경우(그 시간에 이벤트가
    # 전혀 없었던 경우) — 예외 없이 0건 처리로 끝나야 한다(#345 완료 조건).
    bronze = write_bronze_parquet(
        spark,
        tmp_path,
        valid_value(event_time=(TARGET_HOUR + timedelta(hours=3)).isoformat()),
    )

    summary, rows = run_job(spark, tmp_path, bronze, monkeypatch)

    assert summary.processed_count == 0
    assert summary.accepted_count == 0
    assert summary.quarantined_count == 0
    assert rows == []


def test_job_conserves_target_hour_rows(spark, tmp_path, monkeypatch):
    bronze = write_bronze_parquet(
        spark,
        tmp_path,
        valid_value(),
        MALFORMED_VALUE,
        valid_value(event_id="other", trip_seq=48),
    )

    summary, _ = run_job(spark, tmp_path, bronze, monkeypatch)

    assert summary.input_count == 3
    assert summary.accepted_count + summary.quarantined_count == 3


def test_rerunning_same_hour_replaces_quarantine(spark, tmp_path, monkeypatch):
    bronze = write_bronze_parquet(
        spark,
        tmp_path,
        valid_value(event_id="first"),
        valid_value(event_id="bad-first", speed_mps=999.0),
    )
    run_job(spark, tmp_path, bronze, monkeypatch, run_id="run-1")

    write_bronze_parquet(
        spark,
        tmp_path,
        valid_value(event_id="second"),
        valid_value(event_id="bad-second", speed_mps=999.0),
    )
    summary, _ = run_job(spark, tmp_path, bronze, monkeypatch, run_id="run-2")

    quarantined = spark.read.parquet(summary.quarantine_output_path).collect()
    assert [row["event_id"] for row in quarantined] == ["bad-second"]
    assert quarantined[0]["_run_id"] == "run-2"


def test_replacing_one_hour_preserves_adjacent_quarantine(spark, tmp_path, monkeypatch):
    next_hour = TARGET_HOUR + timedelta(hours=1)
    bronze = write_bronze_parquet(
        spark,
        tmp_path,
        valid_value(event_id="bad-5", speed_mps=999.0),
        valid_value(
            event_id="bad-6",
            event_time=next_hour.isoformat(),
            speed_mps=999.0,
        ),
    )
    config = cleansing_config(tmp_path, bronze)
    features = feature_config(tmp_path)
    captured_rows: list[dict[str, object]] = []
    stub_feature_job(monkeypatch, captured_rows)

    run_cleansing_job(
        spark,
        config,
        features,
        "run-5",
        TARGET_HOUR,
        ROAD_SNAPSHOT_DATE,
        FEATURE_VERSION,
        PROCESSED_AT,
    )
    run_cleansing_job(
        spark,
        config,
        features,
        "run-6",
        next_hour,
        ROAD_SNAPSHOT_DATE,
        FEATURE_VERSION,
        PROCESSED_AT,
    )

    hour_5 = spark.read.parquet(
        quarantine_hour_path(config.quarantine_output_path, TARGET_HOUR)
    ).collect()
    hour_6 = spark.read.parquet(
        quarantine_hour_path(config.quarantine_output_path, next_hour)
    ).collect()
    assert [row["event_id"] for row in hour_5] == ["bad-5"]
    assert [row["event_id"] for row in hour_6] == ["bad-6"]


def test_cleanse_and_feature_steps_are_timed_separately_in_the_fused_job(
    spark, tmp_path, monkeypatch, caplog
):
    """융합된 sensor_processing 안에서 T1과 T2 시간이 각각 따로 남아야 한다(#461, #527).

    예전에는 T2(features)만 재고 T1은 CLI 경계의 sensor_processing.job에서 빼서
    얻었다. 두 구간을 다 재므로 뺄셈이 필요 없다.
    """
    import json
    import logging

    from de4_core import PERF_LOG_PREFIX

    bronze = write_bronze_parquet(spark, tmp_path, valid_value(event_id="first"))

    with caplog.at_level(logging.INFO):
        run_job(spark, tmp_path, bronze, monkeypatch)

    payloads = [
        json.loads(message[len(PERF_LOG_PREFIX) + 1 :])
        for message in caplog.messages
        if message.startswith(f"{PERF_LOG_PREFIX} ")
    ]
    by_phase = {payload["phase"]: payload for payload in payloads}
    assert "sensor_processing.cleanse" in by_phase
    assert "sensor_processing.features" in by_phase
    # cleanse가 features보다 먼저 끝나야 두 구간이 겹치지 않는다.
    phases = [payload["phase"] for payload in payloads]
    assert phases.index("sensor_processing.cleanse") < phases.index(
        "sensor_processing.features"
    )
    # 정제를 통과한 1행 + 격리 0행.
    assert by_phase["sensor_processing.cleanse"]["rows"] == 1
