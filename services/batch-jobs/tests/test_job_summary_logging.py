"""각 job의 요약 로그가 입출력 경로와 대상 시간대를 담는지 검증한다 (#406).

Airflow Log 탭에서는 EMR Serverless Job Run의 로그를 볼 수 없고, 그 로그는 S3에
따로 쌓인다. 그래서 "이번 실행이 어느 시간대를, 어떤 경로를 대상으로 처리했는지"가
각 job의 요약 로그 한 줄에 다 들어 있어야 나중에 추적이 된다.

요약 로깅을 순수 함수로 분리해 두었기 때문에 SparkSession 없이 검증할 수 있다.
"""

from __future__ import annotations

import datetime as dt
import logging
from pathlib import Path

from batch_jobs.cleansing.job import _log_summary as log_cleansing_summary
from batch_jobs.comfort_score.standard_job import (
    StandardComfortScoreJobConfig,
    StandardComfortScoreJobSummary,
)
from batch_jobs.comfort_score.standard_job import _log_summary as log_standard_summary
from batch_jobs.gold_audit_validation import GoldAuditValidationConfig
from batch_jobs.gold_audit_validation import _log_summary as log_gold_audit_summary
from batch_jobs.hourly_comfort_job import (
    HourlyComfortJobConfig,
    HourlyComfortJobSummary,
)
from batch_jobs.hourly_comfort_job import _log_summary as log_hourly_comfort_summary

_TARGET_HOUR = dt.datetime(2026, 8, 24, 4, tzinfo=dt.UTC)


def test_hourly_comfort_summary_logs_paths_and_time_reference(caplog):
    config = HourlyComfortJobConfig(
        feature_input_path="s3://lake/silver/hourly_segment_features/hour=2026-08-24T04",
        score_output_path="s3://lake/silver/hourly_comfort_score/hour=2026-08-24T04",
        rejected_output_path="s3://lake/quarantine/hourly_comfort_score/hour=2026-08-24T04",
        scoring_config_path=Path("config/hourly_scoring.yaml"),
    )

    with caplog.at_level(logging.INFO):
        log_hourly_comfort_summary(
            config,
            run_id="run-1",
            processed_at=_TARGET_HOUR,
            summary=HourlyComfortJobSummary(scored_count=10, rejected_count=2),
        )

    message = caplog.text
    assert "s3://lake/silver/hourly_segment_features/hour=2026-08-24T04" in message
    assert "s3://lake/silver/hourly_comfort_score/hour=2026-08-24T04" in message
    assert "s3://lake/quarantine/hourly_comfort_score/hour=2026-08-24T04" in message
    assert "2026-08-24T04:00:00+00:00" in message
    assert "scored=10" in message
    assert "rejected=2" in message


def test_standard_summary_logs_paths_and_the_scoring_window(caplog):
    config = StandardComfortScoreJobConfig(
        data_lake_uri="s3://lake",
        road_environment_uri="s3://ref/road-environment",
        window_hours=24,
        comfort_score_config_path=Path("config/comfort_score.yaml"),
        gold_output_uri="s3://lake/gold/standard_segment_comfort_score",
        postgres_host="db.example.com",
        postgres_port=5432,
        postgres_db="de4",
        postgres_user="de4",
        postgres_password="super-secret",
    )

    with caplog.at_level(logging.INFO):
        log_standard_summary(
            config,
            as_of=_TARGET_HOUR,
            summary=StandardComfortScoreJobSummary(
                scored_count=5, merged_count=5, inserted_count=3, updated_count=2
            ),
            gold_version_uri="s3://lake/gold/standard_segment_comfort_score/version=42",
        )

    message = caplog.text
    assert "s3://lake" in message
    assert "s3://ref/road-environment" in message
    assert "s3://lake/gold/standard_segment_comfort_score/version=42" in message
    # 집계 대상 구간은 [as_of - window_hours, as_of)다 — 양 끝을 다 남긴다.
    assert "2026-08-23T04:00:00+00:00" in message
    assert "2026-08-24T04:00:00+00:00" in message
    assert "db.example.com:5432/de4" in message
    # 비밀번호는 어떤 경로로도 로그에 남지 않아야 한다.
    assert "super-secret" not in message


def test_gold_audit_summary_logs_the_data_docs_target_without_credentials(caplog):
    config = GoldAuditValidationConfig(
        postgres_host="db.example.com",
        postgres_port=5432,
        postgres_db="de4",
        postgres_user="de4",
        postgres_password="super-secret",
        s3_bucket="de4-data-quality-docs",
        range_suite_paths={},
        summary_suite_paths={},
    )

    with caplog.at_level(logging.INFO):
        log_gold_audit_summary(
            config,
            table="standard_segment_comfort_score",
            row_count=1234,
            success=True,
            data_docs_uri="s3://de4-data-quality-docs/data-quality-audit/gold/standard_segment_comfort_score",
        )

    message = caplog.text
    assert "standard_segment_comfort_score" in message
    assert "row_count=1234" in message
    assert (
        "s3://de4-data-quality-docs/data-quality-audit/gold/standard_segment_comfort_score"
        in message
    )
    assert "db.example.com:5432/de4" in message
    assert "super-secret" not in message


def test_cleansing_summary_logs_paths_and_the_feature_input_window(caplog):
    # 격리 쓰기가 feature job 안으로 옮겨가면서(#438) 실제로 쓰인 격리/feature
    # 경로는 feature_summary에만 남는다 — 설정 root가 아니라 그 값을 받는다.
    with caplog.at_level(logging.INFO):
        log_cleansing_summary(
            target_hour=_TARGET_HOUR,
            window_start=dt.datetime(2026, 8, 24, 3, tzinfo=dt.UTC),
            window_end=_TARGET_HOUR,
            bronze_input_path="s3://lake/bronze/sensor-events",
            quarantine_output_path="s3://lake/quarantine/sensor-events/hour=2026-08-24T04",
            feature_output_path="s3://lake/silver/hourly_segment_features/hour=2026-08-24T04",
            processed_count=100,
            accepted_count=88,
            cleansing_quarantined_count=4,
            map_matching_quarantined_count=2,
            quarantined_count=6,
            feature_count=88,
        )

    message = caplog.text
    assert "s3://lake/bronze/sensor-events" in message
    assert "s3://lake/quarantine/sensor-events/hour=2026-08-24T04" in message
    assert "s3://lake/silver/hourly_segment_features/hour=2026-08-24T04" in message
    assert "2026-08-24T03:00:00+00:00" in message
    assert "2026-08-24T04:00:00+00:00" in message
    # develop에서 들어온 세분화된 건수(#438)도 그대로 남아야 한다.
    assert "accepted=88" in message
    assert "cleansing_quarantined=4" in message
    assert "map_match_quarantined=2" in message
    assert "quarantined=6" in message
