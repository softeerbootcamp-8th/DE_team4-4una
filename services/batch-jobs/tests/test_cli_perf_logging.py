"""CLI 커맨드 경계가 PERF 로그를 남기는지 검증한다 (#461).

Job Run 총시간(EMR GetJobRun)만으로는 Spark 세션을 띄우는 데 쓴 시간과 job 로직에
쓴 시간이 구분되지 않는다. 베이스라인(#460)의 "오버헤드 대 실제 계산" 분해가
이 두 줄에 의존한다.

계측은 액션을 새로 강제하지 않는다 — 이미 끝난 호출의 벽시계 시간만 잰다.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from dataclasses import dataclass

import pytest
from batch_jobs.cli import main
from de4_core import PERF_LOG_PREFIX


@dataclass(frozen=True)
class FakeValidationSummary:
    target_hour: dt.datetime = dt.datetime(2026, 8, 25, 2, tzinfo=dt.UTC)
    row_count: int = 10
    success: bool = True


class FakeSpark:
    def __init__(self) -> None:
        self.stopped = False

    def stop(self) -> None:
        self.stopped = True


def _perf_payloads(caplog) -> dict[str, dict]:
    payloads = {}
    for message in caplog.messages:
        if not message.startswith(f"{PERF_LOG_PREFIX} "):
            continue
        payload = json.loads(message[len(PERF_LOG_PREFIX) + 1 :])
        payloads[payload["phase"]] = payload
    return payloads


@pytest.fixture
def _fake_hourly_scoring_validation(monkeypatch):
    from batch_jobs import hourly_scoring_validation

    monkeypatch.setattr(
        hourly_scoring_validation, "build_spark_session", lambda: FakeSpark()
    )
    monkeypatch.setattr(
        hourly_scoring_validation,
        "run_hourly_scoring_validation",
        lambda spark, config, target_hour: FakeValidationSummary(),
    )


def test_cli_logs_spark_session_build_and_job_separately(
    caplog, capsys, _fake_hourly_scoring_validation
):
    with caplog.at_level(logging.INFO):
        main(
            [
                "validate-hourly-scoring",
                "--target-hour",
                "2026-08-25T02:00:00+00:00",
                "--output-path",
                "s3://lake/scores",
            ]
        )

    capsys.readouterr()
    payloads = _perf_payloads(caplog)
    assert set(payloads) == {
        "validate_hourly_scoring.spark_session",
        "validate_hourly_scoring.job",
    }
    assert payloads["validate_hourly_scoring.job"]["ok"] is True


@pytest.fixture
def _postgres_env(monkeypatch):
    """*_ValidationConfig.from_env()가 요구하는 값만 채운다. 실제 접속은 하지 않는다."""
    for key, value in {
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "de4",
        "POSTGRES_USER": "de4",
        "POSTGRES_PASSWORD": "unused-in-tests",
    }.items():
        monkeypatch.setenv(key, value)


class FakeConnection:
    def close(self) -> None:
        pass


@dataclass(frozen=True)
class FakeSummary:
    """각 job summary가 print하는 필드를 전부 갖는 하나의 대역."""

    scored_count: int = 1
    rejected_count: int = 0
    merged_count: int = 1
    inserted_count: int = 1
    updated_count: int = 0
    row_count: int = 1
    success: bool = True
    feature_row_count: int = 1
    accepted_sample_count: int = 1
    quarantine_row_count: int = 0
    cleansing_quarantine_row_count: int = 0
    map_matching_quarantine_row_count: int = 0
    quarantine_rate: float = 0.0
    cleansing_quarantine_rate: float = 0.0
    map_matching_quarantine_rate: float = 0.0
    input_count: int = 1
    processed_count: int = 1
    accepted_count: int = 1
    cleansing_quarantined_count: int = 0
    map_matching_quarantined_count: int = 0
    quarantined_count: int = 0
    result_count: int = 1
    output_path: str = "s3://lake/silver/hourly_segment_features"
    run_id: str = "run-1"
    feature_summary: object = None


def _fake_cleansing_summary() -> FakeSummary:
    return FakeSummary(
        feature_summary=FakeSummary(row_count=1),
    )


_CLEANSE_ARGV = [
    "cleanse-sensor-events",
    "--run-id",
    "run-1",
    "--target-hour",
    "2026-08-25T02:00:00+00:00",
    "--road-snapshot-date",
    "2026-08-01",
    "--feature-version",
    "v1",
]


def test_cleanse_sensor_events_logs_session_job_and_feature_split(
    caplog, capsys, monkeypatch
):
    """융합된 sensor_processing 안에서 T1 cleanse와 T2 feature 시간이 갈려야 한다."""
    from batch_jobs.cleansing import job as cleansing_job

    monkeypatch.setattr(cleansing_job, "build_spark_session", lambda: FakeSpark())
    monkeypatch.setattr(
        cleansing_job,
        "run_cleansing_job",
        lambda *args, **kwargs: _fake_cleansing_summary(),
    )

    with caplog.at_level(logging.INFO):
        main(_CLEANSE_ARGV)

    capsys.readouterr()
    assert set(_perf_payloads(caplog)) == {
        "sensor_processing.spark_session",
        "sensor_processing.job",
    }


def test_score_hourly_comfort_logs_session_and_job(caplog, capsys, monkeypatch):
    from batch_jobs import hourly_comfort_job

    monkeypatch.setattr(hourly_comfort_job, "build_spark_session", lambda: FakeSpark())
    monkeypatch.setattr(
        hourly_comfort_job, "run_hourly_comfort_job", lambda *args: FakeSummary()
    )

    with caplog.at_level(logging.INFO):
        main(
            [
                "score-hourly-comfort",
                "--run-id",
                "run-1",
                "--target-hour",
                "2026-08-25T02:00:00+00:00",
            ]
        )

    capsys.readouterr()
    assert set(_perf_payloads(caplog)) == {
        "hourly_scoring.spark_session",
        "hourly_scoring.job",
    }


def test_validate_sensor_processing_logs_session_and_job(caplog, capsys, monkeypatch):
    from batch_jobs import sensor_processing_validation

    monkeypatch.setattr(
        sensor_processing_validation, "build_spark_session", lambda: FakeSpark()
    )
    monkeypatch.setattr(
        sensor_processing_validation,
        "run_sensor_processing_validation",
        lambda *args: FakeSummary(),
    )

    with caplog.at_level(logging.INFO):
        main(
            [
                "validate-sensor-processing",
                "--target-hour",
                "2026-08-25T02:00:00+00:00",
            ]
        )

    capsys.readouterr()
    assert set(_perf_payloads(caplog)) == {
        "validate_sensor_processing.spark_session",
        "validate_sensor_processing.job",
    }


def test_load_standard_score_logs_session_connect_and_job(
    caplog, capsys, monkeypatch, _postgres_env
):
    import psycopg2
    from batch_jobs.comfort_score import standard_job

    monkeypatch.setattr(standard_job, "build_spark_session", lambda: FakeSpark())
    monkeypatch.setattr(
        standard_job, "run_standard_comfort_score_job", lambda *args: FakeSummary()
    )
    monkeypatch.setattr(psycopg2, "connect", lambda **kwargs: FakeConnection())

    with caplog.at_level(logging.INFO):
        main(
            [
                "load-standard-segment-comfort-score",
                "--as-of",
                "2026-08-25T03:00:00+00:00",
            ]
        )

    capsys.readouterr()
    assert set(_perf_payloads(caplog)) == {
        "standard_score.spark_session",
        "standard_score.postgres_connect",
        "standard_score.job",
    }


def test_validate_standard_score_logs_connect_and_job_without_spark(
    caplog, capsys, monkeypatch, _postgres_env
):
    """이 커맨드는 Spark를 쓰지 않는다 — spark_session 구간이 없어야 한다."""
    import psycopg2
    from batch_jobs import standard_score_validation

    monkeypatch.setattr(
        standard_score_validation,
        "run_standard_score_validation",
        lambda *args: FakeSummary(),
    )
    monkeypatch.setattr(psycopg2, "connect", lambda **kwargs: FakeConnection())

    with caplog.at_level(logging.INFO):
        main(["validate-standard-score", "--as-of", "2026-08-25T03:00:00+00:00"])

    capsys.readouterr()
    assert set(_perf_payloads(caplog)) == {
        "validate_standard_score.postgres_connect",
        "validate_standard_score.job",
    }
