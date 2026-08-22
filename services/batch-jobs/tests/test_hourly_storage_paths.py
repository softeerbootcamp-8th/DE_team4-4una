"""Regression tests for cleansing/hourly_storage.py path helpers on S3 URIs (#290).

`quarantine_hour_path`/`_backup_path`는 순수 문자열 조립이라 로컬 경로와
`s3://` URI를 구분하지 않고 그대로 동작해야 한다 — EMR Serverless 전환 전에
이 가정을 명시적으로 고정해 둔다.
"""

from __future__ import annotations

from datetime import UTC, datetime

from batch_jobs.cleansing.hourly_storage import _backup_path, quarantine_hour_path


def test_quarantine_hour_path_builds_correct_subpath_for_an_s3_root():
    target_hour = datetime(2026, 8, 1, 5, tzinfo=UTC)

    path = quarantine_hour_path("s3://de4-silver/hourly_quarantine", target_hour)

    assert path == (
        "s3://de4-silver/hourly_quarantine/target_date=2026-08-01/target_hour=05"
    )


def test_backup_path_derives_a_sibling_key_under_an_s3_root():
    final_path = "s3://de4-silver/hourly_quarantine/target_date=2026-08-01/target_hour=05"

    backup_path = _backup_path(final_path)

    assert backup_path == (
        "s3://de4-silver/hourly_quarantine/target_date=2026-08-01/_backup_target_hour=05"
    )
