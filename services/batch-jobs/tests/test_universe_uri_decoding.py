"""Regression tests for comfort_score/universe.py's local file:// URI decoding (#290).

`_decode_local_file_uri()`는 `de4_core.join_uri()`가 로컬 `file://` URI에서
`=`를 `%3D`로 인코딩하는 걸 Spark가 못 읽어서 디코딩한다. `s3://` URI는 이미
인코딩되지 않으므로 그대로 통과해야 한다 — EMR Serverless에서 S3 경로를 쓸 때
이 함수가 실수로 `=`를 건드리지 않는지 고정해 둔다.
"""

from __future__ import annotations

from batch_jobs.comfort_score.universe import _decode_local_file_uri


def test_s3_uri_passes_through_unchanged():
    uri = "s3://de4-lake/prepared/reference_date=2026-08-01/build_id=7/segments.parquet"

    assert _decode_local_file_uri(uri) == uri


def test_local_file_uri_is_percent_decoded():
    uri = "file:///data/local-lake/reference_date%3D2026-08-01/build_id%3D7/segments.parquet"

    assert _decode_local_file_uri(uri) == (
        "file:///data/local-lake/reference_date=2026-08-01/build_id=7/segments.parquet"
    )
