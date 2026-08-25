"""PERF 로그 수집 테스트.

fixture의 스트림 배치는 실제 Job Run을 따른다. batch-jobs의 PERF 줄은 driver
**stdout**에 남고 stderr에는 Spark 자신의 log4j 출력만 있다. stderr에 심어 둔
`postgres_merge` 한 줄은 실제 로그를 흉내낸 것이 아니라, 두 스트림에 같은 줄이
잡혔을 때 중복으로 세지 않는지 고정하기 위한 것이다.
"""

import gzip

from de4_core import ObjectStore
from pipeline_perf.perf_log import collect_perf_phases, parse_perf_lines


def test_collects_perf_phases_from_the_driver_stdout(job_run_dir):
    result = collect_perf_phases(ObjectStore(), str(job_run_dir))

    assert result["available"] is True
    assert [phase["phase"] for phase in result["phases"]] == [
        "standard_score.gold_snapshot_write",
        "standard_score.postgres_merge",
    ]
    assert result["phases"][1] == {
        "phase": "standard_score.postgres_merge",
        "elapsed_s": 160.193,
        "ok": True,
        "rows": 184213,
    }


def test_both_driver_streams_are_read_and_duplicates_counted_once(job_run_dir):
    result = collect_perf_phases(ObjectStore(), str(job_run_dir))

    assert [uri.rsplit("/", 1)[-1] for uri in result["source_uris"]] == ["stdout", "stderr"]
    # 같은 payload가 stdout과 stderr 양쪽에 있어도 한 번만 센다.
    assert sum(1 for phase in result["phases"] if phase["phase"].endswith("postgres_merge")) == 1


def test_gzipped_driver_logs_are_decompressed(tmp_path):
    driver_dir = tmp_path / "SPARK_DRIVER"
    driver_dir.mkdir()
    line = 'INFO x: PERF {"phase": "current_score.refresh", "elapsed_s": 1.5, "ok": true}\n'
    (driver_dir / "stdout.gz").write_bytes(gzip.compress(line.encode()))

    result = collect_perf_phases(ObjectStore(), str(tmp_path))

    assert result["source_uris"][0].endswith("stdout.gz")
    assert result["phases"] == [{"phase": "current_score.refresh", "elapsed_s": 1.5, "ok": True}]


def test_missing_driver_logs_are_reported_as_unavailable(tmp_path):
    result = collect_perf_phases(ObjectStore(), str(tmp_path))

    assert result == {"source_uris": [], "available": False, "phases": []}


def test_lines_without_the_prefix_or_with_broken_json_are_skipped():
    lines = [
        "INFO SparkContext: Running Spark version 4.0.2",
        'INFO x: PERF {"phase": "a", "elapsed_s": }',
        'INFO x: PERF {"phase": "b", "elapsed_s": 2.0, "ok": false}',
    ]

    assert list(parse_perf_lines(lines)) == [{"phase": "b", "elapsed_s": 2.0, "ok": False}]
