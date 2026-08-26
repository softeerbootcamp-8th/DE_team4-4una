"""jobs/failure_rules.py의 로그 분류를 실제 로그 모양의 샘플로 검증한다.

샘플은 이 저장소가 실제로 겪은 실패(#360/#368, #372, #386, #443, #508)의 로그 형태를
따른다. Airflow/S3/Slack 없이 문자열만으로 도는 순수 단위 테스트다.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from jobs.failure_rules import (
    MASK,
    UNKNOWN_ERROR,
    classify,
    extract_error_window,
    mask_values,
)


def _classify(log_text: str):
    return classify(extract_error_window(log_text))


EXECUTOR_OOM_LOG = """
25/08/26 04:11:02 INFO TaskSetManager: Starting task 0.0 in stage 3.0
25/08/26 04:11:40 WARN TaskSetManager: Lost task 12.0 in stage 3.0
25/08/26 04:11:40 ERROR YarnScheduler: Lost executor 4: Container killed by the framework, memory usage exceeded configured memory size
25/08/26 04:11:41 ERROR TaskSetManager: ExecutorLostFailure (executor 4 exited caused by one of the running tasks) Reason: Container killed, exit code 137
25/08/26 04:11:42 INFO DAGScheduler: Job 3 failed
"""

DRIVER_OOM_LOG = """
25/08/26 03:02:11 INFO SparkContext: Running Spark version 3.5.0
25/08/26 03:04:55 INFO GreatExpectations: validating gold table
Traceback (most recent call last):
  File "/usr/local/lib/python3.12/site-packages/batch_jobs/gold_audit_validation.py", line 112, in run
    frame = pandas.read_sql(f"SELECT * FROM {table}", connection)
MemoryError
25/08/26 03:05:02 INFO ShutdownHookManager: Shutdown hook called
"""

CAPACITY_LOG = """
25/08/26 05:00:11 INFO ExecutorAllocationManager: Requesting 4 new executors
25/08/26 05:02:30 ERROR AmazonEMRServerless: ApplicationMaxCapacityExceededException: The application has exceeded its maximum capacity
25/08/26 05:12:42 ERROR Client: Job run FAILED
"""

DISK_LOG = """
25/08/26 06:10:00 INFO BlockManager: writing shuffle data
25/08/26 06:12:31 ERROR DiskBlockObjectWriter: Uncaught exception while reverting partial writes
java.io.IOException: No space left on device
25/08/26 06:12:32 ERROR Executor: Exception in task 5.0 in stage 2.0
"""

DEPENDENCY_LOG = """
25/08/26 07:00:01 INFO SparkSubmit: submitting application
Traceback (most recent call last):
  File "/home/hadoop/entrypoint.py", line 1, in <module>
    from batch_jobs.pipeline import main
ModuleNotFoundError: No module named 'batch_jobs'
"""


def test_classifies_executor_memory_kill():
    result = _classify(EXECUTOR_OOM_LOG)

    assert result.error_type == "EXECUTOR_MEMORY_EXCEEDED"
    assert result.is_known
    assert any("ExecutorLostFailure" in line for line in result.evidence)


def test_classifies_driver_memory_kill():
    result = _classify(DRIVER_OOM_LOG)

    assert result.error_type == "DRIVER_MEMORY_EXCEEDED"
    assert any("MemoryError" in line for line in result.evidence)


def test_classifies_capacity_exceeded():
    assert _classify(CAPACITY_LOG).error_type == "EMR_CAPACITY_EXCEEDED"


def test_classifies_disk_exhaustion():
    assert _classify(DISK_LOG).error_type == "DISK_EXCEEDED"


def test_classifies_missing_python_module():
    assert _classify(DEPENDENCY_LOG).error_type == "PYTHON_DEPENDENCY"


def test_executor_oom_wins_over_capacity_symptom():
    # executor가 메모리로 죽으면 Spark가 executor를 다시 요청하다 capacity 예외를
    # 반복하고 마지막에 FAILED가 찍힌다. 첫 매치 승리로 두면 가장 나중 증상인 capacity가
    # 원인으로 잡혀 엉뚱한 곳을 보게 된다.
    combined = EXECUTOR_OOM_LOG + CAPACITY_LOG

    assert _classify(combined).error_type == "EXECUTOR_MEMORY_EXCEEDED"


def test_dependency_failure_wins_over_later_job_failed_line():
    combined = DEPENDENCY_LOG + "\n25/08/26 07:00:09 ERROR Client: Job run FAILED\n"

    assert _classify(combined).error_type == "PYTHON_DEPENDENCY"


def test_bare_exit_137_without_side_marker_is_not_guessed():
    # driver인지 executor인지 알 수 없으면 둘 중 하나로 단정하지 않는다 — 조치가 정반대다.
    log_text = "25/08/26 08:00:00 ERROR Client: Job failed with ExitCode: 137\n"

    result = _classify(log_text)

    assert result.error_type == UNKNOWN_ERROR
    assert not result.is_known


def test_unknown_error_returns_keyword_lines_without_inventing_a_cause():
    log_text = """
25/08/26 09:00:00 INFO Spark: starting
25/08/26 09:00:05 ERROR Client: something nobody has a rule for yet
"""

    result = _classify(log_text)

    assert result.error_type == UNKNOWN_ERROR
    assert result.rule is None
    assert any("nobody has a rule" in line for line in result.evidence)


def test_extract_error_window_drops_info_only_logs():
    log_text = "\n".join(f"25/08/26 10:00:{index:02d} INFO Spark: step" for index in range(50))

    assert extract_error_window(log_text) == []


def test_extract_error_window_keeps_lines_around_the_error():
    lines = [f"INFO line {index}" for index in range(100)]
    lines[60] = "ERROR boom"
    window = extract_error_window("\n".join(lines))

    assert "ERROR boom" in window
    assert "INFO line 0" not in window  # 앞쪽 무관한 INFO는 잘린다


def test_mask_values_removes_known_secret():
    log_text = "--conf spark.emr-serverless.driverEnv.POSTGRES_PASSWORD=sup3rs3cret --conf x=1"

    masked = mask_values(log_text, ["sup3rs3cret"])

    assert "sup3rs3cret" not in masked
    assert MASK in masked


def test_mask_values_masks_by_pattern_when_value_is_unknown():
    # Variable 조회가 실패해도 형태만으로 가릴 수 있어야 한다.
    log_text = "--conf spark.emr-serverless.driverEnv.POSTGRES_PASSWORD=sup3rs3cret"

    masked = mask_values(log_text, [])

    assert "sup3rs3cret" not in masked


def test_mask_values_ignores_very_short_values():
    # 짧은 값을 그대로 치환하면 무관한 문자열까지 ***로 바뀐다.
    log_text = "executor exited with code 137"

    assert mask_values(log_text, ["1"]) == log_text


# --- 실제 EMR Serverless 로그(성공한 hourly-sensor-processing Job Run)에서 가져온 회귀 테스트 ---

# 아래 줄들은 실제 Job Run driver stderr에서 그대로 옮긴 것이다. 모두 정상 실행에 매번
# 찍히는 줄인데, 초기 구현이 이걸 오류로 잡아 (1) 정상 종료를 driver 사망으로 판별하고
# (2) 근거 로그를 이 줄들로 채웠다.
REAL_SUCCESS_LOG = """26/08/26 01:15:00 INFO SparkContext: Running Spark version 4.0.2-amzn-0
26/08/26 01:15:01 INFO Utils: Successfully started service 'sparkDriver' on port 45847.
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
26/08/26 01:15:08 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: \
No executor found for 2406:da12:311:3800:9b52:cb96:f7e4:82a7:35042
26/08/26 01:16:13 INFO DAGScheduler: running: HashSet()
26/08/26 01:16:13 INFO DAGScheduler: waiting: HashSet()
26/08/26 01:16:13 INFO DAGScheduler: failed: HashSet()
26/08/26 01:20:00 INFO ShutdownHookManager: Shutdown hook called
26/08/26 01:20:00 INFO SparkContext: Invoking stop() from shutdown hook
"""


def test_real_successful_job_log_yields_no_error_window():
    assert extract_error_window(REAL_SUCCESS_LOG) == []


def test_real_successful_job_log_is_not_detected_as_driver_death():
    # "Shutdown hook called"은 정상 종료에도 매번 찍힌다 — 이걸 사망 신호로 쓰면
    # executor가 죽은 실패까지 전부 driver 문제로 오진한다.
    from jobs.failure_rules import _detect_terminated_side

    assert _detect_terminated_side(REAL_SUCCESS_LOG.splitlines()) is None


def test_benign_startup_noise_is_not_used_as_evidence():
    # 실제 실패 로그에는 이 정상 줄들이 항상 섞여 있다. 근거로 뽑히면 안 된다.
    log_text = REAL_SUCCESS_LOG + EXECUTOR_OOM_LOG

    result = _classify(log_text)

    assert result.error_type == "EXECUTOR_MEMORY_EXCEEDED"
    assert not any("SLF4J" in line for line in result.evidence)
    assert not any("HashSet()" in line for line in result.evidence)
