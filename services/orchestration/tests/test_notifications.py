"""dags/notifications.py의 Slack 콜백(#409)을 가짜 Airflow context로 검증한다.

실제 Airflow 실행/Slack 전송/S3 쓰기 없이, on_failure_callback/on_success_callback이
받는 context의 필요한 속성만 흉내 낸 가짜 객체를 주입한다. 실제 Airflow
context 모양과 일치하는지는 로컬 Airflow 수동 검증(README)에서 확인한다.
"""

from __future__ import annotations

import datetime
import sys
from dataclasses import dataclass, field
from pathlib import Path
from unittest.mock import MagicMock

import pytest

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(DAGS_DIR))
# notifications.py가 콜백 본문 안에서 `from jobs.dag_owners import ...`를 지연 import한다.
# dags/만 sys.path에 있으면 그 top-level `jobs` 패키지가 보이지 않으므로,
# test_dag_owners.py와 같은 방식으로 services/orchestration 자체도 추가한다.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import notifications


@dataclass
class _FakeTaskGroup:
    group_id: str | None


@dataclass
class _FakeTask:
    task_group: _FakeTaskGroup | None = None


@dataclass
class _FakeTaskInstance:
    task_id: str
    log_url: str = "http://localhost:8080/dags/x/grid?task_id=y"
    xcom_store: dict = field(default_factory=dict)

    def xcom_pull(self, task_ids=None, key="return_value"):
        return self.xcom_store.get((task_ids, key))


@dataclass
class _FakeDag:
    dag_id: str


@dataclass
class _FakeDagRun:
    # 실제 Airflow 3 콜백 context의 dag_run(Pydantic 모델)에 맞춰 dag_id/run_id만
    # 흉내 낸다 — get_absolute_url()/get_task_instance()는 구버전 ORM DagRun 전용이라
    # 더 이상 없다(#409 로컬 검증 중 실제 발견).
    dag_id: str
    run_id: str = "manual__2026-08-24T04:17:00+00:00"


def _context(
    dag_id: str,
    task_instance: _FakeTaskInstance,
    *,
    task_group_id: str | None = None,
    exception=None,
    dag_run: _FakeDagRun | None = None,
) -> dict:
    return {
        "dag": _FakeDag(dag_id),
        "task": _FakeTask(
            task_group=_FakeTaskGroup(task_group_id) if task_group_id is not None else None
        ),
        "task_instance": task_instance,
        "dag_run": dag_run if dag_run is not None else _FakeDagRun(dag_id=dag_id),
        "run_id": "manual__2026-08-24T04:17:00+00:00",
        "logical_date": "2026-08-24T04:17:00+00:00",
        "exception": exception,
    }


@pytest.fixture(autouse=True)
def _registry(monkeypatch, tmp_path):
    config_path = tmp_path / "dag_owners.yaml"
    config_path.write_text(
        """
        users:
          alice:
            email: alice@example.com
          bob:
            slack_id: U0456GHIJKL

        dags:
          bronze_compaction:
            owner: bob
            severity: low
          standard_score_pipeline:
            owner: alice
            severity: critical
            tasks:
              transform_sensor_readings:
                owner: bob
                severity: high
          current_score_pipeline:
            owner: alice
            severity: high
        """
    )
    monkeypatch.setenv("DAG_OWNERS_CONFIG_PATH", str(config_path))


@pytest.fixture
def _fake_slack_hook(monkeypatch):
    hook = MagicMock()
    hook.client.users_lookupByEmail.return_value = {"user": {"id": "U0999LOOKEDUP"}}
    monkeypatch.setattr(notifications, "_build_slack_hook", lambda: hook)
    monkeypatch.setenv("AIRFLOW_VAR_SLACK_ALERT_CHANNEL", "#alerts")

    class _FakeVariable:
        @staticmethod
        def get(key, default=None):
            import os

            return os.environ.get(f"AIRFLOW_VAR_{key}", default)

    monkeypatch.setattr(notifications, "_slack_channel", lambda: _FakeVariable.get("SLACK_ALERT_CHANNEL"))
    return hook


@pytest.fixture
def _fake_object_store(monkeypatch):
    # on_failure_callback이 `from de4_core import ObjectStore`를 함수 안에서 지연
    # import하므로, 실제 de4_core.ObjectStore 속성 자체를 patch해야 잡힌다.
    written: dict[str, bytes] = {}

    class _FakeObjectStore:
        def write_bytes(self, uri, value):
            written[uri] = value

    monkeypatch.setattr("de4_core.ObjectStore", _FakeObjectStore)
    # _failed_tasks_s3_root()는 airflow.sdk.Variable.get()을 호출하는데, 이건 실제
    # Airflow DB/API 연결이 필요해 단위 테스트에서 그대로 두면 실패하거나 멈춘다.
    # _slack_channel과 같은 이유로 여기서도 우회한다.
    monkeypatch.setattr(
        notifications,
        "_failed_tasks_s3_root",
        lambda: notifications._DEFAULT_FAILED_TASKS_S3_ROOT,
    )
    # 이 fake에는 read_bytes가 없어 EMR driver 로그 조회가 실패한다(진단 없음 경로).
    # 재시도 대기까지 실제로 자면 테스트가 느려지므로 0으로 둔다.
    monkeypatch.setattr(notifications, "_LOG_FETCH_RETRY_SECONDS", 0)
    return written


def test_failure_callback_mentions_slack_id_owner_directly(_fake_slack_hook, _fake_object_store):
    context = _context(
        "bronze_compaction",
        _FakeTaskInstance(task_id="compact_zone_weather_snapshot"),
        exception=ValueError("boom"),
    )

    notifications.on_failure_callback(context)

    _fake_slack_hook.client.users_lookupByEmail.assert_not_called()
    text = _fake_slack_hook.client.chat_postMessage.call_args.kwargs["text"]
    assert "<@U0456GHIJKL>" in text
    assert "⚪ low" in text
    assert "compact_zone_weather_snapshot" in text
    assert "boom" in text


def test_failure_callback_resolves_email_owner_via_slack_lookup(_fake_slack_hook, _fake_object_store):
    context = _context(
        "standard_score_pipeline", _FakeTaskInstance(task_id="report_pipeline_counts")
    )

    notifications.on_failure_callback(context)

    _fake_slack_hook.client.users_lookupByEmail.assert_called_once_with(email="alice@example.com")
    text = _fake_slack_hook.client.chat_postMessage.call_args.kwargs["text"]
    assert "<@U0999LOOKEDUP>" in text
    assert "🔴 critical" in text


def test_failure_callback_uses_task_level_override_over_dag_default(_fake_slack_hook, _fake_object_store):
    context = _context(
        "standard_score_pipeline",
        _FakeTaskInstance(task_id="transform_sensor_readings"),
        task_group_id="sensor_processing",
    )

    notifications.on_failure_callback(context)

    text = _fake_slack_hook.client.chat_postMessage.call_args.kwargs["text"]
    assert "<@U0456GHIJKL>" in text  # task 오버라이드(bob)
    assert "🟠 high" in text


def test_failure_callback_includes_emr_s3_logs_link_when_xcom_present(_fake_slack_hook, _fake_object_store):
    from airflow.providers.amazon.aws.links.emr import EmrServerlessS3LogsLink

    task_instance = _FakeTaskInstance(task_id="compact_zone_weather_snapshot")
    task_instance.xcom_store[
        ("compact_zone_weather_snapshot", EmrServerlessS3LogsLink.key)
    ] = {
        "region_name": "ap-northeast-2",
        "aws_domain": "aws.amazon.com",
        "log_uri": "s3://de4-emr-serverless-logs/",
        "application_id": "app-1",
        "job_run_id": "job-1",
    }
    context = _context("bronze_compaction", task_instance)

    notifications.on_failure_callback(context)

    text = _fake_slack_hook.client.chat_postMessage.call_args.kwargs["text"]
    assert "app-1" in text
    assert "job-1" in text


def test_failure_callback_omits_emr_link_when_not_an_emr_task(_fake_slack_hook, _fake_object_store):
    context = _context("bronze_compaction", _FakeTaskInstance(task_id="compact_zone_weather_snapshot"))

    notifications.on_failure_callback(context)

    text = _fake_slack_hook.client.chat_postMessage.call_args.kwargs["text"]
    assert "EMR Serverless 원본 로그" not in text


def test_failure_callback_writes_structured_json_record_to_s3(_fake_slack_hook, _fake_object_store):
    import json

    context = _context(
        "bronze_compaction",
        _FakeTaskInstance(task_id="compact_zone_weather_snapshot"),
        exception=ValueError("boom"),
    )

    notifications.on_failure_callback(context)

    assert len(_fake_object_store) == 1
    written_uri, written_bytes = next(iter(_fake_object_store.items()))
    assert written_uri.startswith(
        "s3://de4-observability-473551908409-ap-northeast-2-an/airflow/failed-tasks/"
        "bronze_compaction/compact_zone_weather_snapshot/"
    )
    record = json.loads(written_bytes)
    assert record["dag_id"] == "bronze_compaction"
    assert record["task_id"] == "compact_zone_weather_snapshot"
    assert record["exception"] == "boom"
    assert record["owner"] == "bob"
    assert record["severity"] == "low"
    assert record["counts"] is None  # 이 DAG는 단일 task라 이번 실행에서 아무 요약도 안 남았다.

    text = _fake_slack_hook.client.chat_postMessage.call_args.kwargs["text"]
    assert "실패 상세 기록 열기" in text
    assert (
        "console.aws.amazon.com/s3/object/de4-observability-473551908409-ap-northeast-2-an"
        in text
    )


def test_failure_callback_handles_missing_logical_date(_fake_slack_hook, _fake_object_store):
    # data_interval 없이 트리거된 bare manual run은 context에 logical_date 키
    # 자체가 없다 — EC2 검증 중 KeyError로 실제 발견(#409). 이 경우에도 Slack
    # 알림 자체는 반드시 나가야 한다.
    context = _context(
        "bronze_compaction", _FakeTaskInstance(task_id="compact_zone_weather_snapshot")
    )
    del context["logical_date"]

    notifications.on_failure_callback(context)

    _fake_slack_hook.client.chat_postMessage.assert_called_once()
    text = _fake_slack_hook.client.chat_postMessage.call_args.kwargs["text"]
    assert "처리 일자" in text


def test_failure_callback_includes_counts_when_upstream_summary_already_succeeded(
    _fake_slack_hook, _fake_object_store
):
    # 실제 Airflow에서 task_instance.xcom_pull(task_ids=X)은 호출한 task_instance
    # 자신이 아니라 dag run 내 임의 task X의 XCom을 조회한다 — dag_run.get_task_instance()는
    # 더 이상 쓰지 않는다(#409 로컬 검증 중 실제 발견).
    task_instance = _FakeTaskInstance(task_id="some_later_task")
    task_instance.xcom_store[("report_pipeline_counts", "return_value")] = {
        "standard_segment_comfort_score_count": 80
    }
    context = _context("standard_score_pipeline", task_instance)

    notifications.on_failure_callback(context)

    text = _fake_slack_hook.client.chat_postMessage.call_args.kwargs["text"]
    assert "standard_segment_comfort_score_count" in text
    assert "80" in text


def test_success_callback_posts_summary_from_mapped_task(_fake_slack_hook):
    # 실제 Airflow 3의 DAG-level 성공 콜백 context에도 "마지막으로 실행된 task"의
    # RuntimeTaskInstance가 context["task_instance"]로 들어온다 — 그 xcom_pull(task_ids=X)로
    # dag run 내 임의 task X의 XCom을 조회할 수 있다(#409 로컬 검증 중 실제 발견).
    task_instance = _FakeTaskInstance(task_id="compute_current_score")
    task_instance.xcom_store[("compute_current_score", "return_value")] = {"upserted_count": 42}
    context = {
        "dag": _FakeDag("current_score_pipeline"),
        "dag_run": _FakeDagRun(dag_id="current_score_pipeline"),
        "task_instance": task_instance,
    }

    notifications.on_success_callback(context)

    text = _fake_slack_hook.client.chat_postMessage.call_args.kwargs["text"]
    assert "upserted_count" in text
    assert "42" in text
    assert "alice" in text


def test_success_callback_without_summary_task_still_posts(_fake_slack_hook):
    context = {
        "dag": _FakeDag("bronze_compaction"),
        "dag_run": _FakeDagRun(dag_id="bronze_compaction"),
        "task_instance": _FakeTaskInstance(task_id="compact_zone_weather_snapshot"),
    }

    notifications.on_success_callback(context)

    _fake_slack_hook.client.chat_postMessage.assert_called_once()


def test_failure_callback_still_posts_when_slack_email_lookup_fails(
    _fake_slack_hook, _fake_object_store
):
    # 담당자 멘션은 부가 정보다 — 이메일이 Slack에 없거나 API가 실패해도 핵심
    # 알림까지 막아서는 안 된다(#409). standard_score_pipeline의 담당자 alice는
    # slack_id가 없어 users_lookupByEmail 경로를 탄다.
    _fake_slack_hook.client.users_lookupByEmail.side_effect = RuntimeError("users_not_found")
    context = _context(
        "standard_score_pipeline", _FakeTaskInstance(task_id="report_pipeline_counts")
    )

    notifications.on_failure_callback(context)

    _fake_slack_hook.client.chat_postMessage.assert_called_once()
    text = _fake_slack_hook.client.chat_postMessage.call_args.kwargs["text"]
    assert "alice" in text  # 멘션이 안 되더라도 누구 담당인지는 남아야 한다


def test_failure_callback_still_posts_when_s3_record_write_fails(_fake_slack_hook, monkeypatch):
    # S3 실패 기록도 부가 정보다 — 버킷 오설정/권한 문제로 쓰기가 실패해도 Slack
    # 알림 자체는 나가야 한다(#409 EC2 검증에서 이것 때문에 알림이 침묵했다).
    def _boom(*args, **kwargs):
        raise ValueError("file URI must contain a path")

    monkeypatch.setattr(notifications, "_write_failure_record", _boom)
    context = _context(
        "bronze_compaction",
        _FakeTaskInstance(task_id="compact_zone_weather_snapshot"),
        exception=ValueError("boom"),
    )

    notifications.on_failure_callback(context)

    _fake_slack_hook.client.chat_postMessage.assert_called_once()
    text = _fake_slack_hook.client.chat_postMessage.call_args.kwargs["text"]
    assert "compact_zone_weather_snapshot" in text
    assert "boom" in text
    assert "실패 상세 기록 열기" not in text  # 링크만 빠지고 나머지는 그대로


def test_failed_tasks_s3_root_falls_back_to_default_when_variable_is_empty_string(monkeypatch):
    # infra/compose/airflow.yaml이 AIRFLOW_VAR_OBSERVABILITY_FAILED_TASKS_S3_URI를
    # 항상 선언해서, 호스트 .env에 값이 없으면 docker compose가 컨테이너에 빈
    # 문자열로 채운다 — EC2 실제 배포에서 이 때문에 Variable.get(key, default=...)이
    # "존재하는 값"으로 보고 빈 문자열을 그대로 돌려줘 parse_uri("")가 터지는 것을
    # 발견했다(#409). 빈 문자열도 미설정과 동일하게 기본값으로 대체해야 한다.
    class _FakeVariable:
        @staticmethod
        def get(key, default=None):
            return "" if key == "OBSERVABILITY_FAILED_TASKS_S3_URI" else default

    monkeypatch.setattr("airflow.sdk.Variable", _FakeVariable)

    assert notifications._failed_tasks_s3_root() == notifications._DEFAULT_FAILED_TASKS_S3_ROOT


# --- EMR driver 로그 진단 (실패 알림에 원인 분류를 붙인다) ---


def _emr_task_instance(task_id: str = "transform_sensor_readings") -> _FakeTaskInstance:
    from airflow.providers.amazon.aws.links.emr import EmrServerlessS3LogsLink

    task_instance = _FakeTaskInstance(task_id=task_id)
    task_instance.xcom_store[(task_id, EmrServerlessS3LogsLink.key)] = {
        "region_name": "ap-northeast-2",
        "aws_domain": "aws.amazon.com",
        "log_uri": "s3://de4-observability/emr-serverless/logs/",
        "application_id": "app-1",
        "job_run_id": "job-1",
    }
    return task_instance


@pytest.fixture
def _fake_emr_log(monkeypatch):
    """driver stderr.gz를 흉내 낸다. holder["text"]에 로그를, None이면 조회 실패."""
    import gzip

    holder: dict = {"text": None, "written": {}, "read_uris": []}

    class _FakeObjectStore:
        def write_bytes(self, uri, value):
            holder["written"][uri] = value

        def read_bytes(self, uri):
            holder["read_uris"].append(uri)
            if holder["text"] is None:
                raise FileNotFoundError(uri)
            return gzip.compress(holder["text"].encode("utf-8"))

    monkeypatch.setattr("de4_core.ObjectStore", _FakeObjectStore)
    monkeypatch.setattr(
        notifications, "_failed_tasks_s3_root", lambda: notifications._DEFAULT_FAILED_TASKS_S3_ROOT
    )
    # 조회 실패 경로가 실제로 5초씩 잠들지 않게 한다.
    monkeypatch.setattr(notifications, "_LOG_FETCH_RETRY_SECONDS", 0)

    class _FakeVariable:
        @staticmethod
        def get(key, default=None):
            return "sup3rs3cret" if key == "POSTGRES_PASSWORD" else default

    monkeypatch.setattr("airflow.sdk.Variable", _FakeVariable)
    return holder


_EXECUTOR_OOM_LOG = (
    "25/08/26 04:11:40 ERROR YarnScheduler: Lost executor 4: Container killed by the "
    "framework, memory usage exceeded configured memory size\n"
    "25/08/26 04:11:41 ERROR TaskSetManager: ExecutorLostFailure (executor 4 exited) "
    "Reason: Container killed, exit code 137\n"
)


def test_failure_callback_reports_classified_cause_in_korean(_fake_slack_hook, _fake_emr_log):
    _fake_emr_log["text"] = _EXECUTOR_OOM_LOG
    context = _context("standard_score_pipeline", _emr_task_instance())

    notifications.on_failure_callback(context)

    text = _fake_slack_hook.client.chat_postMessage.call_args.kwargs["text"]
    assert "EXECUTOR_MEMORY_EXCEEDED" in text
    assert "메모리 부족으로 종료" in text
    assert "ExecutorLostFailure" in text  # 근거 로그 줄
    assert "spark.executor.memoryOverhead" in text  # 확인 사항


def test_failure_callback_reads_driver_logs_from_expected_s3_paths(
    _fake_slack_hook, _fake_emr_log
):
    _fake_emr_log["text"] = _EXECUTOR_OOM_LOG
    notifications.on_failure_callback(_context("standard_score_pipeline", _emr_task_instance()))

    root = "s3://de4-observability/emr-serverless/logs/applications/app-1/jobs/job-1/"
    assert _fake_emr_log["read_uris"] == [
        root + "SPARK_DRIVER/stderr.gz",
        root + "SPARK_DRIVER/stdout.gz",
    ]


def test_failure_callback_masks_postgres_password_from_driver_log(
    _fake_slack_hook, _fake_emr_log
):
    # driver는 기동 시 자기 sparkSubmitParameters를 stderr에 그대로 찍는다.
    _fake_emr_log["text"] = (
        "25/08/26 04:00:00 INFO SparkSubmit: --conf "
        "spark.emr-serverless.driverEnv.POSTGRES_PASSWORD=sup3rs3cret\n" + _EXECUTOR_OOM_LOG
    )
    context = _context("standard_score_pipeline", _emr_task_instance())

    notifications.on_failure_callback(context)

    text = _fake_slack_hook.client.chat_postMessage.call_args.kwargs["text"]
    assert "sup3rs3cret" not in text
    for written in _fake_emr_log["written"].values():
        assert b"sup3rs3cret" not in written


def test_failure_callback_marks_unknown_without_inventing_a_cause(
    _fake_slack_hook, _fake_emr_log
):
    _fake_emr_log["text"] = "25/08/26 09:00:05 ERROR Client: nobody has a rule for this yet\n"
    context = _context("standard_score_pipeline", _emr_task_instance())

    notifications.on_failure_callback(context)

    text = _fake_slack_hook.client.chat_postMessage.call_args.kwargs["text"]
    assert "UNKNOWN_ERROR" in text
    assert "등록된 룰에 해당하지 않습니다" in text
    assert "nobody has a rule" in text
    assert "확인 사항" not in text  # 지어낸 조치를 붙이지 않는다


def test_failure_callback_records_error_type_for_unknown_ratio_tracking(
    _fake_slack_hook, _fake_emr_log
):
    import json

    _fake_emr_log["text"] = _EXECUTOR_OOM_LOG
    notifications.on_failure_callback(_context("standard_score_pipeline", _emr_task_instance()))

    record = json.loads(next(iter(_fake_emr_log["written"].values())))
    assert record["error_type"] == "EXECUTOR_MEMORY_EXCEEDED"


def test_failure_callback_still_posts_when_driver_log_is_unavailable(
    _fake_slack_hook, _fake_emr_log
):
    # Job Run 직후라 아직 flush 전이거나 권한 문제인 경우. 진단 줄만 빠지고 나머지는 그대로다.
    _fake_emr_log["text"] = None
    context = _context(
        "standard_score_pipeline", _emr_task_instance(), exception=ValueError("boom")
    )

    notifications.on_failure_callback(context)

    text = _fake_slack_hook.client.chat_postMessage.call_args.kwargs["text"]
    assert "오류 유형" not in text
    assert "boom" in text
    assert "담당자" in text
    assert "EMR Serverless 원본 로그" in text


def test_failure_callback_skips_diagnosis_for_non_emr_task(_fake_slack_hook, _fake_emr_log):
    _fake_emr_log["text"] = _EXECUTOR_OOM_LOG
    context = _context(
        "bronze_compaction", _FakeTaskInstance(task_id="compact_zone_weather_snapshot")
    )

    notifications.on_failure_callback(context)

    text = _fake_slack_hook.client.chat_postMessage.call_args.kwargs["text"]
    assert "오류 유형" not in text
    assert _fake_emr_log["read_uris"] == []


def test_failure_callback_truncates_long_evidence(_fake_slack_hook, _fake_emr_log):
    long_line = "ERROR ExecutorLostFailure " + "x" * 2000
    _fake_emr_log["text"] = _EXECUTOR_OOM_LOG + long_line + "\n"
    context = _context("standard_score_pipeline", _emr_task_instance())

    notifications.on_failure_callback(context)

    text = _fake_slack_hook.client.chat_postMessage.call_args.kwargs["text"]
    assert "이하 생략" in text
    assert len(text) < 4000


_EMPTY_FAILURE_LOG = (
    "WARNING: Using incubator modules: jdk.incubator.vector\n"
    "26/08/25 12:34:57 INFO ShutdownHookManager: Shutdown hook called\n"
    "26/08/25 12:34:57 INFO ShutdownHookManager: Deleting directory /tmp/spark-1d11c120\n"
)


def test_failure_callback_says_log_has_no_error_trace_instead_of_empty_unknown(
    _fake_slack_hook, _fake_emr_log
):
    # 로그는 읽혔는데 오류 흔적이 없는 경우. "UNKNOWN_ERROR, 근거 없음"만 붙이면 알림이
    # 아무 도움도 되지 않는다 — 흔적이 없다는 사실 자체가 단서다.
    _fake_emr_log["text"] = _EMPTY_FAILURE_LOG
    notifications.on_failure_callback(_context("standard_score_pipeline", _emr_task_instance()))

    text = _fake_slack_hook.client.chat_postMessage.call_args.kwargs["text"]
    assert "driver 로그에 오류 흔적이 없습니다" in text
    assert "SPARK_EXECUTOR" in text
    assert "등록된 룰에 해당하지 않습니다" not in text


# --- 1차 실패 알림 + 스레드 (#527) ---


@dataclass
class _FakeRetryTask:
    """retries/retry_delay를 가진 task — 시도 횟수와 재시도 간격 표시에 쓴다."""

    task_group: _FakeTaskGroup | None = None
    retries: int = 1
    retry_delay: object = datetime.timedelta(minutes=5)


def _retry_context(dag_id, task_instance, *, try_number, retries=1, exception=None):
    context = _context(dag_id, task_instance, exception=exception)
    context["task"] = _FakeRetryTask(retries=retries)
    task_instance.try_number = try_number
    return context


def test_retry_callback_posts_first_failure_to_channel_with_mention(
    _fake_slack_hook, _fake_object_store
):
    task_instance = _FakeTaskInstance(task_id="compact_zone_weather_snapshot")
    context = _retry_context("bronze_compaction", task_instance, try_number=1)

    notifications.on_retry_callback(context)

    kwargs = _fake_slack_hook.client.chat_postMessage.call_args.kwargs
    assert "thread_ts" not in kwargs  # 채널에 새 메시지로 나간다
    assert "1차 실패 (1/2)" in kwargs["text"]
    assert "<@U0456GHIJKL>" in kwargs["text"]  # 1차에도 담당자를 부른다
    assert "5분 뒤 재시도합니다" in kwargs["text"]


def test_retry_callback_posts_later_attempt_into_the_thread_without_mention(
    _fake_slack_hook, _fake_object_store
):
    task_instance = _FakeTaskInstance(task_id="compact_zone_weather_snapshot")
    task_instance.xcom_store[
        ("compact_zone_weather_snapshot", notifications._ALERT_THREAD_TS_XCOM_KEY)
    ] = "1724650000.000100"
    context = _retry_context("bronze_compaction", task_instance, try_number=2, retries=2)

    notifications.on_retry_callback(context)

    kwargs = _fake_slack_hook.client.chat_postMessage.call_args.kwargs
    assert kwargs["thread_ts"] == "1724650000.000100"
    assert "2차 시도도 실패 (2/3)" in kwargs["text"]
    assert "<@U0456GHIJKL>" not in kwargs["text"]  # 이미 부른 사람을 또 부르지 않는다


def test_final_failure_goes_into_the_same_thread_and_mentions_again(
    _fake_slack_hook, _fake_object_store
):
    task_instance = _FakeTaskInstance(task_id="compact_zone_weather_snapshot")
    task_instance.xcom_store[
        ("compact_zone_weather_snapshot", notifications._ALERT_THREAD_TS_XCOM_KEY)
    ] = "1724650000.000100"
    context = _retry_context("bronze_compaction", task_instance, try_number=2)

    notifications.on_failure_callback(context)

    kwargs = _fake_slack_hook.client.chat_postMessage.call_args.kwargs
    assert kwargs["thread_ts"] == "1724650000.000100"
    assert "최종 실패 (2/2)" in kwargs["text"]
    assert "<@U0456GHIJKL>" in kwargs["text"]
    assert "뒤 재시도합니다" not in kwargs["text"]


def test_first_failure_alert_remembers_its_thread_ts(_fake_slack_hook, _fake_object_store):
    pushed = {}
    task_instance = _FakeTaskInstance(task_id="compact_zone_weather_snapshot")
    task_instance.xcom_push = lambda key, value: pushed.__setitem__(key, value)
    _fake_slack_hook.client.chat_postMessage.return_value = {"ts": "1724650000.000100"}
    context = _retry_context("bronze_compaction", task_instance, try_number=1)

    notifications.on_retry_callback(context)

    assert pushed[notifications._ALERT_THREAD_TS_XCOM_KEY] == "1724650000.000100"


def test_alert_still_posts_when_thread_ts_cannot_be_stored(_fake_slack_hook, _fake_object_store):
    # 콜백에서 xcom_push가 막히면 스레드만 포기하고 알림은 그대로 나가야 한다.
    def _boom(key, value):
        raise RuntimeError("xcom_push not allowed in callback")

    task_instance = _FakeTaskInstance(task_id="compact_zone_weather_snapshot")
    task_instance.xcom_push = _boom
    _fake_slack_hook.client.chat_postMessage.return_value = {"ts": "1724650000.000100"}

    notifications.on_retry_callback(
        _retry_context("bronze_compaction", task_instance, try_number=1)
    )

    assert "1차 실패" in _fake_slack_hook.client.chat_postMessage.call_args.kwargs["text"]


def test_task_success_callback_reports_recovery_only_after_a_retry(
    _fake_slack_hook, _fake_object_store
):
    task_instance = _FakeTaskInstance(task_id="compact_zone_weather_snapshot")
    task_instance.xcom_store[
        ("compact_zone_weather_snapshot", notifications._ALERT_THREAD_TS_XCOM_KEY)
    ] = "1724650000.000100"
    context = _retry_context("bronze_compaction", task_instance, try_number=2)

    notifications.on_task_success_callback(context)

    kwargs = _fake_slack_hook.client.chat_postMessage.call_args.kwargs
    assert kwargs["thread_ts"] == "1724650000.000100"
    assert "재시도에서 성공했습니다 (2/2)" in kwargs["text"]


def test_task_success_callback_is_silent_when_the_task_never_retried(
    _fake_slack_hook, _fake_object_store
):
    task_instance = _FakeTaskInstance(task_id="compact_zone_weather_snapshot")
    context = _retry_context("bronze_compaction", task_instance, try_number=1)

    notifications.on_task_success_callback(context)

    _fake_slack_hook.client.chat_postMessage.assert_not_called()


def test_failure_alert_still_posts_for_a_dag_missing_from_the_registry(
    _fake_slack_hook, _fake_object_store
):
    # dag_owners.py는 미등록 dag_id에 KeyError를 던진다. 그 예외가 콜백에서 터지면
    # 실패했는데 알림이 오지 않는 최악의 형태가 된다.
    task_instance = _FakeTaskInstance(task_id="some_task")
    context = _retry_context("brand_new_pipeline", task_instance, try_number=1)

    notifications.on_failure_callback(context)

    text = _fake_slack_hook.client.chat_postMessage.call_args.kwargs["text"]
    assert "담당자 미등록" in text
    assert "brand_new_pipeline" in text
    assert "some_task" in text
