"""dags/notifications.py의 Slack 콜백(#409)을 가짜 Airflow context로 검증한다.

실제 Airflow 실행/Slack 전송/S3 쓰기 없이, on_failure_callback/on_success_callback이
받는 context의 필요한 속성만 흉내 낸 가짜 객체를 주입한다. 실제 Airflow
context 모양과 일치하는지는 로컬 Airflow 수동 검증(README)에서 확인한다.
"""

from __future__ import annotations

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
              sensor_processing.run_sensor_processing:
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
        "standard_score_pipeline", _FakeTaskInstance(task_id="report_processing_counts")
    )

    notifications.on_failure_callback(context)

    _fake_slack_hook.client.users_lookupByEmail.assert_called_once_with(email="alice@example.com")
    text = _fake_slack_hook.client.chat_postMessage.call_args.kwargs["text"]
    assert "<@U0999LOOKEDUP>" in text
    assert "🔴 critical" in text


def test_failure_callback_uses_task_level_override_over_dag_default(_fake_slack_hook, _fake_object_store):
    context = _context(
        "standard_score_pipeline",
        _FakeTaskInstance(task_id="sensor_processing.run_sensor_processing"),
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
    task_instance.xcom_store[("report_processing_counts", "return_value")] = {
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
    task_instance = _FakeTaskInstance(task_id="run_current_score")
    task_instance.xcom_store[("run_current_score", "return_value")] = {"upserted_count": 42}
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
