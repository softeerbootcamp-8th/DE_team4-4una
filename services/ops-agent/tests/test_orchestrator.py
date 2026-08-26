from __future__ import annotations

from ops_agent.incident_store import IncidentStore
from ops_agent.models import Incident
from ops_agent.orchestrator import OpsAgentOrchestrator
from ops_agent.owners import ServiceOwner, ServiceOwnersRegistry
from ops_agent.slack_notifier import SlackNotifier
from ops_agent.ssh import SshTarget
from ops_agent_test_support import (
    FakePrometheusClient,
    FakeSlackClient,
    down_status,
    fake_diagnose,
    grafana_alert,
    healthy_status,
    make_fake_remediate,
)

TARGET = SshTarget(host="1.2.3.4", user="ec2-user", key_path="/keys/id_ed25519")
PROJECT_TARGET = SshTarget(host="5.6.7.8", user="ec2-user", key_path="/keys/project.pem")


def main_messages(client) -> list[dict]:
    """본문 메시지만 고른다 — 알림 하나가 본문 + 로그 스레드 답글 2건을 만든다."""
    return [call for call in client.calls if "blocks" in call]


def block_text(call: dict) -> str:
    """한 메시지의 모든 block 텍스트를 이어붙인다 — 단언을 blocks 구조에 덜 묶기 위해서다."""
    parts = []
    for block in call.get("blocks") or []:
        if "text" in block:
            parts.append(block["text"]["text"])
        for element in block.get("elements") or []:
            parts.append(element.get("text", ""))
    return "\n".join(parts)


def make_orchestrator(
    *,
    tmp_path,
    statuses,
    remediate_succeeds: bool = True,
    diagnose=fake_diagnose,
    sleep_calls: list[float] | None = None,
    recovery_poll_interval_seconds: float = 1.0,
    recovery_wait_seconds: float = 5.0,
    cooldown_seconds: float = 600,
    ssh_targets=None,
    remediate=None,
):
    slack_client = FakeSlackClient()

    def record_sleep(seconds: float) -> None:
        if sleep_calls is not None:
            sleep_calls.append(seconds)

    orchestrator = OpsAgentOrchestrator(
        prometheus=FakePrometheusClient(statuses=statuses),
        incident_store=IncidentStore(str(tmp_path / "incidents.sqlite3")),
        owners=ServiceOwnersRegistry(
            services={
                "stream-processor": ServiceOwner(
                    name="bob", email=None, slack_id="U0456GHIJKL", severity="high"
                )
            }
        ),
        ssh_targets=ssh_targets or {"spark": TARGET, "project": PROJECT_TARGET},
        slack=SlackNotifier(channel="#alerts", client=slack_client),
        cooldown_seconds=cooldown_seconds,
        recovery_poll_interval_seconds=recovery_poll_interval_seconds,
        recovery_wait_seconds=recovery_wait_seconds,
        diagnose=diagnose,
        remediate=remediate or make_fake_remediate(succeeded=remediate_succeeds),
        sleep=record_sleep,
    )
    return orchestrator, slack_client


class TestOrchestratorHandle:
    def test_a_resolved_alert_takes_no_action(self, tmp_path):
        orchestrator, slack_client = make_orchestrator(tmp_path=tmp_path, statuses=[healthy_status()])
        incident = Incident.from_grafana_alert(grafana_alert(status_field="resolved"))

        outcome = orchestrator.handle(incident)

        assert outcome.handled is False
        assert outcome.escalated is False
        assert slack_client.messages == []

    def test_a_firing_alert_that_prometheus_now_reports_healthy_still_notifies(self, tmp_path):
        # 조치는 하지 않되 침묵하지는 않는다 — 침묵하면 Grafana의 감지 알림 뒤로 후속이 없어
        # agent가 죽은 것인지 판단하고 넘어간 것인지 구분할 수 없다(설계 §1-1).
        orchestrator, slack_client = make_orchestrator(tmp_path=tmp_path, statuses=[healthy_status()])
        incident = Incident.from_grafana_alert(grafana_alert())

        outcome = orchestrator.handle(incident)

        assert outcome.recovered is True
        assert outcome.escalated is False
        assert outcome.remediation is None
        body = block_text(slack_client.calls[0])
        assert "변경한 명령 없음" in body
        assert "재검증 결과 이미 정상" in body

    def test_stream_processor_down_is_restarted_and_recovers(self, tmp_path):
        orchestrator, slack_client = make_orchestrator(
            tmp_path=tmp_path,
            statuses=[down_status(), healthy_status()],
            remediate_succeeds=True,
        )
        incident = Incident.from_grafana_alert(grafana_alert())

        outcome = orchestrator.handle(incident)

        assert outcome.remediation is not None
        assert outcome.remediation.succeeded is True
        assert outcome.recovered is True
        assert outcome.escalated is False
        assert len(main_messages(slack_client)) == 1
        assert "*복구 여부* 성공" in block_text(slack_client.calls[0])

    def test_stream_processor_still_down_after_restart_escalates(self, tmp_path):
        orchestrator, slack_client = make_orchestrator(
            tmp_path=tmp_path,
            statuses=[down_status(), down_status()],
            remediate_succeeds=True,
        )
        incident = Incident.from_grafana_alert(grafana_alert())

        outcome = orchestrator.handle(incident)

        assert outcome.recovered is False
        assert outcome.escalated is True
        assert len(main_messages(slack_client)) == 1
        body = block_text(slack_client.calls[0])
        assert "실패 — 담당자 확인 필요" in body
        assert "<@U0456GHIJKL>" in body

    def test_recovery_is_polled_instead_of_checked_once_immediately(self, tmp_path):
        # restart 직후 한 번만 확인하면 Spark가 아직 안 떴을 때 "복구 실패"로 오판한다 —
        # 몇 번의 polling 끝에 RUNNING이 되면 성공으로 판정해야 한다.
        sleep_calls: list[float] = []
        orchestrator, slack_client = make_orchestrator(
            tmp_path=tmp_path,
            statuses=[down_status(), down_status(), down_status(), healthy_status()],
            remediate_succeeds=True,
            sleep_calls=sleep_calls,
            recovery_poll_interval_seconds=1.0,
            recovery_wait_seconds=5.0,
        )
        incident = Incident.from_grafana_alert(grafana_alert())

        outcome = orchestrator.handle(incident)

        assert outcome.recovered is True
        assert outcome.escalated is False
        assert "*복구 여부* 성공" in block_text(slack_client.calls[0])
        # 첫 재확인(down) 이후 두 번 더 폴링해서야 healthy가 나왔으니 sleep도 두 번 있어야 한다.
        assert sleep_calls == [1.0, 1.0]

    def test_recovery_polling_gives_up_after_the_wait_budget(self, tmp_path):
        sleep_calls: list[float] = []
        orchestrator, _slack_client = make_orchestrator(
            tmp_path=tmp_path,
            statuses=[down_status(), down_status()],
            remediate_succeeds=True,
            sleep_calls=sleep_calls,
            recovery_poll_interval_seconds=1.0,
            recovery_wait_seconds=3.0,
        )
        incident = Incident.from_grafana_alert(grafana_alert())

        outcome = orchestrator.handle(incident)

        assert outcome.recovered is False
        assert outcome.escalated is True
        # wait budget(3s)을 poll_interval(1s)로 다 채울 때까지만 재확인하고 멈춘다.
        assert sleep_calls == [1.0, 1.0, 1.0]

    def test_an_alert_without_auto_remediate_is_escalated_without_remediation(self, tmp_path):
        orchestrator, slack_client = make_orchestrator(tmp_path=tmp_path, statuses=[down_status()])
        incident = Incident.from_grafana_alert(grafana_alert(auto_remediate="false"))

        outcome = orchestrator.handle(incident)

        assert outcome.remediation is None
        assert outcome.escalated is True
        assert len(main_messages(slack_client)) == 1
        assert "변경한 명령 없음" in block_text(slack_client.calls[0])

    def test_an_unregistered_alertname_is_escalated_without_remediation(self, tmp_path):
        orchestrator, _slack_client = make_orchestrator(tmp_path=tmp_path, statuses=[down_status()])
        incident = Incident.from_grafana_alert(grafana_alert(alertname="SomeUnknownAlert"))

        outcome = orchestrator.handle(incident)

        assert outcome.remediation is None
        assert outcome.escalated is True

    def test_a_second_firing_alert_within_the_cooldown_is_not_remediated_again(self, tmp_path):
        orchestrator, slack_client = make_orchestrator(
            tmp_path=tmp_path,
            statuses=[down_status(), healthy_status(), down_status()],
            remediate_succeeds=True,
        )
        incident = Incident.from_grafana_alert(grafana_alert(fingerprint="fp-repeat"))

        first = orchestrator.handle(incident)
        second = orchestrator.handle(incident)

        assert first.remediation is not None
        assert second.remediation is None
        assert second.escalated is True
        assert len(main_messages(slack_client)) == 2
        assert "cooldown" in block_text(main_messages(slack_client)[-1])

    def test_the_executed_commands_appear_in_the_notification(self, tmp_path):
        orchestrator, slack_client = make_orchestrator(
            tmp_path=tmp_path, statuses=[down_status(), healthy_status()]
        )
        incident = Incident.from_grafana_alert(grafana_alert())

        orchestrator.handle(incident)

        body = block_text(slack_client.calls[0])
        assert "읽기만 한 명령" in body
        assert "docker inspect" in body
        assert "변경한 명령" in body
        assert "docker restart stream-processor" in body

    def test_the_log_tail_goes_to_a_thread_reply_not_the_main_message(self, tmp_path):
        orchestrator, slack_client = make_orchestrator(
            tmp_path=tmp_path, statuses=[down_status(), healthy_status()]
        )
        incident = Incident.from_grafana_alert(grafana_alert())

        orchestrator.handle(incident)

        main, reply = slack_client.calls
        assert "fake logs" not in block_text(main)
        assert "thread_ts" not in main
        # FakeSlackClient가 첫 호출에 돌려준 ts를 답글이 그대로 참조해야 한다.
        assert reply["thread_ts"] == "1700000000.000001"
        assert "fake logs" in reply["text"]

    def test_repeated_incidents_report_a_growing_attempt_count(self, tmp_path):
        # cooldown을 0으로 둬서 두 번째 조치가 실행되게 하고, 이력이 누적되는지 본다.
        orchestrator, slack_client = make_orchestrator(
            tmp_path=tmp_path,
            statuses=[down_status(), healthy_status(), down_status(), healthy_status()],
            cooldown_seconds=0,
        )
        incident = Incident.from_grafana_alert(grafana_alert(fingerprint="fp-count"))

        orchestrator.handle(incident)
        orchestrator.handle(incident)

        posted = main_messages(slack_client)
        assert "1회" in block_text(posted[0])
        assert "2회" in block_text(posted[1])

    def test_each_stage_is_logged_as_one_json_line(self, tmp_path, caplog):
        import json
        import logging

        caplog.set_level(logging.INFO, logger="ops_agent.orchestrator")
        orchestrator, _slack_client = make_orchestrator(
            tmp_path=tmp_path, statuses=[down_status(), healthy_status()]
        )
        incident = Incident.from_grafana_alert(grafana_alert())

        orchestrator.handle(incident)

        stages = [
            json.loads(record.getMessage())["stage"]
            for record in caplog.records
            if record.getMessage().startswith("{")
        ]
        # decide()가 _diagnose()보다 먼저 호출되므로 policy가 diagnose보다 앞선다.
        assert stages == ["reverify", "policy", "diagnose", "remediate", "recovery"]

    def test_the_healthy_path_logs_only_the_stages_it_actually_ran(self, tmp_path):
        import json
        import logging

        orchestrator, _slack_client = make_orchestrator(
            tmp_path=tmp_path, statuses=[healthy_status()]
        )
        incident = Incident.from_grafana_alert(grafana_alert())

        logger = logging.getLogger("ops_agent.orchestrator")
        records: list[str] = []
        handler = logging.Handler()
        handler.emit = lambda record: records.append(record.getMessage())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        try:
            orchestrator.handle(incident)
        finally:
            logger.removeHandler(handler)

        stages = [json.loads(line)["stage"] for line in records if line.startswith("{")]
        # 정상이면 decide()를 부르지 않으므로 policy 단계가 없다.
        assert stages == ["reverify", "diagnose"]

    def test_serving_api_down_is_restarted_on_the_project_host(self, tmp_path):
        seen = {}

        def spy(target, container, *, action):
            seen["target"] = target
            seen["container"] = container
            return make_fake_remediate()(target, container, action=action)

        orchestrator, _slack = make_orchestrator(
            tmp_path=tmp_path, statuses=[down_status(), healthy_status()], remediate=spy
        )
        incident = Incident.from_grafana_alert(
            grafana_alert(alertname="ServingApiDown", service="serving-api")
        )

        outcome = orchestrator.handle(incident)

        assert outcome.remediation is not None
        assert seen["target"] == PROJECT_TARGET
        assert seen["container"] == "serving-api"

    def test_the_node_exporter_is_restarted_on_the_spark_host(self, tmp_path):
        seen = {}

        def spy(target, container, *, action):
            seen["target"] = target
            seen["container"] = container
            return make_fake_remediate()(target, container, action=action)

        orchestrator, _slack = make_orchestrator(
            tmp_path=tmp_path, statuses=[down_status(), healthy_status()], remediate=spy
        )
        incident = Incident.from_grafana_alert(
            grafana_alert(alertname="SparkNodeExporterDown", service="spark-node")
        )

        orchestrator.handle(incident)

        assert seen["target"] == TARGET
        assert seen["container"] == "node-exporter"

    def test_an_unresolvable_ssh_target_escalates_instead_of_guessing(self, tmp_path):
        # project 설정이 없는데 serving-api를 재시작하려 들면 엉뚱한 호스트를 건드릴 수 있다.
        orchestrator, slack_client = make_orchestrator(
            tmp_path=tmp_path, statuses=[down_status()], ssh_targets={"spark": TARGET}
        )
        incident = Incident.from_grafana_alert(
            grafana_alert(alertname="ServingApiDown", service="serving-api")
        )

        outcome = orchestrator.handle(incident)

        assert outcome.remediation is None
        assert outcome.escalated is True
        assert "no ssh target" in block_text(main_messages(slack_client)[0])

    def test_an_alert_without_a_spec_is_escalated_without_touching_any_host(self, tmp_path):
        orchestrator, slack_client = make_orchestrator(
            tmp_path=tmp_path, statuses=[down_status()]
        )
        incident = Incident.from_grafana_alert(grafana_alert(alertname="SomethingNew"))

        outcome = orchestrator.handle(incident)

        assert outcome.remediation is None
        assert outcome.escalated is True
        body = block_text(main_messages(slack_client)[0])
        assert "읽기만 한 명령" not in body
