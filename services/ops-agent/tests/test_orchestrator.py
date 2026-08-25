from __future__ import annotations

from conftest import (
    FakePrometheusClient,
    FakeSlackClient,
    down_status,
    fake_diagnose,
    grafana_alert,
    healthy_status,
    make_fake_remediate,
)
from ops_agent.incident_store import IncidentStore
from ops_agent.models import Incident
from ops_agent.orchestrator import OpsAgentOrchestrator
from ops_agent.owners import ServiceOwner, ServiceOwnersRegistry
from ops_agent.slack_notifier import SlackNotifier
from ops_agent.ssh import SshTarget

TARGET = SshTarget(host="1.2.3.4", user="ec2-user", key_path="/keys/id_ed25519")


def make_orchestrator(
    *,
    tmp_path,
    statuses,
    remediate_succeeds: bool = True,
    diagnose=fake_diagnose,
    sleep_calls: list[float] | None = None,
    recovery_poll_interval_seconds: float = 1.0,
    recovery_wait_seconds: float = 5.0,
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
        ssh_target=TARGET,
        slack=SlackNotifier(channel="#alerts", client=slack_client),
        cooldown_seconds=600,
        recovery_poll_interval_seconds=recovery_poll_interval_seconds,
        recovery_wait_seconds=recovery_wait_seconds,
        diagnose=diagnose,
        remediate=make_fake_remediate(succeeded=remediate_succeeds),
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

    def test_a_firing_alert_that_prometheus_now_reports_healthy_is_ignored(self, tmp_path):
        # Grafana가 firing이라고 보냈어도 재조회 결과가 정상이면 조치하지 않는다.
        orchestrator, slack_client = make_orchestrator(tmp_path=tmp_path, statuses=[healthy_status()])
        incident = Incident.from_grafana_alert(grafana_alert())

        outcome = orchestrator.handle(incident)

        assert outcome.recovered is True
        assert outcome.escalated is False
        assert outcome.remediation is None
        assert slack_client.messages == []

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
        assert len(slack_client.messages) == 1
        assert "복구 여부: 성공" in slack_client.messages[0][1]

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
        assert len(slack_client.messages) == 1
        message = slack_client.messages[0][1]
        assert "복구 여부: 실패" in message
        assert "<@U0456GHIJKL>" in message

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
        assert "복구 여부: 성공" in slack_client.messages[0][1]
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
        assert len(slack_client.messages) == 1
        assert "자동 조치 없음" in slack_client.messages[0][1]

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
        assert len(slack_client.messages) == 2
        assert "cooldown" in slack_client.messages[1][1] or "자동 조치 없음" in slack_client.messages[1][1]
