from __future__ import annotations

from fastapi.testclient import TestClient
from ops_agent.app import create_app
from ops_agent.incident_store import IncidentStore
from ops_agent.orchestrator import OpsAgentOrchestrator
from ops_agent.owners import ServiceOwnersRegistry
from ops_agent.slack_notifier import SlackNotifier
from ops_agent.ssh import SshTarget
from ops_agent_test_support import (
    FakePrometheusClient,
    FakeSlackClient,
    down_status,
    fake_diagnose,
    grafana_alert,
    grafana_webhook_payload,
    healthy_status,
    make_fake_remediate,
)

TARGET = SshTarget(host="1.2.3.4", user="ec2-user", key_path="/keys/id_ed25519")


def make_client(tmp_path, statuses) -> TestClient:
    orchestrator = OpsAgentOrchestrator(
        prometheus=FakePrometheusClient(statuses=statuses),
        incident_store=IncidentStore(str(tmp_path / "incidents.sqlite3")),
        owners=ServiceOwnersRegistry(services={}),
        ssh_targets={"spark": TARGET, "project": TARGET},
        slack=SlackNotifier(channel="#alerts", client=FakeSlackClient()),
        cooldown_seconds=600,
        diagnose=fake_diagnose,
        remediate=make_fake_remediate(succeeded=True),
    )
    return TestClient(create_app(orchestrator))


class TestHealth:
    def test_health_endpoint_reports_ok(self, tmp_path):
        client = make_client(tmp_path, statuses=[healthy_status()])

        response = client.get("/health")

        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


class TestGrafanaWebhook:
    def test_a_recovered_alert_is_reported_as_handled(self, tmp_path):
        client = make_client(tmp_path, statuses=[healthy_status()])

        response = client.post("/webhooks/grafana", json=grafana_webhook_payload(grafana_alert()))

        assert response.status_code == 200
        body = response.json()
        assert body["handled"] == 1
        assert "no action taken" in body["results"][0]

    def test_a_down_alert_is_remediated(self, tmp_path):
        client = make_client(tmp_path, statuses=[down_status(), healthy_status()])

        response = client.post("/webhooks/grafana", json=grafana_webhook_payload(grafana_alert()))

        body = response.json()
        assert body["handled"] == 1
        assert "recovered=True" in body["results"][0]

    def test_a_payload_with_no_alerts_handles_zero_incidents(self, tmp_path):
        client = make_client(tmp_path, statuses=[healthy_status()])

        response = client.post("/webhooks/grafana", json={"status": "firing", "alerts": []})

        assert response.status_code == 200
        assert response.json() == {"handled": 0, "results": []}
