from __future__ import annotations

from ops_agent.models import Incident, parse_grafana_webhook
from ops_agent_test_support import grafana_alert, grafana_webhook_payload


class TestIncidentFromGrafanaAlert:
    def test_parses_the_minimum_fields(self):
        incident = Incident.from_grafana_alert(grafana_alert())

        assert incident.alertname == "StreamProcessorDown"
        assert incident.service == "stream-processor"
        assert incident.severity == "high"
        assert incident.status == "firing"
        assert incident.fingerprint == "fp-1"
        assert incident.auto_remediate is True
        assert incident.is_firing is True

    def test_auto_remediate_defaults_to_false_when_the_label_is_absent(self):
        alert = grafana_alert()
        del alert["labels"]["auto_remediate"]

        incident = Incident.from_grafana_alert(alert)

        assert incident.auto_remediate is False

    def test_resolved_status_is_not_firing(self):
        incident = Incident.from_grafana_alert(grafana_alert(status_field="resolved"))

        assert incident.is_firing is False

    def test_missing_alertname_raises(self):
        alert = grafana_alert()
        del alert["labels"]["alertname"]

        try:
            Incident.from_grafana_alert(alert)
            raised = False
        except ValueError:
            raised = True
        assert raised

    def test_missing_fingerprint_raises(self):
        alert = grafana_alert()
        del alert["fingerprint"]

        try:
            Incident.from_grafana_alert(alert)
            raised = False
        except ValueError:
            raised = True
        assert raised


class TestParseGrafanaWebhook:
    def test_parses_every_alert_in_the_payload(self):
        payload = grafana_webhook_payload(
            grafana_alert(fingerprint="fp-1"), grafana_alert(fingerprint="fp-2")
        )

        incidents = parse_grafana_webhook(payload)

        assert [incident.fingerprint for incident in incidents] == ["fp-1", "fp-2"]

    def test_skips_an_unparseable_alert_but_keeps_the_rest(self):
        broken = grafana_alert(fingerprint="fp-broken")
        del broken["labels"]["alertname"]
        payload = grafana_webhook_payload(broken, grafana_alert(fingerprint="fp-ok"))

        incidents = parse_grafana_webhook(payload)

        assert [incident.fingerprint for incident in incidents] == ["fp-ok"]

    def test_empty_alerts_list_returns_no_incidents(self):
        assert parse_grafana_webhook({"status": "firing", "alerts": []}) == []
