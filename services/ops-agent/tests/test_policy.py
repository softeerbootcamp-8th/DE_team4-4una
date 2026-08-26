from __future__ import annotations

from ops_agent.models import Incident
from ops_agent.policy import (
    ACTION_SPECS,
    ESCALATION_ONLY_ACTIONS,
    RemediationAction,
    decide,
)


def incident(**overrides) -> Incident:
    values = {
        "alertname": "StreamProcessorDown",
        "service": "stream-processor",
        "severity": "high",
        "status": "firing",
        "fingerprint": "fp-1",
        "auto_remediate": True,
        "annotations": {},
    }
    values.update(overrides)
    return Incident(**values)


class TestDecide:
    def test_an_allowlisted_and_implemented_alert_is_allowed(self):
        decision = decide(incident(), ACTION_SPECS["StreamProcessorDown"])

        assert decision.allowed is True
        assert decision.action == RemediationAction.RESTART_STREAM_PROCESSOR

    def test_an_alert_without_auto_remediate_opt_in_is_not_allowed(self):
        decision = decide(incident(auto_remediate=False), ACTION_SPECS["StreamProcessorDown"])

        assert decision.allowed is False
        assert decision.action is None
        assert "auto_remediate" in decision.reason

    def test_an_unregistered_alertname_is_not_allowed(self):
        decision = decide(incident(alertname="SomeUnknownAlert"), None)

        assert decision.allowed is False
        assert decision.action is None

    def test_stream_processor_stale_is_never_allowed_even_if_mislabeled(self):
        # EVENT DATA STALE/PROGRESS STALE(값 1,2)은 잠깐의 지연일 뿐이라 재시작하면
        # 안 된다 — Grafana rule이 auto_remediate 라벨을 실수로 붙여도 allowlist에
        # 없으므로 여기서 다시 막힌다(방어적 이중 체크).
        decision = decide(incident(alertname="StreamProcessorStale", auto_remediate=True), None)

        assert decision.allowed is False
        assert decision.action is None

    def test_a_spec_whose_action_is_not_implemented_is_not_allowed(self, monkeypatch):
        # 명세에 등록돼 있어도 IMPLEMENTED_ACTIONS에 없으면 자동 실행되면 안 된다.
        import ops_agent.policy as policy_module

        monkeypatch.setattr(policy_module, "IMPLEMENTED_ACTIONS", frozenset())

        decision = decide(incident(), ACTION_SPECS["StreamProcessorDown"])

        assert decision.allowed is False
        assert decision.action == RemediationAction.RESTART_STREAM_PROCESSOR
        assert "not yet implemented" in decision.reason

    def test_escalation_only_actions_are_documented_and_not_implemented(self):
        # 정책 문서화용 목록 자체가 실행 가능한 action 이름과 겹치면 안 된다.
        implemented_values = {action.value for action in RemediationAction}
        assert not set(ESCALATION_ONLY_ACTIONS) & implemented_values


class TestActionSpecs:
    def test_every_alert_that_can_be_remediated_has_a_spec(self):
        from ops_agent.policy import ACTION_SPECS

        assert set(ACTION_SPECS) == {
            "StreamProcessorDown",
            "ServingApiDown",
            "SparkNodeExporterDown",
        }

    def test_specs_point_at_known_ssh_hosts(self):
        from ops_agent.policy import ACTION_SPECS

        assert {spec.ssh_target_key for spec in ACTION_SPECS.values()} <= {"spark", "project"}

    def test_serving_api_is_restarted_on_the_project_host(self):
        from ops_agent.policy import ACTION_SPECS

        spec = ACTION_SPECS["ServingApiDown"]

        assert spec.ssh_target_key == "project"
        assert spec.container == "serving-api"

    def test_container_names_are_literals_not_taken_from_alerts(self):
        # ssh.py의 불변식 회귀 방지 — 이름은 명세에 박힌 리터럴이어야 한다.
        from ops_agent.policy import ACTION_SPECS

        assert all(spec.container and " " not in spec.container for spec in ACTION_SPECS.values())

    def test_bigger_is_worse_in_every_health_check(self):
        # evaluate()가 max()로 최악을 고르므로 healthy_code는 항상 최솟값이어야 한다.
        from ops_agent.policy import ACTION_SPECS

        for spec in ACTION_SPECS.values():
            assert spec.health.healthy_code == min(spec.health.labels)

    def test_stream_processor_stale_has_no_spec(self):
        # 잠깐의 지연으로 컨테이너를 재시작하면 안 된다.
        from ops_agent.policy import ACTION_SPECS

        assert "StreamProcessorStale" not in ACTION_SPECS

    def test_every_spec_action_is_implemented(self):
        from ops_agent.policy import ACTION_SPECS, IMPLEMENTED_ACTIONS

        assert all(spec.action in IMPLEMENTED_ACTIONS for spec in ACTION_SPECS.values())

    def test_every_spec_has_a_matching_grafana_alert_rule(self):
        # 이름이 어긋나면 alert가 와도 명세를 못 찾아 조용히 escalation만 된다.
        from pathlib import Path

        import yaml
        from ops_agent.policy import ACTION_SPECS

        repo_root = Path(__file__).resolve().parents[3]
        rules = yaml.safe_load(
            (
                repo_root
                / "infra/monitoring/grafana/provisioning/alerting/rules.yaml"
            ).read_text()
        )
        titles = {rule["title"] for group in rules["groups"] for rule in group["rules"]}

        assert set(ACTION_SPECS) <= titles

    def test_every_auto_remediate_rule_has_a_spec(self):
        # auto_remediate 라벨만 붙이고 명세를 빠뜨리면 조치될 것처럼 보이지만 안 된다.
        from pathlib import Path

        import yaml
        from ops_agent.policy import ACTION_SPECS

        repo_root = Path(__file__).resolve().parents[3]
        rules = yaml.safe_load(
            (
                repo_root
                / "infra/monitoring/grafana/provisioning/alerting/rules.yaml"
            ).read_text()
        )
        opted_in = {
            rule["title"]
            for group in rules["groups"]
            for rule in group["rules"]
            if (rule.get("labels") or {}).get("auto_remediate") == "true"
        }

        assert opted_in == set(ACTION_SPECS)
