from __future__ import annotations

from ops_agent.models import Incident
from ops_agent.policy import ESCALATION_ONLY_ACTIONS, RemediationAction, decide


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
        decision = decide(incident())

        assert decision.allowed is True
        assert decision.action == RemediationAction.RESTART_STREAM_PROCESSOR

    def test_an_alert_without_auto_remediate_opt_in_is_not_allowed(self):
        decision = decide(incident(auto_remediate=False))

        assert decision.allowed is False
        assert decision.action is None
        assert "auto_remediate" in decision.reason

    def test_an_unregistered_alertname_is_not_allowed(self):
        decision = decide(incident(alertname="SomeUnknownAlert"))

        assert decision.allowed is False
        assert decision.action is None

    def test_stream_processor_stale_is_never_allowed_even_if_mislabeled(self):
        # EVENT DATA STALE/PROGRESS STALE(값 1,2)은 잠깐의 지연일 뿐이라 재시작하면
        # 안 된다 — Grafana rule이 auto_remediate 라벨을 실수로 붙여도 allowlist에
        # 없으므로 여기서 다시 막힌다(방어적 이중 체크).
        decision = decide(incident(alertname="StreamProcessorStale", auto_remediate=True))

        assert decision.allowed is False
        assert decision.action is None

    def test_an_allowlisted_but_unimplemented_action_is_not_allowed(self, monkeypatch):
        # allowlist에 등록돼 있어도 IMPLEMENTED_ACTIONS에 없으면 자동 실행되면 안 된다.
        import ops_agent.policy as policy_module

        monkeypatch.setitem(
            policy_module.ALLOWED_ACTIONS_BY_ALERTNAME,
            "ServingApiDown",
            RemediationAction.RESTART_SERVING_API,
        )

        decision = decide(incident(alertname="ServingApiDown"))

        assert decision.allowed is False
        assert decision.action == RemediationAction.RESTART_SERVING_API
        assert "not yet implemented" in decision.reason

    def test_escalation_only_actions_are_documented_and_not_implemented(self):
        # 정책 문서화용 목록 자체가 실행 가능한 action 이름과 겹치면 안 된다.
        implemented_values = {action.value for action in RemediationAction}
        assert not set(ESCALATION_ONLY_ACTIONS) & implemented_values
