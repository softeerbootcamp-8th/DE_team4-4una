from __future__ import annotations

import json

from ops_agent.diagnostics import ContainerDiagnostics
from ops_agent.models import Incident
from ops_agent.notification import (
    MAX_LOG_CHARS,
    NotificationInput,
    build_log_reply,
    build_notification,
)
from ops_agent.owners import ServiceOwner
from ops_agent.policy import PolicyDecision, RemediationAction
from ops_agent.remediation import RemediationResult
from ops_agent.ssh import CommandKind
from ops_agent_test_support import (
    down_status,
    executed,
    fake_diagnose,
    grafana_alert,
    healthy_status,
)

OWNER = ServiceOwner(name="bob", email=None, slack_id="U0456GHIJKL", severity="high")


def diagnostics_with_logs(logs: str) -> ContainerDiagnostics:
    base = fake_diagnose(None)
    return ContainerDiagnostics(
        container_status=base.container_status,
        restart_count=base.restart_count,
        recent_logs=logs,
        commands=base.commands,
    )


def make_input(**overrides) -> NotificationInput:
    base = {
        "incident": Incident.from_grafana_alert(grafana_alert()),
        "status": down_status(),
        "diagnostics": fake_diagnose(None),
        "policy": PolicyDecision(
            action=RemediationAction.RESTART_STREAM_PROCESSOR, allowed=True, reason="allowed"
        ),
        "remediation": RemediationResult(
            action="restart_stream_processor",
            succeeded=True,
            detail="stream-processor",
            command=executed(("docker", "restart", "stream-processor"), kind=CommandKind.MUTATE),
        ),
        "recovered": True,
        "owner": OWNER,
        "recent_attempts": 1,
        "extra_reason": None,
    }
    base.update(overrides)
    return NotificationInput(**base)


def rendered(blocks: list[dict]) -> str:
    return json.dumps(blocks, ensure_ascii=False)


class TestBuildNotification:
    def test_read_and_mutating_commands_are_shown_in_separate_blocks(self):
        _text, blocks = build_notification(make_input())

        body = rendered(blocks)
        assert "읽기만 한 명령" in body
        assert "변경한 명령" in body
        assert "docker inspect" in body
        assert "docker restart stream-processor" in body

    def test_no_mutating_command_is_stated_explicitly(self):
        _text, blocks = build_notification(make_input(remediation=None, recovered=False))

        assert "변경한 명령 없음" in rendered(blocks)

    def test_the_extra_reason_wins_over_the_policy_reason(self):
        _text, blocks = build_notification(
            make_input(remediation=None, recovered=False, extra_reason="cooldown active")
        )

        body = rendered(blocks)
        assert "cooldown active" in body
        assert "allowed" not in body

    def test_a_failed_recovery_mentions_the_owner(self):
        _text, blocks = build_notification(make_input(recovered=False))

        assert "<@U0456GHIJKL>" in rendered(blocks)

    def test_a_successful_recovery_names_the_owner_without_mentioning(self):
        # 이미 복구된 장애로 담당자를 깨우지 않는다.
        _text, blocks = build_notification(make_input(recovered=True))

        body = rendered(blocks)
        assert "<@U0456GHIJKL>" not in body
        assert "bob" in body

    def test_the_reverified_status_is_shown_as_the_basis_for_the_decision(self):
        _text, blocks = build_notification(make_input(status=healthy_status()))

        assert "RUNNING" in rendered(blocks)

    def test_recent_attempt_count_is_included(self):
        _text, blocks = build_notification(make_input(recent_attempts=3))

        assert "3회" in rendered(blocks)

    def test_the_log_tail_is_never_in_the_main_message(self):
        _text, blocks = build_notification(
            make_input(diagnostics=diagnostics_with_logs("secret log line"))
        )

        assert "secret log line" not in rendered(blocks)

    def test_the_fallback_text_names_the_alert(self):
        text, _blocks = build_notification(make_input())

        assert "StreamProcessorDown" in text


class TestBuildLogReply:
    def test_none_when_there_are_no_logs(self):
        assert build_log_reply(diagnostics_with_logs("")) is None

    def test_none_when_the_logs_are_only_whitespace(self):
        assert build_log_reply(diagnostics_with_logs("   \n  ")) is None

    def test_short_logs_are_passed_through_in_a_code_fence(self):
        reply = build_log_reply(diagnostics_with_logs("boom"))

        assert reply is not None
        assert "```\nboom\n```" in reply
        assert "잘렸습니다" not in reply

    def test_long_logs_keep_the_tail_and_say_so(self):
        # 오래된 앞부분보다 최근 줄이 원인에 가까우므로 뒤쪽이 남아야 한다.
        logs = "OLD" + ("x" * MAX_LOG_CHARS) + "NEWEST"
        reply = build_log_reply(diagnostics_with_logs(logs))

        assert reply is not None
        assert "잘렸습니다" in reply
        assert "NEWEST" in reply
        assert "OLD" not in reply
        assert len(reply) <= MAX_LOG_CHARS + 200
