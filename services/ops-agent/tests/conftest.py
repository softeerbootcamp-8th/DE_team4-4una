"""Shared test fakes for ops-agent — never call real Prometheus/SSH/Slack."""

from __future__ import annotations

from dataclasses import dataclass, field

from ops_agent.diagnostics import StreamProcessorDiagnostics
from ops_agent.prometheus_client import StreamProcessorStatus
from ops_agent.remediation import RemediationResult
from ops_agent.ssh import CommandKind, ExecutedCommand


def status(code: int, label: str, instance: str = "spark-ec2:9103") -> StreamProcessorStatus:
    return StreamProcessorStatus(code=code, label=label, instance=instance)


def healthy_status() -> StreamProcessorStatus:
    return status(0, "RUNNING")


def down_status() -> StreamProcessorStatus:
    return status(4, "TARGET DOWN")


@dataclass
class FakePrometheusClient:
    """`stream_processor_status()`가 순서대로 미리 정해둔 값을 돌려준다 —
    reverify(조치 전) -> reverify(조치 후) 흐름을 시뮬레이션한다."""

    statuses: list[StreamProcessorStatus]
    calls: int = 0

    def stream_processor_status(self) -> StreamProcessorStatus:
        index = min(self.calls, len(self.statuses) - 1)
        self.calls += 1
        return self.statuses[index]


@dataclass
class FakeSlackClient:
    """메시지 본문뿐 아니라 호출 인자 전체를 기록한다 — blocks/thread_ts 단언에 필요하다."""

    messages: list[tuple[str, str]] = field(default_factory=list)
    calls: list[dict] = field(default_factory=list)

    def chat_postMessage(self, *, channel: str, text: str, **kwargs) -> dict:
        self.messages.append((channel, text))
        self.calls.append({"channel": channel, "text": text, **kwargs})
        return {"ok": True, "ts": f"1700000000.{len(self.calls):06d}"}


def executed(argv: tuple[str, ...], kind: CommandKind = CommandKind.READ) -> ExecutedCommand:
    return ExecutedCommand(kind=kind, host="1.2.3.4", user="ec2-user", argv=argv)


def fake_diagnose(_target) -> StreamProcessorDiagnostics:
    return StreamProcessorDiagnostics(
        container_status="running",
        restart_count=0,
        recent_logs="fake logs",
        commands=(
            executed(("docker", "inspect", "stream-processor")),
            executed(("docker", "logs", "--tail", "50", "stream-processor")),
        ),
    )


def make_fake_remediate(*, succeeded: bool = True):
    def _remediate(_target) -> RemediationResult:
        return RemediationResult(
            action="restart_stream_processor",
            succeeded=succeeded,
            detail="fake ssh output",
            command=executed(
                ("docker", "restart", "stream-processor"), kind=CommandKind.MUTATE
            ),
        )

    return _remediate


def grafana_alert(
    *,
    alertname: str = "StreamProcessorDown",
    service: str = "stream-processor",
    severity: str = "high",
    status_field: str = "firing",
    fingerprint: str = "fp-1",
    auto_remediate: str = "true",
    annotations: dict | None = None,
) -> dict:
    return {
        "status": status_field,
        "labels": {
            "alertname": alertname,
            "service": service,
            "severity": severity,
            "auto_remediate": auto_remediate,
        },
        "annotations": annotations or {"summary": "stream-processor query stopped"},
        "fingerprint": fingerprint,
        "startsAt": "2026-08-25T00:00:00Z",
    }


def grafana_webhook_payload(*alerts: dict) -> dict:
    return {"status": "firing", "alerts": list(alerts) or [grafana_alert()]}
