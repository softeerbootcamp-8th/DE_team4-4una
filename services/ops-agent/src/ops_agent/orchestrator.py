# end-to-end incident flow(#447): alert -> reverify -> diagnose -> policy -> dedupe -> remediate -> reverify -> notify. 각 단계 구현은 다른 모듈에 위임하고 순서/분기만 책임진다.

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass

from ops_agent.diagnostics import (
    StreamProcessorDiagnostics,
    collect_stream_processor_diagnostics,
)
from ops_agent.incident_store import IncidentStore
from ops_agent.models import Incident
from ops_agent.owners import ServiceOwnersRegistry
from ops_agent.policy import PolicyDecision, decide
from ops_agent.prometheus_client import PrometheusClient, StreamProcessorStatus
from ops_agent.remediation import RemediationResult, restart_stream_processor
from ops_agent.slack_notifier import SlackNotifier, mention_text
from ops_agent.ssh import SshTarget

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class IncidentOutcome:
    incident: Incident
    handled: bool
    reverified_status: StreamProcessorStatus | None
    policy: PolicyDecision | None
    diagnostics: StreamProcessorDiagnostics | None
    remediation: RemediationResult | None
    recovered: bool | None
    escalated: bool
    summary: str


class OpsAgentOrchestrator:
    def __init__(
        self,
        *,
        prometheus: PrometheusClient,
        incident_store: IncidentStore,
        owners: ServiceOwnersRegistry,
        ssh_target: SshTarget,
        slack: SlackNotifier,
        cooldown_seconds: float,
        recovery_poll_interval_seconds: float = 10.0,
        recovery_wait_seconds: float = 90.0,
        diagnose: Callable[[SshTarget], StreamProcessorDiagnostics] = collect_stream_processor_diagnostics,
        remediate: Callable[[SshTarget], RemediationResult] = restart_stream_processor,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        # diagnose/remediate/sleep은 테스트가 실제 SSH/대기 없이 fake를 주입할 수 있게 콜백으로 뒀다.
        self._prometheus = prometheus
        self._incident_store = incident_store
        self._owners = owners
        self._ssh_target = ssh_target
        self._slack = slack
        self._cooldown_seconds = cooldown_seconds
        self._recovery_poll_interval_seconds = recovery_poll_interval_seconds
        self._recovery_wait_seconds = recovery_wait_seconds
        self._diagnose = diagnose
        self._remediate = remediate
        self._sleep = sleep

    def handle(self, incident: Incident) -> IncidentOutcome:
        if not incident.is_firing:
            return IncidentOutcome(
                incident=incident,
                handled=False,
                reverified_status=None,
                policy=None,
                diagnostics=None,
                remediation=None,
                recovered=None,
                escalated=False,
                summary=f"{incident.alertname}: status={incident.status!r}, no action taken",
            )

        status = self._prometheus.stream_processor_status()
        if status.is_healthy:
            return IncidentOutcome(
                incident=incident,
                handled=True,
                reverified_status=status,
                policy=None,
                diagnostics=None,
                remediation=None,
                recovered=True,
                escalated=False,
                summary=(
                    f"{incident.alertname}: Prometheus reports {status.label} on "
                    "reverify — no action taken (Grafana alert may have been stale)"
                ),
            )

        policy_decision = decide(incident)
        diagnostics = self._diagnose(self._ssh_target)

        if not policy_decision.allowed:
            self._notify(incident, status, diagnostics, policy_decision, remediation=None, recovered=False)
            return IncidentOutcome(
                incident=incident,
                handled=True,
                reverified_status=status,
                policy=policy_decision,
                diagnostics=diagnostics,
                remediation=None,
                recovered=False,
                escalated=True,
                summary=f"{incident.alertname}: not auto-remediated ({policy_decision.reason}), escalated",
            )

        if not self._incident_store.should_attempt(incident.fingerprint, self._cooldown_seconds):
            self._notify(
                incident,
                status,
                diagnostics,
                policy_decision,
                remediation=None,
                recovered=False,
                extra_reason="remediation already attempted recently (cooldown active)",
            )
            return IncidentOutcome(
                incident=incident,
                handled=True,
                reverified_status=status,
                policy=policy_decision,
                diagnostics=diagnostics,
                remediation=None,
                recovered=False,
                escalated=True,
                summary=f"{incident.alertname}: cooldown active, skipped remediation, escalated",
            )

        self._incident_store.record_attempt(
            incident.fingerprint, incident.alertname, policy_decision.action.value
        )
        remediation_result = self._remediate(self._ssh_target)

        post_status = self._wait_for_recovery()
        recovered = post_status.is_healthy

        self._notify(incident, post_status, diagnostics, policy_decision, remediation=remediation_result, recovered=recovered)

        return IncidentOutcome(
            incident=incident,
            handled=True,
            reverified_status=post_status,
            policy=policy_decision,
            diagnostics=diagnostics,
            remediation=remediation_result,
            recovered=recovered,
            escalated=not recovered,
            summary=(
                f"{incident.alertname}: remediation={policy_decision.action.value} "
                f"succeeded={remediation_result.succeeded} recovered={recovered}"
            ),
        )

    def _wait_for_recovery(self) -> StreamProcessorStatus:
        # docker restart 직후엔 Spark JVM 기동/Prometheus scrape가 아직 안 끝났을 수 있어 즉시 판정하지 않고 폴링한다.
        status = self._prometheus.stream_processor_status()
        elapsed = 0.0
        while not status.is_healthy and elapsed < self._recovery_wait_seconds:
            self._sleep(self._recovery_poll_interval_seconds)
            elapsed += self._recovery_poll_interval_seconds
            status = self._prometheus.stream_processor_status()
        return status

    def _notify(
        self,
        incident: Incident,
        status: StreamProcessorStatus,
        diagnostics: StreamProcessorDiagnostics,
        policy_decision: PolicyDecision,
        *,
        remediation: RemediationResult | None,
        recovered: bool,
        extra_reason: str | None = None,
    ) -> None:
        owner = self._owners.resolve(incident.service) if incident.service else None
        lines = [
            f"*{incident.alertname}* ({incident.service or 'unknown service'}, severity={incident.severity or 'unknown'})",
            (
                f"진단: Prometheus 상태={status.label}, container={diagnostics.container_status}, "
                f"restart_count={diagnostics.restart_count}"
            ),
        ]
        if remediation is not None:
            lines.append(f"실행한 조치: {remediation.action} (succeeded={remediation.succeeded})")
        else:
            reason = extra_reason or policy_decision.reason
            lines.append(f"자동 조치 없음: {reason}")
        lines.append(f"복구 여부: {'성공' if recovered else '실패 — 담당자 확인 필요'}")
        lines.append(f"담당자: {mention_text(owner)}")

        try:
            self._slack.post("\n".join(lines))
        except Exception:
            logger.exception("failed to post Slack notification for %s", incident.alertname)
