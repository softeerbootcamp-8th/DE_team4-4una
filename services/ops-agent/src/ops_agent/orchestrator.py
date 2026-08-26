# end-to-end incident flow(#447): alert -> reverify -> diagnose -> policy -> dedupe -> remediate -> reverify -> notify. 각 단계 구현은 다른 모듈에 위임하고 순서/분기만 책임진다.

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass

from ops_agent.diagnostics import ContainerDiagnostics, collect_container_diagnostics
from ops_agent.incident_store import IncidentStore
from ops_agent.models import Incident
from ops_agent.notification import (
    NotificationInput,
    build_log_reply,
    build_notification,
)
from ops_agent.owners import ServiceOwnersRegistry
from ops_agent.policy import ACTION_SPECS, PolicyDecision, decide
from ops_agent.prometheus_client import HealthCheck, PrometheusClient, ServiceStatus
from ops_agent.remediation import RemediationResult, restart_container
from ops_agent.slack_notifier import SlackNotifier
from ops_agent.ssh import SshTarget

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class IncidentOutcome:
    incident: Incident
    handled: bool
    reverified_status: ServiceStatus | None
    policy: PolicyDecision | None
    diagnostics: ContainerDiagnostics | None
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
        ssh_targets: Mapping[str, SshTarget],
        slack: SlackNotifier,
        cooldown_seconds: float,
        recovery_poll_interval_seconds: float = 10.0,
        recovery_wait_seconds: float = 90.0,
        history_window_seconds: float = 7 * 24 * 3600,
        diagnose: Callable[..., ContainerDiagnostics] = collect_container_diagnostics,
        remediate: Callable[..., RemediationResult] = restart_container,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        # diagnose/remediate/sleep은 테스트가 실제 SSH/대기 없이 fake를 주입할 수 있게 콜백으로 뒀다.
        self._prometheus = prometheus
        self._incident_store = incident_store
        self._owners = owners
        self._ssh_targets = ssh_targets
        self._slack = slack
        self._cooldown_seconds = cooldown_seconds
        self._recovery_poll_interval_seconds = recovery_poll_interval_seconds
        self._recovery_wait_seconds = recovery_wait_seconds
        self._history_window_seconds = history_window_seconds
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

        spec = ACTION_SPECS.get(incident.alertname)
        if spec is None:
            # 담당 명세가 없으면 확인할 PromQL도 진단할 컨테이너도 정해지지 않는다.
            # 넘겨짚지 않고 어떤 호스트도 건드리지 않는다.
            return self._escalate_without_spec(incident)

        status = self._prometheus.evaluate(spec.health)
        self._log_stage("reverify", incident, status=status.label)
        if status.is_healthy:
            # 조치하지 않더라도 알린다 — 침묵하면 Grafana의 감지 알림 뒤로 후속이 없어
            # agent가 죽은 것인지 판단하고 넘어간 것인지 구분할 수 없다(설계 §1-1).
            diagnostics = self._diagnose(
                self._ssh_targets[spec.ssh_target_key], spec.container
            )
            self._log_diagnose(incident, diagnostics)
            self._notify(
                incident,
                status,
                diagnostics,
                None,
                remediation=None,
                recovered=True,
                extra_reason="재검증 결과 이미 정상 — Grafana alert가 stale이었던 것으로 보인다",
            )
            return IncidentOutcome(
                incident=incident,
                handled=True,
                reverified_status=status,
                policy=None,
                diagnostics=diagnostics,
                remediation=None,
                recovered=True,
                escalated=False,
                summary=(
                    f"{incident.alertname}: Prometheus reports {status.label} on "
                    "reverify — no action taken (Grafana alert may have been stale)"
                ),
            )

        policy_decision = decide(incident, spec)
        self._log_stage(
            "policy", incident, allowed=policy_decision.allowed, reason=policy_decision.reason
        )

        target = self._ssh_targets.get(spec.ssh_target_key)
        if target is None:
            # 설정이 없는 호스트를 넘겨짚어 엉뚱한 곳에 docker restart를 쏘지 않는다.
            policy_decision = PolicyDecision(
                action=spec.action,
                allowed=False,
                reason=f"no ssh target configured for {spec.ssh_target_key!r}",
            )
            diagnostics = ContainerDiagnostics("unknown", None, "", ())
        else:
            diagnostics = self._diagnose(target, spec.container)
        self._log_diagnose(incident, diagnostics)

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

        event_id = self._incident_store.record_attempt(
            incident.fingerprint, incident.alertname, policy_decision.action.value
        )
        remediation_result = self._remediate(target, spec.container, action=spec.action.value)
        self._log_stage(
            "remediate",
            incident,
            action=policy_decision.action.value,
            succeeded=remediation_result.succeeded,
            command=" ".join(remediation_result.command.argv),
        )

        post_status = self._wait_for_recovery(spec.health)
        recovered = post_status.is_healthy
        self._log_stage("recovery", incident, status=post_status.label, recovered=recovered)
        self._incident_store.record_outcome(
            event_id, succeeded=remediation_result.succeeded, recovered=recovered
        )

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

    def _log_stage(self, stage: str, incident: Incident, **fields) -> None:
        # Slack에 싣지 않는 상세를 사후에 추적하려고 단계마다 한 줄 JSON으로 남긴다.
        logger.info(
            json.dumps(
                {
                    "stage": stage,
                    "alertname": incident.alertname,
                    "fingerprint": incident.fingerprint,
                    **fields,
                },
                ensure_ascii=False,
            )
        )

    def _log_diagnose(
        self, incident: Incident, diagnostics: ContainerDiagnostics
    ) -> None:
        self._log_stage(
            "diagnose",
            incident,
            container=diagnostics.container_status,
            restart_count=diagnostics.restart_count,
            commands=[command.display for command in diagnostics.commands],
        )

    def _wait_for_recovery(self, check: HealthCheck) -> ServiceStatus:
        # docker restart 직후엔 프로세스 기동/Prometheus scrape가 아직 안 끝났을 수 있어 즉시 판정하지 않고 폴링한다.
        status = self._prometheus.evaluate(check)
        elapsed = 0.0
        while not status.is_healthy and elapsed < self._recovery_wait_seconds:
            self._sleep(self._recovery_poll_interval_seconds)
            elapsed += self._recovery_poll_interval_seconds
            status = self._prometheus.evaluate(check)
        return status

    def _escalate_without_spec(self, incident: Incident) -> IncidentOutcome:
        decision = decide(incident, None)
        empty = ContainerDiagnostics("unknown", None, "", ())
        status = ServiceStatus(code=None, label="NOT CHECKED", instance=None, healthy_code=0)
        self._notify(incident, status, empty, decision, remediation=None, recovered=False)
        return IncidentOutcome(
            incident=incident,
            handled=True,
            reverified_status=None,
            policy=decision,
            diagnostics=None,
            remediation=None,
            recovered=False,
            escalated=True,
            summary=f"{incident.alertname}: no action spec, escalated",
        )

    def _notify(
        self,
        incident: Incident,
        status: ServiceStatus,
        diagnostics: ContainerDiagnostics,
        policy_decision: PolicyDecision | None,
        *,
        remediation: RemediationResult | None,
        recovered: bool,
        extra_reason: str | None = None,
    ) -> None:
        owner = self._owners.resolve(incident.service) if incident.service else None
        text, blocks = build_notification(
            NotificationInput(
                incident=incident,
                status=status,
                diagnostics=diagnostics,
                policy=policy_decision,
                remediation=remediation,
                recovered=recovered,
                owner=owner,
                recent_attempts=self._incident_store.count_recent(
                    incident.fingerprint, self._history_window_seconds
                ),
                extra_reason=extra_reason,
            )
        )

        try:
            thread_ts = self._slack.post(text, blocks=blocks)
        except Exception:
            logger.exception("failed to post Slack notification for %s", incident.alertname)
            return

        reply = build_log_reply(diagnostics)
        if reply is None or thread_ts is None:
            return
        try:
            self._slack.post_thread_reply(thread_ts, reply)
        except Exception:
            # 본문은 이미 나갔으므로 답글 실패로 전체를 실패시키지 않는다.
            logger.exception("failed to post log tail for %s", incident.alertname)
