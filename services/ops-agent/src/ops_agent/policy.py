# "자동으로 뭘 해도 되는지"를 결정하는 유일한 곳 — allowlist에 없거나 미구현이면 decide()가 escalation으로 처리한다.

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from ops_agent.models import Incident


class RemediationAction(str, Enum):
    RESTART_STREAM_PROCESSOR = "restart_stream_processor"
    RESTART_SERVING_API = "restart_serving_api"
    RESTART_AIRFLOW_SCHEDULER = "restart_airflow_scheduler"


# MVP 구현 범위 — Stream Processor restart만 remediation.py에 실제 실행 코드가 있다.
IMPLEMENTED_ACTIONS: frozenset[RemediationAction] = frozenset(
    {RemediationAction.RESTART_STREAM_PROCESSOR}
)

# alertname -> 허용되는 action. 등록 안 된 alertname은 항상 escalation-only.
ALLOWED_ACTIONS_BY_ALERTNAME: dict[str, RemediationAction] = {
    "StreamProcessorDown": RemediationAction.RESTART_STREAM_PROCESSOR,
}

# escalation-only — remediation.py에 실행 코드가 없고, 정책만 문서화하는 목록이다.
ESCALATION_ONLY_ACTIONS: tuple[str, ...] = (
    "kafka_broker_restart",
    "kafka_offset_reset",
    "emr_job_rerun",
    "delete_data_or_s3_object",
    "database_schema_change",
    "infrastructure_change",
    "arbitrary_shell_command",
)


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    action: RemediationAction | None
    allowed: bool
    reason: str


def decide(incident: Incident) -> PolicyDecision:
    """이 incident에 자동 조치를 해도 되는지 판단한다. 실행은 하지 않는다."""
    if not incident.auto_remediate:
        return PolicyDecision(
            action=None,
            allowed=False,
            reason="alert did not opt into auto_remediate",
        )

    action = ALLOWED_ACTIONS_BY_ALERTNAME.get(incident.alertname)
    if action is None:
        return PolicyDecision(
            action=None,
            allowed=False,
            reason=f"no allowlisted action for alertname={incident.alertname!r}",
        )

    if action not in IMPLEMENTED_ACTIONS:
        return PolicyDecision(
            action=action,
            allowed=False,
            reason=f"action {action.value!r} is allowlisted but not yet implemented",
        )

    return PolicyDecision(action=action, allowed=True, reason="allowed")
