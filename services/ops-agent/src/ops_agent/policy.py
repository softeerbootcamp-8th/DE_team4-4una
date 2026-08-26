# "자동으로 뭘 해도 되는지"를 결정하는 유일한 곳 — ACTION_SPECS에 없거나 미구현이면 decide()가 escalation으로 처리한다.
# 무엇이 저위험인지의 판정 기준은 docs/adr/0013-immediate-remediation-without-slack-approval.md에 있다.

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from ops_agent.models import Incident
from ops_agent.prometheus_client import STREAM_PROCESSOR_HEALTH, HealthCheck


class RemediationAction(str, Enum):
    RESTART_STREAM_PROCESSOR = "restart_stream_processor"
    RESTART_SERVING_API = "restart_serving_api"
    RESTART_NODE_EXPORTER = "restart_node_exporter"
    RESTART_AIRFLOW_SCHEDULER = "restart_airflow_scheduler"


# remediation.py의 restart_container로 실제 실행 가능한 조치. Airflow Scheduler는
# 실행 중인 task에 영향을 주어 ADR-0013의 저위험 조건 4를 위반하므로 여기 없다.
IMPLEMENTED_ACTIONS: frozenset[RemediationAction] = frozenset(
    {
        RemediationAction.RESTART_STREAM_PROCESSOR,
        RemediationAction.RESTART_SERVING_API,
        RemediationAction.RESTART_NODE_EXPORTER,
    }
)

# serving-api는 metrics 서버를 uvicorn과 같은 프로세스에서 띄운다
# (services/serving-api/src/serving_api/__init__.py:21) — 그래서 up이 곧 프로세스 생사다.
# DB만 죽으면 앱은 살아서 metrics를 계속 내보내므로 up은 1이고 이 조치는 발화하지 않는다.
# up은 1이 정상이라 그대로 쓰면 evaluate()의 max()가 최악이 아니라 최선을 고른다.
# 그래서 뒤집어 "값이 클수록 심각"하게 맞춘다.
SERVING_API_HEALTH = HealthCheck(
    query='1 - up{job="serving-api"}', labels={0: "UP", 1: "DOWN"}, healthy_code=0
)

SPARK_NODE_EXPORTER_HEALTH = HealthCheck(
    query='1 - up{job="spark-node"}', labels={0: "UP", 1: "DOWN"}, healthy_code=0
)


@dataclass(frozen=True, slots=True)
class ActionSpec:
    """alert 하나가 어떤 조치로 이어지는지의 전부 — 상태 판정, 접속 대상, 컨테이너 이름."""

    action: RemediationAction
    health: HealthCheck
    # config.ssh_targets()의 키. 설정이 없으면 orchestrator가 조치 없이 escalation한다.
    ssh_target_key: str
    container: str


# alertname -> 명세. 여기 없는 alertname은 항상 escalation-only다.
ACTION_SPECS: dict[str, ActionSpec] = {
    "StreamProcessorDown": ActionSpec(
        action=RemediationAction.RESTART_STREAM_PROCESSOR,
        health=STREAM_PROCESSOR_HEALTH,
        ssh_target_key="spark",
        container="stream-processor",
    ),
    "ServingApiDown": ActionSpec(
        action=RemediationAction.RESTART_SERVING_API,
        health=SERVING_API_HEALTH,
        ssh_target_key="project",
        container="serving-api",
    ),
    # 호스트 자체가 죽은 경우와 exporter만 죽은 경우는 SSH 성공 여부로 자연히 갈린다 —
    # 붙으면 재시작하고, 안 붙으면 escalation된다. 별도 판별 로직이 필요 없다.
    "SparkNodeExporterDown": ActionSpec(
        action=RemediationAction.RESTART_NODE_EXPORTER,
        health=SPARK_NODE_EXPORTER_HEALTH,
        ssh_target_key="spark",
        container="node-exporter",
    ),
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


def decide(incident: Incident, spec: ActionSpec | None) -> PolicyDecision:
    """이 incident에 자동 조치를 해도 되는지 판단한다. 실행은 하지 않는다."""
    if not incident.auto_remediate:
        return PolicyDecision(
            action=None,
            allowed=False,
            reason="alert did not opt into auto_remediate",
        )

    if spec is None:
        return PolicyDecision(
            action=None,
            allowed=False,
            reason=f"no action spec for alertname={incident.alertname!r}",
        )

    if spec.action not in IMPLEMENTED_ACTIONS:
        return PolicyDecision(
            action=spec.action,
            allowed=False,
            reason=f"action {spec.action.value!r} is allowlisted but not yet implemented",
        )

    return PolicyDecision(action=spec.action, allowed=True, reason="allowed")
