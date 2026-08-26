# Slack 메시지 조립만 담당한다 — orchestrator가 흐름 제어와 문자열 서식을 함께 지지 않게 분리했다(#546).

from __future__ import annotations

from dataclasses import dataclass

from ops_agent.diagnostics import ContainerDiagnostics
from ops_agent.models import Incident
from ops_agent.owners import ServiceOwner
from ops_agent.policy import PolicyDecision
from ops_agent.prometheus_client import ServiceStatus
from ops_agent.remediation import RemediationResult
from ops_agent.slack_notifier import mention_text
from ops_agent.ssh import CommandKind, ExecutedCommand

# Slack section block의 text는 3000자를 넘을 수 없다. 코드펜스와 안내문 여유를 뺀 값이다.
MAX_LOG_CHARS = 2600


@dataclass(frozen=True, slots=True)
class NotificationInput:
    incident: Incident
    status: ServiceStatus
    diagnostics: ContainerDiagnostics
    policy: PolicyDecision | None
    remediation: RemediationResult | None
    recovered: bool
    owner: ServiceOwner | None
    recent_attempts: int
    extra_reason: str | None = None


def _section(text: str) -> dict:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def _command_lines(commands: tuple[ExecutedCommand, ...], kind: CommandKind) -> str:
    selected = [command for command in commands if command.kind is kind]
    if not selected:
        return ""
    body = "\n".join(command.display for command in selected)
    return f"```\n{body}\n```"


def build_notification(data: NotificationInput) -> tuple[str, list[dict]]:
    """(fallback text, blocks)를 돌려준다. text는 알림 미리보기와 접근성용이다."""
    incident = data.incident
    fallback = (
        f"{incident.alertname} ({incident.service or 'unknown service'}) — "
        f"복구 {'성공' if data.recovered else '실패'}"
    )

    all_commands = tuple(data.diagnostics.commands)
    if data.remediation is not None:
        all_commands = (*all_commands, data.remediation.command)

    blocks: list[dict] = [
        _section(
            f"*{incident.alertname}* · {incident.service or 'unknown service'} · "
            f"severity={incident.severity or 'unknown'}"
        ),
        _section(
            f"*판단 근거*\nPrometheus 상태 `{data.status.label}` · "
            f"container `{data.diagnostics.container_status}` · "
            f"restart_count `{data.diagnostics.restart_count}`"
        ),
    ]

    read_block = _command_lines(all_commands, CommandKind.READ)
    if read_block:
        blocks.append(_section(f"*읽기만 한 명령*\n{read_block}"))

    mutate_block = _command_lines(all_commands, CommandKind.MUTATE)
    if mutate_block:
        blocks.append(_section(f"*⚠️ 변경한 명령*\n{mutate_block}"))
    else:
        reason = data.extra_reason or (
            data.policy.reason if data.policy else "재검증 결과 이미 정상"
        )
        blocks.append(_section(f"*변경한 명령 없음*\n{reason}"))

    if data.remediation is not None:
        detail = data.remediation.detail or "(출력 없음)"
        blocks.append(
            _section(f"*결과*\n`succeeded={data.remediation.succeeded}`\n```\n{detail}\n```")
        )

    blocks.append(
        _section(
            f"*복구 여부* {'성공' if data.recovered else '실패 — 담당자 확인 필요'}\n"
            f"최근 7일 이 incident에 대한 조치 {data.recent_attempts}회"
        )
    )

    # 이미 복구된 장애로 담당자를 깨우지 않는다 — 실패했을 때만 실제 멘션을 건다.
    owner_text = mention_text(data.owner) if not data.recovered else _owner_name(data.owner)
    blocks.append(
        {"type": "context", "elements": [{"type": "mrkdwn", "text": f"담당자: {owner_text}"}]}
    )

    return fallback, blocks


def _owner_name(owner: ServiceOwner | None) -> str:
    return owner.name if owner is not None else "(no owner configured)"


def build_log_reply(diagnostics: ContainerDiagnostics) -> str | None:
    """로그 tail은 본문이 아니라 스레드 답글로 보낸다 — 내용을 통제할 수 없어 메인 메시지에 싣지 않는다."""
    logs = diagnostics.recent_logs.strip()
    if not logs:
        return None
    if len(logs) > MAX_LOG_CHARS:
        # 오래된 앞부분보다 최근 줄이 원인에 가까우므로 뒤쪽을 남긴다.
        logs = logs[-MAX_LOG_CHARS:]
        return f"수집한 로그 tail (앞부분이 잘렸습니다)\n```\n{logs}\n```"
    return f"수집한 로그 tail\n```\n{logs}\n```"
