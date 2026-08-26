# 진단은 읽기 전용 명령만 쓴다. 대상 컨테이너 이름은 policy.py의 ActionSpec이 주고 여기서 정하지 않는다.

from __future__ import annotations

from dataclasses import dataclass

from ops_agent.ssh import CommandKind, ExecutedCommand, SshTarget, run_remote_command

_LOG_TAIL_LINES = 50


@dataclass(frozen=True, slots=True)
class ContainerDiagnostics:
    container_status: str
    restart_count: int | None
    recent_logs: str
    commands: tuple[ExecutedCommand, ...]


def collect_container_diagnostics(target: SshTarget, container: str) -> ContainerDiagnostics:
    inspect = run_remote_command(
        target,
        ["docker", "inspect", "--format", "{{.State.Status}}|{{.RestartCount}}", container],
        kind=CommandKind.READ,
    )
    if not inspect.ok:
        return ContainerDiagnostics(
            container_status="not found",
            restart_count=None,
            recent_logs=inspect.stderr.strip(),
            commands=(inspect.command,),
        )

    status_field, _, restart_field = inspect.stdout.strip().partition("|")
    try:
        restart_count = int(restart_field)
    except ValueError:
        restart_count = None

    logs = run_remote_command(
        target,
        ["docker", "logs", "--tail", str(_LOG_TAIL_LINES), container],
        kind=CommandKind.READ,
    )
    return ContainerDiagnostics(
        container_status=status_field or "unknown",
        restart_count=restart_count,
        recent_logs=(logs.stdout + logs.stderr).strip(),
        commands=(inspect.command, logs.command),
    )
