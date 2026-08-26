# 조치 3종(stream-processor / serving-api / node-exporter)이 전부 "컨테이너 하나 재시작"이라 함수를 하나로 합쳤다(#547).

from __future__ import annotations

from dataclasses import dataclass

from ops_agent.ssh import CommandKind, ExecutedCommand, SshTarget, run_remote_command


@dataclass(frozen=True, slots=True)
class RemediationResult:
    action: str
    succeeded: bool
    detail: str
    command: ExecutedCommand


def restart_container(target: SshTarget, container: str, *, action: str) -> RemediationResult:
    result = run_remote_command(
        target, ["docker", "restart", container], kind=CommandKind.MUTATE
    )
    return RemediationResult(
        action=action,
        succeeded=result.ok,
        detail=(result.stdout.strip() if result.ok else result.stderr.strip()),
        command=result.command,
    )
