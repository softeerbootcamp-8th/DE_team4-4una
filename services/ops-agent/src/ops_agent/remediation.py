# MVP 범위: restart_stream_processor 하나뿐(#447). image/env/checkpoint 재구성 없이 docker restart만 한다.

from __future__ import annotations

from dataclasses import dataclass

from ops_agent.diagnostics import STREAM_PROCESSOR_CONTAINER_NAME
from ops_agent.ssh import CommandKind, ExecutedCommand, SshTarget, run_remote_command


@dataclass(frozen=True, slots=True)
class RemediationResult:
    action: str
    succeeded: bool
    detail: str
    command: ExecutedCommand


def restart_stream_processor(target: SshTarget) -> RemediationResult:
    result = run_remote_command(
        target,
        ["docker", "restart", STREAM_PROCESSOR_CONTAINER_NAME],
        kind=CommandKind.MUTATE,
    )
    return RemediationResult(
        action="restart_stream_processor",
        succeeded=result.ok,
        detail=(result.stdout.strip() if result.ok else result.stderr.strip()),
        command=result.command,
    )
