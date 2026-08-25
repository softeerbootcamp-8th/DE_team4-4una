# remediation 전 최소 진단. 원격 접근은 ssh.py의 고정 argv 실행만 쓰고 임의 shell 문자열은 조립하지 않는다.

from __future__ import annotations

from dataclasses import dataclass

from ops_agent.ssh import SshTarget, run_remote_command

# deploy-stream-processor.yml의 `CONTAINER: stream-processor`와 반드시 같아야 한다.
STREAM_PROCESSOR_CONTAINER_NAME = "stream-processor"

_LOG_TAIL_LINES = 50


@dataclass(frozen=True, slots=True)
class StreamProcessorDiagnostics:
    container_status: str
    restart_count: int | None
    recent_logs: str


def collect_stream_processor_diagnostics(target: SshTarget) -> StreamProcessorDiagnostics:
    inspect = run_remote_command(
        target,
        [
            "docker",
            "inspect",
            "--format",
            "{{.State.Status}}|{{.RestartCount}}",
            STREAM_PROCESSOR_CONTAINER_NAME,
        ],
    )
    if not inspect.ok:
        return StreamProcessorDiagnostics(
            container_status="not found",
            restart_count=None,
            recent_logs=inspect.stderr.strip(),
        )

    status_field, _, restart_field = inspect.stdout.strip().partition("|")
    try:
        restart_count = int(restart_field)
    except ValueError:
        restart_count = None

    logs = run_remote_command(
        target,
        ["docker", "logs", "--tail", str(_LOG_TAIL_LINES), STREAM_PROCESSOR_CONTAINER_NAME],
    )
    return StreamProcessorDiagnostics(
        container_status=status_field or "unknown",
        restart_count=restart_count,
        recent_logs=(logs.stdout + logs.stderr).strip(),
    )
