# 유일하게 원격 쉘로 나가는 지점. argv는 항상 remediation.py의 고정 문자열이어야 한다 — alert payload 값이 여기 흘러들면 안 된다(SSH가 원격에서 argv를 공백으로 이어붙여 실행하므로 이 불변식이 유일한 방어선).

from __future__ import annotations

import subprocess
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SshTarget:
    host: str
    user: str
    key_path: str


@dataclass(frozen=True, slots=True)
class SshResult:
    exit_code: int
    stdout: str
    stderr: str

    @property
    def ok(self) -> bool:
        return self.exit_code == 0


def run_remote_command(
    target: SshTarget,
    argv: list[str],
    *,
    timeout_seconds: float = 30.0,
) -> SshResult:
    command = [
        "ssh",
        "-i",
        target.key_path,
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        "BatchMode=yes",
        f"{target.user}@{target.host}",
        "--",
        *argv,
    ]
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        return SshResult(
            exit_code=-1,
            stdout=(error.stdout or b"").decode() if isinstance(error.stdout, bytes) else (error.stdout or ""),
            stderr=f"timed out after {timeout_seconds}s",
        )
    return SshResult(exit_code=completed.returncode, stdout=completed.stdout, stderr=completed.stderr)
