# 유일하게 원격 쉘로 나가는 지점. argv는 항상 호출부의 고정 문자열이어야 한다 — alert payload 값이 여기 흘러들면 안 된다(SSH가 원격에서 argv를 공백으로 이어붙여 실행하므로 이 불변식이 유일한 방어선).

from __future__ import annotations

import shlex
import subprocess
from dataclasses import dataclass
from enum import Enum


class CommandKind(str, Enum):
    """읽기만 하는 명령과 상태를 바꾸는 명령을 구분한다 — Slack 알림에서 갈라 보여주기 위해서다."""

    READ = "read"
    MUTATE = "mutate"


@dataclass(frozen=True, slots=True)
class SshTarget:
    host: str
    user: str
    key_path: str


@dataclass(frozen=True, slots=True)
class ExecutedCommand:
    kind: CommandKind
    host: str
    user: str
    argv: tuple[str, ...]

    @property
    def display(self) -> str:
        # 로컬 ssh 래퍼(-i 키 경로 등)는 읽는 사람에게 의미가 없고 키 경로만 드러내므로 원격 명령만 보여준다.
        return f"{self.user}@{self.host} $ {shlex.join(self.argv)}"


@dataclass(frozen=True, slots=True)
class SshResult:
    exit_code: int
    stdout: str
    stderr: str
    command: ExecutedCommand

    @property
    def ok(self) -> bool:
        return self.exit_code == 0


def run_remote_command(
    target: SshTarget,
    argv: list[str],
    *,
    kind: CommandKind,
    timeout_seconds: float = 30.0,
) -> SshResult:
    # kind를 키워드 필수 인자로 둬서 호출부가 읽기/변경 분류를 빠뜨릴 수 없게 한다.
    executed = ExecutedCommand(
        kind=kind, host=target.host, user=target.user, argv=tuple(argv)
    )
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
            command=executed,
        )
    return SshResult(
        exit_code=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
        command=executed,
    )
