# Ops Agent 실행 명령 가시성 구현 계획 (#546)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ops Agent가 어떤 호스트에서 어떤 명령을 실행했는지, 그중 무엇이 상태를 바꾼
명령인지를 Slack 알림만 보고 알 수 있게 한다.

**Architecture:** 원격 명령을 실행하는 유일한 지점(`ssh.py`)이 실행된 argv와 읽기/변경
분류를 결과에 담아 돌려주고, 그 값이 진단·조치 결과를 타고 orchestrator까지 흘러
Slack Block Kit 메시지로 조립된다. 실행 이력은 append-only 테이블에 남기고 cooldown
판정을 그 테이블로 옮긴다. 로그 tail은 본문이 아니라 스레드 답글로 분리한다.

**Tech Stack:** Python 3.12, uv workspace, FastAPI, slack_sdk, sqlite3, pytest

**Spec:** `docs/superpowers/specs/2026-08-26-ops-agent-visibility-and-safe-actions-design.md` §1
**ADR:** `docs/adr/0013-immediate-remediation-without-slack-approval.md`

## Global Constraints

- Python 3.12, 저장소 루트에서 `uv run --package ops-agent pytest services/ops-agent/tests`로 실행한다.
- 모든 모듈은 `from __future__ import annotations`로 시작한다. 이 저장소의 기존 파일이 전부 그렇다.
- 데이터 구조는 `@dataclass(frozen=True, slots=True)`를 쓴다.
- **`ssh.py`의 불변식:** 원격 argv는 코드에 고정된 값으로만 구성한다. alert payload에서 온 값이 argv에 들어가는 경로를 만들지 않는다.
- 자명하지 않은 코드에는 **왜** 그렇게 했는지 한국어 주석을 단다. 무엇을 하는지 반복하는 주석은 달지 않는다.
- 커밋 메시지는 `<type>: <subject>` 형식, 영어 소문자 명령형. Co-author 푸터를 넣지 않는다.
- 이 계획은 조치 추가와 레지스트리 구조 변경(#547)을 다루지 않는다. `StreamProcessorDiagnostics` 같은 stream-processor 전용 이름은 그대로 두고 #547에서 일반화한다.

## 이 계획이 의도적으로 바꾸는 기존 테스트

`services/ops-agent/tests/test_orchestrator.py`의
`test_a_firing_alert_that_prometheus_now_reports_healthy_is_ignored`는 현재
`assert slack_client.messages == []`를 단언한다. Task 5가 바로 이 침묵을 없애므로 이
단언은 **반드시 뒤집힌다.** 원인을 모른 채 실패 테스트를 지우는 것이 아니라, 명세가
바뀌어 기대값을 갱신하는 것이다. Task 5에 수정 방법을 명시했다.

---

### Task 1: 실행된 명령을 결과에 담는다

**Files:**
- Modify: `services/ops-agent/src/ops_agent/ssh.py`
- Modify: `services/ops-agent/src/ops_agent/diagnostics.py`
- Modify: `services/ops-agent/src/ops_agent/remediation.py`
- Modify: `services/ops-agent/tests/test_ssh.py`
- Modify: `services/ops-agent/tests/test_diagnostics.py`
- Modify: `services/ops-agent/tests/test_remediation.py`
- Modify: `services/ops-agent/tests/conftest.py`

**Interfaces:**
- Produces: `CommandKind.READ` / `CommandKind.MUTATE`; `ExecutedCommand(kind, host, user, argv)` with `.display` property; `SshResult.command: ExecutedCommand`; `run_remote_command(target, argv, *, kind, timeout_seconds=30.0)`; `StreamProcessorDiagnostics.commands: tuple[ExecutedCommand, ...]`; `RemediationResult.command: ExecutedCommand`

- [ ] **Step 1: 실행된 명령이 결과에 담기는지 확인하는 실패 테스트를 쓴다**

`services/ops-agent/tests/test_ssh.py`의 `TestRunRemoteCommand` 클래스 안에 추가한다.

```python
    def test_the_result_carries_the_remote_argv_and_its_kind(self, monkeypatch):
        monkeypatch.setattr(
            subprocess, "run", lambda command, **kwargs: _FakeCompleted(0, "ok", "")
        )
        target = SshTarget(host="1.2.3.4", user="ec2-user", key_path="/keys/id_ed25519")

        result = run_remote_command(
            target, ["docker", "restart", "stream-processor"], kind=CommandKind.MUTATE
        )

        assert result.command.kind is CommandKind.MUTATE
        assert result.command.argv == ("docker", "restart", "stream-processor")
        assert result.command.host == "1.2.3.4"

    def test_display_shows_the_remote_command_not_the_local_ssh_wrapper(self, monkeypatch):
        monkeypatch.setattr(
            subprocess, "run", lambda command, **kwargs: _FakeCompleted(0, "ok", "")
        )
        target = SshTarget(host="1.2.3.4", user="ec2-user", key_path="/keys/id_ed25519")

        result = run_remote_command(
            target, ["docker", "logs", "--tail", "50", "stream-processor"], kind=CommandKind.READ
        )

        assert result.command.display == (
            "ec2-user@1.2.3.4 $ docker logs --tail 50 stream-processor"
        )
        assert "/keys/id_ed25519" not in result.command.display

    def test_a_timeout_still_reports_which_command_was_attempted(self, monkeypatch):
        def fake_run(command, **kwargs):
            raise subprocess.TimeoutExpired(cmd=command, timeout=kwargs.get("timeout", 30))

        monkeypatch.setattr(subprocess, "run", fake_run)
        target = SshTarget(host="1.2.3.4", user="ec2-user", key_path="/keys/id_ed25519")

        result = run_remote_command(
            target, ["docker", "restart", "stream-processor"], kind=CommandKind.MUTATE, timeout_seconds=5
        )

        assert result.ok is False
        assert result.command.argv == ("docker", "restart", "stream-processor")
```

같은 파일 상단 import를 바꾼다.

```python
from ops_agent.ssh import CommandKind, SshTarget, run_remote_command
```

기존 3개 테스트의 `run_remote_command(...)` 호출에도 `kind=` 인자를 넣는다.
`test_builds_the_ssh_command_with_the_given_argv_appended`와
`test_a_nonzero_exit_code_is_not_ok`는 `kind=CommandKind.MUTATE`,
`test_a_timeout_is_reported_as_a_failed_result_not_an_exception`도 `kind=CommandKind.MUTATE`.

- [ ] **Step 2: 실패를 확인한다**

Run: `uv run --package ops-agent pytest services/ops-agent/tests/test_ssh.py -v`
Expected: FAIL — `ImportError: cannot import name 'CommandKind' from 'ops_agent.ssh'`

- [ ] **Step 3: `ssh.py`를 구현한다**

파일 전체를 아래로 바꾼다.

```python
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
    # kind를 키워드 필수 인자로 둬서 호출부가 분류를 빠뜨릴 수 없게 한다.
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
```

- [ ] **Step 4: 테스트가 통과하는지 확인한다**

Run: `uv run --package ops-agent pytest services/ops-agent/tests/test_ssh.py -v`
Expected: PASS (6 tests)

- [ ] **Step 5: 호출부 두 곳이 명령을 보존하게 한다**

`services/ops-agent/src/ops_agent/diagnostics.py`에서 import와 dataclass, 함수를 바꾼다.

```python
from ops_agent.ssh import CommandKind, ExecutedCommand, SshTarget, run_remote_command
```

```python
@dataclass(frozen=True, slots=True)
class StreamProcessorDiagnostics:
    container_status: str
    restart_count: int | None
    recent_logs: str
    commands: tuple[ExecutedCommand, ...]
```

```python
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
        kind=CommandKind.READ,
    )
    if not inspect.ok:
        return StreamProcessorDiagnostics(
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
        ["docker", "logs", "--tail", str(_LOG_TAIL_LINES), STREAM_PROCESSOR_CONTAINER_NAME],
        kind=CommandKind.READ,
    )
    return StreamProcessorDiagnostics(
        container_status=status_field or "unknown",
        restart_count=restart_count,
        recent_logs=(logs.stdout + logs.stderr).strip(),
        commands=(inspect.command, logs.command),
    )
```

`services/ops-agent/src/ops_agent/remediation.py`도 바꾼다.

```python
from ops_agent.ssh import CommandKind, ExecutedCommand, SshTarget, run_remote_command
```

```python
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
```

- [ ] **Step 6: conftest의 fake를 새 필드에 맞춘다**

`services/ops-agent/tests/conftest.py`에 import와 헬퍼를 추가하고 두 fake를 고친다.

```python
from ops_agent.ssh import CommandKind, ExecutedCommand
```

```python
def executed(argv: tuple[str, ...], kind: CommandKind = CommandKind.READ) -> ExecutedCommand:
    return ExecutedCommand(kind=kind, host="1.2.3.4", user="ec2-user", argv=argv)


def fake_diagnose(_target) -> StreamProcessorDiagnostics:
    return StreamProcessorDiagnostics(
        container_status="running",
        restart_count=0,
        recent_logs="fake logs",
        commands=(
            executed(("docker", "inspect", "stream-processor")),
            executed(("docker", "logs", "--tail", "50", "stream-processor")),
        ),
    )


def make_fake_remediate(*, succeeded: bool = True):
    def _remediate(_target) -> RemediationResult:
        return RemediationResult(
            action="restart_stream_processor",
            succeeded=succeeded,
            detail="fake ssh output",
            command=executed(
                ("docker", "restart", "stream-processor"), kind=CommandKind.MUTATE
            ),
        )

    return _remediate
```

- [ ] **Step 7: 진단·조치 테스트가 명령을 보존하는지 단언을 추가한다**

`services/ops-agent/tests/test_diagnostics.py`의 정상 경로 테스트에 추가한다.

```python
        assert [command.argv[:2] for command in result.commands] == [
            ("docker", "inspect"),
            ("docker", "logs"),
        ]
        assert all(command.kind is CommandKind.READ for command in result.commands)
```

`services/ops-agent/tests/test_remediation.py`의 정상 경로 테스트에 추가한다.

```python
        assert result.command.kind is CommandKind.MUTATE
        assert result.command.argv == ("docker", "restart", "stream-processor")
```

두 파일 모두 `from ops_agent.ssh import CommandKind`를 import에 추가한다.

- [ ] **Step 8: 전체 테스트를 돌린다**

Run: `uv run --package ops-agent pytest services/ops-agent/tests -v`
Expected: PASS

- [ ] **Step 9: 커밋한다**

```bash
git add services/ops-agent/src/ops_agent/ssh.py services/ops-agent/src/ops_agent/diagnostics.py services/ops-agent/src/ops_agent/remediation.py services/ops-agent/tests/
git commit -m "feat: carry the executed remote command in ssh results"
```

---

### Task 2: 실행 이력을 append-only로 남긴다

**Files:**
- Modify: `services/ops-agent/src/ops_agent/incident_store.py`
- Modify: `services/ops-agent/tests/test_incident_store.py`

**Interfaces:**
- Consumes: 없음
- Produces: `IncidentStore.record_attempt(fingerprint, alertname, action) -> int`, `IncidentStore.record_outcome(event_id, *, succeeded, recovered) -> None`, `IncidentStore.should_attempt(fingerprint, cooldown_seconds) -> bool`, `IncidentStore.count_recent(fingerprint, within_seconds) -> int`

- [ ] **Step 1: 실패 테스트를 쓴다**

`services/ops-agent/tests/test_incident_store.py`의 기존 `record_attempt("fp-1", "restart_stream_processor")` 호출을 모두
`record_attempt("fp-1", "StreamProcessorDown", "restart_stream_processor")`로 바꾸고,
`TestIncidentStore` 클래스 안에 아래를 추가한다.

```python
    def test_attempts_accumulate_instead_of_overwriting(self, tmp_path):
        clock = _Clock()
        store = IncidentStore(str(tmp_path / "incidents.sqlite3"), now=clock)

        for _ in range(3):
            store.record_attempt("fp-1", "StreamProcessorDown", "restart_stream_processor")
            clock.now += 1

        assert store.count_recent("fp-1", within_seconds=600) == 3

    def test_count_recent_ignores_attempts_outside_the_window(self, tmp_path):
        clock = _Clock()
        store = IncidentStore(str(tmp_path / "incidents.sqlite3"), now=clock)

        store.record_attempt("fp-1", "StreamProcessorDown", "restart_stream_processor")
        clock.now += 700

        assert store.count_recent("fp-1", within_seconds=600) == 0

    def test_count_recent_is_scoped_to_one_fingerprint(self, tmp_path):
        store = IncidentStore(str(tmp_path / "incidents.sqlite3"))

        store.record_attempt("fp-1", "StreamProcessorDown", "restart_stream_processor")
        store.record_attempt("fp-2", "StreamProcessorDown", "restart_stream_processor")

        assert store.count_recent("fp-1", within_seconds=600) == 1

    def test_the_outcome_is_recorded_against_the_attempt_row(self, tmp_path):
        store = IncidentStore(str(tmp_path / "incidents.sqlite3"))

        event_id = store.record_attempt("fp-1", "StreamProcessorDown", "restart_stream_processor")
        store.record_outcome(event_id, succeeded=True, recovered=False)

        assert store.read_event(event_id) == {
            "fingerprint": "fp-1",
            "alertname": "StreamProcessorDown",
            "action": "restart_stream_processor",
            "succeeded": True,
            "recovered": False,
        }

    def test_an_attempt_that_never_reports_an_outcome_leaves_recovered_unknown(self, tmp_path):
        # 조치 도중 ops-agent가 죽은 경우다. cooldown은 이미 소진돼야 하고 recovered는 NULL로 남는다.
        store = IncidentStore(str(tmp_path / "incidents.sqlite3"))

        event_id = store.record_attempt("fp-1", "StreamProcessorDown", "restart_stream_processor")

        assert store.should_attempt("fp-1", cooldown_seconds=600) is False
        assert store.read_event(event_id)["recovered"] is None
```

- [ ] **Step 2: 실패를 확인한다**

Run: `uv run --package ops-agent pytest services/ops-agent/tests/test_incident_store.py -v`
Expected: FAIL — `TypeError: record_attempt() takes 3 positional arguments but 4 were given`

- [ ] **Step 3: `incident_store.py`를 구현한다**

파일 전체를 아래로 바꾼다.

```python
# 조치 이력을 append-only로 남기고, 그 이력으로 cooldown을 판정한다(#546). 예전 remediation_attempts 테이블은 fingerprint당 1건만 덮어써 이력이 남지 않았다.

from __future__ import annotations

import sqlite3
import time
from collections.abc import Callable

_SCHEMA = """
CREATE TABLE IF NOT EXISTS remediation_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fingerprint TEXT NOT NULL,
    alertname TEXT NOT NULL,
    action TEXT NOT NULL,
    attempted_at REAL NOT NULL,
    succeeded INTEGER,
    recovered INTEGER
)
"""

_INDEX = """
CREATE INDEX IF NOT EXISTS idx_remediation_events_fingerprint_time
    ON remediation_events (fingerprint, attempted_at)
"""


class IncidentStore:
    def __init__(self, path: str, *, now: Callable[[], float] | None = None) -> None:
        # FastAPI가 sync 핸들러를 스레드풀에서 돌려 요청마다 다른 스레드가 이 connection을 쓸 수 있어 check_same_thread=False로 둔다.
        self._connection = sqlite3.connect(path, check_same_thread=False)
        self._connection.execute(_SCHEMA)
        self._connection.execute(_INDEX)
        self._connection.commit()
        self._now = now or time.time

    def should_attempt(self, fingerprint: str, cooldown_seconds: float) -> bool:
        row = self._connection.execute(
            "SELECT MAX(attempted_at) FROM remediation_events WHERE fingerprint = ?",
            (fingerprint,),
        ).fetchone()
        latest = row[0] if row else None
        if latest is None:
            return True
        return (self._now() - latest) >= cooldown_seconds

    def record_attempt(self, fingerprint: str, alertname: str, action: str) -> int:
        # 조치를 실행하기 "전에" 행을 넣는다 — 도중에 죽어도 cooldown이 소진된 상태로 남아 같은 incident를 무한히 다시 건드리지 않는다.
        cursor = self._connection.execute(
            """
            INSERT INTO remediation_events (fingerprint, alertname, action, attempted_at)
            VALUES (?, ?, ?, ?)
            """,
            (fingerprint, alertname, action, self._now()),
        )
        self._connection.commit()
        return int(cursor.lastrowid)

    def record_outcome(self, event_id: int, *, succeeded: bool, recovered: bool | None) -> None:
        self._connection.execute(
            "UPDATE remediation_events SET succeeded = ?, recovered = ? WHERE id = ?",
            (int(succeeded), None if recovered is None else int(recovered), event_id),
        )
        self._connection.commit()

    def count_recent(self, fingerprint: str, within_seconds: float) -> int:
        (count,) = self._connection.execute(
            "SELECT COUNT(*) FROM remediation_events WHERE fingerprint = ? AND attempted_at >= ?",
            (fingerprint, self._now() - within_seconds),
        ).fetchone()
        return int(count)

    def read_event(self, event_id: int) -> dict:
        row = self._connection.execute(
            "SELECT fingerprint, alertname, action, succeeded, recovered FROM remediation_events WHERE id = ?",
            (event_id,),
        ).fetchone()
        fingerprint, alertname, action, succeeded, recovered = row
        return {
            "fingerprint": fingerprint,
            "alertname": alertname,
            "action": action,
            "succeeded": None if succeeded is None else bool(succeeded),
            "recovered": None if recovered is None else bool(recovered),
        }

    def close(self) -> None:
        self._connection.close()
```

기존 `remediation_attempts` 테이블은 `DROP`하지 않는다 — AGENTS.md가 임의 DROP을
금지한다. 더 이상 읽거나 쓰지 않고 그대로 둔다.

- [ ] **Step 4: 테스트가 통과하는지 확인한다**

Run: `uv run --package ops-agent pytest services/ops-agent/tests/test_incident_store.py -v`
Expected: PASS

- [ ] **Step 5: 커밋한다**

```bash
git add services/ops-agent/src/ops_agent/incident_store.py services/ops-agent/tests/test_incident_store.py
git commit -m "feat: keep an append-only remediation history"
```

---

### Task 3: Slack에 blocks와 스레드 답글을 보낼 수 있게 한다

**Files:**
- Modify: `services/ops-agent/src/ops_agent/slack_notifier.py`
- Modify: `services/ops-agent/tests/test_slack_notifier.py`
- Modify: `services/ops-agent/tests/conftest.py`

**Interfaces:**
- Consumes: 없음
- Produces: `SlackNotifier.post(text, *, blocks=None) -> str | None` (메시지 `ts` 반환), `SlackNotifier.post_thread_reply(thread_ts, text) -> None`

- [ ] **Step 1: 실패 테스트를 쓴다**

`services/ops-agent/tests/test_slack_notifier.py`의 `TestSlackNotifier`에 추가한다.

```python
    def test_post_returns_the_message_timestamp_for_threading(self):
        client = FakeSlackClient()
        notifier = SlackNotifier(channel="#alerts", client=client)

        ts = notifier.post("hello")

        assert ts == "1700000000.000001"

    def test_post_passes_blocks_through(self):
        client = FakeSlackClient()
        notifier = SlackNotifier(channel="#alerts", client=client)
        blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": "hi"}}]

        notifier.post("fallback", blocks=blocks)

        assert client.calls[0]["blocks"] == blocks
        assert client.calls[0]["text"] == "fallback"

    def test_a_thread_reply_targets_the_parent_message(self):
        client = FakeSlackClient()
        notifier = SlackNotifier(channel="#alerts", client=client)

        notifier.post_thread_reply("1700000000.000001", "log tail")

        assert client.calls[0]["thread_ts"] == "1700000000.000001"
        assert client.calls[0]["text"] == "log tail"
```

- [ ] **Step 2: `FakeSlackClient`가 `ts`와 호출 인자를 돌려주게 한다**

`services/ops-agent/tests/conftest.py`의 `FakeSlackClient`를 바꾼다.

```python
@dataclass
class FakeSlackClient:
    """메시지 본문뿐 아니라 호출 인자 전체를 기록한다 — blocks/thread_ts 단언에 필요하다."""

    messages: list[tuple[str, str]] = field(default_factory=list)
    calls: list[dict] = field(default_factory=list)

    def chat_postMessage(self, *, channel: str, text: str, **kwargs) -> dict:
        self.messages.append((channel, text))
        self.calls.append({"channel": channel, "text": text, **kwargs})
        return {"ok": True, "ts": f"1700000000.{len(self.calls):06d}"}
```

- [ ] **Step 3: 실패를 확인한다**

Run: `uv run --package ops-agent pytest services/ops-agent/tests/test_slack_notifier.py -v`
Expected: FAIL — `assert None == '1700000000.000001'` (post가 아직 아무것도 반환하지 않는다)

- [ ] **Step 4: `slack_notifier.py`를 구현한다**

`SlackClient` Protocol과 `SlackNotifier`를 바꾼다. `mention_text`는 그대로 둔다.

```python
class SlackClient(Protocol):
    """slack_sdk.WebClient가 만족하는 최소 인터페이스 — 테스트에서 fake로 대체한다."""

    def chat_postMessage(self, *, channel: str, text: str, **kwargs) -> object: ...


@dataclass(frozen=True, slots=True)
class SlackNotifier:
    channel: str
    client: SlackClient

    def post(self, text: str, *, blocks: list[dict] | None = None) -> str | None:
        # 반환한 ts로 스레드 답글을 단다. blocks를 쓸 때도 text를 함께 보내야 알림 미리보기가 빈칸이 되지 않는다.
        payload: dict = {"channel": self.channel, "text": text}
        if blocks is not None:
            payload["blocks"] = blocks
        response = self.client.chat_postMessage(**payload)
        return _timestamp_of(response)

    def post_thread_reply(self, thread_ts: str, text: str) -> None:
        self.client.chat_postMessage(channel=self.channel, text=text, thread_ts=thread_ts)


def _timestamp_of(response: object) -> str | None:
    # slack_sdk의 SlackResponse는 dict처럼 첨자 접근이 되지만, 실패 응답에는 ts가 없다.
    try:
        return response["ts"]  # type: ignore[index]
    except (KeyError, TypeError):
        return None
```

- [ ] **Step 5: 테스트가 통과하는지 확인한다**

Run: `uv run --package ops-agent pytest services/ops-agent/tests -v`
Expected: PASS

- [ ] **Step 6: 커밋한다**

```bash
git add services/ops-agent/src/ops_agent/slack_notifier.py services/ops-agent/tests/
git commit -m "feat: support slack blocks and thread replies"
```

---

### Task 4: 알림 본문을 조립하는 모듈을 만든다

**Files:**
- Create: `services/ops-agent/src/ops_agent/notification.py`
- Create: `services/ops-agent/tests/test_notification.py`

**Interfaces:**
- Consumes: `ExecutedCommand`, `CommandKind` (Task 1), `StreamProcessorDiagnostics`, `RemediationResult`, `StreamProcessorStatus`, `PolicyDecision`, `Incident`, `mention_text`
- Produces: `NotificationInput` dataclass, `build_notification(data: NotificationInput) -> tuple[str, list[dict]]`, `build_log_reply(diagnostics) -> str | None`, `MAX_LOG_CHARS`

- [ ] **Step 1: 실패 테스트를 쓴다**

`services/ops-agent/tests/test_notification.py`를 새로 만든다.

```python
from __future__ import annotations

import json

from conftest import down_status, executed, fake_diagnose, grafana_alert, healthy_status
from ops_agent.diagnostics import StreamProcessorDiagnostics
from ops_agent.models import Incident
from ops_agent.notification import (
    MAX_LOG_CHARS,
    NotificationInput,
    build_log_reply,
    build_notification,
)
from ops_agent.owners import ServiceOwner
from ops_agent.policy import PolicyDecision, RemediationAction
from ops_agent.remediation import RemediationResult
from ops_agent.ssh import CommandKind


def diagnostics_with_logs(logs: str) -> StreamProcessorDiagnostics:
    base = fake_diagnose(None)
    return StreamProcessorDiagnostics(
        container_status=base.container_status,
        restart_count=base.restart_count,
        recent_logs=logs,
        commands=base.commands,
    )

OWNER = ServiceOwner(name="bob", email=None, slack_id="U0456GHIJKL", severity="high")


def make_input(**overrides) -> NotificationInput:
    base = dict(
        incident=Incident.from_grafana_alert(grafana_alert()),
        status=down_status(),
        diagnostics=fake_diagnose(None),
        policy=PolicyDecision(
            action=RemediationAction.RESTART_STREAM_PROCESSOR, allowed=True, reason="allowed"
        ),
        remediation=RemediationResult(
            action="restart_stream_processor",
            succeeded=True,
            detail="stream-processor",
            command=executed(("docker", "restart", "stream-processor"), kind=CommandKind.MUTATE),
        ),
        recovered=True,
        owner=OWNER,
        recent_attempts=1,
        extra_reason=None,
    )
    base.update(overrides)
    return NotificationInput(**base)


def rendered(blocks: list[dict]) -> str:
    return json.dumps(blocks, ensure_ascii=False)


class TestBuildNotification:
    def test_read_and_mutating_commands_are_shown_in_separate_blocks(self):
        _text, blocks = build_notification(make_input())

        body = rendered(blocks)
        assert "읽기만 한 명령" in body
        assert "변경한 명령" in body
        assert "docker inspect" in body
        assert "docker restart stream-processor" in body

    def test_no_mutating_command_is_stated_explicitly(self):
        _text, blocks = build_notification(make_input(remediation=None, recovered=False))

        assert "변경한 명령 없음" in rendered(blocks)

    def test_a_failed_recovery_mentions_the_owner(self):
        _text, blocks = build_notification(make_input(recovered=False))

        assert "<@U0456GHIJKL>" in rendered(blocks)

    def test_a_successful_recovery_names_the_owner_without_mentioning(self):
        # 복구된 장애로 담당자를 깨우지 않는다.
        _text, blocks = build_notification(make_input(recovered=True))

        body = rendered(blocks)
        assert "<@U0456GHIJKL>" not in body
        assert "bob" in body

    def test_the_reverified_status_is_shown_as_the_basis_for_the_decision(self):
        _text, blocks = build_notification(make_input(status=healthy_status()))

        assert "RUNNING" in rendered(blocks)

    def test_recent_attempt_count_is_included(self):
        _text, blocks = build_notification(make_input(recent_attempts=3))

        assert "3회" in rendered(blocks)

    def test_the_fallback_text_names_the_alert(self):
        text, _blocks = build_notification(make_input())

        assert "StreamProcessorDown" in text


class TestBuildLogReply:
    def test_none_when_there_are_no_logs(self):
        assert build_log_reply(diagnostics_with_logs("")) is None

    def test_none_when_the_logs_are_only_whitespace(self):
        assert build_log_reply(diagnostics_with_logs("   \n  ")) is None

    def test_short_logs_are_passed_through_in_a_code_fence(self):
        reply = build_log_reply(diagnostics_with_logs("boom"))

        assert reply is not None
        assert "```\nboom\n```" in reply
        assert "잘렸습니다" not in reply

    def test_long_logs_keep_the_tail_and_say_so(self):
        # 오래된 앞부분보다 최근 줄이 원인에 가까우므로 뒤쪽이 남아야 한다.
        logs = "OLD" + ("x" * MAX_LOG_CHARS) + "NEWEST"
        reply = build_log_reply(diagnostics_with_logs(logs))

        assert reply is not None
        assert "잘렸습니다" in reply
        assert "NEWEST" in reply
        assert "OLD" not in reply
        assert len(reply) <= MAX_LOG_CHARS + 200
```

- [ ] **Step 2: 실패를 확인한다**

Run: `uv run --package ops-agent pytest services/ops-agent/tests/test_notification.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ops_agent.notification'`

- [ ] **Step 3: `notification.py`를 구현한다**

```python
# Slack 메시지 조립만 담당한다 — orchestrator가 흐름 제어와 문자열 서식을 함께 지지 않게 분리했다(#546).

from __future__ import annotations

from dataclasses import dataclass

from ops_agent.diagnostics import StreamProcessorDiagnostics
from ops_agent.models import Incident
from ops_agent.owners import ServiceOwner
from ops_agent.policy import PolicyDecision
from ops_agent.prometheus_client import StreamProcessorStatus
from ops_agent.remediation import RemediationResult
from ops_agent.slack_notifier import mention_text
from ops_agent.ssh import CommandKind, ExecutedCommand

# Slack section block의 text는 3000자를 넘을 수 없다. 코드펜스와 안내문 여유를 뺀 값이다.
MAX_LOG_CHARS = 2600


@dataclass(frozen=True, slots=True)
class NotificationInput:
    incident: Incident
    status: StreamProcessorStatus
    diagnostics: StreamProcessorDiagnostics
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
        reason = data.extra_reason or (data.policy.reason if data.policy else "재검증 결과 이미 정상")
        blocks.append(_section(f"*변경한 명령 없음*\n{reason}"))

    if data.remediation is not None:
        detail = data.remediation.detail or "(출력 없음)"
        blocks.append(
            _section(
                f"*결과*\n`succeeded={data.remediation.succeeded}`\n```\n{detail}\n```"
            )
        )

    blocks.append(
        _section(
            f"*복구 여부* {'성공' if data.recovered else '실패 — 담당자 확인 필요'}\n"
            f"최근 7일 이 incident에 대한 조치 {data.recent_attempts}회"
        )
    )

    # 복구에 성공한 장애로 담당자를 깨우지 않는다 — 실패했을 때만 실제 멘션을 건다.
    owner_text = mention_text(data.owner) if not data.recovered else _owner_name(data.owner)
    blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": f"담당자: {owner_text}"}]})

    return fallback, blocks


def _owner_name(owner: ServiceOwner | None) -> str:
    return owner.name if owner is not None else "(no owner configured)"


def build_log_reply(diagnostics: StreamProcessorDiagnostics) -> str | None:
    """로그 tail은 본문이 아니라 스레드 답글로 보낸다 — 내용을 통제할 수 없어 메인 메시지에 싣지 않는다."""
    logs = diagnostics.recent_logs.strip()
    if not logs:
        return None
    if len(logs) > MAX_LOG_CHARS:
        # 오래된 앞부분보다 최근 줄이 원인에 가까우므로 뒤쪽을 남긴다.
        logs = logs[-MAX_LOG_CHARS:]
        return f"수집한 로그 tail (앞부분이 잘렸습니다)\n```\n{logs}\n```"
    return f"수집한 로그 tail\n```\n{logs}\n```"
```

- [ ] **Step 4: 테스트가 통과하는지 확인한다**

Run: `uv run --package ops-agent pytest services/ops-agent/tests/test_notification.py -v`
Expected: PASS

- [ ] **Step 5: 커밋한다**

```bash
git add services/ops-agent/src/ops_agent/notification.py services/ops-agent/tests/test_notification.py
git commit -m "feat: build block kit notifications for ops agent incidents"
```

---

### Task 5: orchestrator를 새 알림에 연결하고 침묵 구간을 없앤다

**Files:**
- Modify: `services/ops-agent/src/ops_agent/orchestrator.py`
- Modify: `services/ops-agent/tests/test_orchestrator.py`

**Interfaces:**
- Consumes: Task 1~4의 모든 산출물
- Produces: `IncidentOutcome`은 필드 구성을 유지한다. `OpsAgentOrchestrator.__init__`에 `history_window_seconds: float = 7 * 24 * 3600` 추가

- [ ] **Step 1: 침묵 구간이 사라졌는지 확인하는 테스트로 기존 테스트를 갱신한다**

`services/ops-agent/tests/test_orchestrator.py`의
`test_a_firing_alert_that_prometheus_now_reports_healthy_is_ignored`를 아래로 **교체**한다.
기대값이 뒤집히는 것은 §1-1이 의도한 동작 변경이다.

```python
    def test_a_firing_alert_that_prometheus_now_reports_healthy_takes_no_action_but_still_notifies(
        self, tmp_path
    ):
        # 조치는 하지 않되 침묵하지는 않는다 — 침묵하면 Grafana의 "감지" 알림 뒤로 후속이 없어
        # agent가 죽은 것인지 판단하고 넘어간 것인지 구분할 수 없다(설계 §1-1).
        orchestrator, slack_client = make_orchestrator(tmp_path=tmp_path, statuses=[healthy_status()])
        incident = Incident.from_grafana_alert(grafana_alert())

        outcome = orchestrator.handle(incident)

        assert outcome.recovered is True
        assert outcome.escalated is False
        assert outcome.remediation is None
        body = block_text(slack_client.calls[0])
        assert "변경한 명령 없음" in body
        assert "재검증 결과 이미 정상" in body
```

`make_orchestrator` 헬퍼가 cooldown을 받을 수 있게 시그니처에
`cooldown_seconds: float = 600,`을 추가하고, 본문의 `cooldown_seconds=600`을
`cooldown_seconds=cooldown_seconds`로 바꾼다. 테스트가 private 속성을 직접 건드리지
않게 하기 위해서다.

같은 파일의 나머지 테스트에서 본문을 문자열로 단언하던 곳을 blocks 기준으로 바꾼다.
편의 헬퍼를 파일 상단에 추가한다.

```python
def block_text(call: dict) -> str:
    """한 메시지의 모든 block 텍스트를 이어붙인다 — 단언을 blocks 구조에 덜 묶기 위해서다."""
    parts = []
    for block in call.get("blocks") or []:
        if "text" in block:
            parts.append(block["text"]["text"])
        for element in block.get("elements") or []:
            parts.append(element.get("text", ""))
    return "\n".join(parts)
```

기존 단언을 바꾼다.

- `assert "복구 여부: 성공" in slack_client.messages[0][1]` → `assert "*복구 여부* 성공" in block_text(slack_client.calls[0])`
- `assert "복구 여부: 실패" in message` → `assert "실패 — 담당자 확인 필요" in block_text(slack_client.calls[0])`
- `assert "<@U0456GHIJKL>" in message` → `assert "<@U0456GHIJKL>" in block_text(slack_client.calls[0])`
- `assert "자동 조치 없음" in slack_client.messages[0][1]` → `assert "변경한 명령 없음" in block_text(slack_client.calls[0])`
- cooldown 테스트의 마지막 단언 → `assert "cooldown" in block_text(slack_client.calls[-1])`

새 테스트도 추가한다.

```python
    def test_the_executed_commands_appear_in_the_notification(self, tmp_path):
        orchestrator, slack_client = make_orchestrator(
            tmp_path=tmp_path, statuses=[down_status(), healthy_status()]
        )
        incident = Incident.from_grafana_alert(grafana_alert())

        orchestrator.handle(incident)

        body = block_text(slack_client.calls[0])
        assert "읽기만 한 명령" in body
        assert "docker inspect" in body
        assert "변경한 명령" in body
        assert "docker restart stream-processor" in body

    def test_the_log_tail_goes_to_a_thread_reply_not_the_main_message(self, tmp_path):
        orchestrator, slack_client = make_orchestrator(
            tmp_path=tmp_path, statuses=[down_status(), healthy_status()]
        )
        incident = Incident.from_grafana_alert(grafana_alert())

        orchestrator.handle(incident)

        main, reply = slack_client.calls
        assert "fake logs" not in block_text(main)
        assert "thread_ts" not in main
        # FakeSlackClient가 첫 호출에 돌려준 ts를 답글이 그대로 참조해야 한다.
        assert reply["thread_ts"] == "1700000000.000001"
        assert "fake logs" in reply["text"]

    def test_repeated_incidents_report_a_growing_attempt_count(self, tmp_path):
        # cooldown을 0으로 둬서 두 번째 조치가 실행되게 하고, 이력이 누적되는지 본다.
        orchestrator, slack_client = make_orchestrator(
            tmp_path=tmp_path,
            statuses=[down_status(), healthy_status(), down_status(), healthy_status()],
            cooldown_seconds=0,
        )
        incident = Incident.from_grafana_alert(grafana_alert(fingerprint="fp-count"))

        orchestrator.handle(incident)
        orchestrator.handle(incident)

        main_messages = [call for call in slack_client.calls if "blocks" in call]
        assert "1회" in block_text(main_messages[0])
        assert "2회" in block_text(main_messages[1])
```

- [ ] **Step 2: 실패를 확인한다**

Run: `uv run --package ops-agent pytest services/ops-agent/tests/test_orchestrator.py -v`
Expected: FAIL — healthy 경로에서 `len(slack_client.messages) == 1`이 `0 == 1`로 깨진다

- [ ] **Step 3: `orchestrator.py`의 healthy 경로가 알리게 한다**

`handle()`의 healthy 분기(`orchestrator.py:83`)를 바꾼다.

```python
        status = self._prometheus.stream_processor_status()
        if status.is_healthy:
            # 조치하지 않더라도 알린다 — 침묵하면 Grafana의 감지 알림 뒤로 후속이 없어
            # agent가 죽은 것인지 판단하고 넘어간 것인지 구분할 수 없다(설계 §1-1).
            diagnostics = self._diagnose(self._ssh_target)
            self._notify(
                incident,
                status,
                diagnostics,
                policy_decision=None,
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
```

- [ ] **Step 4: `_notify`를 새 모듈에 연결한다**

`_notify`를 통째로 바꾼다.

```python
    def _notify(
        self,
        incident: Incident,
        status: StreamProcessorStatus,
        diagnostics: StreamProcessorDiagnostics,
        policy_decision: PolicyDecision | None,
        *,
        remediation: RemediationResult | None,
        recovered: bool,
        extra_reason: str | None = None,
    ) -> None:
        owner = self._owners.resolve(incident.service) if incident.service else None
        data = NotificationInput(
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
        text, blocks = build_notification(data)

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
```

기존 `_notify` 호출부 3곳은 `policy_decision`을 키워드가 아닌 위치 인자로 넘기고
있으므로 그대로 동작한다. cooldown 분기의 `extra_reason`도 그대로 둔다.

import를 추가한다.

```python
from ops_agent.notification import NotificationInput, build_log_reply, build_notification
```

- [ ] **Step 5: 조치 기록을 append-only 저장소에 맞춘다**

`record_attempt` 호출부(`orchestrator.py:140` 부근)를 바꾼다.

```python
        event_id = self._incident_store.record_attempt(
            incident.fingerprint, incident.alertname, policy_decision.action.value
        )
        remediation_result = self._remediate(self._ssh_target)

        post_status = self._wait_for_recovery()
        recovered = post_status.is_healthy
        self._incident_store.record_outcome(
            event_id, succeeded=remediation_result.succeeded, recovered=recovered
        )
```

- [ ] **Step 6: `history_window_seconds`를 생성자에 추가한다**

`__init__` 시그니처에 `history_window_seconds: float = 7 * 24 * 3600,`을 추가하고
본문에 `self._history_window_seconds = history_window_seconds`를 넣는다.

- [ ] **Step 7: 테스트가 통과하는지 확인한다**

Run: `uv run --package ops-agent pytest services/ops-agent/tests -v`
Expected: PASS

- [ ] **Step 8: 커밋한다**

```bash
git add services/ops-agent/src/ops_agent/orchestrator.py services/ops-agent/tests/test_orchestrator.py
git commit -m "feat: notify with executed commands and stop silent skips"
```

---

### Task 6: 단계별 구조화 로그를 남긴다

**Files:**
- Modify: `services/ops-agent/src/ops_agent/orchestrator.py`
- Modify: `services/ops-agent/tests/test_orchestrator.py`

**Interfaces:**
- Consumes: Task 5의 `handle()` 흐름
- Produces: 없음 (stdout 로그만)

- [ ] **Step 1: 실패 테스트를 쓴다**

`services/ops-agent/tests/test_orchestrator.py`에 추가한다.

```python
    def test_each_stage_is_logged_as_one_json_line(self, tmp_path, caplog):
        import json
        import logging

        caplog.set_level(logging.INFO, logger="ops_agent.orchestrator")
        orchestrator, _slack_client = make_orchestrator(
            tmp_path=tmp_path, statuses=[down_status(), healthy_status()]
        )
        incident = Incident.from_grafana_alert(grafana_alert())

        orchestrator.handle(incident)

        stages = []
        for record in caplog.records:
            if not record.getMessage().startswith("{"):
                continue
            stages.append(json.loads(record.getMessage())["stage"])
        # decide()가 _diagnose()보다 먼저 호출되므로 policy가 diagnose보다 앞선다.
        assert stages == ["reverify", "policy", "diagnose", "remediate", "recovery"]
```

- [ ] **Step 2: 실패를 확인한다**

Run: `uv run --package ops-agent pytest services/ops-agent/tests/test_orchestrator.py -k json_line -v`
Expected: FAIL — `assert [] == ['reverify', ...]`

- [ ] **Step 3: 로그 헬퍼를 추가한다**

`orchestrator.py` 상단에 `import json`을 추가하고 클래스에 메서드를 넣는다.

```python
    def _log_stage(self, stage: str, incident: Incident, **fields) -> None:
        # Slack에 싣지 않는 상세를 사후에 추적하려고 한 줄 JSON으로 남긴다.
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
```

- [ ] **Step 4: 각 단계에서 호출한다**

`handle()`의 **실제 실행 순서대로** 넣는다. 현재 코드는 `decide()`가
`_diagnose()`보다 먼저 호출되므로(`orchestrator.py:101-102`) policy가 diagnose보다 앞선다.

1. 재검증 직후: `self._log_stage("reverify", incident, status=status.label)`
2. `decide()` 직후: `self._log_stage("policy", incident, allowed=policy_decision.allowed, reason=policy_decision.reason)`
3. 진단 직후: `self._log_stage("diagnose", incident, container=diagnostics.container_status, restart_count=diagnostics.restart_count)`
4. 조치 직후: `self._log_stage("remediate", incident, action=policy_decision.action.value, succeeded=remediation_result.succeeded, command=" ".join(remediation_result.command.argv))`
5. 복구 판정 직후: `self._log_stage("recovery", incident, status=post_status.label, recovered=recovered)`

경로마다 남는 단계가 다르다. healthy 경로는 `reverify` → `diagnose` 둘뿐이고(그쪽은
`decide()`를 부르지 않는다), 정책 거부·cooldown 경로는 `reverify` → `policy` →
`diagnose`까지다. 테스트는 조치가 실행되는 경로를 보므로 다섯 단계가 모두 나온다.

- [ ] **Step 5: 테스트가 통과하는지 확인한다**

Run: `uv run --package ops-agent pytest services/ops-agent/tests -v`
Expected: PASS

- [ ] **Step 6: 전체 검증을 돌린다**

```bash
uv sync --all-packages
uv run --all-packages ruff check .
uv run --package ops-agent pytest services/ops-agent/tests
```

Expected: 모두 통과

- [ ] **Step 7: README를 갱신한다**

`services/ops-agent/README.md`의 "Incident flow" 코드블록에서
`-> 이미 정상이면 종료`를 `-> 이미 정상이면 Slack 알림 후 종료(조치 없음)`로 바꾸고,
마지막 단계 `-> Slack 알림 (성공/실패 모두, 실패 시 담당자 멘션)` 아래에 추가한다.

```text
     (알림에는 실행한 명령 원문이 읽기/변경으로 나뉘어 실리고,
      로그 tail은 스레드 답글로 분리된다. 조치 이력은 append-only로 누적된다.)
```

"자동 조치 가능 범위" 절 첫 항목 위에 한 줄 추가한다.

```markdown
- 자동 조치의 판정 기준은 [ADR-0013](../../docs/adr/0013-immediate-remediation-without-slack-approval.md)에 있다.
```

- [ ] **Step 8: 커밋한다**

```bash
git add services/ops-agent/src/ops_agent/orchestrator.py services/ops-agent/tests/test_orchestrator.py services/ops-agent/README.md
git commit -m "feat: log each incident stage as one json line"
```

---

## 완료 후 확인

#546의 완료 조건과 대조한다.

| 완료 조건 | 담당 Task |
| --- | --- |
| 재검증이 정상이어도 알림이 온다 | Task 5 |
| 어떤 호스트에서 어떤 명령이 돌았는지, 무엇이 상태를 바꿨는지 알 수 있다 | Task 1, 4, 5 |
| 로그 tail이 스레드 답글로 오고 메인 메시지에 없다 | Task 4, 5 |
| 이력이 누적되고 알림에 최근 실행 횟수가 보인다 | Task 2, 4, 5 |
| 조치 도중 죽어도 cooldown이 소진된 상태로 남는다 | Task 2 |
| ruff / pytest 통과 | Task 6 Step 6 |

PR은 `develop`을 대상으로 하고 본문 마지막 줄에 `Closes #546`을 쓴다. 부모 이슈 #545는
닫히면 안 되므로 `Closes #545`를 쓰지 않는다.
