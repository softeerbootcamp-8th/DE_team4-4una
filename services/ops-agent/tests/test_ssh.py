from __future__ import annotations

import subprocess

from ops_agent.ssh import SshTarget, run_remote_command


class _FakeCompleted:
    def __init__(self, returncode: int, stdout: str, stderr: str) -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class TestRunRemoteCommand:
    def test_builds_the_ssh_command_with_the_given_argv_appended(self, monkeypatch):
        captured = {}

        def fake_run(command, **kwargs):
            captured["command"] = command
            return _FakeCompleted(0, "ok", "")

        monkeypatch.setattr(subprocess, "run", fake_run)
        target = SshTarget(host="1.2.3.4", user="ec2-user", key_path="/keys/id_ed25519")

        result = run_remote_command(target, ["docker", "restart", "stream-processor"])

        assert result.ok is True
        assert result.stdout == "ok"
        command = captured["command"]
        assert command[0] == "ssh"
        assert "-i" in command and "/keys/id_ed25519" in command
        assert "ec2-user@1.2.3.4" in command
        assert command[-3:] == ["docker", "restart", "stream-processor"]

    def test_a_nonzero_exit_code_is_not_ok(self, monkeypatch):
        monkeypatch.setattr(
            subprocess, "run", lambda command, **kwargs: _FakeCompleted(1, "", "no such container")
        )
        target = SshTarget(host="1.2.3.4", user="ec2-user", key_path="/keys/id_ed25519")

        result = run_remote_command(target, ["docker", "restart", "stream-processor"])

        assert result.ok is False
        assert "no such container" in result.stderr

    def test_a_timeout_is_reported_as_a_failed_result_not_an_exception(self, monkeypatch):
        def fake_run(command, **kwargs):
            raise subprocess.TimeoutExpired(cmd=command, timeout=kwargs.get("timeout", 30))

        monkeypatch.setattr(subprocess, "run", fake_run)
        target = SshTarget(host="1.2.3.4", user="ec2-user", key_path="/keys/id_ed25519")

        result = run_remote_command(target, ["docker", "restart", "stream-processor"], timeout_seconds=5)

        assert result.ok is False
        assert "timed out" in result.stderr
