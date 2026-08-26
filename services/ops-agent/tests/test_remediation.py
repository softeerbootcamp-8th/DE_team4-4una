from __future__ import annotations

import ops_agent.remediation as remediation_module
from ops_agent.remediation import restart_stream_processor
from ops_agent.ssh import CommandKind, ExecutedCommand, SshResult, SshTarget

TARGET = SshTarget(host="1.2.3.4", user="ec2-user", key_path="/keys/id_ed25519")


def ssh_result(exit_code: int, stdout: str, stderr: str, argv, kind) -> SshResult:
    """실제 run_remote_command처럼 실행한 argv를 결과에 담아 돌려주는 fake 응답."""
    return SshResult(
        exit_code=exit_code,
        stdout=stdout,
        stderr=stderr,
        command=ExecutedCommand(
            kind=kind, host=TARGET.host, user=TARGET.user, argv=tuple(argv)
        ),
    )


class TestRestartStreamProcessor:
    def test_a_successful_restart_targets_the_exact_container_name(self, monkeypatch):
        captured = {}

        def fake_run(target, argv, *, kind, **kwargs):
            captured["argv"] = argv
            return ssh_result(0, "stream-processor", "", argv, kind)

        monkeypatch.setattr(remediation_module, "run_remote_command", fake_run)

        result = restart_stream_processor(TARGET)

        assert result.succeeded is True
        assert result.action == "restart_stream_processor"
        assert captured["argv"] == ["docker", "restart", "stream-processor"]

    def test_the_executed_command_is_preserved_and_marked_as_mutating(self, monkeypatch):
        monkeypatch.setattr(
            remediation_module,
            "run_remote_command",
            lambda target, argv, *, kind, **kwargs: ssh_result(
                0, "stream-processor", "", argv, kind
            ),
        )

        result = restart_stream_processor(TARGET)

        assert result.command.kind is CommandKind.MUTATE
        assert result.command.argv == ("docker", "restart", "stream-processor")

    def test_a_failed_restart_is_reported_with_the_ssh_error(self, monkeypatch):
        monkeypatch.setattr(
            remediation_module,
            "run_remote_command",
            lambda target, argv, *, kind, **kwargs: ssh_result(
                1, "", "connection refused", argv, kind
            ),
        )

        result = restart_stream_processor(TARGET)

        assert result.succeeded is False
        assert "connection refused" in result.detail
