from __future__ import annotations

import ops_agent.remediation as remediation_module
from ops_agent.remediation import restart_container
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


class TestRestartContainer:
    def test_it_restarts_the_container_it_was_given(self, monkeypatch):
        captured = {}

        def fake_run(target, argv, *, kind, **kwargs):
            captured["argv"] = argv
            return ssh_result(0, "serving-api", "", argv, kind)

        monkeypatch.setattr(remediation_module, "run_remote_command", fake_run)

        result = restart_container(TARGET, "serving-api", action="restart_serving_api")

        assert result.succeeded is True
        assert result.action == "restart_serving_api"
        assert captured["argv"] == ["docker", "restart", "serving-api"]

    def test_the_executed_command_is_marked_as_mutating(self, monkeypatch):
        monkeypatch.setattr(
            remediation_module,
            "run_remote_command",
            lambda target, argv, *, kind, **kwargs: ssh_result(0, "x", "", argv, kind),
        )

        result = restart_container(TARGET, "node-exporter", action="restart_node_exporter")

        assert result.command.kind is CommandKind.MUTATE
        assert result.command.argv == ("docker", "restart", "node-exporter")

    def test_a_failed_restart_is_reported_with_the_ssh_error(self, monkeypatch):
        monkeypatch.setattr(
            remediation_module,
            "run_remote_command",
            lambda target, argv, *, kind, **kwargs: ssh_result(
                1, "", "connection refused", argv, kind
            ),
        )

        result = restart_container(
            TARGET, "stream-processor", action="restart_stream_processor"
        )

        assert result.succeeded is False
        assert "connection refused" in result.detail
