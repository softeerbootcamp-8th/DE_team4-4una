from __future__ import annotations

import ops_agent.diagnostics as diagnostics_module
from ops_agent.diagnostics import collect_container_diagnostics
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


class TestCollectStreamProcessorDiagnostics:
    def test_reports_status_and_restart_count_from_docker_inspect(self, monkeypatch):
        outputs = [(0, "running|2", ""), (0, "log line 1\nlog line 2", "")]

        def fake_run(target, argv, *, kind, **kwargs):
            exit_code, stdout, stderr = outputs.pop(0)
            return ssh_result(exit_code, stdout, stderr, argv, kind)

        monkeypatch.setattr(diagnostics_module, "run_remote_command", fake_run)

        result = collect_container_diagnostics(TARGET, "stream-processor")

        assert result.container_status == "running"
        assert result.restart_count == 2
        assert "log line 1" in result.recent_logs

    def test_the_read_only_commands_are_preserved_for_the_notification(self, monkeypatch):
        outputs = [(0, "running|2", ""), (0, "log line 1", "")]

        def fake_run(target, argv, *, kind, **kwargs):
            exit_code, stdout, stderr = outputs.pop(0)
            return ssh_result(exit_code, stdout, stderr, argv, kind)

        monkeypatch.setattr(diagnostics_module, "run_remote_command", fake_run)

        result = collect_container_diagnostics(TARGET, "stream-processor")

        assert [command.argv[:2] for command in result.commands] == [
            ("docker", "inspect"),
            ("docker", "logs"),
        ]
        # 진단은 상태를 바꾸지 않으므로 전부 READ로 분류돼야 한다.
        assert all(command.kind is CommandKind.READ for command in result.commands)

    def test_a_missing_container_is_reported_without_a_restart_count(self, monkeypatch):
        monkeypatch.setattr(
            diagnostics_module,
            "run_remote_command",
            lambda target, argv, *, kind, **kwargs: ssh_result(
                1, "", "No such container", argv, kind
            ),
        )

        result = collect_container_diagnostics(TARGET, "stream-processor")

        assert result.container_status == "not found"
        assert result.restart_count is None
        assert "No such container" in result.recent_logs
        # inspect가 실패하면 logs를 시도하지 않으므로 명령이 하나만 남는다.
        assert len(result.commands) == 1

    def test_it_inspects_the_container_it_was_given(self, monkeypatch):
        seen = []
        outputs = [(0, "running|0", ""), (0, "log", "")]

        def fake_run(target, argv, *, kind, **kwargs):
            seen.append(argv)
            exit_code, stdout, stderr = outputs.pop(0)
            return ssh_result(exit_code, stdout, stderr, argv, kind)

        monkeypatch.setattr(diagnostics_module, "run_remote_command", fake_run)

        collect_container_diagnostics(TARGET, "serving-api")

        assert seen[0][-1] == "serving-api"
        assert seen[1][-1] == "serving-api"
