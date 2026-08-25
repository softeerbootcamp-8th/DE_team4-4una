from __future__ import annotations

import ops_agent.diagnostics as diagnostics_module
from ops_agent.diagnostics import collect_stream_processor_diagnostics
from ops_agent.ssh import SshResult, SshTarget

TARGET = SshTarget(host="1.2.3.4", user="ec2-user", key_path="/keys/id_ed25519")


class TestCollectStreamProcessorDiagnostics:
    def test_reports_status_and_restart_count_from_docker_inspect(self, monkeypatch):
        responses = [
            SshResult(0, "running|2", ""),
            SshResult(0, "log line 1\nlog line 2", ""),
        ]

        def fake_run(target, argv, **kwargs):
            return responses.pop(0)

        monkeypatch.setattr(diagnostics_module, "run_remote_command", fake_run)

        result = collect_stream_processor_diagnostics(TARGET)

        assert result.container_status == "running"
        assert result.restart_count == 2
        assert "log line 1" in result.recent_logs

    def test_a_missing_container_is_reported_without_a_restart_count(self, monkeypatch):
        monkeypatch.setattr(
            diagnostics_module,
            "run_remote_command",
            lambda target, argv, **kwargs: SshResult(1, "", "No such container"),
        )

        result = collect_stream_processor_diagnostics(TARGET)

        assert result.container_status == "not found"
        assert result.restart_count is None
        assert "No such container" in result.recent_logs
