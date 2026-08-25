from __future__ import annotations

import ops_agent.remediation as remediation_module
from ops_agent.remediation import restart_stream_processor
from ops_agent.ssh import SshResult, SshTarget

TARGET = SshTarget(host="1.2.3.4", user="ec2-user", key_path="/keys/id_ed25519")


class TestRestartStreamProcessor:
    def test_a_successful_restart_targets_the_exact_container_name(self, monkeypatch):
        captured = {}

        def fake_run(target, argv, **kwargs):
            captured["argv"] = argv
            return SshResult(0, "stream-processor", "")

        monkeypatch.setattr(remediation_module, "run_remote_command", fake_run)

        result = restart_stream_processor(TARGET)

        assert result.succeeded is True
        assert result.action == "restart_stream_processor"
        assert captured["argv"] == ["docker", "restart", "stream-processor"]

    def test_a_failed_restart_is_reported_with_the_ssh_error(self, monkeypatch):
        monkeypatch.setattr(
            remediation_module,
            "run_remote_command",
            lambda target, argv, **kwargs: SshResult(1, "", "connection refused"),
        )

        result = restart_stream_processor(TARGET)

        assert result.succeeded is False
        assert "connection refused" in result.detail
