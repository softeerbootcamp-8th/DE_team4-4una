from __future__ import annotations

import pytest
from ops_agent.config import OpsAgentConfig

BASE_ENV = {
    "SLACK_BOT_TOKEN": "xoxb-x",
    "SLACK_ALERT_CHANNEL": "#alerts",
    "STREAM_PROCESSOR_SSH_HOST": "spark.example",
    "STREAM_PROCESSOR_SSH_KEY_PATH": "/keys/spark.pem",
    "PROJECT_SSH_HOST": "project.example",
    "PROJECT_SSH_KEY_PATH": "/keys/project.pem",
}


class TestSshTargets:
    def test_both_hosts_are_available_under_their_spec_keys(self):
        targets = OpsAgentConfig.from_env(BASE_ENV).ssh_targets()

        assert targets["spark"].host == "spark.example"
        assert targets["project"].host == "project.example"
        assert targets["project"].key_path == "/keys/project.pem"

    def test_the_user_defaults_to_ec2_user_for_both(self):
        targets = OpsAgentConfig.from_env(BASE_ENV).ssh_targets()

        assert targets["spark"].user == "ec2-user"
        assert targets["project"].user == "ec2-user"

    @pytest.mark.parametrize("missing", ["PROJECT_SSH_HOST", "PROJECT_SSH_KEY_PATH"])
    def test_a_missing_project_setting_fails_at_startup(self, missing):
        # 조용히 기본값으로 채우면 엉뚱한 호스트에 docker restart를 쏠 수 있다.
        env = {key: value for key, value in BASE_ENV.items() if key != missing}

        with pytest.raises(ValueError, match=missing):
            OpsAgentConfig.from_env(env)
