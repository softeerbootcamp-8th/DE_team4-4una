"""Environment-driven configuration for ops-agent."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8080

DEFAULT_PROMETHEUS_URL = "http://prometheus:9090"
DEFAULT_REQUEST_TIMEOUT_SECONDS = 10.0

# 같은 incident(fingerprint)에 대해 remediation을 다시 시도하기까지 기다리는 시간.
DEFAULT_COOLDOWN_SECONDS = 15 * 60

DEFAULT_INCIDENT_STORE_PATH = "ops_agent_incidents.sqlite3"

# restart 직후 Spark JVM/Structured Streaming 기동 + Prometheus scrape 지연을 감안해 복구 여부를 폴링으로 확인한다.
DEFAULT_RECOVERY_POLL_INTERVAL_SECONDS = 10.0
DEFAULT_RECOVERY_WAIT_SECONDS = 90.0

# config/dag_owners.yaml의 users:를 그대로 재사용한다(#409) — 이 프로젝트의 다른
# 서비스는 저장소 루트 기준 상대경로를 쓰지 않고 항상 이 파일 경로를 env로 받는다.
DEFAULT_DAG_OWNERS_CONFIG_PATH = "config/dag_owners.yaml"


@dataclass(frozen=True, slots=True)
class OpsAgentConfig:
    host: str
    port: int
    prometheus_url: str
    request_timeout_seconds: float
    cooldown_seconds: int
    recovery_poll_interval_seconds: float
    recovery_wait_seconds: float
    incident_store_path: str
    dag_owners_config_path: str
    slack_bot_token: str
    slack_alert_channel: str
    stream_processor_ssh_host: str
    stream_processor_ssh_user: str
    stream_processor_ssh_key_path: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> OpsAgentConfig:
        source = env if env is not None else os.environ
        return cls(
            host=source.get("OPS_AGENT_HOST") or DEFAULT_HOST,
            port=int(source.get("OPS_AGENT_PORT") or DEFAULT_PORT),
            prometheus_url=source.get("PROMETHEUS_URL") or DEFAULT_PROMETHEUS_URL,
            request_timeout_seconds=float(
                source.get("OPS_AGENT_REQUEST_TIMEOUT_SECONDS")
                or DEFAULT_REQUEST_TIMEOUT_SECONDS
            ),
            cooldown_seconds=int(
                source.get("OPS_AGENT_COOLDOWN_SECONDS") or DEFAULT_COOLDOWN_SECONDS
            ),
            recovery_poll_interval_seconds=float(
                source.get("OPS_AGENT_RECOVERY_POLL_INTERVAL_SECONDS")
                or DEFAULT_RECOVERY_POLL_INTERVAL_SECONDS
            ),
            recovery_wait_seconds=float(
                source.get("OPS_AGENT_RECOVERY_WAIT_SECONDS") or DEFAULT_RECOVERY_WAIT_SECONDS
            ),
            incident_store_path=(
                source.get("OPS_AGENT_INCIDENT_STORE_PATH") or DEFAULT_INCIDENT_STORE_PATH
            ),
            dag_owners_config_path=(
                source.get("DAG_OWNERS_CONFIG_PATH") or DEFAULT_DAG_OWNERS_CONFIG_PATH
            ),
            slack_bot_token=_require(source, "SLACK_BOT_TOKEN"),
            slack_alert_channel=_require(source, "SLACK_ALERT_CHANNEL"),
            # architecture.md와 배포 워크플로의 EC2 설명이 어긋나 잘못 추측하지 않도록 기본값을 두지 않는다(#447).
            stream_processor_ssh_host=_require(source, "STREAM_PROCESSOR_SSH_HOST"),
            stream_processor_ssh_user=(
                source.get("STREAM_PROCESSOR_SSH_USER") or "ec2-user"
            ),
            stream_processor_ssh_key_path=_require(
                source, "STREAM_PROCESSOR_SSH_KEY_PATH"
            ),
        )


def _require(source: Mapping[str, str], key: str) -> str:
    value = source.get(key)
    if not value:
        raise ValueError(f"{key} must be set")
    return value
