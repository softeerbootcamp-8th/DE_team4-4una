# Ops Agent: Grafana alert를 받아 Prometheus로 재검증하고 allowlist된 remediation만 실행한다(#447).

import logging


def main() -> None:
    # 무거운 임포트는 함수 안에 둔다 — 패키지 임포트만으로 uvicorn 등을 끌고 오지 않게(serving_api.main()과 같은 이유).
    import uvicorn

    from ops_agent.app import create_app
    from ops_agent.config import OpsAgentConfig
    from ops_agent.incident_store import IncidentStore
    from ops_agent.orchestrator import OpsAgentOrchestrator
    from ops_agent.owners import load_service_owners_registry
    from ops_agent.prometheus_client import PrometheusClient
    from ops_agent.slack_notifier import SlackNotifier, build_client
    from ops_agent.ssh import SshTarget

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    config = OpsAgentConfig.from_env()

    orchestrator = OpsAgentOrchestrator(
        prometheus=PrometheusClient(
            config.prometheus_url, timeout_seconds=config.request_timeout_seconds
        ),
        incident_store=IncidentStore(config.incident_store_path),
        owners=load_service_owners_registry(config.dag_owners_config_path),
        ssh_target=SshTarget(
            host=config.stream_processor_ssh_host,
            user=config.stream_processor_ssh_user,
            key_path=config.stream_processor_ssh_key_path,
        ),
        slack=SlackNotifier(
            channel=config.slack_alert_channel,
            client=build_client(config.slack_bot_token),
        ),
        cooldown_seconds=config.cooldown_seconds,
        recovery_poll_interval_seconds=config.recovery_poll_interval_seconds,
        recovery_wait_seconds=config.recovery_wait_seconds,
    )

    uvicorn.run(create_app(orchestrator), host=config.host, port=config.port)
