# create_app()은 이미 구성된 orchestrator만 받는다 — 실제 연결 생성은 ops_agent.main()의 책임(serving_api.app과 같은 원칙).

from __future__ import annotations

import logging

from fastapi import FastAPI, Request

from ops_agent.models import parse_grafana_webhook
from ops_agent.orchestrator import OpsAgentOrchestrator

logger = logging.getLogger(__name__)


def create_app(orchestrator: OpsAgentOrchestrator) -> FastAPI:
    app = FastAPI(title="ops-agent")

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.post("/webhooks/grafana")
    async def grafana_webhook(request: Request) -> dict:
        payload = await request.json()
        incidents = parse_grafana_webhook(payload)
        results = []
        for incident in incidents:
            try:
                outcome = orchestrator.handle(incident)
            except Exception:
                logger.exception("failed to handle incident alertname=%s", incident.alertname)
                results.append(f"{incident.alertname}: internal error while handling incident")
                continue
            results.append(outcome.summary)
        return {"handled": len(incidents), "results": results}

    return app
