"""Ride-comfort serving API package."""

import logging


def main() -> None:
    # 무거운 임포트는 함수 안에 둔다 — 패키지를 임포트하는 것만으로 FastAPI와
    # uvicorn을 끌고 오지 않게 한다.
    import uvicorn
    from prometheus_client import start_http_server

    from serving_api.app import create_app
    from serving_api.config import ServingApiConfig
    from serving_api.metrics import RequestMetrics

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    config = ServingApiConfig.from_env()
    metrics = RequestMetrics()
    # API 요청을 처리하는 uvicorn과 별도로, metrics 전용 포트에서 이 registry를
    # 그대로 노출한다 — /metrics를 FastAPI router에 얹지 않는다.
    start_http_server(config.metrics_port, registry=metrics.registry)
    uvicorn.run(create_app(config, metrics=metrics), host=config.host, port=config.port)
