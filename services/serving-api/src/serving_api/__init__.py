"""Ride-comfort serving API package."""

import logging


def main() -> None:
    # 무거운 임포트는 함수 안에 둔다 — 패키지를 임포트하는 것만으로 FastAPI와
    # uvicorn을 끌고 오지 않게 한다.
    import uvicorn

    from serving_api.app import create_app
    from serving_api.config import ServingApiConfig

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    config = ServingApiConfig.from_env()
    uvicorn.run(create_app(config), host=config.host, port=config.port)
