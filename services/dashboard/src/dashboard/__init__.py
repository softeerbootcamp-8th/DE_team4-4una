"""Ride-comfort dashboard package."""

from __future__ import annotations


def main() -> None:
    """Serve the API and the built React app on the container interface.

    worker는 늘리지 않는다. DashboardService가 road segment 스냅샷과 STRtree를
    프로세스 메모리에 들고 있어서, worker마다 통째로 중복해 올리고 각자 따로
    콜드 스타트를 하게 된다.
    """
    import uvicorn

    from dashboard.api import app
    from dashboard.config import DEFAULT_HOST, DEFAULT_PORT

    uvicorn.run(app, host=DEFAULT_HOST, port=DEFAULT_PORT)
