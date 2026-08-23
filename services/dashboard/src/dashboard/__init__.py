"""Ride-comfort dashboard package."""

from __future__ import annotations

import sys
from pathlib import Path


def main() -> None:
    """Launch the packaged Streamlit application on the container interface."""
    from streamlit.web import cli as streamlit_cli

    app_path = Path(__file__).with_name("app.py")
    sys.argv = [
        "streamlit",
        "run",
        str(app_path),
        "--server.address=0.0.0.0",
        "--server.port=8501",
        "--server.headless=true",
    ]
    streamlit_cli.main()
