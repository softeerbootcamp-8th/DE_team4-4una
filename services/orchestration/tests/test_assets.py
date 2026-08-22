# dags/assets.py 테스트(#230) — 여러 DAG가 공유하는 Asset 정의가 기대한 모양인지만
# 확인한다. 이 모듈은 각 DAG 파일이 파싱 시점에 import하므로, 여기서 깨지면 모든
# DAG 파싱이 실패한다.

from __future__ import annotations

import sys
from pathlib import Path

from airflow.sdk import Asset

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from assets import ZONE_WEATHER_ASSET


def test_zone_weather_asset_is_an_asset():
    assert isinstance(ZONE_WEATHER_ASSET, Asset)


def test_zone_weather_asset_identifies_the_latest_zone_weather_table():
    assert ZONE_WEATHER_ASSET.name == "zone_weather_changed"
    assert ZONE_WEATHER_ASSET.uri == "asset://zone-weather/changed-zones"
