"""Comfort score 3-DAG 분리(ADR-0007)에서 공유하는 Airflow Asset 정의.

standard_score_pipeline(#229)/zone_weather_pipeline(#230)이 producer로 outlet에
쓰고, current_score_pipeline(#231)이 AssetAny(...)로 구독한다.
"""

from __future__ import annotations

from airflow.sdk import Asset

STANDARD_SCORE_ASSET = Asset("standard_segment_comfort_score")
