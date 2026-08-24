from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from jobs import current_score
from jobs.current_score import CurrentScoreJobConfig


def _base_env(**overrides: str) -> dict[str, str]:
    return {
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "de4",
        "POSTGRES_USER": "de4",
        "POSTGRES_PASSWORD": "de4",
        **overrides,
    }


def test_falls_back_to_hardcoded_snapshot_date_without_pointer():
    config = CurrentScoreJobConfig.from_env(
        _base_env(CURRENT_SCORE_ROAD_SNAPSHOT_DATE="2026-08-20")
    )

    assert config.road_snapshot_date == date(2026, 8, 20)


def test_resolves_snapshot_date_from_pointer(monkeypatch):
    monkeypatch.setattr(
        current_score,
        "resolve_active_road_snapshot_date",
        lambda uri: date(2026, 8, 21),
    )

    config = CurrentScoreJobConfig.from_env(
        _base_env(CURRENT_SCORE_ROAD_ENVIRONMENT_URI="s3://de4-reference")
    )

    assert config.road_snapshot_date == date(2026, 8, 21)


def test_pointer_takes_precedence_over_fallback(monkeypatch):
    monkeypatch.setattr(
        current_score,
        "resolve_active_road_snapshot_date",
        lambda uri: date(2026, 8, 21),
    )

    config = CurrentScoreJobConfig.from_env(
        _base_env(
            CURRENT_SCORE_ROAD_ENVIRONMENT_URI="s3://de4-reference",
            CURRENT_SCORE_ROAD_SNAPSHOT_DATE="2026-08-20",
        )
    )

    assert config.road_snapshot_date == date(2026, 8, 21)


def test_raises_when_pointer_and_fallback_are_missing():
    with pytest.raises(ValueError, match="CURRENT_SCORE_ROAD_ENVIRONMENT_URI"):
        CurrentScoreJobConfig.from_env(_base_env())
