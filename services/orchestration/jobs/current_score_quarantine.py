"""current_score의 UPSERT 직전 배치를 GX로 검증해 행 단위로 격리한다 (#251).

이상 행은 UPSERT하지 않고 current_segment_comfort_score_quarantine에 별도
INSERT한다. 서킷브레이커(CurrentScoreCircuitBreakerTripped)는
run_current_score_job이 전체 배치를 처리한 뒤 최종 커밋 직전에 판정한다.
설계 근거: docs/adr/0008-current-score-row-level-quarantine-and-circuit-breaker.md
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import great_expectations as gx
import pandas as pd
from psycopg2.extras import Json, execute_values

from .weather_rules import LOW_VISIBILITY, WeatherRuleConfig, parse_impact_signature

DEFAULT_SUITE_PATH = (
    Path(__file__).parent
    / "resources"
    / "expectations"
    / "current_segment_comfort_score_quarantine_suite.json"
)

QUARANTINE_TABLE = "current_segment_comfort_score_quarantine"

# 정상 행이 0건이거나 이 비율을 넘으면 실행 전체를 hard fail시킨다 (ADR-0008).
DEFAULT_MAX_QUARANTINE_RATE = 0.25

_QUARANTINE_COLUMNS = (
    "segment_id",
    "vehicle_profile_id",
    "calculated_at",
    "reject_reason",
    "reject_detail",
    "raw_row",
)

_INSERT_QUARANTINE_SQL = f"""
INSERT INTO {QUARANTINE_TABLE} ({", ".join(_QUARANTINE_COLUMNS)})
VALUES %s
"""


class CurrentScoreCircuitBreakerTripped(Exception):
    """정상 행이 0건이거나 격리율이 임계치를 넘으면 발생시켜 전체 트랜잭션을 롤백한다."""


@dataclass(frozen=True, slots=True)
class QuarantineSplit:
    normal_rows: list[dict]
    quarantined_records: list[dict]


def load_expectation_suite(path: Path = DEFAULT_SUITE_PATH) -> gx.ExpectationSuite:
    payload = json.loads(Path(path).read_text())
    return gx.ExpectationSuite(**payload)


def compute_identity_diff(row: dict, rule_config: WeatherRuleConfig) -> float:
    """low_visibility가 활성이면 0(검증 스킵), 아니면 방향 가중합과의 차이.

    context/comfort-score.md Step C: low_visibility 감점은 결합된 값에만 걸려서
    comfort_score가 세 방향 점수의 가중합과 일치하지 않는 게 의도된 동작이다.
    """
    signature = row["weather_impact_signature"]
    conditions = parse_impact_signature(signature) if signature is not None else frozenset()
    if LOW_VISIBILITY in conditions:
        return 0.0
    expected = (
        rule_config.vertical_weight.value * row["vertical_score"]
        + rule_config.longitudinal_weight.value * row["longitudinal_score"]
        + rule_config.lateral_weight.value * row["lateral_score"]
    )
    return abs(expected - row["comfort_score"])


def check_circuit_breaker(
    *,
    upserted_count: int,
    quarantined_count: int,
    max_quarantine_rate: float = DEFAULT_MAX_QUARANTINE_RATE,
) -> None:
    rows_seen = upserted_count + quarantined_count
    if rows_seen == 0:
        return
    if upserted_count == 0:
        raise CurrentScoreCircuitBreakerTripped(
            f"all {rows_seen} row(s) in this run were quarantined — refusing to write"
        )
    quarantine_rate = quarantined_count / rows_seen
    if quarantine_rate > max_quarantine_rate:
        raise CurrentScoreCircuitBreakerTripped(
            f"quarantine_rate={quarantine_rate:.2%} exceeds {max_quarantine_rate:.0%} "
            f"threshold ({quarantined_count}/{rows_seen} rows)"
        )
