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
from great_expectations.data_context.types.base import ProgressBarsConfig
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


def split_batch(
    rows: list[dict],
    rule_config: WeatherRuleConfig,
    suite: gx.ExpectationSuite,
) -> QuarantineSplit:
    """배치를 GX로 검증해 정상/격리로 나눈다. UPSERT 이전에 in-memory로 수행한다.

    이미 나쁜 값으로 기존 정상 값을 덮어쓴 뒤 사후 검증하지 않는 이유는
    ADR-0008 참고 — current_segment_comfort_score는 키당 단일 최신 행만
    가져 되돌릴 이전 값이 없다.
    """
    if not rows:
        return QuarantineSplit([], [])

    frame = pd.DataFrame(rows)
    frame["identity_diff"] = [compute_identity_diff(row, rule_config) for row in rows]

    context = gx.get_context(mode="ephemeral")
    # tqdm 진행바는 stderr로 나가고 Airflow supervisor는 task stderr를 내용과 무관하게
    # ERROR로 포워딩해, 정상 검증이 오류처럼 보인다 (#540). context 변수로 꺼 둔다.
    context.variables.progress_bars = ProgressBarsConfig(globally=False)
    datasource = context.data_sources.add_pandas(name="current_score_batch_datasource")
    asset = datasource.add_dataframe_asset(name="current_score_batch")
    batch_definition = asset.add_batch_definition_whole_dataframe(
        "current_score_batch_definition"
    )
    batch = batch_definition.get_batch(batch_parameters={"dataframe": frame})
    result = batch.validate(suite, result_format="COMPLETE")

    violations_by_index: dict[int, list[dict]] = {}
    for expectation_result in result.results:
        unexpected_indexes = expectation_result.result.get("unexpected_index_list") or []
        # 테이블 단위/집계형 expectation(예: expect_table_row_count_to_be_between)은
        # 실패해도 unexpected_index_list를 생성하지 않는다. 이를 빈 리스트로 취급해
        # 넘기면 위반이 0건으로 계산되어 품질 게이트를 조용히 통과하게 되므로,
        # 행 단위로 격리 대상을 특정할 수 없는 실패는 배치 전체를 거부한다.
        if not expectation_result.success and not unexpected_indexes:
            raise CurrentScoreCircuitBreakerTripped(
                f"expectation {expectation_result.expectation_config.type!r} failed without "
                "producing row-level indexes — cannot safely determine which rows to "
                "quarantine, refusing to write this batch"
            )
        unexpected_values = expectation_result.result.get("unexpected_list") or []
        for index, value in zip(unexpected_indexes, unexpected_values, strict=True):
            violations_by_index.setdefault(index, []).append(
                {
                    "expectation_type": expectation_result.expectation_config.type,
                    "column": expectation_result.expectation_config.kwargs.get("column"),
                    "observed_value": value,
                }
            )

    normal_rows: list[dict] = []
    quarantined_records: list[dict] = []
    for index, row in enumerate(rows):
        violations = violations_by_index.get(index)
        if violations is None:
            normal_rows.append(row)
            continue
        quarantined_records.append(
            {
                "segment_id": row["segment_id"],
                "vehicle_profile_id": row["vehicle_profile_id"],
                "calculated_at": row["calculated_at"],
                "reject_reason": ",".join(sorted({v["column"] for v in violations})),
                "reject_detail": violations,
                "raw_row": row,
            }
        )
    return QuarantineSplit(normal_rows=normal_rows, quarantined_records=quarantined_records)


def _json_dumps(value: object) -> str:
    return json.dumps(value, default=str)


def insert_quarantined_rows(cursor, records: list[dict]) -> None:
    if not records:
        return
    execute_values(
        cursor,
        _INSERT_QUARANTINE_SQL,
        [
            (
                record["segment_id"],
                record["vehicle_profile_id"],
                record["calculated_at"],
                record["reject_reason"],
                Json(record["reject_detail"], dumps=_json_dumps),
                Json(record["raw_row"], dumps=_json_dumps),
            )
            for record in records
        ],
    )


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
