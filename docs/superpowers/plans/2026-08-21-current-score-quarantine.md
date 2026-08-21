# current_score_pipeline 행 단위 격리 & GX 서킷브레이커 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `run_current_score_job`이 UPSERT하는 `current_segment_comfort_score`에 행 단위 GX 검증을 추가해, 정상 행은 계속 UPSERT하고 이상 행만 격리하며, 파국적인 실행은 hard fail시킨다 (#251).

**Architecture:** UPSERT 직전 배치를 pandas DataFrame으로 만들어 GX `PandasExecutionEngine`으로 검증한다. 정상 행은 기존 `_UPSERT_SQL`로, 이상 행은 새 Postgres 테이블 `current_segment_comfort_score_quarantine`으로 같은 트랜잭션에 분기한다. 전체 배치 처리 후 커밋 직전에 누적 카운트로 서킷브레이커를 평가해, 트립되면 예외를 던져 트랜잭션 전체를 롤백한다.

**Tech Stack:** Python 3.12, `great-expectations>=1.21.0`, `pandas>=2.0.0`, `psycopg2-binary`, pytest.

**Spec:** [docs/superpowers/specs/2026-08-21-current-score-quarantine-design.md](../specs/2026-08-21-current-score-quarantine-design.md), [ADR-0008](../../adr/0008-current-score-row-level-quarantine-and-circuit-breaker.md)

## Global Constraints

- Python 3.12, `uv` workspace. Run `uv sync --all-packages` after any dependency change.
- `services/orchestration/pyproject.toml`: add `great-expectations>=1.21.0` (no `[spark,postgresql]` extra — this service only uses `PandasExecutionEngine`) and `pandas>=2.0.0`.
- 서킷브레이커 임계치: 격리율 25% 초과, 또는 처리된 행이 있는데 정상 행이 0건이면 hard fail. `rows_seen == 0`(처리 대상이 없었던 정상적인 조기 반환)은 트립 대상이 아니다.
- Identity(방향 가중합) 허용오차: `abs(expected - comfort_score) <= 0.01`.
- 커밋 메시지에 `Co-Authored-By: Claude`나 "Generated with Claude Code" 같은 문구를 넣지 않는다.
- 각 태스크 끝에서 `uv run --all-packages ruff check .`와 해당 서비스의 `pytest`를 통과시킨다.
- 마이그레이션 파일(`services/batch-jobs/src/batch_jobs/resources/migrations/*.sql`)은 적용 후 절대 수정하지 않는다 — 새 파일만 추가한다.

---

### Task 1: `services/orchestration`에 GX/pandas 의존성 추가

**Files:**
- Modify: `services/orchestration/pyproject.toml`

**Interfaces:**
- Produces: `great_expectations`, `pandas` 임포트 가능 (Task 3이 사용)

- [ ] **Step 1: pyproject.toml에 의존성 추가**

`services/orchestration/pyproject.toml`의 `dependencies` 목록에 다음 두 줄을 추가한다(주석 포함, 다른 항목들과 같은 스타일):

```toml
    # current_score_pipeline 행 단위 격리(#251)의 GX PandasExecutionEngine 검증에 쓴다.
    "great-expectations>=1.21.0",
    "pandas>=2.0.0",
```

- [ ] **Step 2: 락파일 갱신**

Run: `cd /Users/yong/PycharmProjects/DE_team4-4una && uv lock`
Expected: `uv.lock`이 갱신되고 에러 없이 종료.

- [ ] **Step 3: 워크스페이스 동기화 및 임포트 확인**

Run: `uv sync --all-packages && uv run --package orchestration python -c "import great_expectations, pandas; print('ok')"`
Expected: `ok` 출력.

- [ ] **Step 4: Commit**

```bash
git add services/orchestration/pyproject.toml uv.lock
git commit -m "chore(orchestration): add great-expectations and pandas for current_score row-level validation"
```

---

### Task 2: 격리 테이블 마이그레이션 추가 (`services/batch-jobs`)

**Files:**
- Create: `services/batch-jobs/src/batch_jobs/resources/migrations/0011_create_current_score_quarantine.sql`

**Interfaces:**
- Produces: Postgres 테이블 `current_segment_comfort_score_quarantine(id, segment_id, vehicle_profile_id, calculated_at, reject_reason, reject_detail, raw_row, rejected_at)` — Task 3의 `insert_quarantined_rows`가 이 스키마로 INSERT한다.

- [ ] **Step 1: 마이그레이션 파일 작성**

```sql
-- current_segment_comfort_score_quarantine: run_current_score_job이 UPSERT
-- 직전 GX로 검증해 걸러낸 이상 행을 격리한다 (#251, docs/adr/0008-current-
-- score-row-level-quarantine-and-circuit-breaker.md).
--
-- 메인 테이블과 달리 "현재 상태"가 아니라 append-only 거부 로그다 — 같은
-- (segment_id, vehicle_profile_id)가 여러 실행에 걸쳐 반복 격리될 수 있어
-- 유니크 제약을 걸지 않는다. raw_row/reject_detail을 JSONB로 남겨 규칙이
-- 추가돼도 메인 테이블과 별도로 마이그레이션할 필요가 없게 한다. 재처리/
-- 복구 워크플로는 이 서브이슈 범위 밖이라(#251 "제외 범위") 스키마는 감사·
-- 조회용 최소 요건까지만 만족한다.
CREATE TABLE current_segment_comfort_score_quarantine (
    id BIGSERIAL PRIMARY KEY,
    segment_id TEXT NOT NULL,
    vehicle_profile_id INTEGER NOT NULL,
    calculated_at TIMESTAMPTZ NOT NULL,
    reject_reason TEXT NOT NULL,
    reject_detail JSONB NOT NULL,
    raw_row JSONB NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX current_segment_comfort_score_quarantine_segment_vehicle_idx
    ON current_segment_comfort_score_quarantine (segment_id, vehicle_profile_id);

CREATE INDEX current_segment_comfort_score_quarantine_calculated_at_idx
    ON current_segment_comfort_score_quarantine (calculated_at);
```

- [ ] **Step 2: 파일 내용 확인 (실제 SQL 문법 검증은 Task 6에서)**

`services/batch-jobs/tests/test_migrate.py`는 `FakeCursor`/`FakeConnection`으로 SQL 문자열을 그대로 기록만 할 뿐 실행하지 않고, 임시 디렉터리에 쓴 파일만 대상으로 하므로 이 새 파일의 실제 SQL 문법을 검증해주지 않는다 — 실행하지 않는다. 대신 파일이 의도한 내용을 담고 있는지 grep으로 확인한다.

Run: `grep -c "CREATE TABLE current_segment_comfort_score_quarantine\|CREATE INDEX" services/batch-jobs/src/batch_jobs/resources/migrations/0011_create_current_score_quarantine.sql`
Expected: `3` (테이블 1개 + 인덱스 2개).

실제 Postgres에 대한 SQL 문법/제약 검증은 이 파일을 직접 실행하지 않고 Task 6에서 `batch-jobs migrate-database`로 한다 — AGENTS.md가 데이터베이스 마이그레이션 실행을 "Ask first" 항목으로 명시하므로, 실행 직전에 반드시 사용자에게 확인을 받는다(Task 6 Step 1 참고).

- [ ] **Step 3: Commit**

```bash
git add services/batch-jobs/src/batch_jobs/resources/migrations/0011_create_current_score_quarantine.sql
git commit -m "feat(batch-jobs): add current_segment_comfort_score_quarantine table"
```

---

### Task 3: `current_score_quarantine.py` 모듈 구현 (GX 검증, 격리 INSERT, 서킷브레이커)

**Files:**
- Create: `services/orchestration/jobs/resources/expectations/current_segment_comfort_score_quarantine_suite.json`
- Create: `services/orchestration/jobs/current_score_quarantine.py`
- Test: `services/orchestration/tests/test_current_score_quarantine.py`

**Interfaces:**
- Consumes: `jobs.weather_rules.WeatherRuleConfig`, `jobs.weather_rules.LOW_VISIBILITY`, `jobs.weather_rules.parse_impact_signature`, `jobs.weather_rules.format_impact_signature`, `jobs.weather_rules.load_weather_rule_config` (Task 1 dependencies)
- Produces:
  - `CurrentScoreCircuitBreakerTripped(Exception)`
  - `QuarantineSplit(normal_rows: list[dict], quarantined_records: list[dict])` (frozen dataclass)
  - `load_expectation_suite(path: Path = DEFAULT_SUITE_PATH) -> gx.ExpectationSuite`
  - `compute_identity_diff(row: dict, rule_config: WeatherRuleConfig) -> float`
  - `split_batch(rows: list[dict], rule_config: WeatherRuleConfig, suite: gx.ExpectationSuite) -> QuarantineSplit`
  - `insert_quarantined_rows(cursor, records: list[dict]) -> None`
  - `check_circuit_breaker(*, upserted_count: int, quarantined_count: int, max_quarantine_rate: float = 0.25) -> None`
  - 이 모든 이름을 Task 4가 `current_score.py`에서 `from . import current_score_quarantine`로 가져다 쓴다.

- [ ] **Step 1: GX Expectation Suite JSON 리소스 작성**

`services/orchestration/jobs/resources/expectations/` 디렉터리를 만들고 파일을 작성한다:

```json
{
  "expectations": [
    {
      "kwargs": {"column": "comfort_score", "min_value": 0.0, "max_value": 100.0},
      "meta": {},
      "severity": "critical",
      "type": "expect_column_values_to_be_between"
    },
    {
      "kwargs": {"column": "vertical_score", "min_value": 0.0, "max_value": 100.0},
      "meta": {},
      "severity": "critical",
      "type": "expect_column_values_to_be_between"
    },
    {
      "kwargs": {"column": "longitudinal_score", "min_value": 0.0, "max_value": 100.0},
      "meta": {},
      "severity": "critical",
      "type": "expect_column_values_to_be_between"
    },
    {
      "kwargs": {"column": "lateral_score", "min_value": 0.0, "max_value": 100.0},
      "meta": {},
      "severity": "critical",
      "type": "expect_column_values_to_be_between"
    },
    {
      "kwargs": {"column": "confidence_score", "min_value": 0.0, "max_value": 1.0},
      "meta": {},
      "severity": "critical",
      "type": "expect_column_values_to_be_between"
    },
    {
      "kwargs": {"column": "sample_count", "min_value": 0},
      "meta": {},
      "severity": "critical",
      "type": "expect_column_values_to_be_between"
    },
    {
      "kwargs": {"column": "standard_score_as_of"},
      "meta": {},
      "severity": "critical",
      "type": "expect_column_values_to_not_be_null"
    },
    {
      "kwargs": {"column": "identity_diff", "min_value": 0.0, "max_value": 0.01},
      "meta": {},
      "severity": "critical",
      "type": "expect_column_values_to_be_between"
    }
  ],
  "id": null,
  "meta": {
    "great_expectations_version": "1.21.0"
  },
  "name": "current_segment_comfort_score_quarantine_suite",
  "notes": null
}
```

이 형식은 `services/batch-jobs/src/batch_jobs/resources/expectations/standard_segment_comfort_score_suite.json`과 동일한 스키마이며, `gx.ExpectationSuite(**payload)`로 그대로 로드된다(`standard_score_validation.py::load_expectation_suite` 선례로 이미 검증된 패턴).

- [ ] **Step 2: `compute_identity_diff`용 실패하는 테스트 작성**

`services/orchestration/tests/test_current_score_quarantine.py`를 새로 만든다:

```python
# jobs/current_score_quarantine.py 테스트 (#251).

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from jobs import current_score_quarantine
from jobs.current_score_quarantine import (
    DEFAULT_SUITE_PATH,
    CurrentScoreCircuitBreakerTripped,
    check_circuit_breaker,
    compute_identity_diff,
    insert_quarantined_rows,
    load_expectation_suite,
    split_batch,
)
from jobs.weather_rules import LOW_VISIBILITY, format_impact_signature, load_weather_rule_config

RULE_CONFIG = load_weather_rule_config()
CALCULATED_AT = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)


def _weighted_sum(vertical: float, longitudinal: float, lateral: float) -> float:
    return (
        RULE_CONFIG.vertical_weight.value * vertical
        + RULE_CONFIG.longitudinal_weight.value * longitudinal
        + RULE_CONFIG.lateral_weight.value * lateral
    )


class TestComputeIdentityDiff:
    def test_zero_diff_when_comfort_score_matches_weighted_sum(self):
        row = {
            "vertical_score": 80.0,
            "longitudinal_score": 70.0,
            "lateral_score": 60.0,
            "comfort_score": _weighted_sum(80.0, 70.0, 60.0),
            "weather_impact_signature": None,
        }

        assert compute_identity_diff(row, RULE_CONFIG) == pytest.approx(0.0, abs=1e-9)

    def test_nonzero_diff_when_comfort_score_does_not_match(self):
        row = {
            "vertical_score": 80.0,
            "longitudinal_score": 70.0,
            "lateral_score": 60.0,
            "comfort_score": 0.0,
            "weather_impact_signature": None,
        }

        assert compute_identity_diff(row, RULE_CONFIG) > 1.0

    def test_skips_check_when_low_visibility_is_active(self):
        signature = format_impact_signature(frozenset({LOW_VISIBILITY}))
        row = {
            "vertical_score": 80.0,
            "longitudinal_score": 70.0,
            "lateral_score": 60.0,
            "comfort_score": 0.0,
            "weather_impact_signature": signature,
        }

        assert compute_identity_diff(row, RULE_CONFIG) == 0.0
```

- [ ] **Step 3: 테스트 실패 확인**

Run: `uv run --package orchestration pytest services/orchestration/tests/test_current_score_quarantine.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'jobs.current_score_quarantine'`.

- [ ] **Step 4: `current_score_quarantine.py` 최소 구현 — `compute_identity_diff`, `load_expectation_suite`, 상수/예외/데이터클래스**

`services/orchestration/jobs/current_score_quarantine.py`:

```python
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
```

- [ ] **Step 5: identity_diff 테스트 통과 확인**

Run: `uv run --package orchestration pytest services/orchestration/tests/test_current_score_quarantine.py::TestComputeIdentityDiff -v`
Expected: PASS (3 tests). 나머지 테스트는 아직 `ImportError`로 실패한다 — 다음 스텝에서 해소한다.

- [ ] **Step 6: `check_circuit_breaker`용 실패하는 테스트 추가**

`test_current_score_quarantine.py`에 추가:

```python
class TestCheckCircuitBreaker:
    def test_does_nothing_when_no_rows_were_processed(self):
        check_circuit_breaker(upserted_count=0, quarantined_count=0)

    def test_does_nothing_when_quarantine_rate_is_within_threshold(self):
        check_circuit_breaker(upserted_count=8, quarantined_count=2)  # 20% <= 25%

    def test_trips_when_all_rows_are_quarantined(self):
        with pytest.raises(CurrentScoreCircuitBreakerTripped, match="quarantined"):
            check_circuit_breaker(upserted_count=0, quarantined_count=5)

    def test_trips_when_quarantine_rate_exceeds_threshold(self):
        with pytest.raises(CurrentScoreCircuitBreakerTripped, match="quarantine_rate"):
            check_circuit_breaker(upserted_count=7, quarantined_count=3)  # 30% > 25%
```

- [ ] **Step 7: `check_circuit_breaker` 구현**

`current_score_quarantine.py` 끝에 추가:

```python
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
```

- [ ] **Step 8: 서킷브레이커 테스트 통과 확인 후 커밋**

Run: `uv run --package orchestration pytest services/orchestration/tests/test_current_score_quarantine.py -v`
Expected: `TestComputeIdentityDiff`, `TestCheckCircuitBreaker` 전부 PASS.

```bash
git add services/orchestration/jobs/current_score_quarantine.py services/orchestration/tests/test_current_score_quarantine.py services/orchestration/jobs/resources/expectations/current_segment_comfort_score_quarantine_suite.json
git commit -m "feat(orchestration): add current_score identity-diff and circuit breaker checks"
```

- [ ] **Step 9: `split_batch`/`insert_quarantined_rows`용 실패하는 테스트 추가**

`test_current_score_quarantine.py`에 추가:

```python
def _valid_row(segment_id: str) -> dict:
    return {
        "segment_id": segment_id,
        "vehicle_profile_id": 1,
        "location_id": 76,
        "standard_score_as_of": CALCULATED_AT,
        "weather_time": None,
        "data_period_start": None,
        "vertical_score": 80.0,
        "longitudinal_score": 70.0,
        "lateral_score": 60.0,
        "comfort_score": _weighted_sum(80.0, 70.0, 60.0),
        "sample_count": 900,
        "confidence_score": 0.9,
        "standard_score_version": "1.0.0",
        "weather_rule_version": None,
        "weather_impact_signature": None,
        "calculated_at": CALCULATED_AT,
    }


class TestSplitBatch:
    def test_all_normal_rows_stay_normal(self):
        suite = load_expectation_suite(DEFAULT_SUITE_PATH)
        rows = [_valid_row("1"), _valid_row("2")]

        split = split_batch(rows, RULE_CONFIG, suite)

        assert [row["segment_id"] for row in split.normal_rows] == ["1", "2"]
        assert split.quarantined_records == []

    def test_out_of_range_row_is_quarantined_and_normal_row_kept(self):
        suite = load_expectation_suite(DEFAULT_SUITE_PATH)
        bad_row = _valid_row("2")
        bad_row["comfort_score"] = 150.0
        rows = [_valid_row("1"), bad_row]

        split = split_batch(rows, RULE_CONFIG, suite)

        assert [row["segment_id"] for row in split.normal_rows] == ["1"]
        assert len(split.quarantined_records) == 1
        record = split.quarantined_records[0]
        assert record["segment_id"] == "2"
        assert record["vehicle_profile_id"] == 1
        assert record["calculated_at"] == CALCULATED_AT
        assert "comfort_score" in record["reject_reason"]
        assert record["raw_row"]["segment_id"] == "2"

    def test_empty_batch_returns_empty_split(self):
        suite = load_expectation_suite(DEFAULT_SUITE_PATH)

        split = split_batch([], RULE_CONFIG, suite)

        assert split.normal_rows == []
        assert split.quarantined_records == []


class FakeQuarantineCursor:
    def __init__(self):
        self.calls: list[tuple] = []

    def execute(self, sql, parameters=None):
        self.calls.append((sql, parameters))


class TestInsertQuarantinedRows:
    def test_does_nothing_for_empty_records(self, monkeypatch):
        called = []
        monkeypatch.setattr(
            current_score_quarantine, "execute_values", lambda *a, **k: called.append(a)
        )

        insert_quarantined_rows(FakeQuarantineCursor(), [])

        assert called == []

    def test_inserts_records_via_execute_values(self, monkeypatch):
        captured = {}

        def fake_execute_values(cursor, sql, argslist):
            captured["sql"] = sql
            captured["argslist"] = argslist

        monkeypatch.setattr(current_score_quarantine, "execute_values", fake_execute_values)
        records = [
            {
                "segment_id": "2",
                "vehicle_profile_id": 1,
                "calculated_at": CALCULATED_AT,
                "reject_reason": "comfort_score",
                "reject_detail": [{"expectation_type": "expect_column_values_to_be_between"}],
                "raw_row": _valid_row("2"),
            }
        ]

        insert_quarantined_rows(FakeQuarantineCursor(), records)

        assert "current_segment_comfort_score_quarantine" in captured["sql"]
        (row,) = captured["argslist"]
        assert row[0] == "2"
        assert row[1] == 1
        assert row[2] == CALCULATED_AT
        assert row[3] == "comfort_score"
        assert len(row) == 6
```

- [ ] **Step 10: 테스트 실패 확인**

Run: `uv run --package orchestration pytest services/orchestration/tests/test_current_score_quarantine.py::TestSplitBatch services/orchestration/tests/test_current_score_quarantine.py::TestInsertQuarantinedRows -v`
Expected: FAIL with `ImportError: cannot import name 'split_batch'`.

- [ ] **Step 11: `split_batch`/`insert_quarantined_rows` 구현**

`current_score_quarantine.py`에 추가 (파일 끝, `check_circuit_breaker` 앞에):

```python
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
```

- [ ] **Step 12: 전체 모듈 테스트 통과 확인**

Run: `uv run --package orchestration pytest services/orchestration/tests/test_current_score_quarantine.py -v`
Expected: 전부 PASS.

- [ ] **Step 13: lint 확인 후 커밋**

Run: `uv run --all-packages ruff check services/orchestration/jobs/current_score_quarantine.py services/orchestration/tests/test_current_score_quarantine.py`
Expected: 에러 없음.

```bash
git add services/orchestration/jobs/current_score_quarantine.py services/orchestration/tests/test_current_score_quarantine.py
git commit -m "feat(orchestration): add current_score row-level GX split and quarantine insert"
```

---

### Task 4: `run_current_score_job`에 격리/서킷브레이커 배선

**Files:**
- Modify: `services/orchestration/jobs/current_score.py`
- Modify: `services/orchestration/tests/test_current_score.py`

**Interfaces:**
- Consumes: Task 3의 `current_score_quarantine.{split_batch, insert_quarantined_rows, check_circuit_breaker, load_expectation_suite, CurrentScoreCircuitBreakerTripped}`
- Produces: `CurrentScoreJobSummary`에 `quarantined_count: int` 필드 추가 — DAG(`_run_current_score`)가 이 값을 로그/XCom으로 소비할 수 있다.

- [ ] **Step 1: import 추가**

`services/orchestration/jobs/current_score.py`의 기존 import 블록:

```python
from .weather_rules import (
    WEATHER_RULE_VERSION,
    WeatherRuleConfig,
    adjust_comfort_scores,
    load_weather_rule_config,
    parse_impact_signature,
)
```

바로 뒤에 추가:

```python

from . import current_score_quarantine
```

- [ ] **Step 2: `CurrentScoreJobSummary`에 `quarantined_count` 필드 추가**

기존:

```python
@dataclass(frozen=True, slots=True)
class CurrentScoreJobSummary:
    zone_count: int
    upserted_count: int
    # zone이 배정되지 않은 segment. location_id가 NOT NULL이라 행을 만들 수 없다.
    skipped_unzoned_count: int
```

변경 후:

```python
@dataclass(frozen=True, slots=True)
class CurrentScoreJobSummary:
    zone_count: int
    upserted_count: int
    # zone이 배정되지 않은 segment. location_id가 NOT NULL이라 행을 만들 수 없다.
    skipped_unzoned_count: int
    quarantined_count: int
```

- [ ] **Step 3: 조기 반환 갱신**

기존:

```python
    if changed_zones_only:
        zones = find_changed_zones(connection)
        if not zones:
            logger.info("no zone changed its weather — nothing to recompute")
            return CurrentScoreJobSummary(0, 0, 0)
        target_zones: set[int] | None = set(zones)
```

변경 후:

```python
    if changed_zones_only:
        zones = find_changed_zones(connection)
        if not zones:
            logger.info("no zone changed its weather — nothing to recompute")
            return CurrentScoreJobSummary(0, 0, 0, 0)
        target_zones: set[int] | None = set(zones)
```

- [ ] **Step 4: 배치 루프에 격리 분기 배선**

기존:

```python
    calculated_at = datetime.now(UTC)
    upserted = 0
    skipped = 0

    with connection.cursor() as write_cursor:
        # 두 트리거(15분/시간별)가 겹쳐도 같은 행을 서로 덮어쓰지 않게 직렬화한다.
        write_cursor.execute("SELECT pg_advisory_lock(%s)", (LOCK_KEY,))
        try:
            for batch in _read_standard_rows(connection, segment_zones, target_zones):
                rows = []
                for record in batch:
                    location_id = segment_zones.get(record[0])
                    if location_id is None:
                        skipped += 1
                        continue
                    rows.append(
                        _build_row(
                            record, location_id, weather_by_zone.get(location_id),
                            rule_config, calculated_at,
                        )
                    )
                if rows:
                    execute_values(
                        write_cursor,
                        _UPSERT_SQL,
                        [tuple(row[column] for column in _ROW_COLUMNS) for row in rows],
                    )
                    upserted += len(rows)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            write_cursor.execute("SELECT pg_advisory_unlock(%s)", (LOCK_KEY,))

    logger.info(
        "current comfort score job finished zones=%d upserted=%d skipped_unzoned=%d",
        len(zones), upserted, skipped,
    )
    return CurrentScoreJobSummary(
        zone_count=len(zones), upserted_count=upserted, skipped_unzoned_count=skipped
    )
```

변경 후:

```python
    calculated_at = datetime.now(UTC)
    upserted = 0
    skipped = 0
    quarantined = 0
    suite = current_score_quarantine.load_expectation_suite()

    with connection.cursor() as write_cursor:
        # 두 트리거(15분/시간별)가 겹쳐도 같은 행을 서로 덮어쓰지 않게 직렬화한다.
        write_cursor.execute("SELECT pg_advisory_lock(%s)", (LOCK_KEY,))
        try:
            for batch in _read_standard_rows(connection, segment_zones, target_zones):
                rows = []
                for record in batch:
                    location_id = segment_zones.get(record[0])
                    if location_id is None:
                        skipped += 1
                        continue
                    rows.append(
                        _build_row(
                            record, location_id, weather_by_zone.get(location_id),
                            rule_config, calculated_at,
                        )
                    )
                if rows:
                    split = current_score_quarantine.split_batch(rows, rule_config, suite)
                    if split.normal_rows:
                        execute_values(
                            write_cursor,
                            _UPSERT_SQL,
                            [
                                tuple(row[column] for column in _ROW_COLUMNS)
                                for row in split.normal_rows
                            ],
                        )
                    current_score_quarantine.insert_quarantined_rows(
                        write_cursor, split.quarantined_records
                    )
                    upserted += len(split.normal_rows)
                    quarantined += len(split.quarantined_records)
            current_score_quarantine.check_circuit_breaker(
                upserted_count=upserted, quarantined_count=quarantined
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            write_cursor.execute("SELECT pg_advisory_unlock(%s)", (LOCK_KEY,))

    logger.info(
        "current comfort score job finished zones=%d upserted=%d skipped_unzoned=%d quarantined=%d",
        len(zones), upserted, skipped, quarantined,
    )
    return CurrentScoreJobSummary(
        zone_count=len(zones),
        upserted_count=upserted,
        skipped_unzoned_count=skipped,
        quarantined_count=quarantined,
    )
```

- [ ] **Step 5: 기존 테스트 스위트가 새 필드를 반영하도록 갱신**

`services/orchestration/tests/test_current_score.py` 상단 import에 추가:

```python
from jobs import current_score_quarantine
```

`captured_upserts` 픽스처를 다음으로 교체:

```python
@pytest.fixture(autouse=True)
def captured_upserts(monkeypatch):
    def record(cursor, sql, argslist):
        cursor.owner.upserted.extend(argslist)

    monkeypatch.setattr(current_score, "execute_values", record)

    def record_quarantine(cursor, sql, argslist):
        cursor.owner.quarantined.extend(argslist)

    monkeypatch.setattr(current_score_quarantine, "execute_values", record_quarantine)
```

`FakeConnection.__init__`에 한 줄 추가:

```python
    def __init__(self, *, weather_rows=(), standard_rows=(), changed_zone_rows=()):
        self.weather_rows = weather_rows
        self.standard_rows = standard_rows
        self.changed_zone_rows = changed_zone_rows
        self.executed: list[tuple] = []
        self.upserted: list[tuple] = []
        self.quarantined: list[tuple] = []
        self.committed = False
```

`test_changed_zones_only_stops_when_nothing_changed`의 단언을 갱신:

```python
        assert summary == type(summary)(0, 0, 0, 0)
```

- [ ] **Step 6: 새 시나리오 테스트 추가**

`TestRunCurrentScoreJob` 클래스 안에 추가:

```python
    def test_quarantines_an_out_of_range_row_and_still_upserts_normal_ones(self, tmp_path):
        # confidence_score를 쓰는 이유: vertical/longitudinal/lateral/comfort_score는
        # _build_row -> adjust_comfort_scores의 _clamp가 항상 [0,100]으로 잘라내므로
        # _build_row를 거치는 경로에서는 범위 위반이 구조적으로 발생할 수 없다.
        # confidence_score/sample_count는 클램프 없이 그대로 통과하므로, 여기서는
        # standard_segment_comfort_score의 CHECK 제약이 어떤 이유로든(마이그레이션
        # 변경, 직접 데이터 수정 등) 뚫렸다고 가정한 방어적 시나리오를 검증한다.
        # 격리율 25% 서킷브레이커 임계값(DEFAULT_MAX_QUARANTINE_RATE) 아래로 유지하려면
        # 정상 행이 3건 이상 필요하다 (1개 격리 / 4개 전체 = 25%는 초과가 아니라 통과).
        path = write_road_segment(
            tmp_path,
            [
                ("11111", SNAPSHOT_DATE, 76),
                ("22222", SNAPSHOT_DATE, 76),
                ("33333", SNAPSHOT_DATE, 76),
                ("99999", SNAPSHOT_DATE, 76),
            ],
        )
        good_rows = [
            ("11111", 1, SCORE_AS_OF, None, 80.0, 70.0, 60.0, 900, 0.9, "1.0.0"),
            ("22222", 1, SCORE_AS_OF, None, 80.0, 70.0, 60.0, 900, 0.9, "1.0.0"),
            ("33333", 1, SCORE_AS_OF, None, 80.0, 70.0, 60.0, 900, 0.9, "1.0.0"),
        ]
        bad_row = ("99999", 1, SCORE_AS_OF, None, 80.0, 70.0, 60.0, 900, 1.5, "1.0.0")
        connection = FakeConnection(weather_rows=[], standard_rows=[*good_rows, bad_row])

        summary = run_current_score_job(
            config_for(path), connection, changed_zones_only=False, rule_config=RULE_CONFIG
        )

        assert summary.upserted_count == 3
        assert summary.quarantined_count == 1
        assert len(connection.quarantined) == 1
        assert connection.quarantined[0][0] == "99999"
        assert connection.committed

    def test_circuit_breaker_trips_when_all_rows_are_quarantined(self, tmp_path):
        path = write_road_segment(tmp_path, [("12345", SNAPSHOT_DATE, 76)])
        bad_row = ("12345", 1, SCORE_AS_OF, None, 80.0, 70.0, 60.0, 900, 1.5, "1.0.0")
        connection = FakeConnection(weather_rows=[], standard_rows=[bad_row])

        with pytest.raises(current_score_quarantine.CurrentScoreCircuitBreakerTripped):
            run_current_score_job(
                config_for(path), connection, changed_zones_only=False, rule_config=RULE_CONFIG
            )

        assert connection.upserted == []
        assert not connection.committed
```

- [ ] **Step 7: 전체 테스트 통과 확인**

Run: `uv run --package orchestration pytest services/orchestration/tests/test_current_score.py services/orchestration/tests/test_current_score_quarantine.py -v`
Expected: 전부 PASS.

- [ ] **Step 8: lint + 워크스페이스 전체 테스트**

Run: `uv run --all-packages ruff check . && uv run --all-packages pytest`
Expected: 전부 통과 (Spark 필요한 통합 테스트는 `RUN_INTEGRATION` 미설정 시 스킵됨 — 기존 컨벤션).

- [ ] **Step 9: Commit**

```bash
git add services/orchestration/jobs/current_score.py services/orchestration/tests/test_current_score.py
git commit -m "feat(orchestration): branch current_score UPSERT into normal/quarantine paths with circuit breaker"
```

---

### Task 5: 컨텍스트 문서 동기화

**Files:**
- Modify (via skill): `context/data/quality-rules.md`, `context/data/schema-catalog.md`, `context/open-questions.md`

**Interfaces:**
- 코드 변경 없음 — Task 1~4에서 확정된 사실을 문서에 반영한다.

- [ ] **Step 1: `update-project-context` 스킬 호출**

다음 사실을 입력으로 전달해 스킬을 호출한다:

- 새 Postgres 테이블 `current_segment_comfort_score_quarantine`(스키마는 Task 2의 마이그레이션 참고) — `schema-catalog.md`에 추가.
- `current_segment_comfort_score`에 새 in-flight GX 검증이 생겼다는 사실을 `quality-rules.md`의 "## Gold quality" 절에 추가 — `standard_score_validation.py`/`sensor_processing_validation.py` 항목과 같은 서술 스타일로: 무엇을 검증하는지(comfort_score 등 0-100, confidence_score 0-1, sample_count >= 0, 방향 가중합 항등식 — low_visibility 비활성 시, `standard_score_as_of` NOT NULL), 어떤 모듈/함수가 하는지(`orchestration.jobs.current_score_quarantine`, issue #251, ADR-0008), `PandasExecutionEngine`으로 UPSERT 이전 in-memory 검증한다는 점, 서킷브레이커(정상 0건 또는 격리율 25% 초과 시 hard fail)를 명시.
- `weather_time`/`weather_rule_version`/`weather_impact_signature` NULL 짝 제약은 GX가 아니라 코드/DB CHECK가 이미 강제하는 하드 인바리언트라 GX Suite에 넣지 않았다는 점(ADR-0004 원칙과의 관계)도 명시.
- `context/open-questions.md`에 새 항목 `OQ-042`를 "## Data decisions" 표에 추가: `standard_score_as_of`의 구체적 신선도 임계치(몇 시간 이내여야 유효한지)가 미정이며, 현재는 NOT NULL만 검증한다는 내용. Status는 `Open`.

- [ ] **Step 2: 문서 diff 확인**

Run: `git diff context/`
Expected: 위 세 파일에 대한 변경만 존재, 다른 컨텍스트 파일은 건드리지 않음.

- [ ] **Step 3: Commit**

```bash
git add context/data/quality-rules.md context/data/schema-catalog.md context/open-questions.md
git commit -m "docs: sync context with current_score row-level quarantine (#251)"
```

---

### Task 6: 로컬 Airflow + 실제 Postgres로 수동 검증

**Files:** 없음 (검증 전용 태스크)

**Interfaces:** 없음 — 이슈 #251의 완료 조건을 실제 환경에서 확인한다.

**실행 주체**: 이 태스크는 구현 서브에이전트에게 위임하지 않는다 — 컨트롤러(나)가 사용자와 함께 직접 진행한다. `POSTGRES_*` 자격 증명은 `.env`에만 있고 AGENTS.md가 `.env` 읽기/수정을 금지하므로, DB 접속이 필요한 명령은 사용자에게 직접 실행을 요청하거나(예: `! make migrate`) 사용자가 이미 내보낸 환경변수가 있는 셸에서만 실행한다. 또한 AGENTS.md는 "데이터베이스 마이그레이션 실행"을 Ask first 항목으로 명시하므로, Step 1을 실행하기 전에 반드시 사용자에게 명시적으로 확인받는다.

- [ ] **Step 1: 마이그레이션 적용 (실행 전 사용자에게 명시적으로 확인받을 것)**

사용자에게 "`0011_create_current_score_quarantine.sql`을 로컬 Postgres에 적용해도 될까요?"라고 확인한 뒤, 승인받으면 사용자가 직접 실행하도록 안내하거나(`! make migrate`) 사용자가 이미 `POSTGRES_*`를 내보낸 셸에서 실행한다:

Run: `uv run --package batch-jobs batch-jobs migrate-database`
Expected: `0011_create_current_score_quarantine.sql`이 `applied`로 표시됨.

- [ ] **Step 2: 로컬 Airflow 기동 확인**

`infra/compose/airflow.yaml` 컨테이너가 떠 있는지 확인하고, 없으면 기존 팀 컨벤션대로 기동한다. `current_score_pipeline` DAG를 언포즈하고 수동 트리거한다.

- [ ] **Step 3: 정상 케이스 확인**

정상 범위의 값으로 `run_current_score` task가 성공하고, `current_segment_comfort_score`에 UPSERT되는지 확인한다.

- [ ] **Step 4: 이상치 혼합 케이스 확인**

`standard_segment_comfort_score`에 임시로 범위를 벗어난 값(예: `comfort_score > 100`)을 가진 행을 하나 넣고(테스트 후 원복) 재실행해, 정상 행은 `current_segment_comfort_score`에 반영되고 이상 행은 `current_segment_comfort_score_quarantine`에 격리되는지 SQL로 직접 확인한다.

- [ ] **Step 5: 파국적 케이스 확인**

임계치(25%)를 넘도록 다수 행을 이상치로 만들어 재실행해, task가 hard fail하고 `current_segment_comfort_score`에 이번 실행분이 전혀 반영되지 않는지 확인한다.

- [ ] **Step 6: 사용자에게 결과 보고**

로컬 Airflow 웹 UI URL과 관리자 계정 정보를 사용자에게 알려 직접 확인할 수 있게 한다(팀 컨벤션).
