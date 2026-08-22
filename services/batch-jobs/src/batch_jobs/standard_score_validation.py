"""`standard_score` TaskGroup 산출물을 Great Expectations로 검증한다 (#249, ADR-0004).

`standard_segment_comfort_score`는 Postgres에 있으므로 Spark가 아니라
`SqlAlchemyExecutionEngine`으로 직접 조회한다(ADR-0004: Gold/Postgres는 SqlAlchemy
경로). `run_standard_score`가 이번 실행에 UPSERT한 행만(`score_as_of = as_of`)
스코프해서 검증한다(in-flight, 전체 이력 아님). 방향별/종합 점수 범위(0~100)와
`score_version` 형식(SemVer)을 GX Suite로 검증한다. 스키마/PK/필수값 같은 하드
인바리언트는 `standard_writer.py`가 쓰기 시점에 이미 강제하므로 여기서 다시
다루지 않는다(ADR-0004: 하드 인바리언트는 GX로 옮기지 않는다).
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import great_expectations as gx
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
)

from batch_jobs.resources import RESOURCE_DIR

DEFAULT_SUITE_PATH = RESOURCE_DIR / "expectations" / "standard_segment_comfort_score_suite.json"

TABLE = "standard_segment_comfort_score"


@dataclass(frozen=True, slots=True)
class StandardScoreValidationConfig:
    """`run_standard_score`(StandardComfortScoreJobConfig)와 같은 POSTGRES_* env var를 재사용한다."""

    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    suite_path: Path

    @property
    def connection_string(self) -> str:
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> StandardScoreValidationConfig:
        source = env if env is not None else os.environ
        return cls(
            postgres_host=_require(source, "POSTGRES_HOST"),
            postgres_port=int(_require(source, "POSTGRES_PORT")),
            postgres_db=_require(source, "POSTGRES_DB"),
            postgres_user=_require(source, "POSTGRES_USER"),
            postgres_password=_require(source, "POSTGRES_PASSWORD"),
            suite_path=Path(source.get("STANDARD_SCORE_SUITE_PATH") or DEFAULT_SUITE_PATH),
        )


def _require(source: Mapping[str, str], key: str) -> str:
    value = source.get(key)
    if not value:
        raise ValueError(f"{key} must be set")
    return value


@dataclass(frozen=True, slots=True)
class StandardScoreValidationSummary:
    row_count: int
    suite_success: bool

    @property
    def success(self) -> bool:
        return self.suite_success


class StandardScoreValidationFailed(Exception):
    """검증 실패 시 발생시켜 Airflow task를 hard fail시킨다(ADR-0004)."""


def build_scope_query(as_of: datetime) -> str:
    """`score_as_of`는 as_of 리터럴 그대로다(standard_job.py::_attach_score_as_of) —
    이번 실행이 UPSERT한 행만 정확히 스코프할 수 있다."""
    if as_of.utcoffset() is None:
        raise ValueError("as_of must be timezone-aware")
    return (
        f"SELECT * FROM {TABLE} "
        f"WHERE score_as_of = '{as_of.isoformat()}'::timestamptz"
    )


def load_expectation_suite(path: Path) -> gx.ExpectationSuite:
    payload = json.loads(Path(path).read_text())
    return gx.ExpectationSuite(**payload)


def count_scope_rows(connection, as_of: datetime) -> int:
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT COUNT(*) FROM {TABLE} WHERE score_as_of = %s",
            (as_of,),
        )
        return cursor.fetchone()[0]


def run_standard_score_validation(
    config: StandardScoreValidationConfig,
    as_of: datetime,
    connection,
) -> StandardScoreValidationSummary:
    row_count = count_scope_rows(connection, as_of)
    if row_count == 0:
        raise StandardScoreValidationFailed(
            f"no {TABLE} rows found for score_as_of={as_of.isoformat()}"
        )

    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_postgres(
        name=f"{TABLE}_datasource", connection_string=config.connection_string
    )
    asset = datasource.add_query_asset(name=TABLE, query=build_scope_query(as_of))
    batch_definition = asset.add_batch_definition_whole_table(f"{TABLE}_batch")
    batch = batch_definition.get_batch()

    suite = load_expectation_suite(config.suite_path)
    result: ExpectationSuiteValidationResult = batch.validate(suite)

    summary = StandardScoreValidationSummary(row_count=row_count, suite_success=result.success)
    if not summary.success:
        raise StandardScoreValidationFailed(
            f"standard_score validation failed for score_as_of={as_of.isoformat()}: {summary}"
        )
    return summary
