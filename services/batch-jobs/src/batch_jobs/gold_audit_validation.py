"""Gold at-rest 감사 (#253, ADR-0004 롤아웃 ④번).

`standard_segment_comfort_score`/`current_segment_comfort_score`(둘 다
Postgres) 전체 범위를 매일 감사한다. in-flight 검증(#220, #249)과 달리
"이번 실행분"이 아니라 이미 적재된 전체 이력을 대상으로 하고, 실패해도
파이프라인을 막지 않는다(soft fail). Gold는 `SqlAlchemyExecutionEngine`으로
직접 조회한다(ADR-0004: Gold/Postgres는 Spark가 아니라 SqlAlchemy 경로).

Data Docs를 S3에서 열람 가능해야 하므로(완료 조건), 다른 GX 검증 모듈들과
달리 `batch.validate(suite)`를 직접 호출하지 않는다 — 그 경로는
`validation_results_store`에 기록을 남기지 않아 Data Docs가 빈 채로
렌더링된다(로컬 재현으로 확인). 대신 `ValidationDefinition` +
`Checkpoint`(`UpdateDataDocsAction` 포함) 경로로 실행한다.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from batch_jobs.resources import RESOURCE_DIR

TABLES: tuple[str, str] = (
    "standard_segment_comfort_score",
    "current_segment_comfort_score",
)

# 두 테이블 모두 최신 행이 이 값(초)보다 오래되면 stale로 본다(spec §4).
FRESHNESS_THRESHOLD_SECONDS = 10800

# freshness를 판정할 기준 컬럼 — current_segment_comfort_score엔 score_as_of가 없다.
_FRESHNESS_COLUMN: dict[str, str] = {
    "standard_segment_comfort_score": "score_as_of",
    "current_segment_comfort_score": "calculated_at",
}

DEFAULT_RANGE_SUITE_PATHS: dict[str, Path] = {
    table: RESOURCE_DIR / "expectations" / f"{table}_audit_range_suite.json"
    for table in TABLES
}
DEFAULT_SUMMARY_SUITE_PATHS: dict[str, Path] = {
    table: RESOURCE_DIR / "expectations" / f"{table}_audit_summary_suite.json"
    for table in TABLES
}

DEFAULT_S3_BUCKET = "de4-data-quality-docs"
S3_PREFIX = "data-quality-audit/gold"


def _validate_table(table: str) -> None:
    if table not in TABLES:
        raise ValueError(f"table must be one of {TABLES}, got {table!r}")


def _require(source: Mapping[str, str], key: str) -> str:
    value = source.get(key)
    if not value:
        raise ValueError(f"{key} must be set")
    return value


@dataclass(frozen=True, slots=True)
class GoldAuditValidationConfig:
    """`load-standard-segment-comfort-score`(StandardComfortScoreJobConfig)와
    같은 POSTGRES_* env var를 재사용한다."""

    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    s3_bucket: str
    range_suite_paths: Mapping[str, Path]
    summary_suite_paths: Mapping[str, Path]

    @property
    def connection_string(self) -> str:
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> GoldAuditValidationConfig:
        source = env if env is not None else os.environ
        return cls(
            postgres_host=_require(source, "POSTGRES_HOST"),
            postgres_port=int(_require(source, "POSTGRES_PORT")),
            postgres_db=_require(source, "POSTGRES_DB"),
            postgres_user=_require(source, "POSTGRES_USER"),
            postgres_password=_require(source, "POSTGRES_PASSWORD"),
            s3_bucket=source.get("GOLD_AUDIT_S3_BUCKET") or DEFAULT_S3_BUCKET,
            range_suite_paths=DEFAULT_RANGE_SUITE_PATHS,
            summary_suite_paths=DEFAULT_SUMMARY_SUITE_PATHS,
        )
