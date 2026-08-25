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

import json
import logging
import os
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import great_expectations as gx
from great_expectations.checkpoint.actions import UpdateDataDocsAction
from great_expectations.checkpoint.checkpoint import Checkpoint
from great_expectations.core.validation_definition import ValidationDefinition

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

logger = logging.getLogger(__name__)


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


def build_range_query(table: str) -> str:
    _validate_table(table)
    return f"SELECT * FROM {table}"


def build_summary_query(table: str) -> str:
    """freshness(초)와 orphan `vehicle_profile_id` 수를 한 행으로 묶어 낸다.

    `current_segment_comfort_score.vehicle_profile_id`엔 DB FK가 없어(0006
    마이그레이션) 이 anti-join이 실제로 의미가 있다. `standard_segment_comfort_score`는
    이미 FK로 이 위반이 불가능하지만, 검증 비용이 저렴해 안전망으로 그대로 둔다.
    """
    _validate_table(table)
    freshness_column = _FRESHNESS_COLUMN[table]
    return (
        f"SELECT EXTRACT(EPOCH FROM (now() - MAX({freshness_column})))"
        f"::double precision AS age_seconds, "
        f"(SELECT count(*) FROM {table} t "
        f"LEFT JOIN vehicle_profile vp "
        f"ON t.vehicle_profile_id = vp.vehicle_profile_id "
        f"WHERE vp.vehicle_profile_id IS NULL) AS orphan_vehicle_profile_count "
        f"FROM {table}"
    )


def count_rows(connection, table: str) -> int:
    _validate_table(table)
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")  # table validated above
        return cursor.fetchone()[0]


def load_expectation_suite(path: Path) -> gx.ExpectationSuite:
    payload = json.loads(Path(path).read_text())
    return gx.ExpectationSuite(**payload)


def upload_data_docs_to_s3(local_dir: Path, bucket: str, prefix: str, s3_client) -> int:
    """렌더된 Data Docs 임시 디렉터리를 S3에 업로드한다.

    GX의 `TupleS3StoreBackend`가 great-expectations==1.21.0엔 없어(#253 스펙
    §1) GX는 로컬에만 렌더링하고, 이 함수가 그 결과물을 boto3로 직접 옮긴다.
    """
    local_dir = Path(local_dir)
    uploaded = 0
    for path in sorted(local_dir.rglob("*")):
        if path.is_file():
            relative = path.relative_to(local_dir).as_posix()
            key = f"{prefix}/{relative}"
            s3_client.put_object(Bucket=bucket, Key=key, Body=path.read_bytes())
            uploaded += 1
    return uploaded


@dataclass(frozen=True, slots=True)
class GoldAuditSummary:
    table: str
    row_count: int
    success: bool


class GoldAuditValidationFailed(Exception):
    """검증 실패 시 발생시켜 Airflow task를 soft fail시킨다(ADR-0004, spec §6)."""


def run_gold_audit(
    config: GoldAuditValidationConfig,
    connection,
    table: str,
    s3_client,
) -> GoldAuditSummary:
    """`table` 전체를 range/freshness/참조무결성 기준으로 감사한다(at-rest,
    이번 실행분이 아니라 전체 이력).

    성공/실패와 무관하게 Data Docs를 항상 렌더링 후 S3에 업로드한 다음에야
    실패 여부를 판정한다(spec §6) — 단, 테이블이 아예 비어 있으면 GX를
    실행할 대상 자체가 없으므로 그 전에 바로 실패시킨다(orchestration의
    `jobs.standard_score_validation`과 동일한 선례).
    """
    _validate_table(table)
    row_count = count_rows(connection, table)
    if row_count == 0:
        raise GoldAuditValidationFailed(f"no rows found in {table}")

    with tempfile.TemporaryDirectory(prefix="gold-audit-data-docs-") as tmp_dir:
        context = gx.get_context(mode="ephemeral")
        context.add_data_docs_site(
            site_name="gold_audit_site",
            site_config={
                "class_name": "SiteBuilder",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": tmp_dir,
                },
                "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
            },
        )

        datasource = context.data_sources.add_postgres(
            name=f"{table}_datasource", connection_string=config.connection_string
        )

        range_asset = datasource.add_query_asset(
            name=f"{table}_range", query=build_range_query(table)
        )
        range_batch_definition = range_asset.add_batch_definition_whole_table(
            f"{table}_range_batch"
        )
        range_suite = context.suites.add(
            load_expectation_suite(config.range_suite_paths[table])
        )
        range_vdef = context.validation_definitions.add(
            ValidationDefinition(
                name=f"{table}_range_vdef", data=range_batch_definition, suite=range_suite
            )
        )

        summary_asset = datasource.add_query_asset(
            name=f"{table}_summary", query=build_summary_query(table)
        )
        summary_batch_definition = summary_asset.add_batch_definition_whole_table(
            f"{table}_summary_batch"
        )
        summary_suite = context.suites.add(
            load_expectation_suite(config.summary_suite_paths[table])
        )
        summary_vdef = context.validation_definitions.add(
            ValidationDefinition(
                name=f"{table}_summary_vdef", data=summary_batch_definition, suite=summary_suite
            )
        )

        checkpoint = context.checkpoints.add(
            Checkpoint(
                name=f"{table}_audit_checkpoint",
                validation_definitions=[range_vdef, summary_vdef],
                actions=[UpdateDataDocsAction(name="update_data_docs")],
            )
        )
        result = checkpoint.run()

        upload_data_docs_to_s3(
            Path(tmp_dir), config.s3_bucket, f"{S3_PREFIX}/{table}", s3_client
        )

    summary = GoldAuditSummary(table=table, row_count=row_count, success=result.success)
    # 실패해도 추적이 되도록 raise 이전에 남긴다(#406).
    _log_summary(
        config,
        table=table,
        row_count=row_count,
        success=summary.success,
        data_docs_uri=f"s3://{config.s3_bucket}/{S3_PREFIX}/{table}",
    )
    if not summary.success:
        raise GoldAuditValidationFailed(f"gold audit failed for table={table}: {summary}")
    return summary


def _log_summary(
    config: GoldAuditValidationConfig,
    *,
    table: str,
    row_count: int,
    success: bool,
    data_docs_uri: str,
) -> None:
    """무엇을 감사했고 Data Docs가 어디에 올라갔는지 남긴다(#406).

    이 job은 at-rest 감사라 대상 시간대라는 개념이 없다 — 대신 감사 대상 테이블과
    조회한 Postgres를 남긴다. config.connection_string에는 비밀번호가 평문으로
    들어 있으므로 절대 로그에 넣지 않고, host:port/db만 남긴다.
    """
    logger.info(
        "gold audit finished table=%s postgres=%s:%d/%s row_count=%d success=%s "
        "data_docs=%s",
        table,
        config.postgres_host,
        config.postgres_port,
        config.postgres_db,
        row_count,
        success,
        data_docs_uri,
    )
