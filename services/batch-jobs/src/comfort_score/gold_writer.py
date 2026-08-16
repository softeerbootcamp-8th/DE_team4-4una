"""Write segment_comfort_score results to PostgreSQL via a staging table +
SQL MERGE (#129).

Spark JDBC는 네이티브 UPSERT를 지원하지 않으므로, staging 테이블에
overwrite로 쓴 뒤 같은 커넥션에서 SQL MERGE(INSERT ... ON CONFLICT)를 한 번
실행한다. staging 보호를 위해 advisory lock을 Spark write보다 먼저 잡는다
— 잠그지 않은 채 write를 먼저 하면 두 실행이 겹칠 때 "검증한 데이터와
적재하는 데이터가 다른" 레이스가 생기기 때문이다. 자세한 설계 근거는
docs/superpowers/specs/2026-08-16-segment-comfort-score-gold-load-design.md 참고.
"""

from __future__ import annotations

from dataclasses import dataclass

from batch_jobs.db_lock_keys import GOLD_JOB_STAGING_LOCK_KEY
from pyspark.sql import DataFrame

STAGING_TABLE = "segment_comfort_score_staging"
TARGET_TABLE = "segment_comfort_score"

# db/migrations/0002_create_segment_comfort_score.sql의 staging DDL과
# 정확히 일치해야 한다. information_schema.columns.data_type이 실제로
# 반환하는 문자열 그대로다.
EXPECTED_STAGING_COLUMNS = {
    "segment_id": "text",
    "vehicle_profile_id": "integer",
    "comfort_score": "double precision",
    "confidence_score": "double precision",
    "sample_count": "bigint",
    "score_version": "text",
    "calculated_at": "timestamp with time zone",
}

_MERGE_SQL = f"""
WITH upserted AS (
    INSERT INTO {TARGET_TABLE}
      (segment_id, vehicle_profile_id, comfort_score, confidence_score,
       sample_count, score_version, calculated_at)
    SELECT segment_id, vehicle_profile_id, comfort_score, confidence_score,
           sample_count, score_version, calculated_at
    FROM {STAGING_TABLE}
    ON CONFLICT (segment_id, vehicle_profile_id) DO UPDATE SET
      comfort_score = EXCLUDED.comfort_score,
      confidence_score = EXCLUDED.confidence_score,
      sample_count = EXCLUDED.sample_count,
      score_version = EXCLUDED.score_version,
      calculated_at = EXCLUDED.calculated_at
    RETURNING (xmax = 0) AS inserted
)
SELECT count(*) FILTER (WHERE inserted)     AS inserted_count,
       count(*) FILTER (WHERE NOT inserted) AS updated_count
FROM upserted;
"""


@dataclass(frozen=True, slots=True)
class WriteSummary:
    staging_count: int
    inserted_count: int
    updated_count: int


def write_segment_comfort_scores(
    df: DataFrame,
    jdbc_url: str,
    postgres_user: str,
    postgres_password: str,
    connection,
) -> WriteSummary:
    """connection: 대상 Postgres에 대한 DB-API 커넥션(psycopg2 또는 테스트 fake).

    호출자(comfort_score.gold_job)가 connection을 열고 닫는다 — 이 함수는
    commit/rollback만 책임진다.
    """
    cursor = connection.cursor()
    try:
        _acquire_lock(cursor)
        _validate_staging_table_shape(cursor)
        _write_staging(df, jdbc_url, postgres_user, postgres_password)
        _validate_no_duplicates_or_nan(cursor)
        inserted_count, updated_count = _merge(cursor)
        connection.commit()
        _truncate_staging(cursor)
        connection.commit()
        return WriteSummary(
            staging_count=inserted_count + updated_count,
            inserted_count=inserted_count,
            updated_count=updated_count,
        )
    except Exception:
        connection.rollback()
        raise
    finally:
        _release_lock(cursor)
        cursor.close()


def _acquire_lock(cursor) -> None:
    cursor.execute("SELECT pg_try_advisory_lock(%s)", (GOLD_JOB_STAGING_LOCK_KEY,))
    acquired = cursor.fetchone()
    if acquired is None or not acquired[0]:
        raise RuntimeError(
            "another segment_comfort_score gold job run holds the staging lock"
        )


def _release_lock(cursor) -> None:
    cursor.execute("SELECT pg_advisory_unlock(%s)", (GOLD_JOB_STAGING_LOCK_KEY,))


def _validate_staging_table_shape(cursor) -> None:
    cursor.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = %s",
        (STAGING_TABLE,),
    )
    actual = dict(cursor.fetchall())
    if not actual:
        raise RuntimeError(f"{STAGING_TABLE} does not exist — run `make migrate` first")
    mismatched = {
        column: (expected, actual.get(column))
        for column, expected in EXPECTED_STAGING_COLUMNS.items()
        if actual.get(column) != expected
    }
    if mismatched:
        raise RuntimeError(
            f"{STAGING_TABLE} schema mismatch (expected vs actual): {mismatched} "
            "— run `make migrate` first"
        )


def _write_staging(
    df: DataFrame, jdbc_url: str, postgres_user: str, postgres_password: str
) -> None:
    (
        df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", STAGING_TABLE)
        .option("user", postgres_user)
        .option("password", postgres_password)
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )


def _validate_no_duplicates_or_nan(cursor) -> None:
    cursor.execute(
        f"SELECT count(*), count(DISTINCT (segment_id, vehicle_profile_id)) "
        f"FROM {STAGING_TABLE}"
    )
    total, distinct = cursor.fetchone()
    if total != distinct:
        raise ValueError(
            f"{STAGING_TABLE} has {total - distinct} duplicate "
            "(segment_id, vehicle_profile_id) rows — formula.py should never "
            "produce these; refusing to merge"
        )
    cursor.execute(
        f"SELECT count(*) FROM {STAGING_TABLE} "
        "WHERE comfort_score = 'NaN' OR confidence_score = 'NaN' "
        "OR comfort_score = 'Infinity' OR confidence_score = 'Infinity'"
    )
    (bad_count,) = cursor.fetchone()
    if bad_count:
        raise ValueError(
            f"{STAGING_TABLE} has {bad_count} row(s) with NaN/Infinity scores"
        )


def _merge(cursor) -> tuple[int, int]:
    cursor.execute(_MERGE_SQL)
    inserted_count, updated_count = cursor.fetchone()
    return inserted_count, updated_count


def _truncate_staging(cursor) -> None:
    cursor.execute(f"TRUNCATE {STAGING_TABLE}")
