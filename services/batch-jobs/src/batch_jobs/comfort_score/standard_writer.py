"""Write standard_segment_comfort_score results to PostgreSQL (#198).

gold_writer.py와 같은 구조다 — Spark JDBC에 네이티브 UPSERT가 없으므로 staging
테이블에 overwrite로 쓴 뒤 같은 커넥션에서 SQL MERGE(INSERT ... ON CONFLICT)를 한 번
실행하고, staging 보호를 위해 advisory lock을 Spark write보다 먼저 잡는다.

Gold와 다른 점은 두 가지다.
  - 충돌 키가 (segment_id, vehicle_profile_id, score_as_of) 세 컬럼이다. 같은
    score_as_of를 다시 돌리면 그 행이 갱신되고, 다른 score_as_of는 누적된다.
  - advisory lock 키를 따로 쓴다(STANDARD_JOB_STAGING_LOCK_KEY) — 두 job이 서로를
    막을 이유가 없다.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame

from batch_jobs.db_lock_keys import STANDARD_JOB_STAGING_LOCK_KEY

STAGING_TABLE = "standard_segment_comfort_score_staging"
TARGET_TABLE = "standard_segment_comfort_score"

# resources/migrations/0007_finalize_standard_segment_comfort_score.sql의 staging
# DDL과 정확히 일치해야 한다. information_schema.columns.data_type이 실제로
# 반환하는 문자열 그대로다.
EXPECTED_STAGING_COLUMNS = {
    "segment_id": "text",
    "vehicle_profile_id": "integer",
    "score_as_of": "timestamp with time zone",
    "data_period_start": "timestamp with time zone",
    "data_period_end": "timestamp with time zone",
    "vertical_score": "double precision",
    "longitudinal_score": "double precision",
    "lateral_score": "double precision",
    "comfort_score": "double precision",
    "sample_count": "bigint",
    "confidence_score": "double precision",
    "score_version": "text",
    "calculated_at": "timestamp with time zone",
}

_INSERT_COLUMNS = ", ".join(EXPECTED_STAGING_COLUMNS)

_UPDATE_ASSIGNMENTS = ",\n      ".join(
    f"{column} = EXCLUDED.{column}"
    for column in EXPECTED_STAGING_COLUMNS
    # 충돌 키는 갱신 대상이 아니다.
    if column not in {"segment_id", "vehicle_profile_id", "score_as_of"}
)

_MERGE_SQL = f"""
WITH upserted AS (
    INSERT INTO {TARGET_TABLE}
      ({_INSERT_COLUMNS})
    SELECT {_INSERT_COLUMNS}
    FROM {STAGING_TABLE}
    ON CONFLICT (segment_id, vehicle_profile_id, score_as_of) DO UPDATE SET
      {_UPDATE_ASSIGNMENTS}
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


def write_standard_comfort_scores(
    df: DataFrame,
    jdbc_url: str,
    postgres_user: str,
    postgres_password: str,
    connection,
) -> WriteSummary:
    """connection: 대상 Postgres에 대한 DB-API 커넥션(psycopg2 또는 테스트 fake).

    호출자(comfort_score.standard_job)가 connection을 열고 닫는다 — 이 함수는
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
    cursor.execute("SELECT pg_try_advisory_lock(%s)", (STANDARD_JOB_STAGING_LOCK_KEY,))
    acquired = cursor.fetchone()
    if acquired is None or not acquired[0]:
        raise RuntimeError(
            "another standard_segment_comfort_score job run holds the staging lock"
        )


def _release_lock(cursor) -> None:
    cursor.execute("SELECT pg_advisory_unlock(%s)", (STANDARD_JOB_STAGING_LOCK_KEY,))


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
        f"SELECT count(*), count(DISTINCT (segment_id, vehicle_profile_id, score_as_of)) "
        f"FROM {STAGING_TABLE}"
    )
    total, distinct = cursor.fetchone()
    if total != distinct:
        raise ValueError(
            f"{STAGING_TABLE} has {total - distinct} duplicate "
            "(segment_id, vehicle_profile_id, score_as_of) rows — formula.py should "
            "never produce these; refusing to merge"
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
