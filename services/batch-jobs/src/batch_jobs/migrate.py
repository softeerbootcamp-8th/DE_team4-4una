"""Apply packaged SQL migrations to Postgres, tracked in schema_migrations (#129)."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from batch_jobs.db_lock_keys import MIGRATION_LOCK_KEY
from batch_jobs.resources import RESOURCE_DIR

DEFAULT_MIGRATIONS_DIR = RESOURCE_DIR / "migrations"


@dataclass(frozen=True, slots=True)
class MigrationConfig:
    migrations_dir: Path
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> MigrationConfig:
        source = env if env is not None else os.environ
        return cls(
            migrations_dir=Path(
                source.get("DB_MIGRATIONS_DIR") or DEFAULT_MIGRATIONS_DIR
            ),
            postgres_host=_require(source, "POSTGRES_HOST"),
            postgres_port=int(_require(source, "POSTGRES_PORT")),
            postgres_db=_require(source, "POSTGRES_DB"),
            postgres_user=_require(source, "POSTGRES_USER"),
            postgres_password=_require(source, "POSTGRES_PASSWORD"),
        )


def _require(source: Mapping[str, str], key: str) -> str:
    value = source.get(key)
    if not value:
        raise ValueError(f"{key} must be set")
    return value


@dataclass(frozen=True, slots=True)
class MigrationResult:
    applied: tuple[str, ...]
    skipped: tuple[str, ...]


def run_migrations(migrations_dir: Path, connection) -> MigrationResult:
    """migrations_dir의 *.sql을 파일명 순으로 적용한다.

    connection: psycopg2 커넥션(또는 테스트용 fake). 커밋/롤백 책임은 이
    함수가 진다 — 호출자는 connection.close()만 하면 된다.
    """
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT pg_try_advisory_lock(%s)", (MIGRATION_LOCK_KEY,))
        acquired = cursor.fetchone()
        if acquired is None or not acquired[0]:
            raise RuntimeError("another migration run holds the migration lock")

        cursor.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            "filename TEXT PRIMARY KEY, checksum TEXT NOT NULL, "
            "applied_at TIMESTAMPTZ NOT NULL)"
        )
        connection.commit()

        applied: list[str] = []
        skipped: list[str] = []
        for path in sorted(migrations_dir.glob("*.sql")):
            checksum = hashlib.sha256(path.read_bytes()).hexdigest()
            cursor.execute(
                "SELECT checksum FROM schema_migrations WHERE filename = %s",
                (path.name,),
            )
            existing = cursor.fetchone()
            if existing is None:
                cursor.execute(path.read_text())
                cursor.execute(
                    "INSERT INTO schema_migrations (filename, checksum, applied_at) "
                    "VALUES (%s, %s, now())",
                    (path.name, checksum),
                )
                connection.commit()
                applied.append(path.name)
            elif existing[0] != checksum:
                raise ValueError(
                    f"{path.name} has been modified after being applied "
                    "(checksum mismatch) — migrations must never be edited "
                    "after being applied; add a new migration file instead"
                )
            else:
                skipped.append(path.name)
        return MigrationResult(tuple(applied), tuple(skipped))
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.execute("SELECT pg_advisory_unlock(%s)", (MIGRATION_LOCK_KEY,))
        cursor.close()
