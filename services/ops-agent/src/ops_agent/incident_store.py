# fingerprint를 키로, cooldown 동안 같은 incident에 remediation을 다시 시도하지 않게 막는다(#447).

from __future__ import annotations

import sqlite3
import time
from collections.abc import Callable

_SCHEMA = """
CREATE TABLE IF NOT EXISTS remediation_attempts (
    fingerprint TEXT PRIMARY KEY,
    attempted_at REAL NOT NULL,
    action TEXT NOT NULL
)
"""


class IncidentStore:
    def __init__(self, path: str, *, now: Callable[[], float] | None = None) -> None:
        # FastAPI가 sync 핸들러를 스레드풀에서 돌려 요청마다 다른 스레드가 이 connection을 쓸 수 있어 check_same_thread=False로 둔다.
        self._connection = sqlite3.connect(path, check_same_thread=False)
        self._connection.execute(_SCHEMA)
        self._connection.commit()
        self._now = now or time.time

    def should_attempt(self, fingerprint: str, cooldown_seconds: float) -> bool:
        row = self._connection.execute(
            "SELECT attempted_at FROM remediation_attempts WHERE fingerprint = ?",
            (fingerprint,),
        ).fetchone()
        if row is None:
            return True
        (attempted_at,) = row
        return (self._now() - attempted_at) >= cooldown_seconds

    def record_attempt(self, fingerprint: str, action: str) -> None:
        self._connection.execute(
            """
            INSERT INTO remediation_attempts (fingerprint, attempted_at, action)
            VALUES (?, ?, ?)
            ON CONFLICT(fingerprint) DO UPDATE SET
                attempted_at = excluded.attempted_at,
                action = excluded.action
            """,
            (fingerprint, self._now(), action),
        )
        self._connection.commit()

    def close(self) -> None:
        self._connection.close()
