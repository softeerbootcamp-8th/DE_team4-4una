# 조치 이력을 append-only로 남기고, 그 이력으로 cooldown을 판정한다(#546). 예전 remediation_attempts 테이블은 fingerprint당 1건만 덮어써 "지난주에 몇 번 재시작했나"를 알 수 없었다.

from __future__ import annotations

import sqlite3
import time
from collections.abc import Callable

_SCHEMA = """
CREATE TABLE IF NOT EXISTS remediation_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fingerprint TEXT NOT NULL,
    alertname TEXT NOT NULL,
    action TEXT NOT NULL,
    attempted_at REAL NOT NULL,
    succeeded INTEGER,
    recovered INTEGER
)
"""

_INDEX = """
CREATE INDEX IF NOT EXISTS idx_remediation_events_fingerprint_time
    ON remediation_events (fingerprint, attempted_at)
"""


class IncidentStore:
    def __init__(self, path: str, *, now: Callable[[], float] | None = None) -> None:
        # FastAPI가 sync 핸들러를 스레드풀에서 돌려 요청마다 다른 스레드가 이 connection을 쓸 수 있어 check_same_thread=False로 둔다.
        self._connection = sqlite3.connect(path, check_same_thread=False)
        self._connection.execute(_SCHEMA)
        self._connection.execute(_INDEX)
        self._connection.commit()
        self._now = now or time.time

    def should_attempt(self, fingerprint: str, cooldown_seconds: float) -> bool:
        row = self._connection.execute(
            "SELECT MAX(attempted_at) FROM remediation_events WHERE fingerprint = ?",
            (fingerprint,),
        ).fetchone()
        latest = row[0] if row else None
        if latest is None:
            return True
        return (self._now() - latest) >= cooldown_seconds

    def record_attempt(self, fingerprint: str, alertname: str, action: str) -> int:
        # 조치를 실행하기 "전에" 행을 넣는다 — 도중에 죽어도 cooldown이 소진된 상태로 남아 재기동 후 같은 incident를 무한히 다시 건드리지 않는다.
        cursor = self._connection.execute(
            """
            INSERT INTO remediation_events (fingerprint, alertname, action, attempted_at)
            VALUES (?, ?, ?, ?)
            """,
            (fingerprint, alertname, action, self._now()),
        )
        self._connection.commit()
        return int(cursor.lastrowid)

    def record_outcome(
        self, event_id: int, *, succeeded: bool, recovered: bool | None
    ) -> None:
        self._connection.execute(
            "UPDATE remediation_events SET succeeded = ?, recovered = ? WHERE id = ?",
            (int(succeeded), None if recovered is None else int(recovered), event_id),
        )
        self._connection.commit()

    def count_recent(self, fingerprint: str, within_seconds: float) -> int:
        (count,) = self._connection.execute(
            "SELECT COUNT(*) FROM remediation_events WHERE fingerprint = ? AND attempted_at >= ?",
            (fingerprint, self._now() - within_seconds),
        ).fetchone()
        return int(count)

    def read_event(self, event_id: int) -> dict:
        row = self._connection.execute(
            """
            SELECT fingerprint, alertname, action, succeeded, recovered
            FROM remediation_events WHERE id = ?
            """,
            (event_id,),
        ).fetchone()
        fingerprint, alertname, action, succeeded, recovered = row
        return {
            "fingerprint": fingerprint,
            "alertname": alertname,
            "action": action,
            # succeeded/recovered가 NULL이면 "결과를 확인하지 못함"이다 — False와 구분해야 한다.
            "succeeded": None if succeeded is None else bool(succeeded),
            "recovered": None if recovered is None else bool(recovered),
        }

    def close(self) -> None:
        self._connection.close()
