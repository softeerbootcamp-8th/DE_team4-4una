"""Read `segment_comfort_score` rows from PostgreSQL (#160).

ORM을 두지 않는다 — 저장소가 전반적으로 raw SQL을 쓰고, 조회가 두 개뿐이라
매핑 계층을 얹을 이유가 없다.

커넥션은 호출자가 넘긴다. 풀에서 꺼내는 책임은 FastAPI 의존성에 있고, 이
모듈은 넘겨받은 커넥션에 SQL을 실행해 응답 모델로 바꾸는 일만 한다.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any

import psycopg

from serving_api.schemas import ComfortScore

TABLE = "segment_comfort_score"

# SELECT 순서와 모델 매핑이 어긋나면 값이 조용히 뒤바뀌므로 한 곳에서만 정의한다.
COLUMNS = (
    "segment_id",
    "vehicle_profile_id",
    "data_period_start",
    "data_period_end",
    "comfort_score",
    "sample_count",
    "confidence_score",
    "score_version",
    "calculated_at",
)

SINGLE_SQL = f"""
SELECT {", ".join(COLUMNS)}
FROM {TABLE}
WHERE segment_id = %s AND vehicle_profile_id = %s
"""

# 구간 수가 늘어도 파라미터는 스칼라 1개 + 배열 1개로 고정된다. IN 목록을
# 문자열로 조립하면 구간 수마다 다른 쿼리가 되어 실행 계획과 prepared statement를
# 재사용할 수 없다.
BATCH_SQL = f"""
SELECT {", ".join(COLUMNS)}
FROM {TABLE}
WHERE vehicle_profile_id = %s AND segment_id = ANY(%s::text[])
"""


def fetch_one(
    connection: psycopg.Connection, segment_id: str, vehicle_profile_id: int
) -> ComfortScore | None:
    """행이 없으면 None을 돌려준다. 404 판단은 HTTP 계층의 몫이다."""
    with connection.cursor() as cursor:
        cursor.execute(SINGLE_SQL, (segment_id, vehicle_profile_id))
        row = cursor.fetchone()
    return _to_score(row) if row is not None else None


def fetch_many(
    connection: psycopg.Connection, vehicle_profile_id: int, segment_ids: Sequence[str]
) -> list[ComfortScore]:
    """찾은 행만 돌려준다. 빠진 구간을 골라내는 것은 HTTP 계층의 몫이다."""
    if not segment_ids:
        return []
    with connection.cursor() as cursor:
        cursor.execute(BATCH_SQL, (vehicle_profile_id, list(segment_ids)))
        rows = cursor.fetchall()
    return [_to_score(row) for row in rows]


def _to_score(row: Iterable[Any]) -> ComfortScore:
    return ComfortScore(**dict(zip(COLUMNS, row, strict=True)))
