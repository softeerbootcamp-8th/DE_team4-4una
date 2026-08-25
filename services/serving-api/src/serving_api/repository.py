"""Read comfort scores from PostgreSQL (#160, #226).

`current_segment_comfort_score`(날씨 반영)를 먼저 보고, 행이 없으면
`standard_segment_comfort_score`(날씨 미보정)의 최신 행으로 대신 응답한다.
current에 행이 없는 경우는 두 가지다 — 어느 taxi zone에도 속하지 않아 날씨를
붙일 수 없는 구간, 그리고 아직 그 zone의 날씨를 한 번도 못 받은 경우.

ORM을 두지 않는다 — 저장소가 전반적으로 raw SQL을 쓴다.

커넥션은 호출자가 넘긴다. 풀에서 꺼내는 책임은 FastAPI 의존성에 있고, 이
모듈은 넘겨받은 커넥션에 SQL을 실행해 응답 모델로 바꾸는 일만 한다.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any

import psycopg

from serving_api.schemas import ComfortScore

CURRENT_TABLE = "current_segment_comfort_score"
STANDARD_TABLE = "standard_segment_comfort_score"
VEHICLE_PROFILE_TABLE = "vehicle_profile"

CURRENT_SOURCE = "current"
STANDARD_SOURCE = "standard"

# 두 SELECT가 돌려주는 컬럼 순서. 응답 모델의 필드 순서와 같아야 하며, `source`는
# DB 컬럼이 아니라 어느 테이블에서 왔는지를 나타내므로 여기 포함하지 않는다.
ROW_FIELDS = (
    "segment_id",
    "vehicle_profile_id",
    "comfort_score",
    "vertical_score",
    "longitudinal_score",
    "lateral_score",
    "confidence_score",
    "sample_count",
    "data_period_start",
    "standard_score_as_of",
    "standard_score_version",
    "weather_time",
    "weather_rule_version",
    "calculated_at",
)

# 모델과 어긋난 채로 배포되면 SELECT 순서와 매핑이 조용히 뒤바뀌어 값이 섞인다.
# 컬럼을 추가하고 한쪽만 고치는 실수를 import 시점에 잡는다.
_expected = set(ROW_FIELDS) | {"source"}
if _expected != set(ComfortScore.model_fields):
    raise RuntimeError(
        f"ROW_FIELDS is out of sync with ComfortScore: "
        f"{_expected ^ set(ComfortScore.model_fields)}"
    )

_CURRENT_PROJECTION = ", ".join(ROW_FIELDS)

# standard 테이블은 컬럼 이름이 조금 다르고 날씨 컬럼이 아예 없다. 두 쿼리의 행
# 모양을 맞춰야 매핑을 하나로 유지할 수 있으므로 별칭과 NULL로 채운다.
_STANDARD_PROJECTION = (
    "segment_id, vehicle_profile_id, comfort_score, "
    "vertical_score, longitudinal_score, lateral_score, "
    "confidence_score, sample_count, data_period_start, "
    "score_as_of AS standard_score_as_of, "
    "score_version AS standard_score_version, "
    "NULL::timestamptz AS weather_time, "
    "NULL::text AS weather_rule_version, "
    "calculated_at"
)

CURRENT_SINGLE_SQL = f"""
SELECT {_CURRENT_PROJECTION}
FROM {CURRENT_TABLE}
WHERE segment_id = %s AND vehicle_profile_id = %s
"""

# 구간 수가 늘어도 파라미터는 스칼라 1개 + 배열 1개로 고정된다. IN 목록을
# 문자열로 조립하면 구간 수마다 다른 쿼리가 되어 실행 계획과 prepared statement를
# 재사용할 수 없다.
CURRENT_BATCH_SQL = f"""
SELECT {_CURRENT_PROJECTION}
FROM {CURRENT_TABLE}
WHERE vehicle_profile_id = %s AND segment_id = ANY(%s::text[])
"""

# standard도 (구간, 프로필)당 1행뿐이라(#503, migration 0012) 최신 세대를 고르는
# DISTINCT ON이 필요 없다. PK가 (segment_id, vehicle_profile_id)이므로 위의
# CURRENT_BATCH_SQL과 똑같이 인덱스만 타고 끝난다.
STANDARD_BATCH_SQL = f"""
SELECT {_STANDARD_PROJECTION}
FROM {STANDARD_TABLE}
WHERE vehicle_profile_id = %s AND segment_id = ANY(%s::text[])
"""

# 점수 테이블이 이 테이블을 FK로 참조하므로 같은 DB에 있다. 프로필 목록을
# 서빙 계층에 복사해 두지 않는 이유는, 이미 마이그레이션과 sensor-producer
# 두 곳에 중복 정의되어 있어 세 번째 사본을 만들면 어긋날 곳만 늘기 때문이다.
ACTIVE_VEHICLE_PROFILE_SQL = f"""
SELECT EXISTS (
    SELECT 1
    FROM {VEHICLE_PROFILE_TABLE}
    WHERE vehicle_profile_id = %s AND is_active = TRUE
)
"""


def is_active_vehicle_profile(
    connection: psycopg.Connection, vehicle_profile_id: int
) -> bool:
    """그 프로필이 `vehicle_profile`에 있고 활성인지 알려준다 (#272).

    없는 프로필과 비활성 프로필을 구분하지 않는다 — 둘 다 그 프로필로는 점수를
    낼 수 없다는 같은 결론이고, 호출자가 할 일도 같다.
    """
    with connection.cursor() as cursor:
        cursor.execute(ACTIVE_VEHICLE_PROFILE_SQL, (vehicle_profile_id,))
        row = cursor.fetchone()
    return bool(row[0]) if row is not None else False


def fetch_one(
    connection: psycopg.Connection, segment_id: str, vehicle_profile_id: int
) -> ComfortScore | None:
    """행이 없으면 None을 돌려준다. 404 판단은 HTTP 계층의 몫이다."""
    with connection.cursor() as cursor:
        cursor.execute(CURRENT_SINGLE_SQL, (segment_id, vehicle_profile_id))
        row = cursor.fetchone()
        if row is not None:
            return _to_score(row, CURRENT_SOURCE)

        cursor.execute(STANDARD_BATCH_SQL, (vehicle_profile_id, [segment_id]))
        row = cursor.fetchone()
    return _to_score(row, STANDARD_SOURCE) if row is not None else None


def fetch_many(
    connection: psycopg.Connection, vehicle_profile_id: int, segment_ids: Sequence[str]
) -> list[ComfortScore]:
    """찾은 행만 돌려준다. 빠진 구간을 골라내는 것은 HTTP 계층의 몫이다.

    폴백이 필요한 구간만 두 번째 쿼리로 모아 조회하므로, 왕복은 항상 최대 2회다.
    """
    if not segment_ids:
        return []

    with connection.cursor() as cursor:
        cursor.execute(CURRENT_BATCH_SQL, (vehicle_profile_id, list(segment_ids)))
        scores = [_to_score(row, CURRENT_SOURCE) for row in cursor.fetchall()]

        found = {score.segment_id for score in scores}
        missing = [
            segment_id for segment_id in segment_ids if segment_id not in found
        ]
        if missing:
            cursor.execute(STANDARD_BATCH_SQL, (vehicle_profile_id, missing))
            scores.extend(_to_score(row, STANDARD_SOURCE) for row in cursor.fetchall())
    return scores


def _to_score(row: Iterable[Any], source: str) -> ComfortScore:
    return ComfortScore(**dict(zip(ROW_FIELDS, row, strict=True)), source=source)
