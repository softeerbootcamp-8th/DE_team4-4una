"""standard_writer의 MERGE가 (구간, 프로필) 하나를 제자리 갱신하는지 검증한다 (#503).

migration 0012가 PK에서 score_as_of를 빼면서 이 테이블은 세대를 누적하지 않는
서빙 스토어가 됐다. 충돌 키가 3컬럼으로 되돌아가면 매시 실행이 다시 전량을 새
행으로 INSERT하게 되는데, 그건 테스트 없이는 조용히 지나간다 — 실행 자체는
성공하고 행 수만 늘기 때문이다.
"""

from __future__ import annotations

from batch_jobs.comfort_score.standard_writer import (
    _MERGE_SQL,
    _validate_no_duplicates_or_nan,
)


def test_merge_conflicts_on_segment_and_vehicle_profile_only() -> None:
    assert "ON CONFLICT (segment_id, vehicle_profile_id) DO UPDATE SET" in _MERGE_SQL


def test_merge_updates_score_as_of() -> None:
    # 충돌 키에서 빠진 이상 갱신 대상이어야 한다. 빠뜨리면 행은 새 값으로 덮이는데
    # score_as_of만 첫 적재 시점에 얼어붙어, 어느 세대인지 알 수 없게 된다.
    assert "score_as_of = EXCLUDED.score_as_of" in _MERGE_SQL


def test_merge_never_updates_the_conflict_key() -> None:
    assert "segment_id = EXCLUDED.segment_id" not in _MERGE_SQL
    assert "vehicle_profile_id = EXCLUDED.vehicle_profile_id" not in _MERGE_SQL


class _RecordingCursor:
    def __init__(self, counts: tuple[int, int]) -> None:
        self.counts = counts
        self.executed: list[str] = []

    def execute(self, sql: str, params: tuple = ()) -> None:
        del params
        self.executed.append(" ".join(sql.split()))

    def fetchone(self):
        if "count(DISTINCT" in self.executed[-1]:
            return self.counts
        return (0,)


def test_staging_duplicate_check_uses_the_merge_conflict_key() -> None:
    """검증 키가 충돌 키보다 넓으면 MERGE에서 터진다.

    staging에 같은 (구간, 프로필)이 두 번 들어와도 score_as_of가 다르면 3컬럼
    검증은 통과하고, 그 뒤 ON CONFLICT가 "한 명령에서 같은 행을 두 번 갱신할 수
    없다"며 실패한다. 검증이 먼저 잡아야 원인이 드러난다.
    """
    cursor = _RecordingCursor((10, 10))

    _validate_no_duplicates_or_nan(cursor)

    assert "count(DISTINCT (segment_id, vehicle_profile_id))" in cursor.executed[0]
