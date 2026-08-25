# jobs/standard_score_validation.py 테스트 (#495, ADR-0012).

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import unquote, urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from de4_core import (
    ObjectStore,
    StandardGoldManifest,
    standard_manifest_uri,
    standard_snapshot_uri,
    standard_version_uri,
)
from jobs.standard_score_validation import (
    StandardScoreValidationFailed,
    validate_standard_score,
)

AS_OF = datetime(2026, 8, 25, 4, 0, 0, tzinfo=UTC)
VERSION_ID = "0" * 32


def _row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "segment_id": "seg-1",
        "vehicle_profile_id": 1,
        "comfort_score": 72.5,
        "vertical_score": 70.0,
        "longitudinal_score": 75.0,
        "lateral_score": 72.0,
        "score_version": "1.0.0",
    }
    row.update(overrides)
    return row


_SCHEMA = pa.schema(
    [
        ("segment_id", pa.string()),
        ("vehicle_profile_id", pa.int64()),
        ("comfort_score", pa.float64()),
        ("vertical_score", pa.float64()),
        ("longitudinal_score", pa.float64()),
        ("lateral_score", pa.float64()),
        ("score_version", pa.string()),
    ]
)


def _as_path(uri: str) -> Path:
    """join_uri는 로컬 경로에도 file:// URI를 돌려준다 — 파일을 직접 쓰려면 되돌린다."""
    return Path(unquote(urlparse(uri).path))


def _write_snapshot(
    root: str,
    rows: list[dict[str, object]],
    *,
    as_of: datetime = AS_OF,
    manifest_as_of: datetime | None = None,
    schema: pa.Schema | None = None,
) -> None:
    """활성 snapshot 하나를 batch-jobs가 쓰는 것과 같은 배치로 만든다."""
    snapshot_root = standard_snapshot_uri(root, as_of)
    version_uri = standard_version_uri(snapshot_root, VERSION_ID)
    version_dir = _as_path(version_uri)
    version_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(rows, schema=schema), version_dir / "part-0.parquet"
    )

    manifest = StandardGoldManifest(
        score_as_of=manifest_as_of or as_of,
        version_id=VERSION_ID,
        snapshot_uri=version_uri,
        row_count=len(rows),
    )
    ObjectStore().write_bytes(standard_manifest_uri(snapshot_root), manifest.to_json())


class TestValidateStandardScore:
    def test_passes_when_every_score_is_in_range(self, tmp_path) -> None:
        _write_snapshot(str(tmp_path), [_row(), _row(segment_id="seg-2")])

        summary = validate_standard_score(str(tmp_path), AS_OF)

        assert summary.success
        assert summary.row_count == 2

    def test_fails_when_a_direction_score_is_out_of_range(self, tmp_path) -> None:
        _write_snapshot(str(tmp_path), [_row(), _row(vertical_score=150.0)])

        with pytest.raises(StandardScoreValidationFailed, match="validation failed"):
            validate_standard_score(str(tmp_path), AS_OF)

    def test_fails_when_the_snapshot_has_no_rows(self, tmp_path) -> None:
        # 빈 파티션이 조용히 통과하면 하류가 빈 결과를 정상으로 받는다. Spark는 0행이어도
        # 스키마를 남기므로 여기서도 스키마를 준다.
        _write_snapshot(str(tmp_path), [], schema=_SCHEMA)

        with pytest.raises(StandardScoreValidationFailed, match="zero rows"):
            validate_standard_score(str(tmp_path), AS_OF)

    def test_fails_when_no_manifest_exists(self, tmp_path) -> None:
        with pytest.raises(ValueError, match="no manifest found"):
            validate_standard_score(str(tmp_path), AS_OF)

    def test_fails_when_the_manifest_points_at_another_as_of(self, tmp_path) -> None:
        # 이전 실행의 manifest가 남아 있는데 이번 as_of로 착각하면 옛 데이터를 통과시킨다.
        other = datetime(2026, 8, 25, 3, 0, 0, tzinfo=UTC)
        _write_snapshot(str(tmp_path), [_row()], manifest_as_of=other)

        with pytest.raises(ValueError, match="does not match requested as_of"):
            validate_standard_score(str(tmp_path), AS_OF)

    def test_reads_only_the_columns_the_suite_checks(self, tmp_path) -> None:
        # suite가 보지 않는 컬럼이 없어도 검증은 성립해야 한다 — 100만 행을 통째로
        # 메모리에 올리지 않으려고 필요한 컬럼만 읽기 때문이다.
        rows = [_row()]
        for row in rows:
            del row["segment_id"]
            del row["vehicle_profile_id"]
        _write_snapshot(str(tmp_path), rows)

        assert validate_standard_score(str(tmp_path), AS_OF).success
