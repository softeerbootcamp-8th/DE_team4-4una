"""Tests for de4_core/gold_snapshot.py (#495, ADR-0012).

batch-jobs가 쓰고 orchestration이 읽는 서비스 간 계약이므로, 경로 규칙과 manifest
스키마 양쪽을 여기서 고정한다.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from de4_core.gold_snapshot import (
    StandardGoldManifest,
    standard_manifest_uri,
    standard_snapshot_uri,
    standard_version_uri,
)

AS_OF = datetime(2026, 8, 25, 4, 0, 0, tzinfo=UTC)
ROOT = "s3://bucket/gold/standard_segment_comfort_score"
SNAPSHOT_ROOT = f"{ROOT}/score_as_of_date=2026-08-25/score_as_of=2026-08-25T04-00-00Z"
VERSION_ID = "0123456789abcdef0123456789abcdef"


def _payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "score_as_of": AS_OF.isoformat(),
        "version_id": VERSION_ID,
        "snapshot_uri": f"{SNAPSHOT_ROOT}/versions/{VERSION_ID}",
        "row_count": 997332,
    }
    payload.update(overrides)
    return payload


class TestSnapshotUris:
    def test_snapshot_uri_partitions_by_date_then_timestamp(self) -> None:
        assert standard_snapshot_uri(ROOT, AS_OF) == SNAPSHOT_ROOT

    def test_snapshot_uri_replaces_colons_so_the_key_is_path_safe(self) -> None:
        # ':'는 S3 키와 로컬 경로 양쪽에서 다루기 번거로워 '-'로 치환한다.
        assert ":" not in standard_snapshot_uri(ROOT, AS_OF).rsplit("/", 1)[-1]

    def test_snapshot_uri_rejects_a_naive_as_of(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            # naive datetime을 거부하는지가 이 테스트의 대상이라 DTZ001은 의도적이다.
            standard_snapshot_uri(ROOT, datetime(2026, 8, 25, 4, 0, 0))  # noqa: DTZ001

    def test_version_uri_nests_under_the_versions_directory(self) -> None:
        assert (
            standard_version_uri(SNAPSHOT_ROOT, VERSION_ID)
            == f"{SNAPSHOT_ROOT}/versions/{VERSION_ID}"
        )

    def test_manifest_uri_sits_beside_the_versions_directory(self) -> None:
        assert standard_manifest_uri(SNAPSHOT_ROOT) == f"{SNAPSHOT_ROOT}/manifest.json"

    def test_two_as_ofs_in_the_same_hour_window_do_not_collide(self) -> None:
        other = datetime(2026, 8, 25, 5, 0, 0, tzinfo=UTC)
        assert standard_snapshot_uri(ROOT, AS_OF) != standard_snapshot_uri(ROOT, other)


class TestStandardGoldManifest:
    def test_accepts_a_well_formed_manifest(self) -> None:
        manifest = StandardGoldManifest.from_json(
            json.dumps(_payload()).encode(), snapshot_root_uri=SNAPSHOT_ROOT
        )

        assert manifest.score_as_of == AS_OF
        assert manifest.version_id == VERSION_ID
        assert manifest.row_count == 997332

    def test_round_trips_through_json(self) -> None:
        original = StandardGoldManifest.from_json(
            json.dumps(_payload()).encode(), snapshot_root_uri=SNAPSHOT_ROOT
        )

        restored = StandardGoldManifest.from_json(
            original.to_json(), snapshot_root_uri=SNAPSHOT_ROOT
        )

        assert restored == original

    def test_rejects_a_payload_that_is_not_an_object(self) -> None:
        with pytest.raises(TypeError, match="must be a JSON object"):
            StandardGoldManifest.from_json(b"[]", snapshot_root_uri=SNAPSHOT_ROOT)

    def test_rejects_bytes_that_are_not_json(self) -> None:
        with pytest.raises(ValueError, match="not valid JSON"):
            StandardGoldManifest.from_json(b"{oops", snapshot_root_uri=SNAPSHOT_ROOT)

    @pytest.mark.parametrize(
        "missing", ["score_as_of", "version_id", "snapshot_uri", "row_count"]
    )
    def test_rejects_a_manifest_missing_a_required_key(self, missing: str) -> None:
        payload = _payload()
        del payload[missing]

        with pytest.raises(ValueError, match=missing):
            StandardGoldManifest.from_json(
                json.dumps(payload).encode(), snapshot_root_uri=SNAPSHOT_ROOT
            )

    def test_rejects_a_score_as_of_that_is_not_a_timestamp(self) -> None:
        with pytest.raises(ValueError, match="ISO timestamp"):
            StandardGoldManifest.from_json(
                json.dumps(_payload(score_as_of="yesterday")).encode(),
                snapshot_root_uri=SNAPSHOT_ROOT,
            )

    def test_rejects_a_naive_score_as_of(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            StandardGoldManifest.from_json(
                json.dumps(_payload(score_as_of="2026-08-25T04:00:00")).encode(),
                snapshot_root_uri=SNAPSHOT_ROOT,
            )

    @pytest.mark.parametrize("empty", ["", None])
    def test_rejects_an_empty_version_id(self, empty: object) -> None:
        with pytest.raises(ValueError, match="version_id"):
            StandardGoldManifest.from_json(
                json.dumps(_payload(version_id=empty)).encode(),
                snapshot_root_uri=SNAPSHOT_ROOT,
            )

    def test_rejects_a_negative_row_count(self) -> None:
        with pytest.raises(ValueError, match="row_count"):
            StandardGoldManifest.from_json(
                json.dumps(_payload(row_count=-1)).encode(),
                snapshot_root_uri=SNAPSHOT_ROOT,
            )

    def test_rejects_a_boolean_row_count(self) -> None:
        # bool은 int의 하위 타입이라 isinstance 검사만으로는 통과해버린다.
        with pytest.raises(ValueError, match="row_count"):
            StandardGoldManifest.from_json(
                json.dumps(_payload(row_count=True)).encode(),
                snapshot_root_uri=SNAPSHOT_ROOT,
            )

    def test_rejects_a_snapshot_uri_that_does_not_match_the_version_id(self) -> None:
        # snapshot_uri는 파생값이 아니라 저장된 값이라 손상·오타로 어긋날 수 있다.
        with pytest.raises(ValueError, match="does not match"):
            StandardGoldManifest.from_json(
                json.dumps(
                    _payload(snapshot_uri=f"{SNAPSHOT_ROOT}/versions/deadbeef")
                ).encode(),
                snapshot_root_uri=SNAPSHOT_ROOT,
            )
