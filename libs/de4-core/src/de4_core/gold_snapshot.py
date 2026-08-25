"""Addressing and manifest contract for the standard Gold snapshot (#495, ADR-0012).

batch-jobs가 `score_as_of`별 snapshot을 쓰고 orchestration이 그것을 검증하려고
읽으므로, 경로 규칙과 manifest 스키마는 어느 한 서비스가 소유할 수 없는 서비스 간
계약이다(AGENTS.md). `RoadEnvironmentManifest`가 같은 이유로 이미 de4-core에 있다.

여기에는 "어디에 있고 무엇을 담는가"만 둔다. snapshot을 안전하게 *쓰는* 절차 —
버전 경로 생성, read-back 검증, 그 뒤에야 manifest를 전환하는 순서(#343) — 는
batch-jobs가 계속 소유한다. 읽는 쪽도 각 서비스가 자기 코드로 읽는다
(`orchestration/jobs/road_environment.py`와 같은 방식으로, 이 모듈은 I/O를 하지
않는다).

각 `score_as_of` 루트 아래 구조:

    score_as_of_date=<date>/score_as_of=<timestamp>/
      versions/<version_id>/part-....parquet   # 매 실행마다 새로 생기는 불변 snapshot
      manifest.json                             # 현재 활성 version을 가리키는 포인터
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime

from de4_core.storage import join_uri

_VERSIONS_DIRNAME = "versions"
_MANIFEST_FILENAME = "manifest.json"


def standard_snapshot_uri(output_root: str, as_of: datetime) -> str:
    """as_of 전용 루트 경로. `versions/`와 `manifest.json`이 이 아래에 함께 있다."""
    _require_aware(as_of, "as_of")
    return join_uri(
        output_root,
        f"score_as_of_date={as_of.date().isoformat()}",
        f"score_as_of={_as_of_path_segment(as_of)}",
    )


def standard_version_uri(snapshot_root_uri: str, version_id: str) -> str:
    return join_uri(snapshot_root_uri, _VERSIONS_DIRNAME, version_id)


def standard_manifest_uri(snapshot_root_uri: str) -> str:
    return join_uri(snapshot_root_uri, _MANIFEST_FILENAME)


@dataclass(frozen=True, slots=True)
class StandardGoldManifest:
    """Pointer to the active snapshot version for one `score_as_of`."""

    score_as_of: datetime
    version_id: str
    snapshot_uri: str
    row_count: int

    def __post_init__(self) -> None:
        # 검증을 __post_init__에 두면 JSON을 거치지 않고 직접 만든 manifest에도
        # 같은 규칙이 걸린다 — 쓰는 쪽(batch-jobs)과 읽는 쪽이 같은 계약을 본다.
        _require_aware(self.score_as_of, "manifest score_as_of")
        if not isinstance(self.version_id, str) or not self.version_id:
            raise ValueError("manifest version_id must be a non-empty string")
        if not isinstance(self.snapshot_uri, str) or not self.snapshot_uri:
            raise ValueError("manifest snapshot_uri must be a non-empty string")
        # bool은 int의 하위 타입이라 isinstance만으로는 True가 통과해버린다.
        if (
            not isinstance(self.row_count, int)
            or isinstance(self.row_count, bool)
            or self.row_count < 0
        ):
            raise ValueError("manifest row_count must be a non-negative integer")

    @classmethod
    def from_json(cls, raw: bytes, *, snapshot_root_uri: str) -> StandardGoldManifest:
        """Parse a manifest payload written under `snapshot_root_uri`.

        오류 메시지에는 manifest 경로를 접두사로 붙인다 — 어느 파일이 잘못됐는지
        로그만 보고 알 수 있어야 한다. 경로는 snapshot_root_uri에서 직접 계산하므로
        호출자가 따로 넘기지 않는다.
        """
        where = standard_manifest_uri(snapshot_root_uri)
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as error:
            raise ValueError(f"{where}: manifest is not valid JSON") from error

        if not isinstance(payload, dict):
            raise TypeError(f"{where}: manifest must be a JSON object")

        missing = [key for key in _REQUIRED_KEYS if key not in payload]
        if missing:
            raise ValueError(
                f"{where}: manifest is missing required key(s): {', '.join(missing)}"
            )

        try:
            score_as_of = datetime.fromisoformat(payload["score_as_of"])
        except (TypeError, ValueError) as error:
            raise ValueError(
                f"{where}: manifest score_as_of is not a valid ISO timestamp"
            ) from error

        try:
            manifest = cls(
                score_as_of=score_as_of,
                version_id=payload["version_id"],
                snapshot_uri=payload["snapshot_uri"],
                row_count=payload["row_count"],
            )
        except ValueError as error:
            raise ValueError(f"{where}: {error}") from error

        # snapshot_uri는 파생값이 아니라 저장된 값이라 손상·오타로 어긋날 수 있다 —
        # version_id가 실제로 가리키는 경로와 대조한다.
        expected = standard_version_uri(snapshot_root_uri, manifest.version_id)
        if manifest.snapshot_uri != expected:
            raise ValueError(
                f"{where}: manifest snapshot_uri {manifest.snapshot_uri!r} does not "
                f"match version_id {manifest.version_id!r} (expected {expected!r})"
            )
        return manifest

    def to_json(self) -> bytes:
        return json.dumps(
            {
                "score_as_of": self.score_as_of.isoformat(),
                "version_id": self.version_id,
                "snapshot_uri": self.snapshot_uri,
                "row_count": self.row_count,
            }
        ).encode()


_REQUIRED_KEYS = ("score_as_of", "version_id", "snapshot_uri", "row_count")


def _as_of_path_segment(as_of: datetime) -> str:
    # ':'는 S3 키/로컬 경로 양쪽에서 다루기 번거로우니 '-'로 치환한다.
    return as_of.strftime("%Y-%m-%dT%H-%M-%SZ")


def _require_aware(value: datetime, field_name: str) -> None:
    # naive datetime을 그대로 strftime하면 조용히 엉뚱한 경로가 나온다 — 쓰는 쪽과
    # 읽는 쪽이 다른 경로를 가리키게 되므로 경로를 만드는 자리에서 막는다.
    if value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
