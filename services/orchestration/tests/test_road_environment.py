# jobs/road_environment.py 테스트 (#402).

from __future__ import annotations

import json
import os
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from de4_core import DataArtifact, RoadEnvironmentManifest, SourceSnapshot
from jobs.road_environment import (
    resolve_active_road_snapshot_date,
    resolve_road_snapshot_date_for_month,
)

CHECKSUM = "a" * 64


def _manifest(
    road_snapshot_date: date,
    reference_date: date = date(2026, 8, 1),
    build_id: str = "build-1",
) -> RoadEnvironmentManifest:
    return RoadEnvironmentManifest(
        schema_version="1",
        environment_id=f"nyc-{reference_date:%Y%m%d}-{build_id}",
        reference_date=reference_date,
        road_snapshot_date=road_snapshot_date,
        build_id=build_id,
        created_at=datetime(2026, 8, 2, tzinfo=UTC),
        mapping_version="mapping-v1",
        status="READY",
        artifacts=(
            DataArtifact(
                "simulation_road_environment",
                "s3://bucket/environment.parquet",
                "application/vnd.apache.parquet",
                CHECKSUM,
                10,
                1,
            ),
            DataArtifact(
                "taxi_zone",
                "s3://bucket/taxi.parquet",
                "application/vnd.apache.parquet",
                CHECKSUM,
                10,
                1,
            ),
        ),
        sources=(
            SourceSnapshot(
                "nyc_lion",
                "snapshot-1",
                "https://example.test/lion",
                "s3://bucket/lion.geojson",
                datetime(2026, 8, 2, tzinfo=UTC),
                "2026-08-01",
                "geojson",
                CHECKSUM,
                CHECKSUM,
                10,
                1,
                "build-1",
            ),
        ),
        quality={"status": "PASSED"},
    )


def _write_active_environment(tmp_path: Path, road_snapshot_date: date) -> str:
    """#389가 build-road-environment로 실제 publish하는 것과 같은 pointer/manifest
    구조를 tmp_path 아래에 만든다(services/batch-jobs/tests/test_standard_job.py의
    _write_universe_environment과 같은 관례)."""
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_bytes(_manifest(road_snapshot_date).to_json())

    pointer_dir = tmp_path / "prepared" / "simulation_environment"
    pointer_dir.mkdir(parents=True)
    (pointer_dir / "active.json").write_text(
        json.dumps({"manifest_uri": manifest_path.resolve().as_uri()})
    )
    return str(tmp_path)


def test_resolves_road_snapshot_date_from_the_active_manifest(tmp_path):
    road_environment_uri = _write_active_environment(tmp_path, date(2026, 8, 20))

    assert resolve_active_road_snapshot_date(road_environment_uri) == date(2026, 8, 20)


def test_raises_when_the_active_pointer_has_no_manifest_uri(tmp_path):
    pointer_dir = tmp_path / "prepared" / "simulation_environment"
    pointer_dir.mkdir(parents=True)
    (pointer_dir / "active.json").write_text(json.dumps({}))

    with pytest.raises(ValueError, match="active pointer has no manifest_uri"):
        resolve_active_road_snapshot_date(str(tmp_path))


# --- 월 기반 조회 (#540) -------------------------------------------------


def _write_build(
    root: Path,
    reference_date: date,
    build_id: str,
    road_snapshot_date: date,
    mtime: float | None = None,
) -> Path:
    """build-road-environment가 publish하는 것과 같은 파티션 경로에 manifest를 쓴다.

    `prepared/simulation_environment/reference_date=<d>/build_id=<id>/manifest.json`
    (services/batch-jobs/src/batch_jobs/pipeline.py:78-81).
    """
    build_dir = (
        root
        / "prepared"
        / "simulation_environment"
        / f"reference_date={reference_date.isoformat()}"
        / f"build_id={build_id}"
    )
    build_dir.mkdir(parents=True)
    manifest_path = build_dir / "manifest.json"
    manifest_path.write_bytes(
        _manifest(road_snapshot_date, reference_date, build_id).to_json()
    )
    if mtime is not None:
        os.utime(manifest_path, (mtime, mtime))
    return manifest_path


def test_picks_the_build_whose_reference_date_falls_in_the_target_month(tmp_path):
    _write_build(tmp_path, date(2026, 7, 1), "build-jul", date(2026, 6, 30))
    _write_build(tmp_path, date(2026, 8, 1), "build-aug", date(2026, 7, 31))

    resolved = resolve_road_snapshot_date_for_month(str(tmp_path), date(2026, 8, 14))

    assert resolved == date(2026, 7, 31)


def test_same_month_prefers_the_latest_reference_date(tmp_path):
    _write_build(tmp_path, date(2026, 8, 1), "build-early", date(2026, 7, 31))
    _write_build(tmp_path, date(2026, 8, 20), "build-late", date(2026, 8, 19))

    resolved = resolve_road_snapshot_date_for_month(str(tmp_path), date(2026, 8, 1))

    assert resolved == date(2026, 8, 19)


def test_same_reference_date_prefers_the_most_recently_written_build(tmp_path):
    # build_id는 임의의 path-safe 문자열이라(pipeline.py:331) 시간순 정렬이 안 된다.
    # 사전순으로는 rebuild가 먼저 오지만, 나중에 쓰인 쪽이 이겨야 한다.
    _write_build(tmp_path, date(2026, 8, 1), "zzz-first", date(2026, 7, 1), mtime=1000)
    _write_build(tmp_path, date(2026, 8, 1), "aaa-rebuild", date(2026, 7, 20), mtime=2000)

    resolved = resolve_road_snapshot_date_for_month(str(tmp_path), date(2026, 8, 1))

    assert resolved == date(2026, 7, 20)


def test_falls_back_to_the_latest_build_before_the_target_month(tmp_path):
    _write_build(tmp_path, date(2026, 5, 1), "build-may", date(2026, 4, 30))
    _write_build(tmp_path, date(2026, 6, 1), "build-jun", date(2026, 5, 31))
    # 논리 시각보다 나중에 만들어진 build는 후보에서 빠져야 한다.
    _write_build(tmp_path, date(2026, 9, 1), "build-sep", date(2026, 8, 31))

    resolved = resolve_road_snapshot_date_for_month(str(tmp_path), date(2026, 8, 1))

    assert resolved == date(2026, 5, 31)


def test_raises_when_no_build_exists_at_or_before_the_target_month(tmp_path):
    _write_build(tmp_path, date(2026, 9, 1), "build-sep", date(2026, 8, 31))

    with pytest.raises(ValueError, match="no road-environment build"):
        resolve_road_snapshot_date_for_month(str(tmp_path), date(2026, 8, 1))


def test_ignores_objects_that_are_not_manifests(tmp_path):
    manifest_path = _write_build(tmp_path, date(2026, 8, 1), "build-aug", date(2026, 7, 31))
    # 같은 build 디렉터리에 함께 놓이는 parquet 산출물(pipeline.py:46-53).
    (manifest_path.parent / "road_environment.parquet").write_bytes(b"not-a-manifest")
    (manifest_path.parent / "taxi_zones.parquet").write_bytes(b"not-a-manifest")

    resolved = resolve_road_snapshot_date_for_month(str(tmp_path), date(2026, 8, 1))

    assert resolved == date(2026, 7, 31)
