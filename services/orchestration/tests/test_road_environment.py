# jobs/road_environment.py 테스트 (#402).

from __future__ import annotations

import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from de4_core import DataArtifact, RoadEnvironmentManifest, SourceSnapshot
from jobs.road_environment import resolve_active_road_snapshot_date

CHECKSUM = "a" * 64


def _manifest(road_snapshot_date: date) -> RoadEnvironmentManifest:
    return RoadEnvironmentManifest(
        schema_version="1",
        environment_id="nyc-20260801-build-1",
        reference_date=date(2026, 8, 1),
        road_snapshot_date=road_snapshot_date,
        build_id="build-1",
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
