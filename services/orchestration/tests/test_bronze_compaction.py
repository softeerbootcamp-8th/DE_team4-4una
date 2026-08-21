"""Tests for jobs/bronze_compaction.py (#271, ADR-0009)."""

from __future__ import annotations

import io
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from de4_core import ObjectStore, join_uri

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from jobs.bronze_compaction import (
    BronzeCompactionConfig,
    BronzeCompactionRowCountMismatch,
    compact_bronze_prefix,
    run_sensor_events_compaction,
    run_zone_weather_snapshot_compaction,
)


def _write_parquet(store: ObjectStore, uri: str, rows: list[dict]) -> None:
    table = pa.Table.from_pylist(rows)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    store.write_bytes(uri, buffer.getvalue())


def _row_count(store: ObjectStore, uri: str) -> int:
    return pq.read_table(io.BytesIO(store.read_bytes(uri))).num_rows


NOW = datetime(2026, 8, 22, 6, 0, tzinfo=UTC)
OLD = NOW - timedelta(hours=2)  # 안전 경계(기본 1시간)보다 오래됨 — 압축 대상
RECENT = NOW - timedelta(minutes=5)  # 안전 경계보다 최근 — 아직 쓰기 중일 수 있음


class TestCompactBronzePrefixMergesClosedGroups:
    def test_merges_a_flat_group_past_the_safety_margin(self, tmp_path) -> None:
        root = tmp_path.as_uri()
        store = ObjectStore()
        for i in range(3):
            path = join_uri(root, f"part-{i}.parquet")
            _write_parquet(store, path, [{"value": f"row-{i}"}])
        import os

        old_epoch = OLD.timestamp()
        for child in tmp_path.iterdir():
            os.utime(child, (old_epoch, old_epoch))

        summary = compact_bronze_prefix(
            store, root, now=NOW, safety_margin=timedelta(hours=1), target_object_count=1
        )

        assert len(summary.compacted_groups) == 1
        group = summary.compacted_groups[0]
        assert group.source_object_count == 3
        assert group.row_count == 3
        assert _row_count(store, group.final_uri) == 3
        remaining = store.list_objects(root)
        assert [obj.uri for obj in remaining] == [group.final_uri]

    def test_skips_a_group_still_within_the_safety_margin(self, tmp_path) -> None:
        root = tmp_path.as_uri()
        store = ObjectStore()
        for i in range(3):
            _write_parquet(store, join_uri(root, f"part-{i}.parquet"), [{"value": i}])
        # 방금 쓴 파일이라 mtime이 RECENT(5분 전)에 가깝다 — 안전 경계(1시간) 안이므로
        # 스킵돼야 한다. 결정론적으로 만들기 위해 mtime을 명시적으로 고정한다.
        import os

        recent_epoch = RECENT.timestamp()
        for child in tmp_path.iterdir():
            os.utime(child, (recent_epoch, recent_epoch))

        summary = compact_bronze_prefix(
            store, root, now=NOW, safety_margin=timedelta(hours=1), target_object_count=1
        )

        assert summary.compacted_groups == ()
        assert summary.skipped_group_count == 1
        remaining = store.list_objects(root)
        assert len(remaining) == 3  # 스킵됐으니 원본 그대로

    def test_idempotent_skip_when_already_at_or_below_target_count(self, tmp_path) -> None:
        root = tmp_path.as_uri()
        store = ObjectStore()
        _write_parquet(store, join_uri(root, "already-compacted.parquet"), [{"value": 1}])
        import os

        past_epoch = OLD.timestamp()
        for child in tmp_path.iterdir():
            os.utime(child, (past_epoch, past_epoch))

        summary = compact_bronze_prefix(
            store, root, now=NOW, safety_margin=timedelta(hours=1), target_object_count=1
        )

        assert summary.compacted_groups == ()
        assert summary.skipped_group_count == 1
        remaining = store.list_objects(root)
        assert len(remaining) == 1


class TestCompactBronzePrefixVerifiesRowCount:
    def test_hard_fails_and_preserves_originals_on_row_count_mismatch(self, tmp_path) -> None:
        root = tmp_path.as_uri()
        store = ObjectStore()
        for i in range(3):
            _write_parquet(store, join_uri(root, f"part-{i}.parquet"), [{"value": i}])
        import os

        past_epoch = OLD.timestamp()
        for child in tmp_path.iterdir():
            os.utime(child, (past_epoch, past_epoch))

        class _CorruptingObjectStore(ObjectStore):
            """merge된 임시 키를 읽을 때만 한 행을 잘라내 검증 실패를 재현한다."""

            def read_bytes(self, uri: str) -> bytes:
                raw = super().read_bytes(uri)
                if "_bronze_compaction_tmp" not in uri:
                    return raw
                table = pq.read_table(io.BytesIO(raw))
                truncated = table.slice(0, table.num_rows - 1)
                buffer = io.BytesIO()
                pq.write_table(truncated, buffer)
                return buffer.getvalue()

        corrupting_store = _CorruptingObjectStore()

        with pytest.raises(BronzeCompactionRowCountMismatch):
            compact_bronze_prefix(
                corrupting_store,
                root,
                now=NOW,
                safety_margin=timedelta(hours=1),
                target_object_count=1,
            )

        remaining = store.list_objects(root)
        # 원본 3개는 그대로 보존된다. 검증에 실패한 임시 병합 키는 정리 대상이
        # 아니므로(원본 보존이 데이터 안전 보장의 전부) 디렉터리에 남아 있을 수 있다.
        remaining_names = {obj.uri.rsplit("/", 1)[-1] for obj in remaining}
        assert {"part-0.parquet", "part-1.parquet", "part-2.parquet"} <= remaining_names


class TestCompactBronzePrefixGroupsByParentDirectory:
    def test_groups_partitioned_zone_weather_snapshot_layout_by_day(self, tmp_path) -> None:
        root = tmp_path.as_uri()
        store = ObjectStore()
        for day in ("2026-08-20", "2026-08-21"):
            for hour_minute in ("00-00", "00-15", "00-30"):
                path = join_uri(
                    root, f"weather_date={day}", f"weather_time={day}T{hour_minute}-00Z.parquet"
                )
                _write_parquet(store, path, [{"value": 1}])
        import os

        past_epoch = OLD.timestamp()
        for path in tmp_path.rglob("*.parquet"):
            os.utime(path, (past_epoch, past_epoch))

        summary = compact_bronze_prefix(
            store, root, now=NOW, safety_margin=timedelta(hours=1), target_object_count=1
        )

        assert len(summary.compacted_groups) == 2  # 날짜 파티션당 한 그룹
        for group in summary.compacted_groups:
            assert group.source_object_count == 3
            assert group.row_count == 3


class TestBronzeCompactionConfig:
    def test_from_env_reads_uris_and_defaults(self) -> None:
        config = BronzeCompactionConfig.from_env({})

        assert config.sensor_events_uri == "data/local-lake/bronze/sensor-events"
        assert config.zone_weather_snapshot_uri == "data/local-lake/bronze/zone_weather_snapshot"
        assert config.safety_margin == timedelta(hours=1)
        assert config.target_object_count == 1

    def test_from_env_overrides(self) -> None:
        config = BronzeCompactionConfig.from_env(
            {
                "BRONZE_COMPACTION_SENSOR_EVENTS_URI": "s3://bucket/sensor-events",
                "BRONZE_COMPACTION_ZONE_WEATHER_SNAPSHOT_URI": "s3://bucket/zone_weather_snapshot",
                "BRONZE_COMPACTION_SAFETY_MARGIN_MINUTES": "30",
            }
        )

        assert config.sensor_events_uri == "s3://bucket/sensor-events"
        assert config.zone_weather_snapshot_uri == "s3://bucket/zone_weather_snapshot"
        assert config.safety_margin == timedelta(minutes=30)


class TestRunCompactionEntrypoints:
    def test_run_sensor_events_compaction_targets_the_configured_uri(self, tmp_path) -> None:
        sensor_events_uri = join_uri(tmp_path.as_uri(), "sensor-events")
        config = BronzeCompactionConfig(
            sensor_events_uri=sensor_events_uri,
            zone_weather_snapshot_uri=join_uri(tmp_path.as_uri(), "zone_weather_snapshot"),
            safety_margin=timedelta(hours=1),
            target_object_count=1,
        )
        store = ObjectStore()
        for i in range(2):
            _write_parquet(store, join_uri(sensor_events_uri, f"part-{i}.parquet"), [{"value": i}])
        import os

        past_epoch = OLD.timestamp()
        for path in tmp_path.rglob("*.parquet"):
            os.utime(path, (past_epoch, past_epoch))

        summary = run_sensor_events_compaction(config, NOW, store=store)

        assert summary.root_uri == sensor_events_uri
        assert len(summary.compacted_groups) == 1

    def test_run_zone_weather_snapshot_compaction_targets_the_configured_uri(self, tmp_path) -> None:
        zone_weather_uri = join_uri(tmp_path.as_uri(), "zone_weather_snapshot")
        config = BronzeCompactionConfig(
            sensor_events_uri=join_uri(tmp_path.as_uri(), "sensor-events"),
            zone_weather_snapshot_uri=zone_weather_uri,
            safety_margin=timedelta(hours=1),
            target_object_count=1,
        )
        store = ObjectStore()
        for i in range(2):
            path = join_uri(zone_weather_uri, "weather_date=2026-08-20", f"part-{i}.parquet")
            _write_parquet(store, path, [{"value": i}])
        import os

        past_epoch = OLD.timestamp()
        for path in tmp_path.rglob("*.parquet"):
            os.utime(path, (past_epoch, past_epoch))

        summary = run_zone_weather_snapshot_compaction(config, NOW, store=store)

        assert summary.root_uri == zone_weather_uri
        assert len(summary.compacted_groups) == 1
