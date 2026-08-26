"""Tests for the bounded-memory Bronze sensor-events compaction job (#585)."""

from __future__ import annotations

import io
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from de4_core import ObjectStore, join_uri

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from jobs.sensor_events_compaction import (
    BronzeCompactionRowCountMismatch,
    SensorEventsCompactionConfig,
    compact_sensor_events,
)

NOW = datetime(2026, 8, 26, 6, tzinfo=UTC)
OLD = NOW - timedelta(hours=3)


def _write_parquet(store: ObjectStore, uri: str, rows: list[dict]) -> None:
    table = pa.Table.from_pylist(rows)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    store.write_bytes(uri, buffer.getvalue())


def _age_files(root: Path, timestamp: datetime = OLD) -> None:
    for path in root.rglob("*"):
        if path.is_file():
            os.utime(path, (timestamp.timestamp(), timestamp.timestamp()))


def _config(root: Path, **overrides: object) -> SensorEventsCompactionConfig:
    return SensorEventsCompactionConfig(
        root_uri=root.as_uri(),
        safety_margin=timedelta(hours=2),
        target_file_mb=1,
        max_groups_per_run=0,
        **overrides,
    )


def test_compacts_each_closed_hour_and_preserves_spark_metadata(tmp_path) -> None:
    root = tmp_path / "sensor-events"
    store = ObjectStore()
    for hour in ("10", "11"):
        group = join_uri(root.as_uri(), "event_date=2026-08-26", f"hour={hour}")
        for index in range(3):
            _write_parquet(store, join_uri(group, f"part-{index}.parquet"), [{"value": index}])
    metadata = root / "_spark_metadata" / "0.parquet"
    metadata.parent.mkdir(parents=True)
    metadata.write_text("writer-owned")
    _age_files(root)

    summary = compact_sensor_events(store, _config(root), now=NOW)

    assert len(summary.compacted_groups) == 2
    assert metadata.read_text() == "writer-owned"
    for hour in ("10", "11"):
        objects = store.list_objects(join_uri(root.as_uri(), "event_date=2026-08-26", f"hour={hour}"))
        assert len(objects) == 1
        assert objects[0].uri.rsplit("/", 1)[-1].startswith("compacted-")
        with store.open_reader(objects[0].uri) as source:
            assert pq.ParquetFile(source).metadata.num_rows == 3


def test_splits_outputs_when_target_size_is_exceeded(tmp_path) -> None:
    root = tmp_path / "sensor-events"
    store = ObjectStore()
    group = join_uri(root.as_uri(), "event_date=2026-08-26", "hour=10")
    for index in range(4):
        _write_parquet(store, join_uri(group, f"part-{index}.parquet"), [{"value": os.urandom(600_000)}])
    _age_files(root)

    summary = compact_sensor_events(store, _config(root), now=NOW)

    assert summary.compacted_groups[0].output_object_count >= 2
    assert summary.compacted_groups[0].row_count == 4


def test_skips_recent_and_already_compacted_or_single_file_groups(tmp_path) -> None:
    root = tmp_path / "sensor-events"
    store = ObjectStore()
    old_group = join_uri(root.as_uri(), "event_date=2026-08-26", "hour=10")
    recent_group = join_uri(root.as_uri(), "event_date=2026-08-26", "hour=11")
    single_group = join_uri(root.as_uri(), "event_date=2026-08-26", "hour=12")
    _write_parquet(store, join_uri(old_group, "compacted-existing.parquet"), [{"value": 1}])
    for index in range(2):
        _write_parquet(store, join_uri(recent_group, f"part-{index}.parquet"), [{"value": index}])
    _write_parquet(store, join_uri(single_group, "part-0.parquet"), [{"value": 1}])
    _age_files(root)
    for path in (root / "event_date=2026-08-26" / "hour=11").glob("*.parquet"):
        os.utime(path, (NOW.timestamp(), NOW.timestamp()))

    summary = compact_sensor_events(store, _config(root), now=NOW)

    assert summary.compacted_groups == ()
    assert summary.skipped_group_count == 2


def test_schema_mismatch_keeps_sources_and_fails(tmp_path) -> None:
    root = tmp_path / "sensor-events"
    store = ObjectStore()
    group = join_uri(root.as_uri(), "event_date=2026-08-26", "hour=10")
    _write_parquet(store, join_uri(group, "part-0.parquet"), [{"value": 1}])
    _write_parquet(store, join_uri(group, "part-1.parquet"), [{"other": 1}])
    _age_files(root)

    with pytest.raises(ValueError, match="schemas do not match"):
        compact_sensor_events(store, _config(root), now=NOW)

    assert len(store.list_objects(group)) == 2


def test_row_count_mismatch_removes_output_and_keeps_sources(tmp_path, monkeypatch) -> None:
    root = tmp_path / "sensor-events"
    store = ObjectStore()
    group = join_uri(root.as_uri(), "event_date=2026-08-26", "hour=10")
    for index in range(2):
        _write_parquet(store, join_uri(group, f"part-{index}.parquet"), [{"value": index}])
    _age_files(root)
    monkeypatch.setattr("jobs.sensor_events_compaction._footer_row_count", lambda *_: 0)

    with pytest.raises(BronzeCompactionRowCountMismatch):
        compact_sensor_events(store, _config(root), now=NOW)

    names = {object.uri.rsplit("/", 1)[-1] for object in store.list_objects(group)}
    assert names == {"part-0.parquet", "part-1.parquet"}


def test_respects_max_groups_per_run_and_reads_environment_config(tmp_path) -> None:
    root = tmp_path / "sensor-events"
    store = ObjectStore()
    for hour in ("10", "11"):
        group = join_uri(root.as_uri(), "event_date=2026-08-26", f"hour={hour}")
        for index in range(2):
            _write_parquet(store, join_uri(group, f"part-{index}.parquet"), [{"value": index}])
    _age_files(root)
    config = SensorEventsCompactionConfig.from_env(
        {
            "SENSOR_EVENTS_COMPACTION_ROOT_URI": root.as_uri(),
            "SENSOR_EVENTS_COMPACTION_SAFETY_MARGIN_MINUTES": "120",
            "SENSOR_EVENTS_COMPACTION_TARGET_FILE_MB": "1",
            "SENSOR_EVENTS_COMPACTION_MAX_GROUPS_PER_RUN": "1",
        }
    )

    summary = compact_sensor_events(store, config, now=NOW)

    assert len(summary.compacted_groups) == 1
    assert summary.skipped_group_count == 1
