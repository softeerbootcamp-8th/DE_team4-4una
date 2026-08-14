from dataclasses import replace
from datetime import UTC, date, datetime, timedelta, timezone

import duckdb
import pytest
import shapely
from road_segment.persist import read_road_segment_parquet, write_road_segment_snapshot
from road_segment.validate import RoadSegmentRecord
from shapely.geometry import LineString

LINE_WKB = shapely.to_wkb(LineString([(0, 0), (1, 1)]))

EXPECTED_COLUMNS = [
    "segment_id",
    "snapshot_date",
    "street_name",
    "from_node_id",
    "to_node_id",
    "traffic_direction",
    "segment_type",
    "feature_type",
    "roadway_type",
    "roadbed_layer",
    "from_node_level",
    "to_node_level",
    "posted_speed_mph",
    "curve_flag",
    "curve_radius_m",
    "length_m",
    "geometry_wkb",
    "source_version",
    "ingested_at",
    "location_id",
]


def sample_record(
    segment_id: str, snapshot_date: date = date(2026, 8, 13), **overrides: object
) -> RoadSegmentRecord:
    fields = {
        "segment_id": segment_id,
        "snapshot_date": snapshot_date,
        "street_name": "TEST STREET",
        "from_node_id": "0047201",
        "to_node_id": "0047258",
        "traffic_direction": "T",
        "segment_type": "U",
        "feature_type": "0",
        "roadway_type": "1",
        "roadbed_layer": "B",
        "from_node_level": "M",
        "to_node_level": "M",
        "posted_speed_mph": 25,
        "curve_flag": None,
        "curve_radius_m": None,
        "length_m": 100.0,
        "geometry_wkb": LINE_WKB,
        "source_version": "26B",
        "ingested_at": datetime(2026, 8, 13, 0, 0, 0, tzinfo=UTC),
    }
    fields.update(overrides)
    return RoadSegmentRecord(**fields)


def test_write_road_segment_snapshot_creates_partition_directory(tmp_path) -> None:
    records = [sample_record("0000001"), sample_record("0000002")]

    path = write_road_segment_snapshot(records, tmp_path)

    assert path == tmp_path / "snapshot_date=2026-08-13" / "data.parquet"
    assert path.is_file()


def test_write_and_read_road_segment_snapshot_round_trips(tmp_path) -> None:
    records = [sample_record("0000001"), sample_record("0000002")]

    path = write_road_segment_snapshot(records, tmp_path)
    summary = read_road_segment_parquet(path)

    assert summary.row_count == 2
    assert [name for name, _ in summary.columns] == EXPECTED_COLUMNS


def test_write_road_segment_snapshot_preserves_geometry_wkb(tmp_path) -> None:
    path = write_road_segment_snapshot([sample_record("0000001")], tmp_path)

    row = duckdb.sql(
        "SELECT segment_id, geometry_wkb FROM read_parquet(?)", params=[str(path)]
    ).fetchone()

    assert row[0] == "0000001"
    assert shapely.from_wkb(row[1]).geom_type == "LineString"


def test_write_road_segment_snapshot_preserves_utc_instant(tmp_path) -> None:
    # plain TIMESTAMP는 DuckDB 세션 타임존으로 값을 재해석해버리므로,
    # TIMESTAMPTZ가 실제 UTC 시각(epoch)을 그대로 보존하는지 확인한다.
    ingested_at = datetime(2026, 8, 13, 12, 0, 0, tzinfo=UTC)
    record = replace(sample_record("0000001"), ingested_at=ingested_at)
    path = write_road_segment_snapshot([record], tmp_path)

    epoch_seconds = duckdb.sql(
        "SELECT epoch(ingested_at) FROM read_parquet(?)", params=[str(path)]
    ).fetchone()[0]

    assert epoch_seconds == ingested_at.timestamp()


def test_write_road_segment_snapshot_rejects_naive_ingested_at(tmp_path) -> None:
    naive = datetime(2026, 8, 13, 12, 0, 0)  # noqa: DTZ001 (deliberately naive for this test)
    record = replace(sample_record("0000001"), ingested_at=naive)

    with pytest.raises(ValueError, match="ingested_at must be UTC"):
        write_road_segment_snapshot([record], tmp_path)


def test_write_road_segment_snapshot_rejects_non_utc_ingested_at(tmp_path) -> None:
    non_utc = datetime(2026, 8, 13, 12, 0, 0, tzinfo=timezone(timedelta(hours=9)))
    record = replace(sample_record("0000001"), ingested_at=non_utc)

    with pytest.raises(ValueError, match="ingested_at must be UTC"):
        write_road_segment_snapshot([record], tmp_path)


def test_write_road_segment_snapshot_rejects_duplicate_segment_id(tmp_path) -> None:
    records = [sample_record("0000001"), sample_record("0000001")]

    with pytest.raises(ValueError, match="duplicate segment_id"):
        write_road_segment_snapshot(records, tmp_path)


def test_write_road_segment_snapshot_rejects_mixed_snapshot_dates(tmp_path) -> None:
    records = [
        sample_record("0000001", snapshot_date=date(2026, 8, 13)),
        sample_record("0000002", snapshot_date=date(2026, 5, 1)),
    ]

    with pytest.raises(ValueError, match="multiple snapshot_date"):
        write_road_segment_snapshot(records, tmp_path)


def test_new_snapshot_does_not_touch_existing_partitions(tmp_path) -> None:
    write_road_segment_snapshot(
        [sample_record("0000001", snapshot_date=date(2026, 5, 1))], tmp_path
    )

    write_road_segment_snapshot(
        [sample_record("0000002", snapshot_date=date(2026, 8, 1))], tmp_path
    )

    old_partition = tmp_path / "snapshot_date=2026-05-01" / "data.parquet"
    new_partition = tmp_path / "snapshot_date=2026-08-01" / "data.parquet"
    assert read_road_segment_parquet(old_partition).row_count == 1
    assert read_road_segment_parquet(new_partition).row_count == 1


def test_rerunning_same_snapshot_date_replaces_instead_of_appending(tmp_path) -> None:
    write_road_segment_snapshot(
        [sample_record("0000001", snapshot_date=date(2026, 8, 1))], tmp_path
    )

    write_road_segment_snapshot(
        [
            sample_record("0000002", snapshot_date=date(2026, 8, 1)),
            sample_record("0000003", snapshot_date=date(2026, 8, 1)),
        ],
        tmp_path,
    )

    partition = tmp_path / "snapshot_date=2026-08-01" / "data.parquet"
    summary = read_road_segment_parquet(partition)
    assert summary.row_count == 2
    segment_ids = duckdb.sql(
        "SELECT segment_id FROM read_parquet(?)", params=[str(partition)]
    ).fetchall()
    assert {row[0] for row in segment_ids} == {"0000002", "0000003"}


def test_read_road_segment_parquet_queries_full_history_across_partitions(tmp_path) -> None:
    write_road_segment_snapshot(
        [sample_record("0000001", snapshot_date=date(2026, 5, 1))], tmp_path
    )
    write_road_segment_snapshot(
        [sample_record("0000002", snapshot_date=date(2026, 8, 1))], tmp_path
    )

    history_glob = str(tmp_path / "snapshot_date=*" / "data.parquet")
    summary = read_road_segment_parquet(history_glob)

    assert summary.row_count == 2
    assert [name for name, _ in summary.columns] == EXPECTED_COLUMNS
