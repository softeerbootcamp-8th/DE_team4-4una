# jobs/current_score.py 테스트 (#216).

from __future__ import annotations

import csv
import io
import os
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import ClassVar

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from botocore.exceptions import ClientError
from de4_core import ObjectStore, join_uri

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from jobs import current_score, current_score_quarantine
from jobs.current_score import (
    DEFAULT_ROAD_SEGMENT_URI,
    CurrentScoreJobConfig,
    find_changed_zones,
    load_segment_zones,
    run_current_score_job,
)
from jobs.weather_rules import (
    ICE,
    WEATHER_RULE_VERSION,
    format_impact_signature,
    load_weather_rule_config,
)

RUN_INTEGRATION = os.environ.get("RUN_INTEGRATION") == "1"

RULE_CONFIG = load_weather_rule_config()
SNAPSHOT_DATE = date(2024, 2, 1)
WEATHER_TIME = datetime(2026, 8, 19, 10, 15, tzinfo=UTC)
SCORE_AS_OF = datetime(2026, 8, 19, 10, 0, tzinfo=UTC)
CLEAR_SIGNATURE = f"{WEATHER_RULE_VERSION}|clear"
ICE_SIGNATURE = format_impact_signature(frozenset({ICE}))

# standard_segment_comfort_score에서 읽는 순서 그대로.
STANDARD_ROW = (
    "12345",
    1,
    SCORE_AS_OF,
    datetime(2026, 8, 12, 10, 0, tzinfo=UTC),
    80.0,
    70.0,
    60.0,
    900,
    0.9,
    "1.0.0",
)


def write_road_segment(tmp_path: Path, rows: list[tuple[str, date, int | None]]) -> Path:
    path = tmp_path / "road_segment.parquet"
    table = pa.table(
        {
            "segment_id": pa.array([row[0] for row in rows], pa.string()),
            "snapshot_date": pa.array([row[1] for row in rows], pa.date32()),
            "location_id": pa.array([row[2] for row in rows], pa.int32()),
        }
    )
    pq.write_table(table, path)
    return path


def write_road_segment_partition(
    root: Path, snapshot_date: date, rows: list[tuple[str, date, int | None]]
) -> Path:
    """root/road_segment/snapshot_date=<date>/ 아래 단일 Parquet에 쓰고 road_segment 루트를 반환한다.

    load_segment_zones()는 이제 road_segment_uri(루트)와 snapshot_date로 정확히 이
    파티션 경로를 조합해 조회하므로, 픽스처도 실제 레이아웃과 같은 모양으로 둔다.
    """
    partition = root / "road_segment" / f"snapshot_date={snapshot_date.isoformat()}"
    partition.mkdir(parents=True, exist_ok=True)
    write_road_segment(partition, rows)
    return root / "road_segment"


def _write_parquet_object(store: ObjectStore, uri: str, rows: list[tuple[str, date, int | None]]) -> None:
    table = pa.table(
        {
            "segment_id": pa.array([row[0] for row in rows], pa.string()),
            "snapshot_date": pa.array([row[1] for row in rows], pa.date32()),
            "location_id": pa.array([row[2] for row in rows], pa.int32()),
        }
    )
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    store.write_bytes(uri, buffer.getvalue())


class FakeS3Client:
    """put_object/get_object/list_objects_v2만 갖춘 최소 in-memory S3."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def put_object(self, **kwargs: object) -> None:
        self.objects[(str(kwargs["Bucket"]), str(kwargs["Key"]))] = kwargs["Body"]  # type: ignore[assignment]

    def get_object(self, **kwargs: object) -> dict[str, object]:
        key = (str(kwargs["Bucket"]), str(kwargs["Key"]))
        if key not in self.objects:
            raise ClientError(
                {
                    "Error": {"Code": "NoSuchKey", "Message": "not found"},
                    "ResponseMetadata": {"HTTPStatusCode": 404},
                },
                "GetObject",
            )
        return {"Body": io.BytesIO(self.objects[key])}

    def list_objects_v2(self, **kwargs: object) -> dict[str, object]:
        bucket = str(kwargs["Bucket"])
        prefix = str(kwargs["Prefix"])
        contents = [
            {"Key": key, "LastModified": datetime(2026, 8, 20, tzinfo=UTC), "Size": len(body)}
            for (obj_bucket, key), body in self.objects.items()
            if obj_bucket == bucket and key.startswith(prefix)
        ]
        return {"Contents": contents, "IsTruncated": False}


# _ROW_COLUMNS 각 컬럼의 파이썬 타입 — copy_expert가 받는 CSV 텍스트를 원래 타입으로 되돌리는 데 쓴다.
_ROW_COLUMN_TYPES: dict[str, type] = {
    "segment_id": str,
    "vehicle_profile_id": int,
    "location_id": int,
    "standard_score_as_of": datetime,
    "weather_time": datetime,
    "data_period_start": datetime,
    "vertical_score": float,
    "longitudinal_score": float,
    "lateral_score": float,
    "comfort_score": float,
    "sample_count": int,
    "confidence_score": float,
    "standard_score_version": str,
    "weather_rule_version": str,
    "weather_impact_signature": str,
    "calculated_at": datetime,
}


def _parse_csv_value(column: str, raw: str):
    if raw == "":
        return None
    kind = _ROW_COLUMN_TYPES[column]
    return datetime.fromisoformat(raw) if kind is datetime else kind(raw)


class FakeCursor:
    """execute한 SQL과 copy_expert로 넘어온 행을 기록하는 최소 구현."""

    def __init__(self, owner):
        self.owner = owner
        self.rows: list[tuple] = []

    def execute(self, sql, parameters=None):
        normalized = " ".join(sql.split())
        self.owner.executed.append((normalized, parameters))
        # 변경 zone 조회도 같은 테이블을 참조하므로 JOIN 쪽을 먼저 본다(staging은 이름이 달라 안 겹친다).
        if "JOIN current_segment_comfort_score" in normalized:
            self.rows = list(self.owner.changed_zone_rows)
        elif "FROM current_segment_comfort_score WHERE" in normalized:
            self.rows = list(self.owner.current_score_rows)
        elif "FROM latest_zone_weather" in normalized:
            self.rows = list(self.owner.weather_rows)
        elif "FROM standard_segment_comfort_score" in normalized:
            self.rows = list(self.owner.standard_rows)
        else:
            self.rows = []

    def fetchall(self):
        return self.rows

    def fetchmany(self, size):
        batch, self.rows = self.rows[:size], self.rows[size:]
        return batch

    def copy_expert(self, sql, file):
        for record in csv.reader(file):
            row = tuple(
                _parse_csv_value(column, value)
                for column, value in zip(current_score._ROW_COLUMNS, record, strict=True)
            )
            self.owner.upserted.append(row)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


# 격리 행은 여전히 execute_values로 넣는다 — 실제 커서 encoding을 요구하므로 넘어온 행만 가로챈다.
@pytest.fixture(autouse=True)
def captured_quarantine_inserts(monkeypatch):
    def record_quarantine(cursor, sql, argslist):
        cursor.owner.quarantined.extend(argslist)

    monkeypatch.setattr(current_score_quarantine, "execute_values", record_quarantine)


class FakeConnection:
    def __init__(
        self, *, weather_rows=(), standard_rows=(), changed_zone_rows=(), current_score_rows=()
    ):
        self.weather_rows = weather_rows
        self.standard_rows = standard_rows
        self.changed_zone_rows = changed_zone_rows
        self.current_score_rows = current_score_rows
        self.executed: list[tuple] = []
        self.upserted: list[tuple] = []
        self.quarantined: list[tuple] = []
        self.committed = False

    def cursor(self, *args, **kwargs):
        return FakeCursor(self)

    def commit(self):
        self.committed = True

    def rollback(self):
        pass


def config_for(path: Path) -> CurrentScoreJobConfig:
    return CurrentScoreJobConfig(
        road_segment_uri=str(path),
        road_snapshot_date=SNAPSHOT_DATE,
        postgres_host="localhost",
        postgres_port=5432,
        postgres_db="de4",
        postgres_user="de4",
        postgres_password="de4",
    )


def upserted_row(connection) -> dict:
    return dict(zip(current_score._ROW_COLUMNS, connection.upserted[0], strict=True))


class TestLoadSegmentZones:
    def test_maps_segment_to_zone(self, tmp_path):
        path = write_road_segment_partition(tmp_path, SNAPSHOT_DATE, [("1", SNAPSHOT_DATE, 76), ("2", SNAPSHOT_DATE, 12)])

        assert load_segment_zones(path, SNAPSHOT_DATE) == {"1": 76, "2": 12}

    def test_drops_segments_without_a_zone(self, tmp_path):
        # location_id는 nullable인데 current 테이블에서는 NOT NULL이라 행을 만들 수 없다.
        path = write_road_segment_partition(tmp_path, SNAPSHOT_DATE, [("1", SNAPSHOT_DATE, None), ("2", SNAPSHOT_DATE, 12)])

        assert load_segment_zones(path, SNAPSHOT_DATE) == {"2": 12}

    def test_reads_a_hive_partitioned_dataset(self, tmp_path):
        # road_segment는 snapshot_date를 파일 안에도, 경로에도 갖고 있다. 파티션 추론을
        # 켜두면 같은 컬럼이 date32와 문자열로 두 번 잡혀 읽기가 실패한다.
        partition = tmp_path / "road_segment" / f"snapshot_date={SNAPSHOT_DATE}" / "build_id=z76"
        partition.mkdir(parents=True)
        write_road_segment(partition, [("1", SNAPSHOT_DATE, 76)])

        assert load_segment_zones(tmp_path / "road_segment", SNAPSHOT_DATE) == {"1": 76}

    def test_rejects_another_snapshot(self, tmp_path):
        # 파티션 폴더 이름(snapshot_date=2024-02-01)은 맞는데 파일 내부 컬럼 값이
        # 다른 경우 — 잘못 배치된 데이터를 잡아내는 방어적 검증이다.
        path = write_road_segment_partition(tmp_path, SNAPSHOT_DATE, [("1", date(2024, 1, 1), 76)])

        with pytest.raises(ValueError, match="expected snapshot_date"):
            load_segment_zones(path, SNAPSHOT_DATE)

    def test_raises_when_the_snapshot_partition_is_missing(self, tmp_path):
        (tmp_path / "road_segment").mkdir()

        with pytest.raises(ValueError, match="no parquet files found"):
            load_segment_zones(tmp_path / "road_segment", SNAPSHOT_DATE)

    def test_reads_a_snapshot_partition_from_s3(self):
        store = ObjectStore(FakeS3Client())  # type: ignore[arg-type]
        root = "s3://de4-reference/normalized/road_segment"
        _write_parquet_object(
            store,
            join_uri(root, f"snapshot_date={SNAPSHOT_DATE}", "part-0.parquet"),
            [("1", SNAPSHOT_DATE, 76), ("2", SNAPSHOT_DATE, 12)],
        )

        assert load_segment_zones(root, SNAPSHOT_DATE, store=store) == {"1": 76, "2": 12}

    def test_only_reads_the_requested_snapshot_partition_on_s3(self):
        # 같은 root 아래 다른 날짜의 partition도 있지만, 요청한 snapshot_date만 읽는다 —
        # S3에서 root 전체를 스캔하면 비용도 크고 다른 날짜 데이터가 섞일 수 있다.
        store = ObjectStore(FakeS3Client())  # type: ignore[arg-type]
        root = "s3://de4-reference/normalized/road_segment"
        _write_parquet_object(
            store,
            join_uri(root, f"snapshot_date={SNAPSHOT_DATE}", "part-0.parquet"),
            [("1", SNAPSHOT_DATE, 76)],
        )
        _write_parquet_object(
            store,
            join_uri(root, "snapshot_date=2024-01-01", "part-0.parquet"),
            [("2", date(2024, 1, 1), 12)],
        )

        assert load_segment_zones(root, SNAPSHOT_DATE, store=store) == {"1": 76}

    def test_raises_when_the_s3_snapshot_partition_is_missing(self):
        store = ObjectStore(FakeS3Client())  # type: ignore[arg-type]
        root = "s3://de4-reference/normalized/road_segment"

        with pytest.raises(ValueError, match="no parquet files found"):
            load_segment_zones(root, SNAPSHOT_DATE, store=store)


class TestCurrentScoreJobConfigFromEnv:
    BASE_ENV: ClassVar[dict[str, str]] = {
        "CURRENT_SCORE_ROAD_SNAPSHOT_DATE": "2024-02-01",
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "de4",
        "POSTGRES_USER": "de4",
        "POSTGRES_PASSWORD": "de4",
    }

    def test_falls_back_to_the_local_default_uri(self):
        config = CurrentScoreJobConfig.from_env(self.BASE_ENV)

        assert config.road_segment_uri == DEFAULT_ROAD_SEGMENT_URI

    def test_reads_an_s3_uri_from_the_environment(self):
        config = CurrentScoreJobConfig.from_env(
            {
                **self.BASE_ENV,
                "CURRENT_SCORE_ROAD_SEGMENT_URI": "s3://de4-reference/normalized/road_segment",
            }
        )

        assert config.road_segment_uri == "s3://de4-reference/normalized/road_segment"


class TestFindChangedZones:
    def test_returns_sorted_zones(self):
        connection = FakeConnection(changed_zone_rows=[(76,), (12,)])

        assert find_changed_zones(connection) == (12, 76)

    def test_no_change_means_no_zone(self):
        assert find_changed_zones(FakeConnection()) == ()


class TestRunCurrentScoreJob:
    def test_applies_the_zone_weather_to_the_standard_scores(self, tmp_path):
        path = write_road_segment_partition(tmp_path, SNAPSHOT_DATE, [("12345", SNAPSHOT_DATE, 76)])
        connection = FakeConnection(
            weather_rows=[(76, WEATHER_TIME, ICE_SIGNATURE)],
            standard_rows=[STANDARD_ROW],
        )

        summary = run_current_score_job(
            config_for(path), connection, changed_zones_only=False, rule_config=RULE_CONFIG
        )
        row = upserted_row(connection)

        assert summary.upserted_count == 1
        assert row["location_id"] == 76
        assert row["standard_score_as_of"] == SCORE_AS_OF
        assert row["weather_time"] == WEATHER_TIME
        assert row["weather_impact_signature"] == ICE_SIGNATURE
        assert row["weather_rule_version"] == WEATHER_RULE_VERSION
        # 결빙은 종방향만 깎는다
        assert row["vertical_score"] == 80.0
        assert row["longitudinal_score"] == 70.0 - RULE_CONFIG.ice_longitudinal_deduction.value
        assert row["lateral_score"] == 60.0
        assert connection.committed

    def test_copies_standard_provenance_unchanged(self, tmp_path):
        path = write_road_segment_partition(tmp_path, SNAPSHOT_DATE, [("12345", SNAPSHOT_DATE, 76)])
        connection = FakeConnection(
            weather_rows=[(76, WEATHER_TIME, CLEAR_SIGNATURE)],
            standard_rows=[STANDARD_ROW],
        )

        run_current_score_job(
            config_for(path), connection, changed_zones_only=False, rule_config=RULE_CONFIG
        )
        row = upserted_row(connection)

        # 날씨 보정은 신뢰도나 표본 수를 바꾸지 않는다
        assert (row["sample_count"], row["confidence_score"]) == (900, 0.9)
        assert row["standard_score_version"] == "1.0.0"

    def test_zone_without_weather_is_written_unadjusted(self, tmp_path):
        path = write_road_segment_partition(tmp_path, SNAPSHOT_DATE, [("12345", SNAPSHOT_DATE, 76)])
        connection = FakeConnection(weather_rows=[], standard_rows=[STANDARD_ROW])

        run_current_score_job(
            config_for(path), connection, changed_zones_only=False, rule_config=RULE_CONFIG
        )
        row = upserted_row(connection)

        assert (row["vertical_score"], row["longitudinal_score"], row["lateral_score"]) == (
            80.0,
            70.0,
            60.0,
        )
        # CHECK 제약이 세 컬럼을 한 묶음으로 NULL이길 요구한다
        assert row["weather_time"] is None
        assert row["weather_rule_version"] is None
        assert row["weather_impact_signature"] is None

    def test_skips_a_segment_without_a_zone(self, tmp_path):
        path = write_road_segment_partition(tmp_path, SNAPSHOT_DATE, [("12345", SNAPSHOT_DATE, None)])
        connection = FakeConnection(standard_rows=[STANDARD_ROW])

        summary = run_current_score_job(
            config_for(path), connection, changed_zones_only=False, rule_config=RULE_CONFIG
        )

        assert (summary.upserted_count, summary.skipped_unzoned_count) == (0, 1)
        assert connection.upserted == []

    def test_changed_zones_only_stops_when_nothing_changed(self, tmp_path):
        # road_segment 파티션을 안 만든다 — S3 읽기가 스킵 안 되면 load_segment_zones가 바로 터진다(#559).
        connection = FakeConnection(
            weather_rows=[(76, WEATHER_TIME, CLEAR_SIGNATURE)],
            standard_rows=[STANDARD_ROW],
            changed_zone_rows=[],
        )

        summary = run_current_score_job(
            config_for(tmp_path), connection, changed_zones_only=True, rule_config=RULE_CONFIG
        )

        assert summary == type(summary)(0, 0, 0, 0)
        assert connection.upserted == []

    def test_changed_zones_only_skips_s3_and_weather_lookup_when_nothing_changed(
        self, tmp_path, monkeypatch
    ):
        # 위 테스트의 간접 증명과 달리, 호출 자체를 감시해 명시적으로 증명한다(#559).
        def fail(*args, **kwargs):
            raise AssertionError("changed zone이 없으면 이 함수는 호출되면 안 된다")

        monkeypatch.setattr(current_score, "load_segment_zones", fail)
        monkeypatch.setattr(current_score, "load_latest_zone_weather", fail)
        connection = FakeConnection(changed_zone_rows=[])

        run_current_score_job(
            config_for(tmp_path), connection, changed_zones_only=True, rule_config=RULE_CONFIG
        )

    def test_changed_zones_only_narrows_the_standard_query(self, tmp_path):
        # road_segment 파티션을 안 만든다 — incremental 경로는 이제 S3 대신 location_id로 대상을 찾는다(#559).
        connection = FakeConnection(
            weather_rows=[(76, WEATHER_TIME, ICE_SIGNATURE)],
            standard_rows=[STANDARD_ROW],
            changed_zone_rows=[(76,)],
            current_score_rows=[("12345", 76), ("99999", 12)],
        )

        run_current_score_job(
            config_for(tmp_path), connection, changed_zones_only=True, rule_config=RULE_CONFIG
        )

        standard_queries = [
            (sql, parameters)
            for sql, parameters in connection.executed
            if "FROM standard_segment_comfort_score" in sql
        ]
        assert len(standard_queries) == 1
        sql, parameters = standard_queries[0]
        assert "WHERE segment_id = ANY(%s::text[])" in sql
        # 바뀐 zone 76의 segment("12345")만 조회 대상이다 — zone 12의 "99999"는 빠진다
        assert parameters == (["12345"],)

    def test_changed_zones_only_reuses_the_current_score_location_id_index(self, tmp_path):
        # location_id 인덱스(migration 0009)로 대상을 찾는 쿼리가 실제로 나가는지 파라미터까지 확인한다(#559).
        connection = FakeConnection(
            weather_rows=[(76, WEATHER_TIME, ICE_SIGNATURE)],
            standard_rows=[STANDARD_ROW],
            changed_zone_rows=[(76,)],
            current_score_rows=[("12345", 76)],
        )

        run_current_score_job(
            config_for(tmp_path), connection, changed_zones_only=True, rule_config=RULE_CONFIG
        )

        zone_target_queries = [
            (sql, parameters)
            for sql, parameters in connection.executed
            if "FROM current_segment_comfort_score WHERE" in sql
        ]
        assert len(zone_target_queries) == 1
        sql, parameters = zone_target_queries[0]
        assert "WHERE location_id = ANY(%s)" in sql
        assert parameters == ([76],)

    def test_zone_deduction_is_cached_and_matches_per_row_calculation(self, tmp_path):
        # 같은 zone(76)에 걸린 두 row 모두 adjust_comfort_scores를 매번 부른 것과 동일한 값을 받아야 한다(#559).
        path = write_road_segment_partition(
            tmp_path,
            SNAPSHOT_DATE,
            [("11111", SNAPSHOT_DATE, 76), ("22222", SNAPSHOT_DATE, 76)],
        )
        rows = [
            ("11111", 1, SCORE_AS_OF, None, 80.0, 70.0, 60.0, 900, 0.9, "1.0.0"),
            ("22222", 2, SCORE_AS_OF, None, 80.0, 70.0, 60.0, 900, 0.9, "1.0.0"),
        ]
        connection = FakeConnection(
            weather_rows=[(76, WEATHER_TIME, ICE_SIGNATURE)], standard_rows=rows
        )

        run_current_score_job(
            config_for(path), connection, changed_zones_only=False, rule_config=RULE_CONFIG
        )

        from jobs.weather_rules import adjust_comfort_scores

        expected = adjust_comfort_scores(80.0, 70.0, 60.0, frozenset({ICE}), RULE_CONFIG)
        upserted_rows = [
            dict(zip(current_score._ROW_COLUMNS, upserted, strict=True))
            for upserted in connection.upserted
        ]
        assert len(upserted_rows) == 2
        for row in upserted_rows:
            assert row["vertical_score"] == expected.vertical_score
            assert row["longitudinal_score"] == expected.longitudinal_score
            assert row["lateral_score"] == expected.lateral_score
            assert row["comfort_score"] == expected.comfort_score

    def test_takes_the_advisory_lock_before_writing(self, tmp_path):
        path = write_road_segment_partition(tmp_path, SNAPSHOT_DATE, [("12345", SNAPSHOT_DATE, 76)])
        connection = FakeConnection(
            weather_rows=[(76, WEATHER_TIME, CLEAR_SIGNATURE)], standard_rows=[STANDARD_ROW]
        )

        run_current_score_job(
            config_for(path), connection, changed_zones_only=False, rule_config=RULE_CONFIG
        )
        statements = [sql for sql, _ in connection.executed]

        assert "SELECT pg_advisory_lock(%s)" == statements[1]
        assert "SELECT pg_advisory_unlock(%s)" == statements[-1]

    def test_quarantines_an_out_of_range_row_and_still_upserts_normal_ones(self, tmp_path):
        # confidence_score를 쓰는 이유: vertical/longitudinal/lateral/comfort_score는
        # _build_row -> adjust_comfort_scores의 _clamp가 항상 [0,100]으로 잘라내므로
        # _build_row를 거치는 경로에서는 범위 위반이 구조적으로 발생할 수 없다.
        # confidence_score/sample_count는 클램프 없이 그대로 통과하므로, 여기서는
        # standard_segment_comfort_score의 CHECK 제약이 어떤 이유로든(마이그레이션
        # 변경, 직접 데이터 수정 등) 뚫렸다고 가정한 방어적 시나리오를 검증한다.
        # 격리율 25% 서킷브레이커 임계값(DEFAULT_MAX_QUARANTINE_RATE) 아래로 유지하려면
        # 정상 행이 3건 이상 필요하다 (1개 격리 / 4개 전체 = 25%는 초과가 아니라 통과).
        path = write_road_segment_partition(
            tmp_path,
            SNAPSHOT_DATE,
            [
                ("11111", SNAPSHOT_DATE, 76),
                ("22222", SNAPSHOT_DATE, 76),
                ("33333", SNAPSHOT_DATE, 76),
                ("99999", SNAPSHOT_DATE, 76),
            ],
        )
        good_rows = [
            ("11111", 1, SCORE_AS_OF, None, 80.0, 70.0, 60.0, 900, 0.9, "1.0.0"),
            ("22222", 1, SCORE_AS_OF, None, 80.0, 70.0, 60.0, 900, 0.9, "1.0.0"),
            ("33333", 1, SCORE_AS_OF, None, 80.0, 70.0, 60.0, 900, 0.9, "1.0.0"),
        ]
        bad_row = ("99999", 1, SCORE_AS_OF, None, 80.0, 70.0, 60.0, 900, 1.5, "1.0.0")
        connection = FakeConnection(weather_rows=[], standard_rows=[*good_rows, bad_row])

        summary = run_current_score_job(
            config_for(path), connection, changed_zones_only=False, rule_config=RULE_CONFIG
        )

        assert summary.upserted_count == 3
        assert summary.quarantined_count == 1
        assert len(connection.quarantined) == 1
        assert connection.quarantined[0][0] == "99999"
        assert connection.committed

    def test_circuit_breaker_trips_when_all_rows_are_quarantined(self, tmp_path):
        path = write_road_segment_partition(tmp_path, SNAPSHOT_DATE, [("12345", SNAPSHOT_DATE, 76)])
        bad_row = ("12345", 1, SCORE_AS_OF, None, 80.0, 70.0, 60.0, 900, 1.5, "1.0.0")
        connection = FakeConnection(weather_rows=[], standard_rows=[bad_row])

        with pytest.raises(current_score_quarantine.CurrentScoreCircuitBreakerTripped):
            run_current_score_job(
                config_for(path), connection, changed_zones_only=False, rule_config=RULE_CONFIG
            )

        assert connection.upserted == []
        assert not connection.committed


@pytest.mark.skipif(
    not RUN_INTEGRATION, reason="set RUN_INTEGRATION=1 to run against a real Postgres"
)
class TestCurrentScoreJobIntegration:
    def test_rerunning_updates_the_same_row(self, tmp_path):
        import psycopg2

        path = write_road_segment_partition(tmp_path, SNAPSHOT_DATE, [("12345", SNAPSHOT_DATE, 76)])
        connection = psycopg2.connect(
            host=os.environ["POSTGRES_HOST"],
            port=int(os.environ["POSTGRES_PORT"]),
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        )
        try:
            first = run_current_score_job(config_for(path), connection, changed_zones_only=False)
            second = run_current_score_job(config_for(path), connection, changed_zones_only=False)

            assert first.upserted_count == second.upserted_count
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT count(*) FROM current_segment_comfort_score WHERE segment_id = %s",
                    ("12345",),
                )
                (count,) = cursor.fetchone()
            assert count <= 6  # 차량 프로필 5개 + sentinel 0
        finally:
            connection.close()

    def test_staging_table_is_empty_after_a_successful_run(self, tmp_path):
        # MERGE 후 TRUNCATE가 실제로 실행되는지는 Fake로 검증 못 한다 — 실제 Postgres에서 확인한다(#559).
        import psycopg2

        path = write_road_segment_partition(tmp_path, SNAPSHOT_DATE, [("12345", SNAPSHOT_DATE, 76)])
        connection = psycopg2.connect(
            host=os.environ["POSTGRES_HOST"],
            port=int(os.environ["POSTGRES_PORT"]),
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        )
        try:
            run_current_score_job(config_for(path), connection, changed_zones_only=False)
            with connection.cursor() as cursor:
                cursor.execute("SELECT count(*) FROM current_segment_comfort_score_staging")
                (count,) = cursor.fetchone()
            assert count == 0
        finally:
            connection.close()
