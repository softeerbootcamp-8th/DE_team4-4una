# jobs/current_score.py 테스트 (#216).

from __future__ import annotations

import os
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from jobs import current_score
from jobs.current_score import (
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


class FakeCursor:
    """execute한 SQL과 execute_values로 넘어온 행을 기록하는 최소 구현."""

    def __init__(self, owner):
        self.owner = owner
        self.rows: list[tuple] = []

    def execute(self, sql, parameters=None):
        self.owner.executed.append((" ".join(sql.split()), parameters))
        # 변경 zone 조회도 latest_zone_weather를 참조하므로 JOIN 쪽을 먼저 본다.
        if "JOIN current_segment_comfort_score" in sql:
            self.rows = list(self.owner.changed_zone_rows)
        elif "FROM latest_zone_weather" in sql:
            self.rows = list(self.owner.weather_rows)
        elif "FROM standard_segment_comfort_score" in sql:
            self.rows = list(self.owner.standard_rows)
        else:
            self.rows = []

    def fetchall(self):
        return self.rows

    def fetchmany(self, size):
        batch, self.rows = self.rows[:size], self.rows[size:]
        return batch

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


# execute_values는 실제 커서의 encoding까지 요구하므로, 넘어온 행만 가로채 기록한다.
@pytest.fixture(autouse=True)
def captured_upserts(monkeypatch):
    def record(cursor, sql, argslist):
        cursor.owner.upserted.extend(argslist)

    monkeypatch.setattr(current_score, "execute_values", record)


class FakeConnection:
    def __init__(self, *, weather_rows=(), standard_rows=(), changed_zone_rows=()):
        self.weather_rows = weather_rows
        self.standard_rows = standard_rows
        self.changed_zone_rows = changed_zone_rows
        self.executed: list[tuple] = []
        self.upserted: list[tuple] = []
        self.committed = False

    def cursor(self, *args, **kwargs):
        return FakeCursor(self)

    def commit(self):
        self.committed = True

    def rollback(self):
        pass


def config_for(path: Path) -> CurrentScoreJobConfig:
    return CurrentScoreJobConfig(
        road_segment_path=path,
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
        path = write_road_segment(tmp_path, [("1", SNAPSHOT_DATE, 76), ("2", SNAPSHOT_DATE, 12)])

        assert load_segment_zones(path, SNAPSHOT_DATE) == {"1": 76, "2": 12}

    def test_drops_segments_without_a_zone(self, tmp_path):
        # location_id는 nullable인데 current 테이블에서는 NOT NULL이라 행을 만들 수 없다.
        path = write_road_segment(tmp_path, [("1", SNAPSHOT_DATE, None), ("2", SNAPSHOT_DATE, 12)])

        assert load_segment_zones(path, SNAPSHOT_DATE) == {"2": 12}

    def test_rejects_another_snapshot(self, tmp_path):
        path = write_road_segment(tmp_path, [("1", date(2024, 1, 1), 76)])

        with pytest.raises(ValueError, match="expected snapshot_date"):
            load_segment_zones(path, SNAPSHOT_DATE)


class TestFindChangedZones:
    def test_returns_sorted_zones(self):
        connection = FakeConnection(changed_zone_rows=[(76,), (12,)])

        assert find_changed_zones(connection) == (12, 76)

    def test_no_change_means_no_zone(self):
        assert find_changed_zones(FakeConnection()) == ()


class TestRunCurrentScoreJob:
    def test_applies_the_zone_weather_to_the_standard_scores(self, tmp_path):
        path = write_road_segment(tmp_path, [("12345", SNAPSHOT_DATE, 76)])
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
        path = write_road_segment(tmp_path, [("12345", SNAPSHOT_DATE, 76)])
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
        path = write_road_segment(tmp_path, [("12345", SNAPSHOT_DATE, 76)])
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
        path = write_road_segment(tmp_path, [("12345", SNAPSHOT_DATE, None)])
        connection = FakeConnection(standard_rows=[STANDARD_ROW])

        summary = run_current_score_job(
            config_for(path), connection, changed_zones_only=False, rule_config=RULE_CONFIG
        )

        assert (summary.upserted_count, summary.skipped_unzoned_count) == (0, 1)
        assert connection.upserted == []

    def test_changed_zones_only_stops_when_nothing_changed(self, tmp_path):
        path = write_road_segment(tmp_path, [("12345", SNAPSHOT_DATE, 76)])
        connection = FakeConnection(
            weather_rows=[(76, WEATHER_TIME, CLEAR_SIGNATURE)],
            standard_rows=[STANDARD_ROW],
            changed_zone_rows=[],
        )

        summary = run_current_score_job(
            config_for(path), connection, changed_zones_only=True, rule_config=RULE_CONFIG
        )

        assert summary == type(summary)(0, 0, 0)
        assert connection.upserted == []

    def test_changed_zones_only_narrows_the_standard_query(self, tmp_path):
        path = write_road_segment(
            tmp_path, [("12345", SNAPSHOT_DATE, 76), ("99999", SNAPSHOT_DATE, 12)]
        )
        connection = FakeConnection(
            weather_rows=[(76, WEATHER_TIME, ICE_SIGNATURE)],
            standard_rows=[STANDARD_ROW],
            changed_zone_rows=[(76,)],
        )

        run_current_score_job(
            config_for(path), connection, changed_zones_only=True, rule_config=RULE_CONFIG
        )

        standard_queries = [
            (sql, parameters)
            for sql, parameters in connection.executed
            if "FROM standard_segment_comfort_score" in sql
        ]
        assert len(standard_queries) == 1
        sql, parameters = standard_queries[0]
        assert "WHERE segment_id = ANY(%s::text[])" in sql
        # 바뀐 zone 76의 segment만 조회 대상이다
        assert parameters == (["12345"],)

    def test_takes_the_advisory_lock_before_writing(self, tmp_path):
        path = write_road_segment(tmp_path, [("12345", SNAPSHOT_DATE, 76)])
        connection = FakeConnection(
            weather_rows=[(76, WEATHER_TIME, CLEAR_SIGNATURE)], standard_rows=[STANDARD_ROW]
        )

        run_current_score_job(
            config_for(path), connection, changed_zones_only=False, rule_config=RULE_CONFIG
        )
        statements = [sql for sql, _ in connection.executed]

        assert "SELECT pg_advisory_lock(%s)" == statements[1]
        assert "SELECT pg_advisory_unlock(%s)" == statements[-1]


@pytest.mark.skipif(
    not RUN_INTEGRATION, reason="set RUN_INTEGRATION=1 to run against a real Postgres"
)
class TestCurrentScoreJobIntegration:
    def test_rerunning_updates_the_same_row(self, tmp_path):
        import psycopg2

        path = write_road_segment(tmp_path, [("12345", SNAPSHOT_DATE, 76)])
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
