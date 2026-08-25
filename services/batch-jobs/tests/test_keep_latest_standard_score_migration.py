"""migration 0012(#503) 통합 테스트: standard가 (구간, 프로필)별 최신 세대 1행만 담는다."""

from __future__ import annotations

import os
from datetime import UTC, datetime

import psycopg2
import pytest
from batch_jobs.comfort_score.standard_writer import _MERGE_SQL
from batch_jobs.migrate import MigrationConfig, run_migrations

RUN_INTEGRATION = os.environ.get("RUN_INTEGRATION") == "1"

MIGRATION_FILENAME = "0012_keep_latest_standard_segment_comfort_score.sql"
SCHEMA = "test_503_latest_generation"

GEN_1 = datetime(2026, 8, 25, 10, 0, tzinfo=UTC)
GEN_2 = datetime(2026, 8, 25, 11, 0, tzinfo=UTC)
GEN_3 = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)

_INSERT_STANDARD = """
INSERT INTO standard_segment_comfort_score
  (segment_id, vehicle_profile_id, score_as_of, data_period_start, data_period_end,
   vertical_score, longitudinal_score, lateral_score, comfort_score, sample_count,
   confidence_score, score_version, calculated_at)
VALUES (%s, 1, %s, %s - interval '168 hour', %s, 80, 80, 80, 80, 100, 0.8, '1.0.0', %s)
"""

# 0012 뒤에는 (구간, 프로필)이 이미 있으므로 이 INSERT는 UPSERT여야 한다.
_INSERT_STAGING = """
INSERT INTO standard_segment_comfort_score_staging
  (segment_id, vehicle_profile_id, score_as_of, data_period_start, data_period_end,
   vertical_score, longitudinal_score, lateral_score, comfort_score, sample_count,
   confidence_score, score_version, calculated_at)
VALUES (%s, 1, %s, %s - interval '168 hour', %s, 80, 80, 80, 80, 100, 0.8, '1.0.0', %s)
"""

_INSERT_CURRENT = """
INSERT INTO current_segment_comfort_score
  (segment_id, vehicle_profile_id, location_id, standard_score_as_of, weather_time,
   data_period_start, vertical_score, longitudinal_score, lateral_score, comfort_score,
   sample_count, confidence_score, standard_score_version, weather_rule_version,
   weather_impact_signature, calculated_at)
VALUES (%s, 1, 181, %s, NULL, %s, 80, 80, 80, 80, 100, 0.8, '1.0.0', NULL, NULL, now())
"""


def _connect():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def _replay(cursor, *, through: str | None) -> None:
    """임시 스키마에 마이그레이션을 재생한다 — public 스키마는 건드리지 않는다.

    through가 None이면 0012 직전까지(옛 3컬럼 PK 상태), 파일명이면 그 파일까지 적용한다.
    """
    migrations_dir = MigrationConfig.from_env().migrations_dir
    for path in sorted(migrations_dir.glob("*.sql")):
        if path.name > (through or MIGRATION_FILENAME):
            break
        if through is None and path.name == MIGRATION_FILENAME:
            break
        cursor.execute(path.read_text())


@pytest.mark.skipif(
    not RUN_INTEGRATION, reason="set RUN_INTEGRATION=1 to run against a real Postgres"
)
class TestKeepLatestStandardScoreMigration:
    @staticmethod
    @pytest.fixture(scope="class", autouse=True)
    def migrated():
        # 아래 skip 검증은 이미 적용된 DB를 전제한다 — 빈 DB면 첫 호출이 applied로 잡힌다.
        connection = _connect()
        try:
            run_migrations(MigrationConfig.from_env().migrations_dir, connection)
        finally:
            connection.close()

    @staticmethod
    @pytest.fixture
    def scratch_cursor():
        """0012 직전 상태까지 재생한 임시 스키마의 커서. 항상 롤백하고 스키마를 지운다."""
        connection = _connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
                cursor.execute(f"CREATE SCHEMA {SCHEMA}")
                cursor.execute(f"SET search_path TO {SCHEMA}")
                _replay(cursor, through=None)
                yield cursor
            connection.rollback()
        finally:
            with connection.cursor() as cursor:
                cursor.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
            connection.commit()
            connection.close()

    @staticmethod
    def _apply_0012(cursor) -> None:
        migrations_dir = MigrationConfig.from_env().migrations_dir
        cursor.execute((migrations_dir / MIGRATION_FILENAME).read_text())

    def test_migration_applies_once_and_skips_on_rerun(self):
        connection = _connect()
        try:
            result = run_migrations(MigrationConfig.from_env().migrations_dir, connection)
            assert MIGRATION_FILENAME not in result.applied
            assert MIGRATION_FILENAME in result.skipped
        finally:
            connection.close()

    def test_dedup_keeps_only_the_latest_generation(self, scratch_cursor):
        for segment_id, generations in (("seg-1", (GEN_1, GEN_2, GEN_3)), ("seg-2", (GEN_1, GEN_2))):
            for as_of in generations:
                scratch_cursor.execute(_INSERT_STANDARD, (segment_id, as_of, as_of, as_of, as_of))

        self._apply_0012(scratch_cursor)

        scratch_cursor.execute(
            "SELECT segment_id, score_as_of FROM standard_segment_comfort_score ORDER BY 1"
        )
        assert scratch_cursor.fetchall() == [("seg-1", GEN_3), ("seg-2", GEN_2)]

    def test_dedup_survives_a_current_row_pinned_to_an_older_generation(self, scratch_cursor):
        """옛 3컬럼 FK였다면 여기서 DELETE가 FK 위반으로 실패한다.

        current_score_pipeline은 standard 적재 "뒤에" 도므로 갱신 직전의 current는
        항상 이전 세대를 가리킨다. GX 격리에 걸린 행과 changed_zones_only가 놓친
        행은 그보다 더 오래 옛 세대에 묶여 있다.
        """
        for as_of in (GEN_1, GEN_2):
            scratch_cursor.execute(_INSERT_STANDARD, ("seg-1", as_of, as_of, as_of, as_of))
        scratch_cursor.execute(_INSERT_CURRENT, ("seg-1", GEN_1, GEN_1))

        self._apply_0012(scratch_cursor)

        scratch_cursor.execute("SELECT score_as_of FROM standard_segment_comfort_score")
        assert scratch_cursor.fetchall() == [(GEN_2,)]

    def test_merge_keeps_the_row_count_flat_across_generations(self, scratch_cursor):
        """완료 조건: 연속 2회 실행 후 (행 수, 세대 수)가 (구간x프로필, 1)이다."""
        self._apply_0012(scratch_cursor)

        for as_of in (GEN_1, GEN_2):
            for segment_id in ("seg-1", "seg-2"):
                scratch_cursor.execute(_INSERT_STAGING, (segment_id, as_of, as_of, as_of, as_of))
            scratch_cursor.execute(_MERGE_SQL)
            scratch_cursor.execute("TRUNCATE standard_segment_comfort_score_staging")

        scratch_cursor.execute(
            "SELECT count(*), count(DISTINCT score_as_of), max(score_as_of) "
            "FROM standard_segment_comfort_score"
        )
        assert scratch_cursor.fetchone() == (2, 1, GEN_2)

    def test_primary_key_is_segment_and_vehicle_profile(self, scratch_cursor):
        self._apply_0012(scratch_cursor)

        scratch_cursor.execute(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conrelid = 'standard_segment_comfort_score'::regclass AND contype = 'p'"
        )
        assert scratch_cursor.fetchone() == ("PRIMARY KEY (segment_id, vehicle_profile_id)",)

    def test_score_as_of_stays_not_null_outside_the_primary_key(self, scratch_cursor):
        self._apply_0012(scratch_cursor)

        scratch_cursor.execute(
            "SELECT attnotnull FROM pg_attribute "
            "WHERE attrelid = 'standard_segment_comfort_score'::regclass "
            "AND attname = 'score_as_of'"
        )
        assert scratch_cursor.fetchone() == (True,)

    def test_the_only_index_is_the_primary_key(self, scratch_cursor):
        """score_as_of에 인덱스가 붙으면 HOT update가 깨진다 — 매시 UPSERT가 갱신하는
        컬럼이 전부 비인덱스일 때만 성립하기 때문이다."""
        self._apply_0012(scratch_cursor)

        scratch_cursor.execute(
            "SELECT indexname FROM pg_indexes "
            "WHERE schemaname = %s AND tablename = 'standard_segment_comfort_score'",
            (SCHEMA,),
        )
        assert [row[0] for row in scratch_cursor.fetchall()] == [
            "standard_segment_comfort_score_pkey"
        ]

    def test_hot_update_storage_parameters_are_set(self, scratch_cursor):
        self._apply_0012(scratch_cursor)

        scratch_cursor.execute(
            "SELECT reloptions FROM pg_class "
            "WHERE oid = 'standard_segment_comfort_score'::regclass"
        )
        (reloptions,) = scratch_cursor.fetchone()
        assert set(reloptions) == {"fillfactor=80", "autovacuum_vacuum_scale_factor=0.05"}

    def test_current_standard_fk_no_longer_includes_score_as_of(self, scratch_cursor):
        self._apply_0012(scratch_cursor)

        scratch_cursor.execute(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conrelid = 'current_segment_comfort_score'::regclass AND contype = 'f'"
        )
        expected = (
            "FOREIGN KEY (segment_id, vehicle_profile_id) "
            "REFERENCES standard_segment_comfort_score(segment_id, vehicle_profile_id)"
        )
        assert scratch_cursor.fetchall() == [(expected,)]

    def test_current_still_requires_the_standard_row_to_exist(self, scratch_cursor):
        # 2컬럼으로 좁혀도 (구간, 프로필) 자체가 없으면 여전히 막아야 한다.
        self._apply_0012(scratch_cursor)

        with pytest.raises(psycopg2.errors.ForeignKeyViolation):
            scratch_cursor.execute(_INSERT_CURRENT, ("seg-missing", GEN_1, GEN_1))
