"""Integration tests for segment_comfort_score gold loading (#129).

RUN_INTEGRATION 미설정 시 skip(로컬 편의). RUN_INTEGRATION=1인데 Postgres
접속이 실패하면 skip이 아니라 fail한다 — "접속 안 되면 조용히 스킵"은
CI에서 영원히 초록불이 켜지는 결과를 낳으므로 채택하지 않는다.
"""

from __future__ import annotations

import os
import threading
import time
from datetime import UTC, datetime, timedelta

import psycopg2
import pytest
from batch_jobs.db_lock_keys import GOLD_JOB_STAGING_LOCK_KEY
from batch_jobs.migrate import MigrationConfig, run_migrations
from batch_jobs.schemas import HOURLY_COMFORT_SCORE_SCHEMA
from comfort_score.config import DEFAULT_COMFORT_SCORE_CONFIG_PATH
from comfort_score.gold_job import (
    SegmentComfortScoreJobConfig,
    build_spark_session,
    run_segment_comfort_score_job,
)
from comfort_score.gold_writer import STAGING_TABLE, TARGET_TABLE

RUN_INTEGRATION = os.environ.get("RUN_INTEGRATION") == "1"

pytestmark = pytest.mark.skipif(
    not RUN_INTEGRATION, reason="set RUN_INTEGRATION=1 to run against a real Postgres"
)


def _connect():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


@pytest.fixture(scope="module")
def spark():
    session = build_spark_session()
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def clean_tables():
    # RUN_INTEGRATION=1인데 접속이 안 되면 여기서 바로 fail한다(skip 아님) —
    # 이 fixture가 실패하면 pytest가 해당 테스트를 error로 보고하지 skip으로
    # 보고하지 않는다.
    connection = _connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"TRUNCATE {TARGET_TABLE}, {STAGING_TABLE}")
            cursor.execute(
                "DELETE FROM vehicle_profile WHERE vehicle_profile_id != 0"
            )
        connection.commit()
    finally:
        connection.close()
    yield


@pytest.fixture(scope="module", autouse=True)
def migrated():
    connection = _connect()
    try:
        run_migrations(MigrationConfig.from_env().migrations_dir, connection)
    finally:
        connection.close()


def hourly_row(**overrides):
    row = {
        "segment_id": "seg-1",
        "vehicle_profile_id": 1,
        "data_period_start": datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
        "data_period_end": datetime(2026, 8, 15, 13, 0, tzinfo=UTC),
        "road_snapshot_date": datetime(2026, 8, 1).date(),  # noqa: DTZ001
        "vertical_score": 80.0,
        "longitudinal_score": 80.0,
        "lateral_score": 80.0,
        "scoring_version": "hourly-comfort-v1",
        "sample_count": 100,
        "trip_count": 10,
        "_run_id": "test-run",
        "_processed_at": datetime(2026, 8, 15, 13, 5, tzinfo=UTC),
    }
    row.update(overrides)
    return tuple(row[field.name] for field in HOURLY_COMFORT_SCORE_SCHEMA)


def write_hourly_scores(spark, tmp_path, *rows) -> str:
    data_lake_uri = str(tmp_path)
    (
        spark.createDataFrame(list(rows), HOURLY_COMFORT_SCORE_SCHEMA)
        .write.parquet(str(tmp_path / "silver" / "hourly_comfort_score"))
    )
    return data_lake_uri


def make_config(data_lake_uri: str) -> SegmentComfortScoreJobConfig:
    env = os.environ
    return SegmentComfortScoreJobConfig(
        data_lake_uri=data_lake_uri,
        window_hours=168,
        comfort_score_config_path=DEFAULT_COMFORT_SCORE_CONFIG_PATH,
        postgres_host=env["POSTGRES_HOST"],
        postgres_port=int(env["POSTGRES_PORT"]),
        postgres_db=env["POSTGRES_DB"],
        postgres_user=env["POSTGRES_USER"],
        postgres_password=env["POSTGRES_PASSWORD"],
    )


def fetch_rows(connection) -> list[tuple]:
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT segment_id, vehicle_profile_id, comfort_score, calculated_at "
            f"FROM {TARGET_TABLE} ORDER BY segment_id, vehicle_profile_id"
        )
        return cursor.fetchall()


def test_loads_and_upserts_on_rerun_without_duplicating_rows(spark, tmp_path):
    as_of = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
    data_lake_uri = write_hourly_scores(spark, tmp_path, hourly_row())
    config = make_config(data_lake_uri)
    connection = _connect()
    try:
        first = run_segment_comfort_score_job(spark, config, as_of, connection)
        assert first.inserted_count >= 1
        first_rows = fetch_rows(connection)

        # 같은 조합을 다른 값으로 재실행 -> 행 수는 그대로, 값만 갱신
        later_as_of = as_of + timedelta(hours=1)
        write_hourly_scores(
            spark, tmp_path, hourly_row(vertical_score=0.0, longitudinal_score=0.0)
        )
        second = run_segment_comfort_score_job(spark, config, later_as_of, connection)
        second_rows = fetch_rows(connection)

        assert second.updated_count >= 1
        assert len(second_rows) == len(first_rows)
        assert second_rows != first_rows  # comfort_score/calculated_at이 바뀜
    finally:
        connection.close()


def test_fk_violation_rejected_for_unknown_vehicle_profile(spark, tmp_path):
    as_of = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
    data_lake_uri = write_hourly_scores(
        spark, tmp_path, hourly_row(vehicle_profile_id=999)  # vehicle_profile에 없는 ID
    )
    config = make_config(data_lake_uri)
    connection = _connect()
    try:
        with pytest.raises(psycopg2.errors.ForeignKeyViolation):
            run_segment_comfort_score_job(spark, config, as_of, connection)
    finally:
        connection.rollback()
        connection.close()


def test_staging_shape_check_fails_clearly_when_staging_table_is_missing(
    spark, tmp_path
):
    data_lake_uri = write_hourly_scores(spark, tmp_path, hourly_row())
    config = make_config(data_lake_uri)
    connection = _connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE {STAGING_TABLE}")
        connection.commit()

        with pytest.raises(RuntimeError, match="make migrate"):
            run_segment_comfort_score_job(
                spark, config, datetime(2026, 8, 16, 0, 0, tzinfo=UTC), connection
            )
    finally:
        connection.rollback()
        connection.close()
        # 다음 테스트를 위해 staging을 되살린다
        restore = _connect()
        try:
            run_migrations(MigrationConfig.from_env().migrations_dir, restore)
        finally:
            restore.close()


def test_concurrent_run_fails_fast_on_advisory_lock(spark, tmp_path):
    as_of = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
    data_lake_uri = write_hourly_scores(spark, tmp_path, hourly_row())
    config = make_config(data_lake_uri)

    holder = _connect()
    blocker_cursor = holder.cursor()
    blocker_cursor.execute(
        "SELECT pg_try_advisory_lock(%s)", (GOLD_JOB_STAGING_LOCK_KEY,)
    )
    assert blocker_cursor.fetchone()[0] is True

    connection = _connect()
    try:
        with pytest.raises(RuntimeError, match="staging lock"):
            run_segment_comfort_score_job(spark, config, as_of, connection)
    finally:
        connection.rollback()
        connection.close()
        blocker_cursor.execute(
            "SELECT pg_advisory_unlock(%s)", (GOLD_JOB_STAGING_LOCK_KEY,)
        )
        holder.close()


def test_reads_are_never_blocked_while_merge_runs(spark, tmp_path):
    as_of = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
    rows = tuple(
        hourly_row(segment_id=f"seg-{i}") for i in range(200)
    )  # MERGE를 조금이라도 오래 걸리게
    data_lake_uri = write_hourly_scores(spark, tmp_path, *rows)
    config = make_config(data_lake_uri)
    connection = _connect()

    read_durations: list[float] = []
    stop = threading.Event()

    def read_loop():
        reader = _connect()
        try:
            while not stop.is_set():
                started = time.monotonic()
                with reader.cursor() as cursor:
                    cursor.execute(f"SELECT count(*) FROM {TARGET_TABLE}")
                    cursor.fetchone()
                read_durations.append(time.monotonic() - started)
                time.sleep(0.01)
        finally:
            reader.close()

    reader_thread = threading.Thread(target=read_loop)
    reader_thread.start()
    try:
        run_segment_comfort_score_job(spark, config, as_of, connection)
    finally:
        stop.set()
        reader_thread.join(timeout=5)
        connection.close()

    assert read_durations  # 읽기 스레드가 실제로 최소 한 번은 실행됨
    assert max(read_durations) < 1.0  # 어떤 읽기도 1초 이상 블록되지 않음
