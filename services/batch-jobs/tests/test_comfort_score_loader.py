"""Tests for batch_jobs/comfort_score/loader.py (#117, #469).

168시간 윈도우가 논리적으로만 존재하던 것을(루트 전체를 읽고 필터) 파티션 프루닝으로
바꿨다(#469). 경계가 정확한지와 결손 파티션을 견디는지를 여기서 고정한다.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from batch_jobs.comfort_score.loader import load_hourly_comfort_score_for_gold
from batch_jobs.hourly_comfort_storage import hour_output_path
from batch_jobs.schemas import HOURLY_COMFORT_SCORE_SCHEMA
from de4_core import join_uri
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

AS_OF = datetime(2026, 8, 25, 0, tzinfo=UTC)
WINDOW_HOURS = 168


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("comfort-score-loader-tests")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def _write_partition(spark, data_lake_uri: str, period_start: datetime) -> None:
    """`silver/hourly_comfort_score`의 해당 시간 파티션에 한 행을 쓴다.

    naive datetime을 넣으면 PySpark가 `time.mktime`으로 호스트 로컬 타임존 기준
    epoch을 만들어, UTC 기준인 필터 리터럴과 어긋난다. tz-aware로 넣어 고정한다.
    """
    row = (
        "S1",
        1,
        period_start,
        period_start + timedelta(hours=1),
        period_start.date(),
        50.0,
        50.0,
        50.0,
        "1.0.0",
        10,
        2,
        "run-1",
        period_start,
    )
    root = join_uri(data_lake_uri, "silver", "hourly_comfort_score")
    spark.createDataFrame([row], HOURLY_COMFORT_SCORE_SCHEMA).write.parquet(
        hour_output_path(root, period_start)
    )


def _loaded_starts(frame) -> set[str]:
    """collect()의 datetime은 로컬 타임존으로 변환되므로, 세션 tz(UTC)로 포맷해 비교한다."""
    formatted = frame.select(
        F.date_format("data_period_start", "yyyy-MM-dd HH:mm").alias("start")
    )
    return {row.start for row in formatted.collect()}


def _utc_label(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%d %H:%M")


def test_reads_exactly_the_window_partitions(spark, tmp_path):
    """윈도우는 `[as_of - 168h, as_of)`다 — 양 끝 경계가 정확해야 한다."""
    data_lake_uri = str(tmp_path)
    just_before = AS_OF - timedelta(hours=WINDOW_HOURS + 1)  # 제외
    first_in = AS_OF - timedelta(hours=WINDOW_HOURS)  # 포함 (하한 포함)
    last_in = AS_OF - timedelta(hours=1)  # 포함
    at_as_of = AS_OF  # 제외 (상한 배타)
    for period_start in (just_before, first_in, last_in, at_as_of):
        _write_partition(spark, data_lake_uri, period_start)

    frame = load_hourly_comfort_score_for_gold(
        spark, data_lake_uri, AS_OF, WINDOW_HOURS
    )

    assert _loaded_starts(frame) == {_utc_label(first_in), _utc_label(last_in)}


def test_tolerates_missing_partitions_inside_the_window(spark, tmp_path):
    """윈도우 168시간 중 두 시간만 존재해도 실패하지 않는다."""
    data_lake_uri = str(tmp_path)
    _write_partition(spark, data_lake_uri, AS_OF - timedelta(hours=100))
    _write_partition(spark, data_lake_uri, AS_OF - timedelta(hours=2))

    frame = load_hourly_comfort_score_for_gold(
        spark, data_lake_uri, AS_OF, WINDOW_HOURS
    )

    assert frame.count() == 2


def test_a_shorter_window_reads_fewer_partitions(spark, tmp_path):
    """window_hours가 경계 계산에 실제로 반영되는지 확인한다."""
    data_lake_uri = str(tmp_path)
    for hours_ago in (1, 2, 3):
        _write_partition(spark, data_lake_uri, AS_OF - timedelta(hours=hours_ago))

    frame = load_hourly_comfort_score_for_gold(spark, data_lake_uri, AS_OF, 2)

    assert _loaded_starts(frame) == {
        _utc_label(AS_OF - timedelta(hours=1)),
        _utc_label(AS_OF - timedelta(hours=2)),
    }


def test_window_is_pushed_down_to_partition_filters(spark, tmp_path, capsys):
    """윈도우를 파티션 컬럼으로 걸어야 Spark가 파일을 열기 전에 디렉터리를 걸러낸다.

    `data_period_start`(데이터 컬럼)로만 거르면 조건이 DataFilters에 들어가 모든 파일을
    연 뒤에 걸러진다 — 168시간 윈도우가 논리적으로만 존재하던 상태다(#469).
    """
    data_lake_uri = str(tmp_path)
    for hours_ago in (200, 100, 2):
        _write_partition(spark, data_lake_uri, AS_OF - timedelta(hours=hours_ago))

    frame = load_hourly_comfort_score_for_gold(
        spark, data_lake_uri, AS_OF, WINDOW_HOURS
    )
    frame.explain()

    scan_plan = capsys.readouterr().out
    partition_filters = scan_plan.split("PartitionFilters: ")[1].split("],")[0]
    assert "data_period_date" in partition_filters
    assert "hour" in partition_filters
