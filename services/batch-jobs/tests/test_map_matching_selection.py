import os
import time
from datetime import date

import pytest
from batch_jobs.map_matching.selection import select_best_segment
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType

# collect()가 돌려주는 timestamp는 이 파이썬 프로세스의 로컬 타임존으로 변환되어,
# 고정하지 않으면 실행 머신마다(Asia/Seoul vs UTC) 값이 달라진다.
os.environ["TZ"] = "UTC"
time.tzset()

SNAPSHOT = date(2026, 8, 11)

SCORED_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("candidate_segment_id", StringType(), nullable=True),
        StructField("road_snapshot_date", DateType(), nullable=True),
        StructField("distance_m", DoubleType(), nullable=True),
        StructField("heading_diff_deg", DoubleType(), nullable=True),
        StructField("match_score", DoubleType(), nullable=True),
    ]
)


@pytest.fixture(scope="session")
def spark():
    # 세션 전체에서 재사용: SparkSession 기동에 몇 초가 걸린다.
    session = (
        SparkSession.builder.appName("batch-jobs-tests")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def create_scored_df(spark, rows: list[tuple]):
    return spark.createDataFrame(rows, SCORED_SCHEMA)


def test_selects_candidate_with_highest_score(spark) -> None:
    rows = [
        ("E1", "S1", SNAPSHOT, 5.0, 30.0, 0.8),
        ("E1", "S2", SNAPSHOT, 8.0, 10.0, 0.9),
    ]

    result = select_best_segment(create_scored_df(spark, rows)).collect()

    assert len(result) == 1
    assert result[0]["segment_id"] == "S2"


def test_score_tie_selects_nearest_candidate(spark) -> None:
    rows = [
        ("E1", "S1", SNAPSHOT, 10.0, 20.0, 0.8),
        ("E1", "S2", SNAPSHOT, 5.0, 30.0, 0.8),
    ]

    result = select_best_segment(create_scored_df(spark, rows)).first()

    assert result["segment_id"] == "S2"


def test_distance_tie_selects_smallest_heading_diff(spark) -> None:
    rows = [
        ("E1", "S1", SNAPSHOT, 5.0, 30.0, 0.8),
        ("E1", "S2", SNAPSHOT, 5.0, 10.0, 0.8),
    ]

    result = select_best_segment(create_scored_df(spark, rows)).first()

    assert result["segment_id"] == "S2"


def test_exact_tie_uses_segment_id(spark) -> None:
    rows = [
        ("E1", "S20", SNAPSHOT, 5.0, 10.0, 0.8),
        ("E1", "S10", SNAPSHOT, 5.0, 10.0, 0.8),
    ]

    result = select_best_segment(create_scored_df(spark, rows)).first()

    assert result["segment_id"] == "S10"


def test_unmatched_event_is_retained(spark) -> None:
    rows = [("E1", None, SNAPSHOT, None, None, None)]

    result = select_best_segment(create_scored_df(spark, rows)).first()

    assert result["segment_id"] is None
    assert result["road_snapshot_date"] == SNAPSHOT
    assert result["map_match_distance_m"] is None
    assert result["map_match_heading_diff_deg"] is None
    assert result["map_match_score"] is None
    assert result["map_match_status"] == "unmatched"


def test_each_event_produces_exactly_one_row(spark) -> None:
    rows = [
        ("E1", "S1", SNAPSHOT, 5.0, 30.0, 0.8),
        ("E1", "S2", SNAPSHOT, 8.0, 10.0, 0.9),
        ("E2", "S3", SNAPSHOT, 3.0, 5.0, 0.95),
        ("E3", None, SNAPSHOT, None, None, None),
    ]

    result = select_best_segment(create_scored_df(spark, rows))

    assert result.count() == 3
    assert result.groupBy("event_id").count().filter("count != 1").count() == 0


def test_selection_is_deterministic_regardless_of_input_order(spark) -> None:
    rows = [
        ("E1", "S1", SNAPSHOT, 5.0, 30.0, 0.8),
        ("E1", "S2", SNAPSHOT, 8.0, 10.0, 0.9),
        ("E1", "S3", SNAPSHOT, 3.0, 5.0, 0.7),
    ]

    forward = select_best_segment(create_scored_df(spark, rows)).first()
    reversed_result = select_best_segment(create_scored_df(spark, list(reversed(rows)))).first()

    assert forward["segment_id"] == reversed_result["segment_id"] == "S2"


def test_missing_required_column_is_rejected(spark) -> None:
    incomplete_schema = StructType([StructField("event_id", StringType(), nullable=False)])
    candidate_df = spark.createDataFrame([("E1",)], incomplete_schema)

    with pytest.raises(ValueError, match="missing required columns"):
        select_best_segment(candidate_df)
