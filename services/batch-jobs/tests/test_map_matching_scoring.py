import math
import os
import time
from datetime import date

import pytest
import shapely
from batch_jobs.map_matching.candidates import CANDIDATE_SCHEMA, OUTPUT_COLUMNS
from batch_jobs.map_matching.scoring import score_segment_candidates
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from shapely.geometry import LineString

# collect()가 돌려주는 timestamp는 이 파이썬 프로세스의 로컬 타임존으로 변환되어,
# 고정하지 않으면 실행 머신마다(Asia/Seoul vs UTC) 값이 달라진다.
os.environ["TZ"] = "UTC"
time.tzset()

SENSOR_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("heading", DoubleType(), nullable=True),
    ]
)

SNAPSHOT = date(2026, 8, 11)


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


def bearing_line(bearing_deg: float, length: float = 100.0) -> LineString:
    """(0,0)에서 지정한 방향각(0=N, 시계방향)으로 향하는 LineString을 만든다."""
    dx = length * math.sin(math.radians(bearing_deg))
    dy = length * math.cos(math.radians(bearing_deg))
    return LineString([(0.0, 0.0), (dx, dy)])


def candidate_row(
    event_id: str,
    *,
    candidate_segment_id: str | None = "S1",
    distance_m: float | None = 10.0,
    bearing_deg: float = 0.0,
    traffic_direction: str | None = "W",
    from_node_id: str | None = "N1",
    to_node_id: str | None = "N2",
) -> tuple:
    geometry_wkb = (
        shapely.to_wkb(bearing_line(bearing_deg)) if candidate_segment_id is not None else None
    )
    return (
        event_id,
        candidate_segment_id,
        SNAPSHOT if candidate_segment_id is not None else None,
        distance_m if candidate_segment_id is not None else None,
        geometry_wkb,
        traffic_direction if candidate_segment_id is not None else None,
        from_node_id if candidate_segment_id is not None else None,
        to_node_id if candidate_segment_id is not None else None,
    )


def score_by_event(spark, candidate_rows, sensor_rows, search_radius_m=30.0, dw=0.7, hw=0.3):
    candidate_df = spark.createDataFrame(candidate_rows, CANDIDATE_SCHEMA)
    sensor_df = spark.createDataFrame(sensor_rows, SENSOR_SCHEMA)
    result = score_segment_candidates(candidate_df, sensor_df, search_radius_m, dw, hw)
    return {row["event_id"]: row for row in result.collect()}, result


@pytest.mark.parametrize(
    "rows, heading, better, worse",
    [
        (
            [
                candidate_row("e1", candidate_segment_id="NEAR", distance_m=5.0),
                candidate_row("e1", candidate_segment_id="FAR", distance_m=25.0),
            ],
            None,
            "NEAR",
            "FAR",
        ),
        (
            [
                candidate_row("e1", candidate_segment_id="ALIGNED", distance_m=10.0, bearing_deg=0.0),
                candidate_row(
                    "e1", candidate_segment_id="OPPOSITE", distance_m=10.0, bearing_deg=180.0
                ),
            ],
            0.0,
            "ALIGNED",
            "OPPOSITE",
        ),
    ],
    ids=["closer-candidate", "matching-heading"],
)
def test_better_candidate_scores_higher(spark, rows, heading, better, worse) -> None:
    result_rows = score_by_event(spark, rows, [("e1", heading)])[1].collect()
    scores = {row["candidate_segment_id"]: row["match_score"] for row in result_rows}

    assert scores[better] > scores[worse]


def test_heading_diff_wraps_around_compass_north(spark) -> None:
    # 359도와 1도의 차이는 단순 뺄셈(358)이 아니라 원형 차이(2)여야 한다.
    rows = [candidate_row("e1", bearing_deg=1.0, traffic_direction="W")]
    by_event, _ = score_by_event(spark, rows, [("e1", 359.0)])

    assert by_event["e1"]["heading_diff_deg"] == pytest.approx(2.0)


@pytest.mark.parametrize(
    "traffic_direction, heading, expected_bearing",
    [
        ("W", 10.0, 0.0),  # 순방향 그대로
        ("A", 10.0, 180.0),  # 역방향(180도 반대)
        ("T", 10.0, 0.0),  # 양방향, heading이 정방향에 더 가까움
        ("T", 170.0, 180.0),  # 양방향, heading이 역방향에 더 가까움
    ],
)
def test_traffic_direction_resolves_road_bearing(
    spark, traffic_direction: str, heading: float, expected_bearing: float
) -> None:
    rows = [candidate_row("e1", bearing_deg=0.0, traffic_direction=traffic_direction)]
    by_event, _ = score_by_event(spark, rows, [("e1", heading)])

    assert by_event["e1"]["road_bearing_deg"] == pytest.approx(expected_bearing)


def test_null_heading_uses_distance_score_only(spark) -> None:
    rows = [candidate_row("e1", distance_m=15.0)]
    by_event, _ = score_by_event(spark, rows, [("e1", None)], search_radius_m=30.0)

    row = by_event["e1"]
    assert row["heading_diff_deg"] is None
    assert row["match_score"] == pytest.approx(0.5)  # distance_score = 1 - 15/30


def test_no_candidate_row_keeps_all_scores_null(spark) -> None:
    rows = [candidate_row("e1", candidate_segment_id=None)]
    by_event, _ = score_by_event(spark, rows, [("e1", 90.0)])

    row = by_event["e1"]
    assert row["road_bearing_deg"] is None
    assert row["heading_diff_deg"] is None
    assert row["match_score"] is None


@pytest.mark.parametrize(
    "search_radius_m, distance_weight, heading_weight",
    [
        (30.0, 0.5, 0.5 + 1e-3),  # 가중치 합이 1.0이 아님
        (30.0, -0.2, 1.2),  # 합은 1.0이지만 음수 가중치가 섞임
        (0.0, 0.7, 0.3),  # 반경이 0
        (-1.0, 0.7, 0.3),  # 반경이 음수
        (float("nan"), 0.7, 0.3),  # 반경이 NaN
        (float("inf"), 0.7, 0.3),  # 반경이 inf
    ],
)
def test_invalid_settings_are_rejected(
    spark, search_radius_m: float, distance_weight: float, heading_weight: float
) -> None:
    rows = [candidate_row("e1")]
    candidate_df = spark.createDataFrame(rows, CANDIDATE_SCHEMA)
    sensor_df = spark.createDataFrame([("e1", 0.0)], SENSOR_SCHEMA)

    with pytest.raises(ValueError):
        score_segment_candidates(
            candidate_df, sensor_df, search_radius_m, distance_weight, heading_weight
        )


def test_match_score_matches_the_documented_formula(spark) -> None:
    # distance_score=0.5, heading_score=0.5 -> match_score = 0.7*0.5 + 0.3*0.5 = 0.5
    rows = [candidate_row("e1", distance_m=15.0, bearing_deg=0.0, traffic_direction="W")]
    by_event, _ = score_by_event(spark, rows, [("e1", 90.0)], search_radius_m=30.0)

    row = by_event["e1"]
    assert row["heading_diff_deg"] == pytest.approx(90.0)
    assert row["match_score"] == pytest.approx(0.5)


def test_existing_candidate_columns_are_retained(spark) -> None:
    rows = [
        candidate_row(
            "e1",
            candidate_segment_id="S1",
            distance_m=5.0,
            from_node_id="NA",
            to_node_id="NB",
        )
    ]
    by_event, _ = score_by_event(spark, rows, [("e1", 0.0)])

    row = by_event["e1"]
    for column in OUTPUT_COLUMNS:
        assert column in row.asDict()
    assert row["candidate_segment_id"] == "S1"
    assert row["distance_m"] == pytest.approx(5.0)
    assert row["candidate_from_node_id"] == "NA"
    assert row["candidate_to_node_id"] == "NB"
