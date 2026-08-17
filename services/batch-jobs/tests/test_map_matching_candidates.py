import os
import time
from datetime import date

import pandas as pd
import pytest
import shapely
from batch_jobs.map_matching.candidates import (
    SOURCE_CRS,
    TARGET_CRS,
    RoadSegmentCandidate,
    find_segment_candidates,
    process_batch,
)
from pyproj import Transformer
from pyspark.sql import SparkSession
from pyspark.sql.types import BinaryType, DateType, StringType, StructField, StructType
from shapely import STRtree
from shapely.geometry import LineString

# collect()가 돌려주는 timestamp는 이 파이썬 프로세스의 로컬 타임존으로 변환되어,
# 고정하지 않으면 실행 머신마다(Asia/Seoul vs UTC) 값이 달라진다.
os.environ["TZ"] = "UTC"
time.tzset()

ROAD_SEGMENT_COLUMNS = (
    "segment_id",
    "snapshot_date",
    "geometry_wkb",
    "traffic_direction",
    "from_node_id",
    "to_node_id",
)
SENSOR_COLUMNS = ("event_id", "latitude", "longitude")

ROAD_SEGMENT_SCHEMA = StructType(
    [
        StructField("segment_id", StringType()),
        StructField("snapshot_date", DateType()),
        StructField("geometry_wkb", BinaryType()),
        StructField("traffic_direction", StringType()),
        StructField("from_node_id", StringType()),
        StructField("to_node_id", StringType()),
    ]
)

# LION 좌표계(EPSG:32118) 대상 지역(NYC) 근처의 임의 위경도. 이 지점을 기준으로
# EPSG:32118 좌표계에서 원하는 거리만큼 떨어진 지점에 테스트용 road segment를 둔다.
BASE_LON, BASE_LAT = -73.9857, 40.7484
_TRANSFORMER = Transformer.from_crs(SOURCE_CRS, TARGET_CRS, always_xy=True)
BASE_X, BASE_Y = _TRANSFORMER.transform(BASE_LON, BASE_LAT)


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


def offset_line(dx: float, dy: float, length: float = 100.0) -> LineString:
    x = BASE_X + dx
    y = BASE_Y + dy
    return LineString([(x, y - length / 2), (x, y + length / 2)])


def make_candidate(segment_id: str, line: LineString, **overrides: object) -> RoadSegmentCandidate:
    defaults: dict[str, object] = {
        "segment_id": segment_id,
        "snapshot_date": date(2026, 8, 11),
        "geometry_wkb": shapely.to_wkb(line),
        "traffic_direction": "T",
        "from_node_id": "N1",
        "to_node_id": "N2",
    }
    defaults.update(overrides)
    return RoadSegmentCandidate(**defaults)


def build_context(records: list[RoadSegmentCandidate]):
    geometries = shapely.from_wkb([record.geometry_wkb for record in records])
    tree = STRtree(geometries)
    transformer = Transformer.from_crs(SOURCE_CRS, TARGET_CRS, always_xy=True)
    return tree, geometries, transformer


def sensor_batch(rows: list[tuple[str, float | None, float | None]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["event_id", "latitude", "longitude"])


def road_segment_row(
    segment_id: str,
    line: LineString,
    snapshot_date_: date = date(2026, 8, 11),
) -> tuple:
    return (segment_id, snapshot_date_, shapely.to_wkb(line), "T", "N1", "N2")


# --- process_batch(): 배치 전체를 벡터화해 처리하는 핵심 로직 (Spark 불필요) ---


@pytest.mark.parametrize(
    "segment_offsets, search_radius_m, expected_distances",
    [
        ([("S1", 10.0)], 15.0, {"S1": 10.0}),  # 반경 안
        ([("A", 10.0), ("B", -10.0)], 15.0, {"A": 10.0, "B": 10.0}),  # 여러 후보
        ([("NEAR", 10.0), ("FAR", 1000.0)], 15.0, {"NEAR": 10.0}),  # 반경 밖 제외
        ([("EDGE", 15.0)], 15.0, {"EDGE": 15.0}),  # 테스트 반경 경계
        ([("EDGE", 30.0)], 30.0, {"EDGE": 30.0}),  # 운영 잠정값(30m) 경계
    ],
)
def test_process_batch_matches_expected_segments(
    segment_offsets: list[tuple[str, float]],
    search_radius_m: float,
    expected_distances: dict[str, float],
) -> None:
    records = [
        make_candidate(segment_id, offset_line(dx, 0.0)) for segment_id, dx in segment_offsets
    ]
    tree, geometries, transformer = build_context(records)
    batch = sensor_batch([("e1", BASE_LAT, BASE_LON)])

    result = process_batch(batch, tree, geometries, records, transformer, search_radius_m)
    distances = dict(zip(result["candidate_segment_id"], result["distance_m"], strict=True))

    assert distances.keys() == expected_distances.keys()
    for segment_id, expected_distance in expected_distances.items():
        assert distances[segment_id] == pytest.approx(expected_distance)


def test_process_batch_treats_invalid_gps_as_no_candidate() -> None:
    segment = make_candidate("NEAR", offset_line(5.0, 0.0))
    tree, geometries, transformer = build_context([segment])
    batch = sensor_batch(
        [
            ("null_lat", None, BASE_LON),
            ("null_lon", BASE_LAT, None),
            ("nan_lat", float("nan"), BASE_LON),
            ("out_of_range_lat", 999.0, BASE_LON),
            ("out_of_range_lon", BASE_LAT, -999.0),
            ("inf_lon", BASE_LAT, float("inf")),
            ("valid", BASE_LAT, BASE_LON),
        ]
    )

    result = process_batch(batch, tree, geometries, [segment], transformer, search_radius_m=15.0)
    by_event = result.set_index("event_id")

    for event_id in (
        "null_lat",
        "null_lon",
        "nan_lat",
        "out_of_range_lat",
        "out_of_range_lon",
        "inf_lon",
    ):
        assert pd.isna(by_event.loc[event_id, "candidate_segment_id"])
        # 후보가 없어도 어떤 snapshot으로 매칭을 시도했는지는 남는다
        assert by_event.loc[event_id, "road_snapshot_date"] == segment.snapshot_date
    assert by_event.loc["valid", "candidate_segment_id"] == "NEAR"


# --- find_segment_candidates(): Spark 파이프라인 전체 ---


def test_multiple_nearby_segments_produce_one_row_each(spark) -> None:
    road_rows = [
        road_segment_row("A", offset_line(10.0, 0.0)),
        road_segment_row("B", offset_line(-10.0, 0.0)),
        road_segment_row("C", offset_line(0.0, 10.0)),
    ]
    road_df = spark.createDataFrame(road_rows, ROAD_SEGMENT_COLUMNS)
    sensor_df = spark.createDataFrame([("e1", BASE_LAT, BASE_LON)], SENSOR_COLUMNS)

    result = find_segment_candidates(sensor_df, road_df, search_radius_m=15.0).collect()

    assert len(result) == 3
    assert {row["candidate_segment_id"] for row in result} == {"A", "B", "C"}
    assert all(row["event_id"] == "e1" for row in result)


def test_find_segment_candidates_keeps_unmatched_events_with_snapshot_date(spark) -> None:
    snapshot = date(2026, 8, 11)
    road_df = spark.createDataFrame(
        [road_segment_row("NEAR", offset_line(10.0, 0.0), snapshot_date_=snapshot)],
        ROAD_SEGMENT_COLUMNS,
    )
    sensor_df = spark.createDataFrame(
        [
            ("matched", BASE_LAT, BASE_LON),
            ("too_far", BASE_LAT + 1.0, BASE_LON),  # 반경 밖(약 111km)
            ("null_gps", None, None),
        ],
        SENSOR_COLUMNS,
    )

    result = {
        row["event_id"]: row
        for row in find_segment_candidates(sensor_df, road_df, search_radius_m=15.0).collect()
    }

    assert result["matched"]["candidate_segment_id"] == "NEAR"
    assert result["matched"]["distance_m"] == pytest.approx(10.0, abs=0.5)

    for event_id in ("too_far", "null_gps"):
        assert result[event_id]["candidate_segment_id"] is None
        assert result[event_id]["distance_m"] is None
        assert result[event_id]["candidate_geometry_wkb"] is None
        # 후보가 없어도 어떤 snapshot으로 매칭을 시도했는지는 추적할 수 있게 남긴다
        assert result[event_id]["road_snapshot_date"] == snapshot


@pytest.mark.parametrize("search_radius_m", [0.0, -1.0, float("nan"), float("inf"), float("-inf")])
def test_invalid_search_radius_is_rejected(spark, search_radius_m: float) -> None:
    road_df = spark.createDataFrame(
        [road_segment_row("A", offset_line(0.0, 0.0))], ROAD_SEGMENT_COLUMNS
    )
    sensor_df = spark.createDataFrame([("e1", BASE_LAT, BASE_LON)], SENSOR_COLUMNS)

    with pytest.raises(ValueError, match="search_radius_m"):
        find_segment_candidates(sensor_df, road_df, search_radius_m)


MULTIPLE_SNAPSHOT_ROWS = [
    road_segment_row("A", offset_line(0.0, 0.0), snapshot_date_=date(2026, 8, 11)),
    road_segment_row("B", offset_line(5.0, 0.0), snapshot_date_=date(2026, 8, 12)),
]
NULL_SNAPSHOT_ROW = [("A", None, shapely.to_wkb(offset_line(0.0, 0.0)), "T", "N1", "N2")]
MISSING_COLUMN_ROW = [("A", date(2026, 8, 11), shapely.to_wkb(offset_line(0.0, 0.0)))]
NULL_SEGMENT_ID_ROW = [
    (None, date(2026, 8, 11), shapely.to_wkb(offset_line(0.0, 0.0)), "T", "N1", "N2")
]


@pytest.mark.parametrize(
    "build_road_df, expected_message",
    [
        (
            lambda spark: spark.createDataFrame(MULTIPLE_SNAPSHOT_ROWS, ROAD_SEGMENT_COLUMNS),
            "snapshot_date",
        ),
        (
            lambda spark: spark.createDataFrame(NULL_SNAPSHOT_ROW, ROAD_SEGMENT_SCHEMA),
            "snapshot_date",
        ),
        (lambda spark: spark.createDataFrame([], ROAD_SEGMENT_SCHEMA), "empty"),
        (
            lambda spark: spark.createDataFrame(
                MISSING_COLUMN_ROW, ("segment_id", "snapshot_date", "geometry_wkb")
            ),
            "missing required columns",
        ),
        (
            lambda spark: spark.createDataFrame(NULL_SEGMENT_ID_ROW, ROAD_SEGMENT_SCHEMA),
            "null segment_id",
        ),
    ],
    ids=["multiple-snapshots", "null-snapshot", "empty", "missing-column", "null-segment-id"],
)
def test_invalid_road_segment_df_is_rejected(spark, build_road_df, expected_message) -> None:
    road_df = build_road_df(spark)
    sensor_df = spark.createDataFrame([("e1", BASE_LAT, BASE_LON)], SENSOR_COLUMNS)

    with pytest.raises(ValueError, match=expected_message):
        find_segment_candidates(sensor_df, road_df, search_radius_m=25.0)
