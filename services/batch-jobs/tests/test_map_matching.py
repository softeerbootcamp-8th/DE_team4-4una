import math
import os
import time
from datetime import date

import pandas as pd
import pytest
import shapely
from batch_jobs.map_matching.candidates import (
    CANDIDATE_SCHEMA,
    OUTPUT_COLUMNS,
    SOURCE_CRS,
    TARGET_CRS,
    RoadSegmentCandidate,
    find_segment_candidates,
    process_batch,
)
from batch_jobs.map_matching.config import load_map_matching_config
from batch_jobs.map_matching.matching import (
    match_segment_candidates,
    select_best_candidates,
)
from batch_jobs.map_matching.scoring import score_segment_candidates
from batch_jobs.map_matching.selection import select_best_segment
from pyproj import Transformer
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BinaryType,
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)
from shapely import STRtree
from shapely.geometry import LineString

# collect()가 돌려주는 timestamp는 이 파이썬 프로세스의 로컬 타임존으로 변환되어,
# 고정하지 않으면 실행 머신마다(Asia/Seoul vs UTC) 값이 달라진다.
os.environ["TZ"] = "UTC"
time.tzset()

SNAPSHOT = date(2026, 8, 11)

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

SENSOR_HEADING_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("heading", DoubleType(), nullable=True),
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


class TestLoadMapMatchingConfig:
    def test_load_map_matching_config_reads_provisional_threshold(self) -> None:
        config = load_map_matching_config()

        assert config.candidate_search_radius_m.value == 30.0
        assert config.candidate_search_radius_m.provisional is True
        assert config.distance_weight.value == 0.7
        assert config.distance_weight.provisional is True
        assert config.heading_weight.value == 0.3
        assert config.heading_weight.provisional is True

    @staticmethod
    def _write_config(tmp_path, radius: float, distance_weight: float, heading_weight: float):
        path = tmp_path / "map_matching.yaml"
        path.write_text(
            f"""
candidate_search_radius_m:
  value: {radius}
  provisional: true
distance_weight:
  value: {distance_weight}
  provisional: true
heading_weight:
  value: {heading_weight}
  provisional: true
"""
        )
        return path

    @pytest.mark.parametrize(
        "radius, distance_weight, heading_weight",
        [
            (0.0, 0.7, 0.3),  # 반경이 0
            (-1.0, 0.7, 0.3),  # 반경이 음수
            (30.0, -0.2, 1.2),  # 가중치가 범위 밖(합은 1.0)
            (30.0, 1.2, -0.2),  # 가중치가 범위 밖(합은 1.0)
            (30.0, 0.5, 0.6),  # 가중치 합이 1.0이 아님
        ],
    )
    def test_load_map_matching_config_rejects_invalid_values(
        self, tmp_path, radius: float, distance_weight: float, heading_weight: float
    ) -> None:
        path = self._write_config(tmp_path, radius, distance_weight, heading_weight)

        with pytest.raises(ValueError):
            load_map_matching_config(path)


def offset_line(dx: float, dy: float, length: float = 100.0) -> LineString:
    x = BASE_X + dx
    y = BASE_Y + dy
    return LineString([(x, y - length / 2), (x, y + length / 2)])


def make_candidate(segment_id: str, line: LineString, **overrides: object) -> RoadSegmentCandidate:
    defaults: dict[str, object] = {
        "segment_id": segment_id,
        "snapshot_date": SNAPSHOT,
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
    snapshot_date_: date = SNAPSHOT,
) -> tuple:
    return (segment_id, snapshot_date_, shapely.to_wkb(line), "T", "N1", "N2")


_MULTIPLE_SNAPSHOT_ROWS = [
    road_segment_row("A", offset_line(0.0, 0.0), snapshot_date_=date(2026, 8, 11)),
    road_segment_row("B", offset_line(5.0, 0.0), snapshot_date_=date(2026, 8, 12)),
]
_NULL_SNAPSHOT_ROW = [("A", None, shapely.to_wkb(offset_line(0.0, 0.0)), "T", "N1", "N2")]
_MISSING_COLUMN_ROW = [("A", date(2026, 8, 11), shapely.to_wkb(offset_line(0.0, 0.0)))]
_NULL_SEGMENT_ID_ROW = [
    (None, date(2026, 8, 11), shapely.to_wkb(offset_line(0.0, 0.0)), "T", "N1", "N2")
]


class TestFindSegmentCandidates:
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
        self,
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

    def test_process_batch_treats_invalid_gps_as_no_candidate(self) -> None:
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

    def test_multiple_nearby_segments_produce_one_row_each(self, spark) -> None:
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

    def test_find_segment_candidates_keeps_unmatched_events_with_snapshot_date(self, spark) -> None:
        snapshot = SNAPSHOT
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
    def test_invalid_search_radius_is_rejected(self, spark, search_radius_m: float) -> None:
        road_df = spark.createDataFrame(
            [road_segment_row("A", offset_line(0.0, 0.0))], ROAD_SEGMENT_COLUMNS
        )
        sensor_df = spark.createDataFrame([("e1", BASE_LAT, BASE_LON)], SENSOR_COLUMNS)

        with pytest.raises(ValueError, match="search_radius_m"):
            find_segment_candidates(sensor_df, road_df, search_radius_m)

    @pytest.mark.parametrize(
        "build_road_df, expected_message",
        [
            (
                lambda spark: spark.createDataFrame(_MULTIPLE_SNAPSHOT_ROWS, ROAD_SEGMENT_COLUMNS),
                "snapshot_date",
            ),
            (
                lambda spark: spark.createDataFrame(_NULL_SNAPSHOT_ROW, ROAD_SEGMENT_SCHEMA),
                "snapshot_date",
            ),
            (lambda spark: spark.createDataFrame([], ROAD_SEGMENT_SCHEMA), "empty"),
            (
                lambda spark: spark.createDataFrame(
                    _MISSING_COLUMN_ROW, ("segment_id", "snapshot_date", "geometry_wkb")
                ),
                "missing required columns",
            ),
            (
                lambda spark: spark.createDataFrame(_NULL_SEGMENT_ID_ROW, ROAD_SEGMENT_SCHEMA),
                "null segment_id",
            ),
        ],
        ids=["multiple-snapshots", "null-snapshot", "empty", "missing-column", "null-segment-id"],
    )
    def test_invalid_road_segment_df_is_rejected(self, spark, build_road_df, expected_message) -> None:
        road_df = build_road_df(spark)
        sensor_df = spark.createDataFrame([("e1", BASE_LAT, BASE_LON)], SENSOR_COLUMNS)

        with pytest.raises(ValueError, match=expected_message):
            find_segment_candidates(sensor_df, road_df, search_radius_m=25.0)


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
    sensor_df = spark.createDataFrame(sensor_rows, SENSOR_HEADING_SCHEMA)
    result = score_segment_candidates(candidate_df, sensor_df, search_radius_m, dw, hw)
    return {row["event_id"]: row for row in result.collect()}, result


class TestScoreSegmentCandidates:
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
                    candidate_row(
                        "e1", candidate_segment_id="ALIGNED", distance_m=10.0, bearing_deg=0.0
                    ),
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
    def test_better_candidate_scores_higher(self, spark, rows, heading, better, worse) -> None:
        result_rows = score_by_event(spark, rows, [("e1", heading)])[1].collect()
        scores = {row["candidate_segment_id"]: row["match_score"] for row in result_rows}

        assert scores[better] > scores[worse]

    def test_heading_diff_wraps_around_compass_north(self, spark) -> None:
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
        self, spark, traffic_direction: str, heading: float, expected_bearing: float
    ) -> None:
        rows = [candidate_row("e1", bearing_deg=0.0, traffic_direction=traffic_direction)]
        by_event, _ = score_by_event(spark, rows, [("e1", heading)])

        assert by_event["e1"]["road_bearing_deg"] == pytest.approx(expected_bearing)

    def test_null_heading_uses_distance_score_only(self, spark) -> None:
        rows = [candidate_row("e1", distance_m=15.0)]
        by_event, _ = score_by_event(spark, rows, [("e1", None)], search_radius_m=30.0)

        row = by_event["e1"]
        assert row["heading_diff_deg"] is None
        assert row["match_score"] == pytest.approx(0.5)  # distance_score = 1 - 15/30

    def test_no_candidate_row_keeps_all_scores_null(self, spark) -> None:
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
        self, spark, search_radius_m: float, distance_weight: float, heading_weight: float
    ) -> None:
        rows = [candidate_row("e1")]
        candidate_df = spark.createDataFrame(rows, CANDIDATE_SCHEMA)
        sensor_df = spark.createDataFrame([("e1", 0.0)], SENSOR_HEADING_SCHEMA)

        with pytest.raises(ValueError):
            score_segment_candidates(
                candidate_df, sensor_df, search_radius_m, distance_weight, heading_weight
            )

    def test_match_score_matches_the_documented_formula(self, spark) -> None:
        # distance_score=0.5, heading_score=0.5 -> match_score = 0.7*0.5 + 0.3*0.5 = 0.5
        rows = [candidate_row("e1", distance_m=15.0, bearing_deg=0.0, traffic_direction="W")]
        by_event, _ = score_by_event(spark, rows, [("e1", 90.0)], search_radius_m=30.0)

        row = by_event["e1"]
        assert row["heading_diff_deg"] == pytest.approx(90.0)
        assert row["match_score"] == pytest.approx(0.5)

    def test_existing_candidate_columns_are_retained(self, spark) -> None:
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


def create_scored_df(spark, rows: list[tuple]):
    return spark.createDataFrame(rows, SCORED_SCHEMA)


class TestSelectBestSegment:
    def test_selects_candidate_with_highest_score(self, spark) -> None:
        rows = [
            ("E1", "S1", SNAPSHOT, 5.0, 30.0, 0.8),
            ("E1", "S2", SNAPSHOT, 8.0, 10.0, 0.9),
        ]

        result = select_best_segment(create_scored_df(spark, rows)).collect()

        assert len(result) == 1
        assert result[0]["segment_id"] == "S2"

    def test_score_tie_selects_nearest_candidate(self, spark) -> None:
        rows = [
            ("E1", "S1", SNAPSHOT, 10.0, 20.0, 0.8),
            ("E1", "S2", SNAPSHOT, 5.0, 30.0, 0.8),
        ]

        result = select_best_segment(create_scored_df(spark, rows)).first()

        assert result["segment_id"] == "S2"

    def test_distance_tie_selects_smallest_heading_diff(self, spark) -> None:
        rows = [
            ("E1", "S1", SNAPSHOT, 5.0, 30.0, 0.8),
            ("E1", "S2", SNAPSHOT, 5.0, 10.0, 0.8),
        ]

        result = select_best_segment(create_scored_df(spark, rows)).first()

        assert result["segment_id"] == "S2"

    def test_exact_tie_uses_segment_id(self, spark) -> None:
        rows = [
            ("E1", "S20", SNAPSHOT, 5.0, 10.0, 0.8),
            ("E1", "S10", SNAPSHOT, 5.0, 10.0, 0.8),
        ]

        result = select_best_segment(create_scored_df(spark, rows)).first()

        assert result["segment_id"] == "S10"

    def test_unmatched_event_is_retained(self, spark) -> None:
        rows = [("E1", None, SNAPSHOT, None, None, None)]

        result = select_best_segment(create_scored_df(spark, rows)).first()

        assert result["segment_id"] is None
        assert result["road_snapshot_date"] == SNAPSHOT
        assert result["map_match_distance_m"] is None
        assert result["map_match_heading_diff_deg"] is None
        assert result["map_match_score"] is None
        assert result["map_match_status"] == "unmatched"

    def test_each_event_produces_exactly_one_row(self, spark) -> None:
        rows = [
            ("E1", "S1", SNAPSHOT, 5.0, 30.0, 0.8),
            ("E1", "S2", SNAPSHOT, 8.0, 10.0, 0.9),
            ("E2", "S3", SNAPSHOT, 3.0, 5.0, 0.95),
            ("E3", None, SNAPSHOT, None, None, None),
        ]

        result = select_best_segment(create_scored_df(spark, rows))

        assert result.count() == 3
        assert result.groupBy("event_id").count().filter("count != 1").count() == 0

    def test_selection_is_deterministic_regardless_of_input_order(self, spark) -> None:
        rows = [
            ("E1", "S1", SNAPSHOT, 5.0, 30.0, 0.8),
            ("E1", "S2", SNAPSHOT, 8.0, 10.0, 0.9),
            ("E1", "S3", SNAPSHOT, 3.0, 5.0, 0.7),
        ]

        forward = select_best_segment(create_scored_df(spark, rows)).first()
        reversed_result = select_best_segment(create_scored_df(spark, list(reversed(rows)))).first()

        assert forward["segment_id"] == reversed_result["segment_id"] == "S2"

    def test_missing_required_column_is_rejected(self, spark) -> None:
        incomplete_schema = StructType([StructField("event_id", StringType(), nullable=False)])
        candidate_df = spark.createDataFrame([("E1",)], incomplete_schema)

        with pytest.raises(ValueError, match="missing required columns"):
            select_best_segment(candidate_df)


# #479: match_segment_candidates()가 legacy 3단계 파이프라인과 동일한 결과를 내는지 검증한다.

SENSOR_HEADING_FULL_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("latitude", DoubleType(), nullable=True),
        StructField("longitude", DoubleType(), nullable=True),
        StructField("heading", DoubleType(), nullable=True),
    ]
)


def oriented_line(distance_m: float, bearing_deg: float, length: float = 200.0) -> LineString:
    """BASE 지점에서 정확히 distance_m만큼 떨어진 곳에 bearing_deg 방향(0=N, 시계방향)으로 뻗는 직선을 만든다."""
    theta = math.radians(bearing_deg)
    direction = (math.sin(theta), math.cos(theta))
    perpendicular = (math.cos(theta), -math.sin(theta))
    center_x = BASE_X + perpendicular[0] * distance_m
    center_y = BASE_Y + perpendicular[1] * distance_m
    half = length / 2
    start = (center_x - direction[0] * half, center_y - direction[1] * half)
    end = (center_x + direction[0] * half, center_y + direction[1] * half)
    return LineString([start, end])


def run_legacy_pipeline(
    spark,
    sensor_rows: list[tuple],
    road_rows: list[tuple],
    search_radius_m: float = 30.0,
    distance_weight: float = 0.7,
    heading_weight: float = 0.3,
):
    """legacy 3단계 파이프라인 결과를 (event_id -> row dict, 펼쳐진 candidate row 수)로 반환한다."""
    road_df = spark.createDataFrame(road_rows, ROAD_SEGMENT_COLUMNS)
    sensor_df = spark.createDataFrame(sensor_rows, SENSOR_HEADING_FULL_SCHEMA)

    candidates = find_segment_candidates(sensor_df, road_df, search_radius_m)
    scored = score_segment_candidates(
        candidates, sensor_df, search_radius_m, distance_weight, heading_weight
    )
    selected = select_best_segment(scored).collect()
    return {row["event_id"]: row.asDict() for row in selected}, candidates.count()


def run_optimized_pipeline(
    spark,
    sensor_rows: list[tuple],
    road_rows: list[tuple],
    search_radius_m: float = 30.0,
    distance_weight: float = 0.7,
    heading_weight: float = 0.3,
):
    """match_segment_candidates() 결과를 (event_id -> row dict, 결과 row 수)로 반환한다."""
    road_df = spark.createDataFrame(road_rows, ROAD_SEGMENT_COLUMNS)
    sensor_df = spark.createDataFrame(sensor_rows, SENSOR_HEADING_FULL_SCHEMA)

    result = match_segment_candidates(
        sensor_df, road_df, search_radius_m, distance_weight, heading_weight
    ).collect()
    return {row["event_id"]: row.asDict() for row in result}, len(result)


def assert_same_match_result(legacy_row: dict, optimized_row: dict) -> None:
    """legacy와 optimized 결과 한 쌍이 event_id별로 완전히 동일한지 검증한다."""
    assert legacy_row["segment_id"] == optimized_row["segment_id"]
    assert legacy_row["road_snapshot_date"] == optimized_row["road_snapshot_date"]
    assert legacy_row["candidate_count"] == optimized_row["candidate_count"]
    assert legacy_row["map_match_status"] == optimized_row["map_match_status"]
    for field in ("map_match_distance_m", "map_match_heading_diff_deg", "map_match_score"):
        legacy_value, optimized_value = legacy_row[field], optimized_row[field]
        if legacy_value is None or optimized_value is None:
            assert legacy_value is None and optimized_value is None, field
        else:
            assert legacy_value == pytest.approx(optimized_value, abs=1e-9), field


class TestSelectBestCandidates:
    """select_best_candidates()가 select_best_segment()과 동일한 tie-break 순서를 갖는지 하드코딩된 값으로 검증한다."""

    @staticmethod
    def _candidates_df(rows: list[tuple]) -> pd.DataFrame:
        return pd.DataFrame(
            rows, columns=["position", "segment_id", "distance_m", "heading_diff_deg", "match_score"]
        )

    def test_selects_candidate_with_highest_score(self) -> None:
        rows = [
            (0, "S1", 5.0, 30.0, 0.8),
            (0, "S2", 8.0, 10.0, 0.9),
        ]

        best = select_best_candidates(self._candidates_df(rows))

        assert len(best) == 1
        assert best.iloc[0]["segment_id"] == "S2"

    def test_score_tie_selects_nearest_candidate(self) -> None:
        rows = [
            (0, "S1", 10.0, 20.0, 0.8),
            (0, "S2", 5.0, 30.0, 0.8),
        ]

        best = select_best_candidates(self._candidates_df(rows))

        assert best.iloc[0]["segment_id"] == "S2"

    def test_distance_tie_selects_smallest_heading_diff(self) -> None:
        rows = [
            (0, "S1", 5.0, 30.0, 0.8),
            (0, "S2", 5.0, 10.0, 0.8),
        ]

        best = select_best_candidates(self._candidates_df(rows))

        assert best.iloc[0]["segment_id"] == "S2"

    def test_exact_tie_uses_segment_id(self) -> None:
        rows = [
            (0, "S20", 5.0, 10.0, 0.8),
            (0, "S10", 5.0, 10.0, 0.8),
        ]

        best = select_best_candidates(self._candidates_df(rows))

        assert best.iloc[0]["segment_id"] == "S10"

    def test_each_position_produces_exactly_one_row(self) -> None:
        rows = [
            (0, "S1", 5.0, 30.0, 0.8),
            (0, "S2", 8.0, 10.0, 0.9),
            (1, "S3", 3.0, 5.0, 0.95),
        ]

        best = select_best_candidates(self._candidates_df(rows))

        assert len(best) == 2
        assert best.set_index("position")["segment_id"].to_dict() == {0: "S2", 1: "S3"}

    def test_selection_is_deterministic_regardless_of_input_order(self) -> None:
        rows = [
            (0, "S1", 5.0, 30.0, 0.8),
            (0, "S2", 8.0, 10.0, 0.9),
            (0, "S3", 3.0, 5.0, 0.7),
        ]

        forward = select_best_candidates(self._candidates_df(rows))
        backward = select_best_candidates(self._candidates_df(list(reversed(rows))))

        assert forward.iloc[0]["segment_id"] == backward.iloc[0]["segment_id"] == "S2"


class TestMatchSegmentCandidatesMatchesLegacy:
    """match_segment_candidates()가 legacy 3단계 파이프라인과 동일한 결과를 내는지 검증한다(#479)."""

    def test_zero_candidates_is_unmatched(self, spark) -> None:
        road_rows = [road_segment_row("FAR", offset_line(1000.0, 0.0))]
        sensor_rows = [("e1", BASE_LAT, BASE_LON, 0.0)]

        legacy, _ = run_legacy_pipeline(spark, sensor_rows, road_rows)
        optimized, row_count = run_optimized_pipeline(spark, sensor_rows, road_rows)

        assert_same_match_result(legacy["e1"], optimized["e1"])
        assert optimized["e1"]["candidate_count"] == 0
        assert optimized["e1"]["map_match_status"] == "unmatched"
        assert row_count == 1

    def test_one_candidate(self, spark) -> None:
        road_rows = [road_segment_row("S1", offset_line(10.0, 0.0), snapshot_date_=SNAPSHOT)]
        sensor_rows = [("e1", BASE_LAT, BASE_LON, 0.0)]

        legacy, _ = run_legacy_pipeline(spark, sensor_rows, road_rows)
        optimized, _ = run_optimized_pipeline(spark, sensor_rows, road_rows)

        assert_same_match_result(legacy["e1"], optimized["e1"])
        assert optimized["e1"]["segment_id"] == "S1"
        assert optimized["e1"]["candidate_count"] == 1

    def test_multiple_candidates(self, spark) -> None:
        road_rows = [
            road_segment_row("A", offset_line(10.0, 0.0)),
            road_segment_row("B", offset_line(-10.0, 0.0)),
            road_segment_row("C", offset_line(0.0, 10.0)),
        ]
        sensor_rows = [("e1", BASE_LAT, BASE_LON, 0.0)]

        legacy, legacy_candidate_rows = run_legacy_pipeline(spark, sensor_rows, road_rows)
        optimized, optimized_row_count = run_optimized_pipeline(spark, sensor_rows, road_rows)

        assert_same_match_result(legacy["e1"], optimized["e1"])
        assert optimized["e1"]["candidate_count"] == 3
        # 핵심 성능 개선 지점: legacy는 후보 수만큼(3) row가 펼쳐지지만 optimized는 event당 1행이다.
        assert legacy_candidate_rows == 3
        assert optimized_row_count == 1

    def test_distance_difference_prefers_closer_candidate(self, spark) -> None:
        # 두 후보 모두 heading과 완전히 정렬돼(heading_diff=0) match_score 차이가 distance에서만 비롯된다.
        road_rows = [
            ("NEAR", SNAPSHOT, shapely.to_wkb(oriented_line(5.0, 0.0)), "W", "N1", "N2"),
            ("FAR", SNAPSHOT, shapely.to_wkb(oriented_line(20.0, 0.0)), "W", "N1", "N2"),
        ]
        sensor_rows = [("e1", BASE_LAT, BASE_LON, 0.0)]

        legacy, _ = run_legacy_pipeline(spark, sensor_rows, road_rows)
        optimized, _ = run_optimized_pipeline(spark, sensor_rows, road_rows)

        assert_same_match_result(legacy["e1"], optimized["e1"])
        assert optimized["e1"]["segment_id"] == "NEAR"

    def test_heading_difference_prefers_aligned_candidate(self, spark) -> None:
        road_rows = [
            ("ALIGNED", SNAPSHOT, shapely.to_wkb(oriented_line(10.0, 0.0)), "W", "N1", "N2"),
            ("OFFANGLE", SNAPSHOT, shapely.to_wkb(oriented_line(10.0, 90.0)), "W", "N1", "N2"),
        ]
        sensor_rows = [("e1", BASE_LAT, BASE_LON, 0.0)]

        legacy, _ = run_legacy_pipeline(spark, sensor_rows, road_rows)
        optimized, _ = run_optimized_pipeline(spark, sensor_rows, road_rows)

        assert_same_match_result(legacy["e1"], optimized["e1"])
        assert optimized["e1"]["segment_id"] == "ALIGNED"

    def test_null_heading_uses_distance_score_only(self, spark) -> None:
        road_rows = [road_segment_row("S1", offset_line(10.0, 0.0))]
        sensor_rows = [("e1", BASE_LAT, BASE_LON, None)]

        legacy, _ = run_legacy_pipeline(spark, sensor_rows, road_rows)
        optimized, _ = run_optimized_pipeline(spark, sensor_rows, road_rows)

        assert_same_match_result(legacy["e1"], optimized["e1"])
        assert optimized["e1"]["map_match_heading_diff_deg"] is None
        assert optimized["e1"]["map_match_score"] is not None

    @pytest.mark.parametrize(
        "traffic_direction, heading, expect_diff",
        [
            ("W", 10.0, 10.0),  # 순방향(bearing=0)과 heading 차이 그대로
            ("A", 10.0, 170.0),  # 역방향(bearing=180)과의 차이
            ("T", 10.0, 10.0),  # 양방향, heading이 순방향에 더 가까움
            ("T", 170.0, 10.0),  # 양방향, heading이 역방향에 더 가까움
        ],
    )
    def test_traffic_direction_resolves_road_bearing(
        self, spark, traffic_direction: str, heading: float, expect_diff: float
    ) -> None:
        road_rows = [
            ("S1", SNAPSHOT, shapely.to_wkb(oriented_line(10.0, 0.0)), traffic_direction, "N1", "N2")
        ]
        sensor_rows = [("e1", BASE_LAT, BASE_LON, heading)]

        legacy, _ = run_legacy_pipeline(spark, sensor_rows, road_rows)
        optimized, _ = run_optimized_pipeline(spark, sensor_rows, road_rows)

        assert_same_match_result(legacy["e1"], optimized["e1"])
        assert optimized["e1"]["map_match_heading_diff_deg"] == pytest.approx(expect_diff, abs=1e-6)

    def test_tie_on_distance_falls_back_to_heading_diff(self, spark) -> None:
        # 동일 geometry(distance 동일)에 방향만 W/A로 반대라 heading_diff만 다르며, distance_weight=1.0으로 match_score까지 tie를 만들어 heading_diff tie-break를 강제한다.
        line = oriented_line(10.0, 0.0)
        road_rows = [
            ("W_DIR", SNAPSHOT, shapely.to_wkb(line), "W", "N1", "N2"),
            ("A_DIR", SNAPSHOT, shapely.to_wkb(line), "A", "N1", "N2"),
        ]
        sensor_rows = [("e1", BASE_LAT, BASE_LON, 0.0)]

        legacy, _ = run_legacy_pipeline(
            spark, sensor_rows, road_rows, distance_weight=1.0, heading_weight=0.0
        )
        optimized, _ = run_optimized_pipeline(
            spark, sensor_rows, road_rows, distance_weight=1.0, heading_weight=0.0
        )

        assert legacy["e1"]["map_match_score"] == legacy["e1"]["map_match_score"]  # sanity
        assert_same_match_result(legacy["e1"], optimized["e1"])
        assert optimized["e1"]["segment_id"] == "W_DIR"

    def test_tie_on_score_falls_back_to_distance(self, spark) -> None:
        # 같은 bearing으로 heading_diff를 동일하게 만들고 distance_weight=0.0으로 match_score까지 tie가 되게 해 distance tie-break를 강제한다.
        road_rows = [
            ("NEAR", SNAPSHOT, shapely.to_wkb(oriented_line(5.0, 0.0)), "W", "N1", "N2"),
            ("FAR", SNAPSHOT, shapely.to_wkb(oriented_line(15.0, 0.0)), "W", "N1", "N2"),
        ]
        sensor_rows = [("e1", BASE_LAT, BASE_LON, 0.0)]

        legacy, _ = run_legacy_pipeline(
            spark, sensor_rows, road_rows, distance_weight=0.0, heading_weight=1.0
        )
        optimized, _ = run_optimized_pipeline(
            spark, sensor_rows, road_rows, distance_weight=0.0, heading_weight=1.0
        )

        assert legacy["e1"]["map_match_score"] == pytest.approx(
            optimized["e1"]["map_match_score"], abs=1e-9
        )
        assert_same_match_result(legacy["e1"], optimized["e1"])
        assert optimized["e1"]["segment_id"] == "NEAR"

    def test_exact_tie_uses_segment_id_lexicographic_order(self, spark) -> None:
        # 완전히 동일한 geometry/traffic_direction이라 distance/heading_diff/match_score도 전부 동일해 segment_id tie-break만 남는다.
        line = oriented_line(10.0, 0.0)
        road_rows = [
            ("S20", SNAPSHOT, shapely.to_wkb(line), "W", "N1", "N2"),
            ("S10", SNAPSHOT, shapely.to_wkb(line), "W", "N1", "N2"),
        ]
        sensor_rows = [("e1", BASE_LAT, BASE_LON, 0.0)]

        legacy, _ = run_legacy_pipeline(spark, sensor_rows, road_rows)
        optimized, _ = run_optimized_pipeline(spark, sensor_rows, road_rows)

        assert_same_match_result(legacy["e1"], optimized["e1"])
        assert optimized["e1"]["segment_id"] == "S10"

    def test_invalid_and_null_gps_are_unmatched(self, spark) -> None:
        road_rows = [road_segment_row("NEAR", offset_line(10.0, 0.0))]
        sensor_rows = [
            ("matched", BASE_LAT, BASE_LON, 0.0),
            ("null_lat", None, BASE_LON, 0.0),
            ("null_lon", BASE_LAT, None, 0.0),
            ("nan_lat", float("nan"), BASE_LON, 0.0),
            ("out_of_range_lat", 999.0, BASE_LON, 0.0),
            ("inf_lon", BASE_LAT, float("inf"), 0.0),
        ]

        legacy, _ = run_legacy_pipeline(spark, sensor_rows, road_rows)
        optimized, _ = run_optimized_pipeline(spark, sensor_rows, road_rows)

        for event_id in legacy:
            assert_same_match_result(legacy[event_id], optimized[event_id])

        assert optimized["matched"]["map_match_status"] == "matched"
        for event_id in ("null_lat", "null_lon", "nan_lat", "out_of_range_lat", "inf_lon"):
            assert optimized[event_id]["map_match_status"] == "unmatched"
            assert optimized[event_id]["candidate_count"] == 0
            # 후보가 없어도 어떤 snapshot으로 매칭을 시도했는지는 남아야 한다.
            assert optimized[event_id]["road_snapshot_date"] == SNAPSHOT

    def test_candidate_count_matches_number_of_real_candidates(self, spark) -> None:
        road_rows = [
            road_segment_row("A", offset_line(10.0, 0.0)),
            road_segment_row("B", offset_line(-10.0, 0.0)),
            road_segment_row("C", offset_line(0.0, 10.0)),
        ]
        sensor_rows = [
            ("three_candidates", BASE_LAT, BASE_LON, 0.0),
            ("zero_candidates", BASE_LAT + 1.0, BASE_LON, 0.0),
        ]

        legacy, _ = run_legacy_pipeline(spark, sensor_rows, road_rows)
        optimized, _ = run_optimized_pipeline(spark, sensor_rows, road_rows)

        assert optimized["three_candidates"]["candidate_count"] == 3
        assert optimized["zero_candidates"]["candidate_count"] == 0
        for event_id in legacy:
            assert_same_match_result(legacy[event_id], optimized[event_id])

    def test_optimized_pipeline_returns_one_row_per_event_not_per_candidate(self, spark) -> None:
        road_rows = [
            road_segment_row("A", offset_line(10.0, 0.0)),
            road_segment_row("B", offset_line(-10.0, 0.0)),
            road_segment_row("C", offset_line(0.0, 10.0)),
            road_segment_row("D", offset_line(-1.0, 3.0)),
        ]
        sensor_rows = [
            ("e1", BASE_LAT, BASE_LON, 0.0),
            ("e2", BASE_LAT, BASE_LON, 0.0),
            ("e3", BASE_LAT + 1.0, BASE_LON, 0.0),  # 후보 없음
        ]

        road_df = spark.createDataFrame(road_rows, ROAD_SEGMENT_COLUMNS)
        sensor_df = spark.createDataFrame(sensor_rows, SENSOR_HEADING_FULL_SCHEMA)

        legacy_candidates = find_segment_candidates(sensor_df, road_df, search_radius_m=30.0)
        optimized_result = match_segment_candidates(sensor_df, road_df, 30.0, 0.7, 0.3)

        # 이벤트 2개 * 후보 4개 + 후보 없는 이벤트 1개(빈 행 1개) = 9. legacy는 이만큼 펼쳐진다.
        assert legacy_candidates.count() == 9
        # optimized는 이벤트 수(3)만큼만 반환된다 -- candidate 펼침/Window 제거를 구조적으로 증명한다.
        assert optimized_result.count() == 3
        assert optimized_result.count() == sensor_df.count()

    def test_optimized_pipeline_plan_has_no_window_stage(self, spark) -> None:
        import contextlib
        import io

        road_rows = [road_segment_row("A", offset_line(10.0, 0.0))]
        sensor_rows = [("e1", BASE_LAT, BASE_LON, 0.0)]
        road_df = spark.createDataFrame(road_rows, ROAD_SEGMENT_COLUMNS)
        sensor_df = spark.createDataFrame(sensor_rows, SENSOR_HEADING_FULL_SCHEMA)

        candidates = find_segment_candidates(sensor_df, road_df, search_radius_m=30.0)
        scored = score_segment_candidates(candidates, sensor_df, 30.0, 0.7, 0.3)
        legacy_selected = select_best_segment(scored)
        optimized_selected = match_segment_candidates(sensor_df, road_df, 30.0, 0.7, 0.3)

        def captured_plan(df) -> str:
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                df.explain(mode="extended")
            return buffer.getvalue()

        legacy_plan = captured_plan(legacy_selected)
        optimized_plan = captured_plan(optimized_selected)

        # legacy 경로엔 Window 연산자가 남지만, mapInPandas 안에서 선택까지 끝내는 optimized 경로엔 없어야 한다(#479).
        assert "Window" in legacy_plan
        assert "Window" not in optimized_plan
