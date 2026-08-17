# build_road_segments()와, 그 결과가 build-road-environment를 거쳐 Transform 2까지 이어지는지 확인한다.

import json
import os
import time
import urllib.parse
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest
import shapefile
import shapely
from batch_jobs.hourly_segment_feature_job import (
    HourlySegmentFeatureJobConfig,
    run_hourly_segment_feature_job,
)
from batch_jobs.pipeline import build_and_publish_environment
from batch_jobs.road_segment.build import build_road_segments
from batch_jobs.road_segment.geometry import geometry_from_wkb
from batch_jobs.schemas import PROCESSED_SENSOR_EVENT_SCHEMA
from pyproj import CRS, Transformer
from pyspark.sql import SparkSession
from shapely import wkt as shapely_wkt

# collect() timestamp가 실행 머신 로컬 타임존을 타므로 고정한다.
os.environ["TZ"] = "UTC"
time.tzset()

SNAPSHOT = date(2026, 8, 13)
INGESTED_AT = datetime(2026, 8, 13, 0, 0, 0, tzinfo=UTC)
SOURCE_VERSION = "26B"
TARGET_HOUR = datetime(2026, 8, 16, 10, 0, 0, tzinfo=UTC)
PROCESSED_AT = datetime(2026, 8, 16, 12, 0, 0, tzinfo=UTC)
FEATURE_VERSION = "v1"
RUN_ID = "run-1"

# Manhattan 근처 실제 위경도. 이 영역을 덮는 Taxi Zone 하나만 두고, 완전히 벗어난 지점도 함께 쓴다.
IN_ZONE_COORDINATES = [(-73.99, 40.67), (-73.989, 40.671)]
OUT_OF_ZONE_COORDINATES = [(-73.5, 40.0), (-73.499, 40.001)]
ZONE_BBOX = [(-74.0, 40.60), (-73.95, 40.60), (-73.95, 40.80), (-74.0, 40.80), (-74.0, 40.60)]

# 짧은 남북 도로(약 111m)의 중앙점 — 기본 Map Matching 반경(30m) 안에서 매칭돼야 한다.
BASE_LAT, BASE_LON = 40.7484, -73.9857
LAT_OFFSET = 0.0005
_FORWARD = Transformer.from_crs("EPSG:4326", "EPSG:32118", always_xy=True)


@pytest.fixture(scope="module")
def spark():
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


def lion_row(
    segment_id: str,
    coordinates: list[tuple[float, float]],
    object_id: int = 1,
    **overrides: object,
) -> dict[str, object]:
    properties = {
        "SegmentID": segment_id,
        "OBJECTID": object_id,
        "NodeIDFrom": "0047201",
        "NodeIDTo": "0047258",
        "TrafDir": "T",
        "SegmentTyp": "U",
        "FeatureTyp": "0",
        "Status": "2",
        "RW_TYPE": "1",
        "RB_Layer": "B",
        "NodeLevelF": "M",
        "NodeLevelT": "M",
        "POSTED_SPEED": "25",
        "CurveFlag": None,
        "Radius": 0,
        "Shape__Length": 300.0,
        "Street": "TEST STREET",
    }
    properties.update(overrides)
    return {
        "type": "Feature",
        "properties": properties,
        "geometry": {"type": "LineString", "coordinates": coordinates},
    }


def write_lion(path: Path, features: list[dict[str, object]]) -> None:
    path.write_text(json.dumps({"type": "FeatureCollection", "features": features}))


def write_source_files(source_dir: Path) -> None:
    source_dir.mkdir()
    write_lion(
        source_dir / "lion.geojson",
        [
            lion_row(
                "REAL-SEG-1",
                [[BASE_LON, BASE_LAT - LAT_OFFSET], [BASE_LON, BASE_LAT + LAT_OFFSET]],
                Shape__Length=364.0,
            )
        ],
    )
    empty_collection = {"type": "FeatureCollection", "features": []}
    (source_dir / "pavement.geojson").write_text(json.dumps(empty_collection))
    (source_dir / "speed_humps.geojson").write_text(json.dumps(empty_collection))
    write_taxi_zone_zip(source_dir / "taxi_zones.zip")


def write_taxi_zone_zip(output_path: Path) -> None:
    shapefile_dir = output_path.parent / "taxi-shape"
    shapefile_dir.mkdir(exist_ok=True)
    stem = shapefile_dir / "taxi_zones"
    with shapefile.Writer(str(stem), shapeType=shapefile.POLYGON) as writer:
        writer.field("LocationID", "N", decimal=0)
        writer.poly([ZONE_BBOX])
        writer.record(181)
    stem.with_suffix(".prj").write_text(CRS.from_epsg(4326).to_wkt())
    with zipfile.ZipFile(output_path, "w") as archive:
        for suffix in (".shp", ".shx", ".dbf", ".prj"):
            path = stem.with_suffix(suffix)
            archive.write(path, arcname=path.name)


def local_path_from_uri(uri: str) -> Path:
    return Path(urllib.parse.unquote(urllib.parse.urlparse(uri).path))


def sensor_row(event_id: str) -> tuple:
    return (
        event_id,
        1,
        "T1",
        0,
        TARGET_HOUR,
        TARGET_HOUR.date(),
        BASE_LAT,
        BASE_LON,
        10.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        PROCESSED_AT,
        RUN_ID,
    )


# --- build_road_segments(): transform -> geometry -> validate -> taxi zone (Spark 불필요) ---


def test_build_road_segments_produces_records_in_epsg_32118_with_taxi_zone_assigned(
    tmp_path,
) -> None:
    lion_path = tmp_path / "lion.geojson"
    write_lion(lion_path, [lion_row("0000001", IN_ZONE_COORDINATES)])
    taxi_zone_zip = tmp_path / "taxi_zones.zip"
    write_taxi_zone_zip(taxi_zone_zip)

    report = build_road_segments(lion_path, taxi_zone_zip, SNAPSHOT, SOURCE_VERSION, INGESTED_AT)

    assert len(report.records) == 1
    record = report.records[0]
    assert record.segment_id == "0000001"
    assert record.location_id == 181
    assert record.source_version == SOURCE_VERSION
    assert record.ingested_at == INGESTED_AT

    geometry = geometry_from_wkb(record.geometry_wkb)
    # EPSG:32118(NY State Plane, 미터)은 WGS84 도 단위와 달리 수십만 단위 offset을 갖는다.
    assert abs(geometry.coords[0][0]) > 10_000
    assert abs(geometry.coords[0][1]) > 10_000
    assert set(report.taxi_zones) == {181}


def test_build_road_segments_excludes_non_vehicle_segments(tmp_path) -> None:
    lion_path = tmp_path / "lion.geojson"
    write_lion(
        lion_path,
        [
            lion_row("0000001", IN_ZONE_COORDINATES),
            lion_row("0000002", IN_ZONE_COORDINATES, object_id=2, Status="3"),  # 미시공
        ],
    )
    taxi_zone_zip = tmp_path / "taxi_zones.zip"
    write_taxi_zone_zip(taxi_zone_zip)

    report = build_road_segments(lion_path, taxi_zone_zip, SNAPSHOT, SOURCE_VERSION, INGESTED_AT)

    assert [record.segment_id for record in report.records] == ["0000001"]


def test_build_road_segments_reports_segments_unmatched_to_any_taxi_zone(tmp_path) -> None:
    lion_path = tmp_path / "lion.geojson"
    write_lion(
        lion_path,
        [
            lion_row("0000001", IN_ZONE_COORDINATES),
            lion_row("0000002", OUT_OF_ZONE_COORDINATES, object_id=2),
        ],
    )
    taxi_zone_zip = tmp_path / "taxi_zones.zip"
    write_taxi_zone_zip(taxi_zone_zip)

    report = build_road_segments(lion_path, taxi_zone_zip, SNAPSHOT, SOURCE_VERSION, INGESTED_AT)

    by_id = {record.segment_id: record for record in report.records}
    assert by_id["0000001"].location_id == 181
    assert by_id["0000002"].location_id is None
    assert report.unmatched_taxi_zone_segment_ids == ("0000002",)


def test_build_road_segments_excludes_segments_with_non_positive_length(tmp_path) -> None:
    lion_path = tmp_path / "lion.geojson"
    write_lion(
        lion_path,
        [
            lion_row("0000001", IN_ZONE_COORDINATES),
            lion_row("0000002", IN_ZONE_COORDINATES, object_id=2, Shape__Length=0.0),
        ],
    )
    taxi_zone_zip = tmp_path / "taxi_zones.zip"
    write_taxi_zone_zip(taxi_zone_zip)

    report = build_road_segments(lion_path, taxi_zone_zip, SNAPSHOT, SOURCE_VERSION, INGESTED_AT)

    assert [record.segment_id for record in report.records] == ["0000001"]
    assert report.rule_failures["length_not_positive"] == ("0000002",)


# --- build-road-environment -> Manifest -> Transform 2, 경로 우회 없이 이어지는지 ---


def test_build_road_environment_output_feeds_transform2_without_bypass(spark, tmp_path) -> None:
    source_dir = tmp_path / "source"
    data_lake = tmp_path / "lake"
    write_source_files(source_dir)

    result = build_and_publish_environment(
        source_dir,
        data_lake.as_uri(),
        reference_date=SNAPSHOT,
        road_snapshot_date=SNAPSHOT,
        build_id="build-1",
    )

    # 1. Manifest 품질 지표가 실제 LION Segment 1건 + Taxi Zone 1건을 반영하는지
    assert result.manifest.quality["lion_segment_count"] == 1
    assert result.manifest.quality["taxi_zone_count"] == 1

    road_segment_uri = result.manifest.artifact("road_segment").uri
    road_segment_path = local_path_from_uri(road_segment_uri)

    # 2. geometry_wkb가 Binary이고 EPSG:32118(미터) 좌표인지
    row = duckdb.sql(
        "SELECT geometry_wkb FROM read_parquet(?) WHERE segment_id = 'REAL-SEG-1'",
        params=[str(road_segment_path)],
    ).fetchone()
    assert isinstance(row[0], (bytes, bytearray))
    geometry = shapely.from_wkb(bytes(row[0]))
    expected_x, expected_y = _FORWARD.transform(BASE_LON, BASE_LAT - LAT_OFFSET)
    actual_x, actual_y = geometry.coords[0]
    assert actual_x == pytest.approx(expected_x, abs=1.0)
    assert actual_y == pytest.approx(expected_y, abs=1.0)

    # 3. 시뮬레이터용 simulation_road_environment.geometry_wkt는 여전히 EPSG:4326(도)인지
    simulation_path = local_path_from_uri(result.manifest.artifact("simulation_road_environment").uri)
    sim_row = duckdb.sql(
        "SELECT geometry_wkt FROM read_parquet(?) WHERE segment_id = 'REAL-SEG-1'",
        params=[str(simulation_path)],
    ).fetchone()
    sim_geometry = shapely_wkt.loads(sim_row[0])
    assert sim_geometry.coords[0][0] == pytest.approx(BASE_LON, abs=1e-6)
    assert sim_geometry.coords[0][1] == pytest.approx(BASE_LAT - LAT_OFFSET, abs=1e-6)

    # 4. Transform 2가 경로를 추가로 조립하지 않고 Manifest URI를 그대로 읽어 실제 GPS를 매칭하는지
    sensor_path = str(tmp_path / "processed_sensor_event")
    spark.createDataFrame([sensor_row("e1")], PROCESSED_SENSOR_EVENT_SCHEMA).write.mode(
        "overwrite"
    ).parquet(sensor_path)

    config = HourlySegmentFeatureJobConfig.from_env(
        {
            "HOURLY_SEGMENT_FEATURE_INPUT_PATH": sensor_path,
            "HOURLY_SEGMENT_FEATURE_ROAD_SEGMENT_PATH": road_segment_uri,
            "HOURLY_SEGMENT_FEATURE_OUTPUT_PATH": str(tmp_path / "hourly_segment_features"),
        }
    )

    summary = run_hourly_segment_feature_job(
        spark, config, TARGET_HOUR, SNAPSHOT, FEATURE_VERSION, RUN_ID, PROCESSED_AT
    )

    assert summary.result_count == 1
    output_rows = spark.read.parquet(summary.output_path).collect()
    assert len(output_rows) == 1
    assert output_rows[0]["segment_id"] == "REAL-SEG-1"
    assert output_rows[0]["road_snapshot_date"] == SNAPSHOT


def test_hourly_segment_feature_job_rejects_road_segment_snapshot_date_mismatch(
    spark, tmp_path
) -> None:
    source_dir = tmp_path / "source"
    data_lake = tmp_path / "lake"
    write_source_files(source_dir)

    result = build_and_publish_environment(
        source_dir,
        data_lake.as_uri(),
        reference_date=SNAPSHOT,
        road_snapshot_date=SNAPSHOT,
        build_id="build-1",
    )
    road_segment_uri = result.manifest.artifact("road_segment").uri

    sensor_path = str(tmp_path / "processed_sensor_event")
    spark.createDataFrame([sensor_row("e1")], PROCESSED_SENSOR_EVENT_SCHEMA).write.mode(
        "overwrite"
    ).parquet(sensor_path)

    config = HourlySegmentFeatureJobConfig.from_env(
        {
            "HOURLY_SEGMENT_FEATURE_INPUT_PATH": sensor_path,
            "HOURLY_SEGMENT_FEATURE_ROAD_SEGMENT_PATH": road_segment_uri,
            "HOURLY_SEGMENT_FEATURE_OUTPUT_PATH": str(tmp_path / "hourly_segment_features"),
        }
    )

    wrong_snapshot_date = date(2020, 1, 1)
    with pytest.raises(ValueError, match="snapshot_date"):
        run_hourly_segment_feature_job(
            spark, config, TARGET_HOUR, wrong_snapshot_date, FEATURE_VERSION, RUN_ID, PROCESSED_AT
        )
