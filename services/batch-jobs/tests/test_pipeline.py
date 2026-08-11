import io
import json
import zipfile
from datetime import date
from pathlib import Path

import duckdb
import pytest
import shapefile
from batch_jobs.pipeline import build_and_publish_environment
from de4_core import ObjectStore, RoadEnvironmentManifest
from pyproj import CRS


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def put_object(self, **kwargs: object) -> None:
        self.objects[(str(kwargs["Bucket"]), str(kwargs["Key"]))] = kwargs["Body"]  # type: ignore[assignment]

    def get_object(self, **kwargs: object) -> dict[str, object]:
        value = self.objects[(str(kwargs["Bucket"]), str(kwargs["Key"]))]
        return {"Body": io.BytesIO(value)}

    def upload_file(self, filename: str, bucket: str, key: str) -> None:
        self.objects[(bucket, key)] = Path(filename).read_bytes()

    def download_file(self, bucket: str, key: str, filename: str) -> None:
        Path(filename).write_bytes(self.objects[(bucket, key)])

    def head_object(self, **kwargs: object) -> object:
        key = (str(kwargs["Bucket"]), str(kwargs["Key"]))
        if key not in self.objects:
            raise KeyError(key)
        return {}


def write_source_files(source_dir: Path, pavement_rating: float = 9.0) -> None:
    source_dir.mkdir()
    lion = {
        "type": "FeatureCollection",
        "features": [
            lion_feature("1001", 1, 2, -73.9900, -73.9890),
            lion_feature("1002", 2, 3, -73.9890, -73.9880),
        ],
    }
    pavement = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "onstreetna": "TEST STREET",
                    "systemrating": pavement_rating,
                    "inspectiontime": "08/01/2026 10:00:00 AM",
                },
                "geometry": {
                    "type": "LineString",
                    "coordinates": [[-73.9900, 40.6700], [-73.9890, 40.6700]],
                },
            }
        ],
    }
    humps = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"on_street": "TEST STREET", "humps": 1},
                "geometry": {
                    "type": "LineString",
                    "coordinates": [[-73.9899, 40.6700], [-73.9891, 40.6700]],
                },
            }
        ],
    }
    (source_dir / "lion.geojson").write_text(json.dumps(lion))
    (source_dir / "pavement.geojson").write_text(json.dumps(pavement))
    (source_dir / "speed_humps.geojson").write_text(json.dumps(humps))
    write_taxi_zone_zip(source_dir / "taxi_zones.zip")


def lion_feature(
    segment_id: str,
    from_node: int,
    to_node: int,
    start_lon: float,
    end_lon: float,
) -> dict[str, object]:
    return {
        "type": "Feature",
        "properties": {
            "SegmentID": segment_id,
            "NodeIDFrom": from_node,
            "NodeIDTo": to_node,
            "TrafDir": "T",
            "SegmentTyp": "B",
            "FeatureTyp": "0",
            "RB_Layer": "R",
            "NodeLevelF": "M",
            "NodeLevelT": "M",
            "POSTED_SPEED": 25,
            "CurveFlag": None,
            "Radius": None,
            "Street": "TEST STREET",
        },
        "geometry": {
            "type": "LineString",
            "coordinates": [[start_lon, 40.6700], [end_lon, 40.6700]],
        },
    }


def write_taxi_zone_zip(output_path: Path) -> None:
    shapefile_dir = output_path.parent / "taxi-shape"
    shapefile_dir.mkdir()
    stem = shapefile_dir / "taxi_zones"
    with shapefile.Writer(str(stem), shapeType=shapefile.POLYGON) as writer:
        writer.field("LocationID", "N", decimal=0)
        writer.poly(
            [
                [
                    [-74.0, 40.66],
                    [-73.98, 40.66],
                    [-73.98, 40.68],
                    [-74.0, 40.68],
                    [-74.0, 40.66],
                ]
            ]
        )
        writer.record(181)
    stem.with_suffix(".prj").write_text(CRS.from_epsg(4326).to_wkt())
    with zipfile.ZipFile(output_path, "w") as archive:
        for suffix in (".shp", ".shx", ".dbf", ".prj"):
            path = stem.with_suffix(suffix)
            archive.write(path, arcname=path.name)


def test_batch_publishes_raw_normalized_and_prepared_local_layers(tmp_path) -> None:
    source_dir = tmp_path / "source"
    data_lake = tmp_path / "lake"
    write_source_files(source_dir)

    result = build_and_publish_environment(
        source_dir,
        data_lake.as_uri(),
        date(2026, 8, 1),
        date(2026, 8, 1),
        "build-1",
        activate=True,
        minimum_pavement_segment_match_rate=0.5,
        minimum_hump_source_match_rate=1.0,
    )

    assert result.active_pointer_uri is not None
    assert (data_lake / "source/nyc_lion").is_dir()
    assert {artifact.role for artifact in result.manifest.artifacts} == {
        "road_segment",
        "enriched_segment_reference",
        "simulation_road_environment",
        "taxi_zone",
    }
    assert result.manifest.artifact("enriched_segment_reference").row_count == 2
    enriched_path = data_lake / (
        "prepared/enriched_segment_reference/reference_date=2026-08-01/"
        "build_id=build-1/part-00000.parquet"
    )
    row = duckdb.sql(
        """
        SELECT pavement_rating, pavement_condition, speed_hump_count,
               traffic_signal_count, signal_quality_flag
        FROM read_parquet(?)
        WHERE segment_id = '1001'
        """,
        params=[str(enriched_path)],
    ).fetchone()
    assert row == (9.0, "Good", 1, 0, "NOT_INCLUDED")

    with pytest.raises(FileExistsError, match="immutable road-environment build"):
        build_and_publish_environment(
            source_dir,
            data_lake.as_uri(),
            date(2026, 8, 1),
            date(2026, 8, 1),
            "build-1",
        )


def test_batch_publishes_the_same_manifest_contract_to_s3(tmp_path) -> None:
    source_dir = tmp_path / "source"
    write_source_files(source_dir, pavement_rating=7.0)
    client = FakeS3Client()
    store = ObjectStore(client)  # type: ignore[arg-type]

    result = build_and_publish_environment(
        source_dir,
        "s3://de4-reference-test",
        date(2026, 8, 1),
        date(2026, 8, 1),
        "s3-build-1",
        activate=True,
        object_store=store,
    )

    manifest = RoadEnvironmentManifest.from_json(store.read_bytes(result.manifest_uri))
    assert result.active_pointer_uri is not None
    assert manifest.environment_id == "nyc-20260801-s3-build-1"
    assert all(artifact.uri.startswith("s3://") for artifact in manifest.artifacts)
    assert len(manifest.sources) == 4
    assert (
        "de4-reference-test",
        "prepared/simulation_environment/active.json",
    ) in client.objects
