"""Acquire immutable NYC road-reference source snapshots."""

from __future__ import annotations

import hashlib
import io
import json
import urllib.parse
import urllib.request
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path

import shapefile

LION_QUERY_URL = (
    "https://services5.arcgis.com/GfwWNkhOj9bNBqoJ/arcgis/rest/services/"
    "LION/FeatureServer/0/query"
)
PAVEMENT_DATASET_ID = "6yyb-pb25"
HUMP_DATASET_ID = "jknp-skuy"
TAXI_ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
USER_AGENT = "DE4-batch-jobs/0.1 (+https://github.com/softeerbootcamp-8th/DE_team4-4una)"


def fetch_reference_sources(
    output_dir: Path,
    snapshot_date: date,
    bbox: tuple[float, float, float, float] | None = None,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    retrieved_at = datetime.now(UTC)
    paths = {
        "nyc_lion": output_dir / "lion.geojson",
        "nyc_street_pavement_ratings": output_dir / "pavement.geojson",
        "nyc_speed_humps": output_dir / "speed_humps.geojson",
        "tlc_taxi_zones": output_dir / "taxi_zones.zip",
    }
    fetch_lion(paths["nyc_lion"], bbox)
    fetch_socrata(PAVEMENT_DATASET_ID, paths["nyc_street_pavement_ratings"], bbox)
    fetch_socrata(HUMP_DATASET_ID, paths["nyc_speed_humps"], bbox)
    download_file(TAXI_ZONE_URL, paths["tlc_taxi_zones"])

    source_urls = {
        "nyc_lion": LION_QUERY_URL,
        "nyc_street_pavement_ratings": (
            f"https://data.cityofnewyork.us/resource/{PAVEMENT_DATASET_ID}.geojson"
        ),
        "nyc_speed_humps": (
            f"https://data.cityofnewyork.us/resource/{HUMP_DATASET_ID}.geojson"
        ),
        "tlc_taxi_zones": TAXI_ZONE_URL,
    }
    manifest = {
        "schema_version": "1",
        "snapshot_date": snapshot_date.isoformat(),
        "retrieved_at": retrieved_at.isoformat(),
        "bbox_wgs84": list(bbox) if bbox else None,
        "sources": [
            {
                "source_id": source_id,
                "source_uri": source_urls[source_id],
                "local_file": path.name,
                "source_period_or_version": snapshot_date.isoformat(),
                "sha256": file_sha256(path),
                "size_bytes": path.stat().st_size,
                "row_count": (
                    geojson_count(path)
                    if path.suffix == ".geojson"
                    else taxi_zone_count(path)
                ),
            }
            for source_id, path in paths.items()
        ],
    }
    manifest_path = output_dir / "source_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))
    return manifest_path


def fetch_lion(
    output_path: Path,
    bbox: tuple[float, float, float, float] | None,
) -> None:
    features: list[dict[str, object]] = []
    offset = 0
    while True:
        parameters = {
            "where": "1=1",
            "outFields": (
                "SegmentID,NodeIDFrom,NodeIDTo,TrafDir,SegmentTyp,FeatureTyp,"
                "RB_Layer,NodeLevelF,NodeLevelT,POSTED_SPEED,CurveFlag,Radius,"
                "Street,OBJECTID"
            ),
            "returnGeometry": "true",
            "outSR": "4326",
            "f": "geojson",
            "resultRecordCount": "2000",
            "resultOffset": str(offset),
            "orderByFields": "OBJECTID ASC",
        }
        if bbox:
            west, south, east, north = bbox
            parameters.update(
                {
                    "geometry": f"{west},{south},{east},{north}",
                    "geometryType": "esriGeometryEnvelope",
                    "inSR": "4326",
                    "spatialRel": "esriSpatialRelIntersects",
                }
            )
        document = request_json(f"{LION_QUERY_URL}?{urllib.parse.urlencode(parameters)}")
        page = document.get("features", [])
        if not isinstance(page, list):
            raise TypeError("LION endpoint did not return a feature list")
        features.extend(page)
        if not document.get("properties", {}).get("exceededTransferLimit") or not page:
            break
        offset += len(page)
    output_path.write_text(
        json.dumps(
            {"type": "FeatureCollection", "features": features},
            separators=(",", ":"),
        )
    )


def fetch_socrata(
    dataset_id: str,
    output_path: Path,
    bbox: tuple[float, float, float, float] | None,
) -> None:
    features: list[dict[str, object]] = []
    limit = 50_000
    offset = 0
    while True:
        parameters = {
            "$limit": str(limit),
            "$offset": str(offset),
            "$order": ":id",
        }
        if bbox:
            west, south, east, north = bbox
            parameters["$where"] = (
                f"within_box(the_geom, {north}, {west}, {south}, {east})"
            )
        url = (
            f"https://data.cityofnewyork.us/resource/{dataset_id}.geojson?"
            f"{urllib.parse.urlencode(parameters)}"
        )
        document = request_json(url)
        page = document.get("features", [])
        if not isinstance(page, list):
            raise TypeError(f"Socrata dataset {dataset_id} did not return features")
        features.extend(page)
        if len(page) < limit:
            break
        offset += len(page)
    output_path.write_text(
        json.dumps(
            {"type": "FeatureCollection", "features": features},
            separators=(",", ":"),
        )
    )


def request_json(url: str) -> dict[str, object]:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=120) as response:
        document = json.load(response)
    if not isinstance(document, dict):
        raise TypeError("source endpoint did not return a JSON object")
    return document


def download_file(url: str, output_path: Path) -> None:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with (
        urllib.request.urlopen(request, timeout=120) as response,
        output_path.open("wb") as destination,
    ):
        while chunk := response.read(1024 * 1024):
            destination.write(chunk)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def geojson_count(path: Path) -> int:
    document = json.loads(path.read_text())
    features = document.get("features", [])
    return len(features) if isinstance(features, list) else 0


def taxi_zone_count(path: Path) -> int:
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        shp_name = next(name for name in names if name.lower().endswith(".shp"))
        stem = shp_name[:-4]
        reader = shapefile.Reader(
            shp=io.BytesIO(archive.read(f"{stem}.shp")),
            shx=io.BytesIO(archive.read(f"{stem}.shx")),
            dbf=io.BytesIO(archive.read(f"{stem}.dbf")),
        )
        return len(reader)
