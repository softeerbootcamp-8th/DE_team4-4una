from io import BytesIO

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import shapely
from dashboard.zone_master import borough_outlines, zone_boroughs
from shapely.geometry import Polygon


def _square(x: float, y: float) -> bytes:
    return shapely.to_wkb(Polygon([(x, y), (x + 1, y), (x + 1, y + 1), (x, y + 1)]))


def _zone_master_bytes(rows: list[dict]) -> bytes:
    table = pa.table(
        {
            "location_id": pa.array([r["location_id"] for r in rows], pa.int64()),
            "borough": pa.array([r["borough"] for r in rows], pa.string()),
            "geometry": pa.array([r["geometry"] for r in rows], pa.binary()),
            # Present in the real file and deliberately not read.
            "zone": pa.array([r.get("zone") for r in rows], pa.string()),
        }
    )
    buffer = BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue()


def test_zone_boroughs_maps_every_labelled_zone() -> None:
    payload = _zone_master_bytes(
        [
            {"location_id": 4, "borough": "Manhattan", "geometry": _square(0, 0)},
            {"location_id": 7, "borough": "Queens", "geometry": _square(2, 2)},
        ]
    )

    assert zone_boroughs(payload) == {4: "Manhattan", 7: "Queens"}


def test_zone_boroughs_skips_zones_without_a_borough() -> None:
    """location_id 264/265 are the non-spatial TLC placeholders."""
    payload = _zone_master_bytes(
        [
            {"location_id": 4, "borough": "Manhattan", "geometry": _square(0, 0)},
            {"location_id": 264, "borough": None, "geometry": None},
        ]
    )

    assert zone_boroughs(payload) == {4: "Manhattan"}


def test_borough_outlines_merges_the_zones_of_one_borough() -> None:
    """zone_master is keyed by zone, so a borough shape only exists once its
    zones are dissolved together."""
    payload = _zone_master_bytes(
        [
            {"location_id": 4, "borough": "Manhattan", "geometry": _square(0, 0)},
            {"location_id": 12, "borough": "Manhattan", "geometry": _square(1, 0)},
            {"location_id": 7, "borough": "Queens", "geometry": _square(5, 5)},
        ]
    )

    outlines = borough_outlines(payload)

    assert [borough.name for borough in outlines] == ["Manhattan", "Queens"]
    manhattan = outlines[0]
    assert manhattan.bounds == (0.0, 0.0, 2.0, 1.0)
    assert manhattan.center == (0.5, 1.0)


def test_borough_outlines_skips_zones_without_geometry() -> None:
    payload = _zone_master_bytes(
        [
            {"location_id": 4, "borough": "Manhattan", "geometry": _square(0, 0)},
            {"location_id": 265, "borough": "Unknown", "geometry": None},
        ]
    )

    assert [borough.name for borough in borough_outlines(payload)] == ["Manhattan"]


def test_newark_airport_is_not_a_borough() -> None:
    """TLC의 EWR(location_id 1)은 뉴저지라 NYC borough가 아니다.

    polygon이 있어 걸러내지 않으면 outline이 만들어지고 selector에 끼어든다.
    """
    payload = _zone_master_bytes(
        [
            {"location_id": 1, "borough": "EWR", "geometry": _square(9, 9)},
            {"location_id": 4, "borough": "Manhattan", "geometry": _square(0, 0)},
        ]
    )

    assert zone_boroughs(payload) == {4: "Manhattan"}
    assert [borough.name for borough in borough_outlines(payload)] == ["Manhattan"]


def test_reading_rejects_a_file_missing_required_columns() -> None:
    buffer = BytesIO()
    pq.write_table(pa.table({"location_id": pa.array([1], pa.int64())}), buffer)

    with pytest.raises(ValueError, match="missing columns"):
        zone_boroughs(buffer.getvalue())
