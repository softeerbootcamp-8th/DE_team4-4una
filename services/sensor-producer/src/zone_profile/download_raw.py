"""
Download raw datasets for TLC Zone Profile features.

Outputs:
data/raw/zone_profile/
├── mappluto.csv
├── acs_block_group.csv
├── tl_2024_36_bg.zip
├── ny_wac_S000_JT00_2023.csv.gz
├── ny_xwalk.csv.gz
├── osm_poi.json
├── mta_stations.geojson
├── facilities.json
└── parks.geojson
"""

import json
import os
import time
from collections.abc import Callable
from pathlib import Path

import pandas as pd
import requests

RAW_DIR = Path("data/raw/zone_profile")

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 10

# Overpass는 요청 주체를 식별할 수 있는 User-Agent를 요구하고,
# python-requests의 기본 User-Agent는 406으로 차단한다.
USER_AGENT = "de4-zone-profile/0.1 (softeer bootcamp team4-4una)"

MAPPLUTO_QUERY_URL = (
    "https://services5.arcgis.com/GfwWNkhOj9bNBqoJ/"
    "arcgis/rest/services/MAPPLUTO/FeatureServer/0/query"
)

ACS_API_URL = "https://api.census.gov/data/2024/acs/acs5"

ACS_GEOMETRY_URL = (
    "https://www2.census.gov/geo/tiger/TIGER2024/BG/"
    "tl_2024_36_bg.zip"
)

LODES_WAC_URL = (
    "https://lehd.ces.census.gov/data/lodes/LODES8/ny/wac/"
    "ny_wac_S000_JT00_2023.csv.gz"
)

LODES_XWALK_URL = (
    "https://lehd.ces.census.gov/data/lodes/LODES8/ny/"
    "ny_xwalk.csv.gz"
)

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

MTA_URL = (
    "https://data.ny.gov/resource/"
    "5f5g-n3cz.geojson?$limit=5000"
)

# 2fpa-bnsx는 실제 데이터 없는 지도 시각화 뷰라 export가 빈 응답을 반환한다.
# 원본 테이블(67g2-p84d, 35,387 rows)을 직접 조회한다.
FACILITIES_URL = (
    "https://data.cityofnewyork.us/resource/"
    "67g2-p84d.json?$limit=50000"
)

PARKS_URL = (
    "https://data.cityofnewyork.us/api/geospatial/"
    "enfh-gkve?method=export&format=GeoJSON"
)


def request_with_retry(method, url: str, **kwargs) -> requests.Response:
    """
    일시적인 서버 과부하/타임아웃(Overpass 공개 서버에서 흔함)에 대응해
    지수 backoff로 재시도한다.
    """

    headers = kwargs.pop("headers", {})
    headers.setdefault("User-Agent", USER_AGENT)
    kwargs["headers"] = headers

    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = method(url, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as error:
            last_error = error
            print(f"Request failed (attempt {attempt}/{MAX_RETRIES}): {error}")

            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    raise last_error


def download_file(url: str, path: Path) -> None:
    """URL의 파일을 그대로 다운로드한다."""

    print(f"Downloading: {url}")

    response = request_with_retry(
        requests.get,
        url,
        timeout=180,
        stream=True,
    )

    with path.open("wb") as file:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                file.write(chunk)

    print(f"Saved: {path}")


# --------------------------------------------------
# MapPLUTO
# --------------------------------------------------

def download_mappluto() -> None:
    """
    MapPLUTO 전체 polygon을 받지 않고,
    현재 Feature 생성에 필요한 컬럼과 위경도만 수집한다.
    """

    output_path = RAW_DIR / "mappluto.csv"

    fields = [
        "OBJECTID",
        "BBL",
        "BldgArea",
        "ResArea",
        "OfficeArea",
        "ComArea",
        "RetailArea",
        "UnitsRes",
        "Latitude",
        "Longitude",
    ]

    page_size = 2000
    offset = 0
    rows = []

    while True:
        params = {
            "where": "1=1",
            "outFields": ",".join(fields),
            "returnGeometry": "false",
            "resultOffset": offset,
            "resultRecordCount": page_size,
            "orderByFields": "OBJECTID",
            "f": "json",
        }

        response = request_with_retry(
            requests.get,
            MAPPLUTO_QUERY_URL,
            params=params,
            timeout=180,
        )

        data = response.json()

        if "error" in data:
            raise RuntimeError(data["error"])

        features = data.get("features", [])

        if not features:
            break

        rows.extend(
            feature["attributes"]
            for feature in features
        )

        offset += len(features)

        print(f"MapPLUTO: {offset:,} rows")

        if len(features) < page_size:
            break

    df = pd.DataFrame(rows)

    # 컬럼명 snake_case
    df = df.rename(
        columns={
            "OBJECTID": "object_id",
            "BBL": "bbl",
            "BldgArea": "bldg_area",
            "ResArea": "res_area",
            "OfficeArea": "office_area",
            "ComArea": "commercial_area",
            "RetailArea": "retail_area",
            "UnitsRes": "residential_units",
            "Latitude": "latitude",
            "Longitude": "longitude",
        }
    )

    df.to_csv(output_path, index=False)

    print(f"Saved: {output_path}")
    print(f"Rows: {len(df):,}")


# --------------------------------------------------
# ACS
# --------------------------------------------------

def download_acs() -> None:
    """
    NY State의 모든 Census Block Group에 대해
    Zone Profile에 필요한 ACS 변수만 수집한다.
    """

    api_key = os.getenv("CENSUS_API_KEY")

    if not api_key:
        raise RuntimeError(
            "CENSUS_API_KEY 환경변수를 설정해야 합니다."
        )

    # 인구 / 가구 / 자녀 / 소득 / 주택가치 / 임대료
    variables = [
        "NAME",

        # population
        "B01003_001E",

        # household / family
        "B11001_001E",
        "B11001_002E",

        # household with people under 18
        "B11005_002E",

        # median income
        "B19013_001E",

        # median home value
        "B25077_001E",

        # median gross rent
        "B25064_001E",

        # male 65+
        "B01001_020E",
        "B01001_021E",
        "B01001_022E",
        "B01001_023E",
        "B01001_024E",
        "B01001_025E",

        # female 65+
        "B01001_044E",
        "B01001_045E",
        "B01001_046E",
        "B01001_047E",
        "B01001_048E",
        "B01001_049E",
    ]

    params = {
        "get": ",".join(variables),
        "for": "block group:*",
        "in": "state:36 county:* tract:*",
        "key": api_key,
    }

    response = request_with_retry(
        requests.get,
        ACS_API_URL,
        params=params,
        timeout=180,
    )

    data = response.json()

    df = pd.DataFrame(
        data[1:],
        columns=data[0],
    )

    output_path = RAW_DIR / "acs_block_group.csv"

    df.to_csv(
        output_path,
        index=False,
    )

    print(f"Saved: {output_path}")
    print(f"Rows: {len(df):,}")


# --------------------------------------------------
# OSM POI
# --------------------------------------------------

def download_osm_poi() -> None:
    """
    NYC bbox 내 쇼핑 / 외식·야간 / 관광 POI를 수집한다.

    bbox:
    south, west, north, east
    """

    bbox = "40.4774,-74.2591,40.9176,-73.7004"

    queries = [
        f"""
        [out:json][timeout:180];
        (
          nwr["shop"]({bbox});
        );
        out center tags;
        """,
        f"""
        [out:json][timeout:180];
        (
          nwr["amenity"~"^(restaurant|cafe|bar|pub|nightclub)$"]({bbox});
        );
        out center tags;
        """,
        f"""
        [out:json][timeout:180];
        (
          nwr["tourism"~"^(hotel|museum|attraction)$"]({bbox});
        );
        out center tags;
        """,
    ]

    elements = {}

    for index, query in enumerate(queries, start=1):
        print(f"OSM query {index}/{len(queries)}")

        response = request_with_retry(
            requests.post,
            OVERPASS_URL,
            data={"data": query},
            timeout=240,
        )

        result = response.json()

        for element in result.get("elements", []):
            key = (
                element["type"],
                element["id"],
            )
            elements[key] = element

    output = {
        "elements": list(elements.values())
    }

    output_path = RAW_DIR / "osm_poi.json"

    with output_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            output,
            file,
            ensure_ascii=False,
        )

    print(f"Saved: {output_path}")
    print(f"POIs: {len(elements):,}")


# --------------------------------------------------
# Main
# --------------------------------------------------

def run_step(name: str, step: Callable[[], None]) -> bool:
    """개별 소스 다운로드를 실행하고, 실패해도 나머지 소스를 막지 않는다."""

    # 이 소스가 어떤 이유로 실패하든(네트워크 에러, 잘못된 응답,
    # 환경변수 누락 등) 나머지 소스 다운로드는 계속 진행돼야 하므로,
    # 예외 종류를 가리지 않고 다 잡아야 한다.
    try:
        step()
        return True
    except Exception as error:  # noqa: BLE001
        print(f"FAILED: {name}: {error}")
        return False


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    steps = [
        ("mappluto", download_mappluto),
        ("acs", download_acs),
        ("acs_geometry", lambda: download_file(
            ACS_GEOMETRY_URL, RAW_DIR / "tl_2024_36_bg.zip",
        )),
        ("lodes_wac", lambda: download_file(
            LODES_WAC_URL, RAW_DIR / "ny_wac_S000_JT00_2023.csv.gz",
        )),
        ("lodes_xwalk", lambda: download_file(
            LODES_XWALK_URL, RAW_DIR / "ny_xwalk.csv.gz",
        )),
        ("osm_poi", download_osm_poi),
        ("mta", lambda: download_file(
            MTA_URL, RAW_DIR / "mta_stations.geojson",
        )),
        ("facilities", lambda: download_file(
            FACILITIES_URL, RAW_DIR / "facilities.geojson",
        )),
        ("parks", lambda: download_file(
            PARKS_URL, RAW_DIR / "parks.geojson",
        )),
    ]

    failed = [name for name, step in steps if not run_step(name, step)]

    if failed:
        print(f"Failed sources: {', '.join(failed)}")
    else:
        print("All sources downloaded successfully")


if __name__ == "__main__":
    main()
