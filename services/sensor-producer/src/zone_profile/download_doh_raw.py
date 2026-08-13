"""
Download raw NYS DOH health facility data for zone profile features.

Outputs:
data/raw/zone_profile/
├── doh_facilities.json
└── doh_certification.json
"""

from pathlib import Path

import pandas as pd
import requests

from zone_profile.download_raw import request_with_retry

RAW_DIR = Path("data/raw/zone_profile")

DOH_FACILITY_URL = "https://health.data.ny.gov/resource/vn5v-hh5r.json"
DOH_CERT_URL = "https://health.data.ny.gov/resource/2g9y-7kqm.json"


def download_socrata(url: str, limit: int = 50000) -> pd.DataFrame:
    rows = []
    offset = 0

    while True:
        params = {"$limit": limit, "$offset": offset}
        response = request_with_retry(requests.get, url, params=params, timeout=180)

        batch = response.json()

        if not batch:
            break

        rows.extend(batch)
        offset += len(batch)
        print(f"DOH downloaded: {offset:,}")

        if len(batch) < limit:
            break

    return pd.DataFrame(rows)


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    facilities = download_socrata(DOH_FACILITY_URL)
    facilities.to_json(RAW_DIR / "doh_facilities.json", orient="records")
    print(f"Saved: {RAW_DIR / 'doh_facilities.json'} ({len(facilities):,} rows)")

    certification = download_socrata(DOH_CERT_URL)
    certification.to_json(RAW_DIR / "doh_certification.json", orient="records")
    print(f"Saved: {RAW_DIR / 'doh_certification.json'} ({len(certification):,} rows)")


if __name__ == "__main__":
    main()