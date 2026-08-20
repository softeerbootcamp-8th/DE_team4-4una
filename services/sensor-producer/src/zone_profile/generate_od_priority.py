"""
Generate OD Demand & Priority Dataset

Input
-----
NYC TLC HVFHV trip records, 2021-01 ~ 2025-12 (다운로드해 data/raw/hvfhv/에 캐시)
data/processed/zone_scores.parquet

Output
------
data/processed/od_priority.parquet

가장 수요가 많고 Comfort 개선이 필요한 상위 1000개 PU/DO 조합을 뽑아낸다. HVFHV는 월별로 DuckDB에서 PULocationID/DOLocationID만 집계해 합친다.
"""

import time
from pathlib import Path

import duckdb
import pandas as pd
import requests

# ============================================================
# Path
# ============================================================

DATA_DIR = Path("data")

RAW_HVFHV_DIR = DATA_DIR / "raw/hvfhv"
ZONE_SCORES_PATH = DATA_DIR / "processed/zone_scores.parquet"
OUTPUT_PATH = DATA_DIR / "processed/od_priority.parquet"

# ============================================================
# HVFHV 월별 URL / 다운로드
# ============================================================

HVFHV_URL_TEMPLATE = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{year}-{month:02d}.parquet"
)

START_YEAR_MONTH = (2021, 1)
END_YEAR_MONTH = (2025, 12)

# EWR(1)와 TLC 룩업 전용 placeholder(264, 265)는 zone_scores 제외 대상과 동일해 OD에서도 제외한다. PU == DO는 라우터가 다른 LION node를 골라 경로를 만들 수 있어 유지한다.
EXCLUDED_LOCATION_IDS = {1, 264, 265}

TOP_N = 1000

# CloudFront가 간헐적으로 커넥션을 끊어서 각 월을 로컬에 먼저 받아두고 DuckDB는 로컬 파일만 읽는다.
MAX_DOWNLOAD_RETRIES = 5
DOWNLOAD_RETRY_BACKOFF_SECONDS = 10.0


def build_hvfhv_urls(
    start: tuple[int, int] = START_YEAR_MONTH,
    end: tuple[int, int] = END_YEAR_MONTH,
) -> list[str]:
    urls = []
    year, month = start

    while (year, month) <= end:
        urls.append(HVFHV_URL_TEMPLATE.format(year=year, month=month))
        month += 1
        if month > 12:
            month = 1
            year += 1

    return urls


def local_hvfhv_path(url: str, raw_dir: Path = RAW_HVFHV_DIR) -> Path:
    return raw_dir / url.rsplit("/", 1)[-1]


# 이미 존재하면 다시 받지 않고, 임시 파일에 쓴 뒤 완료 시 rename해 부분 다운로드가 남지 않게 한다.
def download_hvfhv_month(url: str, path: Path) -> None:
    if path.exists():
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(f"{path.suffix}.tmp")

    print(f"Downloading: {url}")
    last_error = None

    for attempt in range(1, MAX_DOWNLOAD_RETRIES + 1):
        try:
            response = requests.get(url, timeout=180, stream=True)
            response.raise_for_status()

            with tmp_path.open("wb") as file:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        file.write(chunk)

            tmp_path.rename(path)
            print(f"Saved: {path}")
            return
        except requests.exceptions.RequestException as error:
            last_error = error
            print(f"[WARNING] {url} 다운로드 실패 ({attempt}/{MAX_DOWNLOAD_RETRIES}): {error}")
            if attempt < MAX_DOWNLOAD_RETRIES:
                time.sleep(DOWNLOAD_RETRY_BACKOFF_SECONDS * attempt)

    tmp_path.unlink(missing_ok=True)
    raise last_error


# ============================================================
# HVFHV 월별 집계
# ============================================================

# 한 달치 HVFHV Parquet에서 PULocationID/DOLocationID 조합별 trip_count만 집계한다.
def aggregate_monthly_od(path: Path) -> pd.DataFrame:
    escaped_path = str(path).replace("'", "''")
    query = f"""
        SELECT
            CAST(PULocationID AS BIGINT) AS pu_location_id,
            CAST(DOLocationID AS BIGINT) AS do_location_id,
            COUNT(*) AS trip_count
        FROM read_parquet('{escaped_path}')
        GROUP BY PULocationID, DOLocationID
    """

    connection = duckdb.connect()
    connection.execute("SET enable_progress_bar = false")
    monthly_od = connection.execute(query).df()
    connection.close()

    return monthly_od


# 월별 OD 집계를 합쳐 2021~2025 누적 trip_count를 계산한다.
def aggregate_total_od(monthly_od_frames: list[pd.DataFrame]) -> pd.DataFrame:
    combined = pd.concat(monthly_od_frames, ignore_index=True)

    total_od = (
        combined.groupby(["pu_location_id", "do_location_id"], dropna=False)["trip_count"]
        .sum()
        .reset_index()
    )

    return total_od


# ============================================================
# 유효 OD 필터링
# ============================================================

# PU/DO가 결측이거나 EXCLUDED_LOCATION_IDS에 속하는 OD를 제거한다. PU == DO는 유지한다.
def filter_valid_od(od: pd.DataFrame) -> pd.DataFrame:
    valid = od.dropna(subset=["pu_location_id", "do_location_id"]).copy()
    valid["pu_location_id"] = valid["pu_location_id"].astype("int64")
    valid["do_location_id"] = valid["do_location_id"].astype("int64")

    excluded_mask = valid["pu_location_id"].isin(EXCLUDED_LOCATION_IDS) | valid[
        "do_location_id"
    ].isin(EXCLUDED_LOCATION_IDS)

    return valid.loc[~excluded_mask].reset_index(drop=True)


# ============================================================
# zone_scores JOIN
# ============================================================

def load_zone_scores(path: Path = ZONE_SCORES_PATH) -> pd.DataFrame:
    zone_scores = pd.read_parquet(path, columns=["location_id", "comfort_relevance_score"])
    zone_scores["location_id"] = zone_scores["location_id"].astype("int64")
    return zone_scores


# zone_scores.comfort_relevance_score를 PU/DO 각각에 JOIN하고, 어느 한쪽이라도 없으면 제거한다.
def join_comfort_relevance(od: pd.DataFrame, zone_scores: pd.DataFrame) -> pd.DataFrame:
    relevance = zone_scores[["location_id", "comfort_relevance_score"]]

    joined = od.merge(
        relevance.rename(
            columns={
                "location_id": "pu_location_id",
                "comfort_relevance_score": "pu_comfort_relevance_score",
            }
        ),
        on="pu_location_id",
        how="left",
    ).merge(
        relevance.rename(
            columns={
                "location_id": "do_location_id",
                "comfort_relevance_score": "do_comfort_relevance_score",
            }
        ),
        on="do_location_id",
        how="left",
    )

    return joined.dropna(
        subset=["pu_comfort_relevance_score", "do_comfort_relevance_score"],
    ).reset_index(drop=True)


# ============================================================
# Score 계산
# ============================================================

# demand_score(trip_count percentile) * od_relevance_score(PU/DO comfort_relevance_score 평균) = priority_score.
def calculate_priority_scores(od: pd.DataFrame) -> pd.DataFrame:
    od = od.copy()

    od["demand_score"] = od["trip_count"].rank(method="average", pct=True)
    od["od_relevance_score"] = (
        od["pu_comfort_relevance_score"] + od["do_comfort_relevance_score"]
    ) / 2
    od["priority_score"] = od["demand_score"] * od["od_relevance_score"]

    return od


# ============================================================
# Top N 선정
# ============================================================

# priority_score DESC, trip_count DESC, pu_location_id ASC, do_location_id ASC로 정렬해 동점을 결정적으로 깨고 top_n을 뽑는다.
def select_top_od(od: pd.DataFrame, top_n: int = TOP_N) -> pd.DataFrame:
    ordered = od.sort_values(
        by=["priority_score", "trip_count", "pu_location_id", "do_location_id"],
        ascending=[False, False, True, True],
    )

    top = ordered.head(top_n).reset_index(drop=True)
    top["priority_rank"] = range(1, len(top) + 1)

    return top


# ============================================================
# Validation
# ============================================================

OUTPUT_COLUMNS = [
    "pu_location_id",
    "do_location_id",
    "trip_count",
    "demand_score",
    "pu_comfort_relevance_score",
    "do_comfort_relevance_score",
    "od_relevance_score",
    "priority_score",
    "priority_rank",
]

SCORE_COLUMNS = [
    "demand_score",
    "pu_comfort_relevance_score",
    "do_comfort_relevance_score",
    "od_relevance_score",
    "priority_score",
]


def validate_output(valid_od: pd.DataFrame, top_od: pd.DataFrame, top_n: int = TOP_N) -> None:
    assert len(top_od) == top_n, f"결과 행 수가 {top_n}이 아님: {len(top_od)}"

    assert not top_od.duplicated(subset=["pu_location_id", "do_location_id"]).any(), (
        "(pu_location_id, do_location_id) 중복 발생"
    )

    assert top_od["pu_location_id"].notna().all(), "pu_location_id에 NULL 존재"
    assert top_od["do_location_id"].notna().all(), "do_location_id에 NULL 존재"

    assert not top_od["pu_location_id"].isin(EXCLUDED_LOCATION_IDS).any(), (
        "제외 대상 location_id가 pu_location_id에 포함됨"
    )
    assert not top_od["do_location_id"].isin(EXCLUDED_LOCATION_IDS).any(), (
        "제외 대상 location_id가 do_location_id에 포함됨"
    )

    assert (top_od["trip_count"] > 0).all(), "trip_count가 0 이하인 행 존재"

    for col in SCORE_COLUMNS:
        assert top_od[col].between(0, 1).all(), f"{col}이 0~1 범위를 벗어남"

    assert list(top_od["priority_rank"]) == list(range(1, top_n + 1)), (
        f"priority_rank가 1~{top_n} 순열이 아님"
    )

    same_zone = top_od["pu_location_id"] == top_od["do_location_id"]

    print("\n=== OD Priority Summary ===")
    print(f"Valid OD count       : {len(valid_od):,}")
    print(f"Selected OD          : {len(top_od):,}")
    print()
    print(f"Inter-zone OD        : {(~same_zone).sum():,}")
    print(f"Same-zone OD         : {same_zone.sum():,}")
    print()
    print(f"Min priority         : {top_od['priority_score'].min():.3f}")
    print(f"Max priority         : {top_od['priority_score'].max():.3f}")


# ============================================================
# Save
# ============================================================

def save_output(top_od: pd.DataFrame, path: Path = OUTPUT_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    top_od[OUTPUT_COLUMNS].to_parquet(path, index=False)
    print(f"\nSaved: {path}")


# ============================================================
# Main
# ============================================================

def main():
    urls = build_hvfhv_urls()

    # 다운로드를 집계와 분리해, 실패한 달이 있으면 멈추고 재실행 시 남은 달만 이어받는다.
    paths = []
    failed_urls = []

    for url in urls:
        path = local_hvfhv_path(url)
        try:
            download_hvfhv_month(url, path)
        except requests.exceptions.RequestException as error:
            print(f"[FAILED] {url}: {error}")
            failed_urls.append(url)
            continue
        paths.append(path)

    if failed_urls:
        raise RuntimeError(
            f"{len(failed_urls)}개 월 다운로드 실패: {failed_urls}. "
            "다시 실행하면 이미 받은 월은 건너뛰고 실패한 월만 재시도합니다."
        )

    monthly_od_frames = []
    for path in paths:
        print(f"Aggregating: {path}")
        monthly_od_frames.append(aggregate_monthly_od(path))

    total_od = aggregate_total_od(monthly_od_frames)
    print(f"total OD combinations (raw): {len(total_od)}")

    valid_od = filter_valid_od(total_od)
    valid_od = join_comfort_relevance(valid_od, load_zone_scores())
    valid_od = calculate_priority_scores(valid_od)

    top_od = select_top_od(valid_od)

    validate_output(valid_od, top_od)
    save_output(top_od)

    print("\n=== Sample ===")
    print(top_od.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
