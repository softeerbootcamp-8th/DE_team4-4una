"""OD demand & priority dataset generation (#33)."""

import numpy as np
import pandas as pd
import pytest
from zone_profile.generate_od_priority import (
    EXCLUDED_LOCATION_IDS,
    aggregate_total_od,
    build_hvfhv_urls,
    calculate_priority_scores,
    filter_valid_od,
    join_comfort_relevance,
    select_top_od,
    validate_output,
)


def od(pairs: list[tuple], columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(pairs, columns=columns)


def test_build_hvfhv_urls_covers_the_full_range_in_order():
    urls = build_hvfhv_urls(start=(2024, 11), end=(2025, 2))

    assert urls == [
        "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-11.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-12.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2025-01.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2025-02.parquet",
    ]


def test_aggregate_total_od_sums_trip_counts_across_months():
    january = od(
        [(236, 161, 12000), (236, 236, 3200)],
        ["pu_location_id", "do_location_id", "trip_count"],
    )
    february = od(
        [(236, 161, 13000), (236, 236, 3500), (161, 87, 9100)],
        ["pu_location_id", "do_location_id", "trip_count"],
    )

    total = aggregate_total_od([january, february])
    total = total.set_index(["pu_location_id", "do_location_id"])["trip_count"]

    assert total.loc[(236, 161)] == 25000
    assert total.loc[(236, 236)] == 6700
    assert total.loc[(161, 87)] == 9100


def test_filter_valid_od_drops_null_and_excluded_locations_but_keeps_same_zone():
    total = od(
        [
            (236, 161, 100),
            (236, 236, 50),  # same-zone: kept
            (1, 236, 30),  # EWR: dropped
            (236, 264, 30),  # placeholder: dropped
            (np.nan, 236, 30),  # null PU: dropped
            (236, np.nan, 30),  # null DO: dropped
        ],
        ["pu_location_id", "do_location_id", "trip_count"],
    )

    valid = filter_valid_od(total)

    assert set(zip(valid["pu_location_id"], valid["do_location_id"], strict=True)) == {(236, 161), (236, 236)}
    assert not valid["pu_location_id"].isin(EXCLUDED_LOCATION_IDS).any()
    assert not valid["do_location_id"].isin(EXCLUDED_LOCATION_IDS).any()


def test_join_comfort_relevance_drops_od_missing_either_side():
    valid_od = od(
        [(236, 161, 100), (236, 999, 50)],
        ["pu_location_id", "do_location_id", "trip_count"],
    )
    zone_scores = pd.DataFrame(
        {"location_id": [236, 161], "comfort_relevance_score": [0.87, 0.74]}
    )

    joined = join_comfort_relevance(valid_od, zone_scores)

    assert len(joined) == 1
    row = joined.iloc[0]
    assert row["pu_comfort_relevance_score"] == pytest.approx(0.87)
    assert row["do_comfort_relevance_score"] == pytest.approx(0.74)


def test_calculate_priority_scores_averages_relevance_and_multiplies_by_demand():
    joined = od(
        [
            (236, 161, 100, 0.87, 0.74),
            (161, 87, 10, 0.60, 0.50),
        ],
        [
            "pu_location_id",
            "do_location_id",
            "trip_count",
            "pu_comfort_relevance_score",
            "do_comfort_relevance_score",
        ],
    )

    scored = calculate_priority_scores(joined)
    high_demand = scored[scored["trip_count"] == 100].iloc[0]

    assert high_demand["od_relevance_score"] == pytest.approx((0.87 + 0.74) / 2)
    assert high_demand["demand_score"] == 1.0
    assert high_demand["priority_score"] == pytest.approx(1.0 * (0.87 + 0.74) / 2)


def test_select_top_od_breaks_ties_deterministically():
    scored = od(
        [
            (236, 161, 100, 0.5),
            (161, 87, 100, 0.5),  # same priority/trip_count, lower pu_location_id wins
            (300, 400, 50, 0.9),
        ],
        ["pu_location_id", "do_location_id", "trip_count", "priority_score"],
    )

    top = select_top_od(scored, top_n=2)

    assert list(zip(top["pu_location_id"], top["do_location_id"], strict=True)) == [(300, 400), (161, 87)]
    assert list(top["priority_rank"]) == [1, 2]


def test_validate_output_raises_when_top_od_has_wrong_row_count():
    top_od = od(
        [(236, 161, 100, 0.9, 0.9, 0.9, 0.9, 0.9, 1)],
        [
            "pu_location_id",
            "do_location_id",
            "trip_count",
            "demand_score",
            "pu_comfort_relevance_score",
            "do_comfort_relevance_score",
            "od_relevance_score",
            "priority_score",
            "priority_rank",
        ],
    )

    with pytest.raises(AssertionError):
        validate_output(top_od, top_od, top_n=1000)
