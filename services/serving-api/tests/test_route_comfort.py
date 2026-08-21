"""Tests for serving_api/route_comfort.py (#269)."""

from __future__ import annotations

import pytest
from serving_api.config import RouteComfortConfig
from serving_api.route_comfort import score_route, worst_segment_count

CONFIG = RouteComfortConfig(
    average_weight=0.7, worst_quartile_weight=0.3, worst_ratio=0.25
)


def test_score_route_mixes_the_average_and_the_worst_quartile() -> None:
    # [85, 82, 40, 45] -> 평균 63, 하위 25%(1개) 40, 0.7 x 63 + 0.3 x 40.
    breakdown = score_route([85.0, 82.0, 40.0, 45.0], CONFIG)

    assert breakdown.average_comfort_score == 63.0
    assert breakdown.worst_quartile_comfort_score == 40.0
    assert breakdown.comfort_score == 56.1


def test_score_route_penalizes_a_route_with_a_rough_stretch() -> None:
    # 평균이 같아도 불편한 구간이 섞인 경로는 뒤로 밀려야 한다 — 평균만 쓰면
    # 두 경로가 구분되지 않는 것이 이 정책을 두는 이유다.
    smooth = score_route([70.0, 70.0, 70.0, 70.0], CONFIG)
    rough = score_route([90.0, 90.0, 50.0, 50.0], CONFIG)

    assert smooth.average_comfort_score == rough.average_comfort_score
    assert smooth.comfort_score > rough.comfort_score


def test_score_route_counts_a_repeated_segment_once_per_traversal() -> None:
    # 같은 구간을 두 번 지나면 그 구간을 두 번 주행한다. 조회는 한 번이지만
    # 평균에는 두 번 들어가야 한다.
    once = score_route([90.0, 30.0], CONFIG)
    twice = score_route([90.0, 30.0, 30.0], CONFIG)

    assert once.average_comfort_score == 60.0
    assert twice.average_comfort_score == 50.0


def test_score_route_rounds_away_binary_floating_point_noise() -> None:
    # 반올림하지 않으면 56.099999999999994가 그대로 응답에 실린다.
    assert score_route([85.0, 82.0, 40.0, 45.0], CONFIG).comfort_score == 56.1


def test_score_route_returns_the_only_score_for_a_single_segment_route() -> None:
    # 구간이 하나면 평균과 하위 평균이 같아 가중치와 무관하게 그 값이 나온다.
    breakdown = score_route([64.0], CONFIG)

    assert breakdown == type(breakdown)(64.0, 64.0, 64.0)


def test_score_route_rejects_an_empty_route() -> None:
    # 구간이 없는 경로는 평균을 정의할 수 없다. HTTP 계층이 미리 막지만,
    # 계산 계층도 0으로 나누는 대신 분명하게 실패해야 한다.
    with pytest.raises(ValueError, match="at least one segment"):
        score_route([], CONFIG)


@pytest.mark.parametrize(
    ("segment_count", "expected"),
    [
        pytest.param(1, 1, id="single-segment-still-counts-one"),
        pytest.param(3, 1, id="rounds-up-from-0.75"),
        pytest.param(4, 1, id="exact-quarter"),
        pytest.param(5, 2, id="rounds-up-from-1.25"),
        pytest.param(100, 25, id="exact-quarter-of-a-long-route"),
    ],
)
def test_worst_segment_count_rounds_up_and_keeps_at_least_one(
    segment_count: int, expected: int
) -> None:
    assert worst_segment_count(segment_count, 0.25) == expected


def test_worst_segment_count_never_exceeds_the_route_length() -> None:
    assert worst_segment_count(3, 1.0) == 3


def test_a_worst_ratio_of_one_makes_the_score_the_plain_average() -> None:
    # 하위 비율이 100%면 두 항이 같은 값이 되므로 가중 평균도 그 값이다.
    config = RouteComfortConfig(
        average_weight=0.7, worst_quartile_weight=0.3, worst_ratio=1.0
    )

    breakdown = score_route([85.0, 82.0, 40.0, 45.0], config)

    assert breakdown.comfort_score == 63.0


def test_weights_may_shift_the_emphasis_without_changing_the_scale() -> None:
    # 정책값은 설정으로 바꿀 수 있어야 한다. 평균만 100% 반영하면 하위 평균은
    # 계산해 보여주되 최종 점수에는 영향을 주지 않는다.
    config = RouteComfortConfig(
        average_weight=1.0, worst_quartile_weight=0.0, worst_ratio=0.25
    )

    breakdown = score_route([85.0, 82.0, 40.0, 45.0], config)

    assert breakdown.comfort_score == 63.0
    assert breakdown.worst_quartile_comfort_score == 40.0
