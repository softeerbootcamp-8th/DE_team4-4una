"""Turn a route's segment comfort scores into one route comfort score (#269).

DB나 HTTP를 모르는 순수 계산 계층이다. 조회는 `repository`가, 상태 코드는
`routes`가 맡고, 여기서는 점수 목록 하나를 점수 하나로 줄이는 일만 한다.

정책은 두 값을 섞는다.

- **전체 평균**: 경로가 전반적으로 얼마나 편안한가.
- **하위 구간 평균**: 낮은 쪽 `worst_ratio` 만큼의 구간이 얼마나 불편한가.

평균만 보면 중간에 심하게 불편한 구간이 섞인 경로가 그렇지 않은 경로와 같은
점수를 받는다. 반대로 최저점 하나만 보면 구간 하나의 값에 순위가 좌우된다.
하위 구간을 여러 개 평균해 두 극단 사이를 택한다.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal

from serving_api.config import RouteComfortConfig

# 응답에 실을 소수 자릿수. 반올림하지 않으면 0.7 x 63 + 0.3 x 40이
# 56.099999999999994로 직렬화된다.
SCORE_DECIMAL_PLACES = 2

_QUANTUM = Decimal(1).scaleb(-SCORE_DECIMAL_PLACES)


@dataclass(frozen=True, slots=True)
class RouteComfortBreakdown:
    """최종 점수와, 그 점수를 만든 두 중간값."""

    comfort_score: float
    average_comfort_score: float
    worst_quartile_comfort_score: float


def score_route(
    comfort_scores: Sequence[float], config: RouteComfortConfig
) -> RouteComfortBreakdown:
    """경로 위 구간 점수들을 경로 점수 하나로 줄인다.

    `comfort_scores`는 경로가 지나는 순서 그대로의 구간 점수다. 같은 구간을 두
    번 지나면 두 번 들어온다 — 그만큼 그 구간을 실제로 주행하기 때문이다.
    """
    if not comfort_scores:
        raise ValueError("a route needs at least one segment comfort score")

    average = sum(comfort_scores) / len(comfort_scores)
    worst = sorted(comfort_scores)[: worst_segment_count(len(comfort_scores), config.worst_ratio)]
    worst_average = sum(worst) / len(worst)
    total = config.average_weight * average + config.worst_quartile_weight * worst_average
    return RouteComfortBreakdown(
        comfort_score=_round(total),
        average_comfort_score=_round(average),
        worst_quartile_comfort_score=_round(worst_average),
    )


def worst_segment_count(segment_count: int, worst_ratio: float) -> int:
    """하위 평균에 넣을 구간 수.

    올림이라 구간이 몇 개든 최소 하나는 잡히고, 비율이 1이어도 구간 수를 넘지
    않는다.
    """
    return min(segment_count, max(1, math.ceil(segment_count * worst_ratio)))


def _round(value: float) -> float:
    # float를 그대로 quantize하면 56.099999999999994가 56.09로 내려간다.
    # str()로 한 번 거쳐 사람이 읽는 값 기준으로 반올림한다.
    return float(Decimal(str(value)).quantize(_QUANTUM, rounding=ROUND_HALF_UP))
