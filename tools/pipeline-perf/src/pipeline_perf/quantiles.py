"""Exact quantiles over a bounded per-stage duration summary."""

from __future__ import annotations

from array import array
from collections.abc import Sequence


class DurationSummary:
    """Task duration 표본을 모아 분위수를 낸다.

    개별 task 레코드는 버리고 duration(ms)만 `array('q')`에 담는다. 요소당 8바이트라
    Job Run 하나(수만 task)에서도 수백 KB 수준이고, t-digest 같은 근사 없이 정확한
    분위수를 낼 수 있다. 이 클래스가 스테이지별로 하나씩 붙는다.
    """

    __slots__ = ("_values",)

    def __init__(self) -> None:
        self._values = array("q")

    def add(self, value_ms: int) -> None:
        self._values.append(int(value_ms))

    def __len__(self) -> int:
        return len(self._values)

    def percentile(self, fraction: float) -> int | None:
        return percentile(sorted(self._values), fraction)

    def summary(self) -> dict[str, int | None]:
        ordered = sorted(self._values)
        if not ordered:
            return {"count": 0, "p50_ms": None, "p95_ms": None, "max_ms": None, "sum_ms": 0}
        return {
            "count": len(ordered),
            "p50_ms": percentile(ordered, 0.50),
            "p95_ms": percentile(ordered, 0.95),
            "max_ms": ordered[-1],
            "sum_ms": int(sum(ordered)),
        }


def percentile(ordered: Sequence[int], fraction: float) -> int | None:
    """정렬된 표본의 `fraction` 분위수를 선형 보간으로 구한다(numpy 기본과 같은 방식).

    보간값은 ms 단위라 반올림해 정수로 돌려준다.
    """
    if not ordered:
        return None
    if not 0.0 <= fraction <= 1.0:
        raise ValueError(f"fraction must be within [0, 1]: {fraction}")
    position = fraction * (len(ordered) - 1)
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    weight = position - lower_index
    value = ordered[lower_index] * (1 - weight) + ordered[upper_index] * weight
    return round(value)


def skew_ratio(max_ms: int | None, p50_ms: int | None) -> float | None:
    """`max / p50`. p50이 0이면 비율이 정의되지 않아 None을 돌려준다."""
    if max_ms is None or not p50_ms:
        return None
    return round(max_ms / p50_ms, 2)
