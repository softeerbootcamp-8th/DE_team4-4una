"""Load and validate sensor-event cleansing thresholds from YAML."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class ValueRange:
    """OUT_OF_RANGE 판정에 쓰는 컬럼별 유효 범위. min/max는 없으면 None."""

    minimum: float | None
    maximum: float | None
    # heading의 360처럼 상한을 포함하지 않는 범위(예: [0, 360))를 표현하기 위한 플래그
    max_exclusive: bool = False


@dataclass(frozen=True, slots=True)
class EventTimeBounds:
    """event_time의 절대 상하한. 정밀한 배치창 계산이 아니라 명백히 잘못된 값만 거른다."""

    minimum: datetime
    maximum: datetime
    # 실측 근거 없이 잠정적으로 정한 값이면 True
    provisional: bool


@dataclass(frozen=True, slots=True)
class DeduplicationRule:
    """중복 제거 키와 우선순위."""

    key: tuple[str, ...]
    # 동일 key를 가진 행이 여럿일 때 무엇을 남길지 나타내는 식별자(예: "latest_ingested_at")
    priority: str


@dataclass(frozen=True, slots=True)
class CleansingConfig:
    """cleansing_rules.yaml 한 파일의 파싱 결과 전체."""

    required_columns: tuple[str, ...]
    value_ranges: dict[str, ValueRange]
    event_time_bounds: EventTimeBounds
    negative_allowed: dict[str, bool]
    deduplication: DeduplicationRule
