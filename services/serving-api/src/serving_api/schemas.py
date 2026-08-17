"""Request and response models for the serving API (#160).

필드 구성과 순서는 Gold `segment_comfort_score` 노션 스키마를 따른다.
`data_period_start`/`data_period_end`는 Score 계산에 사용한 주행 데이터 기간이다.
"""

from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, Field

from serving_api.config import MAX_BATCH_ITEMS

# vehicle_profile_id=0은 차량 구분 없는 전체 대표값을 뜻한다 (OQ-038).
VEHICLE_AGNOSTIC_VEHICLE_PROFILE_ID = 0


class ComfortScoreKey(BaseModel):
    """조회 대상 복합키."""

    segment_id: str = Field(min_length=1, max_length=64)
    vehicle_profile_id: int = Field(ge=0)


class ComfortScore(BaseModel):
    segment_id: str
    vehicle_profile_id: int
    data_period_start: date
    data_period_end: date
    comfort_score: float
    sample_count: int
    confidence_score: float
    score_version: str
    calculated_at: datetime


class ComfortScoreBatchRequest(BaseModel):
    items: list[ComfortScoreKey] = Field(min_length=1, max_length=MAX_BATCH_ITEMS)


class ComfortScoreBatchResponse(BaseModel):
    """조회된 점수와, 점수가 없어 조회되지 않은 키를 구분해 담는다.

    다건 조회에서 일부 키가 비어 있는 것은 오류가 아니다 — Gold는 최소 트래픽
    임계값을 넘긴 조합만 담으므로, 지나간 적 없는 구간은 애초에 행이 없다.
    """

    scores: list[ComfortScore]
    not_found: list[ComfortScoreKey]
