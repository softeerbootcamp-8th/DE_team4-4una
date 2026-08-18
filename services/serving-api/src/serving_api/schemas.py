"""Request and response models for the serving API (#160)."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, Field, StringConstraints

from serving_api.config import MAX_BATCH_ITEMS

# vehicle_profile_id=0은 차량 구분 없는 전체 대표값을 뜻한다 (OQ-038).
VEHICLE_AGNOSTIC_VEHICLE_PROFILE_ID = 0

SegmentId = Annotated[str, StringConstraints(min_length=1, max_length=64)]


class ComfortScore(BaseModel):
    segment_id: str
    vehicle_profile_id: int
    data_period_start: datetime
    data_period_end: datetime
    comfort_score: float
    sample_count: int
    confidence_score: float
    score_version: str
    calculated_at: datetime


class ComfortScoreBatchRequest(BaseModel):
    """경로 조회는 차량 하나를 기준으로 하므로 프로필은 요청당 하나만 받는다."""

    vehicle_profile_id: int = Field(ge=0)
    segment_ids: list[SegmentId] = Field(min_length=1, max_length=MAX_BATCH_ITEMS)


class ComfortScoreBatchResponse(BaseModel):
    """조회된 점수와, 점수가 없어 조회되지 않은 구간을 구분해 담는다.

    일부 구간이 비어 있는 것은 오류가 아니다 — Gold는 최소 트래픽 임계값을 넘긴
    조합만 담으므로, 지나간 적 없는 구간은 애초에 행이 없다.
    """

    scores: list[ComfortScore]
    not_found_segment_ids: list[str]
