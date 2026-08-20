"""Request and response models for the serving API (#160)."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, Field, StringConstraints

from serving_api.config import MAX_BATCH_ITEMS

# vehicle_profile_id=0은 차량 구분 없는 전체 대표값을 뜻한다 (OQ-038).
VEHICLE_AGNOSTIC_VEHICLE_PROFILE_ID = 0

SegmentId = Annotated[str, StringConstraints(min_length=1, max_length=64)]


class ComfortScore(BaseModel):
    """한 구간 x 차량 프로필의 최신 승차감 점수.

    `source`는 이 값이 어디서 왔는지를 알려준다. `current`는 날씨가 반영된 값이고,
    `standard`는 날씨를 반영할 수 없어 날씨 미보정 점수로 대신 응답한 경우다
    (zone이 없는 구간, 아직 그 zone의 날씨를 못 받은 경우). `standard`일 때
    `weather_time`과 `weather_rule_version`은 항상 null이다.
    """

    segment_id: str
    vehicle_profile_id: int
    comfort_score: float
    vertical_score: float
    longitudinal_score: float
    lateral_score: float
    confidence_score: float
    sample_count: int
    data_period_start: datetime
    standard_score_as_of: datetime
    standard_score_version: str
    weather_time: datetime | None
    weather_rule_version: str | None
    calculated_at: datetime
    source: Literal["current", "standard"]


class ComfortScoreBatchRequest(BaseModel):
    """경로 조회는 차량 하나를 기준으로 하므로 프로필은 요청당 하나만 받는다."""

    vehicle_profile_id: int = Field(ge=0)
    segment_ids: list[SegmentId] = Field(min_length=1, max_length=MAX_BATCH_ITEMS)


class ComfortScoreBatchResponse(BaseModel):
    """조회된 점수와, 점수가 없어 조회되지 않은 구간을 구분해 담는다.

    일부 구간이 비어 있는 것은 오류가 아니다 — current에도 standard에도 행이 없는
    구간은 애초에 점수가 계산된 적이 없다.
    """

    scores: list[ComfortScore]
    not_found_segment_ids: list[str]
