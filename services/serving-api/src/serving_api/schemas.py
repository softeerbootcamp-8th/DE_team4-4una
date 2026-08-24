"""Request and response models for the serving API (#160)."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal, Self

from pydantic import BaseModel, Field, StringConstraints, model_validator

from serving_api.config import (
    MAX_COMFORT_SCORE_BATCH_ITEMS,
    MAX_ROUTE_SEGMENTS,
    MAX_ROUTES_PER_REQUEST,
)

# vehicle_profile_id=0은 차량 구분 없는 전체 대표값을 뜻한다 (OQ-038).
VEHICLE_AGNOSTIC_VEHICLE_PROFILE_ID = 0

SegmentId = Annotated[str, StringConstraints(min_length=1, max_length=64)]
RouteId = Annotated[str, StringConstraints(min_length=1, max_length=64)]


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


class ComfortScoreResponse(ComfortScore):
    """단건 조회 응답 — 점수에 어떤 차량 프로필로 조회했는지를 함께 담는다."""

    requested_vehicle_profile_id: int
    effective_vehicle_profile_id: int
    vehicle_profile_fallback: bool


class ComfortScoreBatchRequest(BaseModel):
    """경로 조회는 차량 하나를 기준으로 하므로 프로필은 요청당 하나만 받는다."""

    vehicle_profile_id: int = Field(ge=0)
    segment_ids: list[SegmentId] = Field(
        min_length=1, max_length=MAX_COMFORT_SCORE_BATCH_ITEMS
    )


class ComfortScoreBatchResponse(BaseModel):
    """조회된 점수와, 점수가 없어 조회되지 않은 구간을 구분해 담는다.

    일부 구간이 비어 있는 것은 오류가 아니다 — current에도 standard에도 행이 없는
    구간은 애초에 점수가 계산된 적이 없다.
    """

    requested_vehicle_profile_id: int
    effective_vehicle_profile_id: int
    vehicle_profile_fallback: bool
    scores: list[ComfortScore]
    not_found_segment_ids: list[str]


class RouteCandidate(BaseModel):
    """내비게이션이 이미 만들어 둔 후보 경로 하나.

    길이나 예상 소요 시간은 받지 않는다 — 이 API는 경로를 고르는 데 필요한
    승차감만 계산하고, 거리·시간 비교는 호출자가 자기 값으로 한다.
    `segment_ids`는 주행 순서이며, 같은 구간을 두 번 지나면 두 번 들어온다.
    """

    route_id: RouteId
    segment_ids: list[SegmentId] = Field(min_length=1, max_length=MAX_ROUTE_SEGMENTS)


class RouteEvaluationRequest(BaseModel):
    """후보 경로 여러 개를 차량 프로필 하나 기준으로 함께 비교한다."""

    vehicle_profile_id: int = Field(ge=0)
    routes: list[RouteCandidate] = Field(min_length=1, max_length=MAX_ROUTES_PER_REQUEST)

    @model_validator(mode="after")
    def check_route_ids_and_segment_budget(self) -> Self:
        route_ids = [route.route_id for route in self.routes]
        # 응답은 route_id로 결과를 되짚어 보게 되어 있다. 같은 id가 두 번 오면
        # 어느 쪽이 어느 경로인지 호출자가 구분할 수 없다.
        if len(set(route_ids)) != len(route_ids):
            raise ValueError("route_id must be unique within a request")

        # 후보 경로가 몇 개든 조회는 한 번이므로, 상한도 조회 한 번 기준인
        # 중복 제거 후 구간 수에 건다.
        unique_segment_ids = {
            segment_id for route in self.routes for segment_id in route.segment_ids
        }
        if len(unique_segment_ids) > MAX_ROUTE_SEGMENTS:
            raise ValueError(
                f"a request may reference at most {MAX_ROUTE_SEGMENTS} distinct segments, "
                f"got {len(unique_segment_ids)}"
            )
        return self


class RouteComfortScore(BaseModel):
    """후보 경로 하나의 점수.

    최종 점수만으로는 '전반적으로 무난한 경로'와 '평균은 높지만 불편한 구간이
    섞인 경로'가 구분되지 않으므로 두 중간값을 함께 실어 보낸다.
    """

    route_id: str
    comfort_score: float
    average_comfort_score: float
    worst_quartile_comfort_score: float


class RouteEvaluationResponse(BaseModel):
    """`routes`는 comfort_score 내림차순이고, 맨 앞이 추천 경로다.

    순위 필드는 두지 않는다 — 배열 순서와 같은 말이라 서로 어긋날 여지만 생긴다.
    점수가 같으면 요청에 실려 온 순서를 유지한다.

    요청한 차량 프로필이 활성이 아니면 차량 무관 sentinel로 내려가 응답하므로
    (#272), 어느 프로필로 계산한 점수인지를 함께 싣는다. 이것이 없으면 호출자는
    다른 차량의 점수를 자기 차량 점수로 오해한다.
    """

    requested_vehicle_profile_id: int
    effective_vehicle_profile_id: int
    vehicle_profile_fallback: bool
    recommended_route_id: str
    routes: list[RouteComfortScore]
