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

_VEHICLE_PROFILE_ID_DESCRIPTION = (
    "차량 프로필. `0`은 특정 차종이 아니라 차량 구분 없는 전체 대표값이다. "
    "없거나 비활성인 값은 거절하지 않고 `0`으로 대체 조회한다."
)

# 세 응답이 같은 필드를 공유한다. 설명이 서로 어긋나지 않게 한 번만 적는다.
_REQUESTED_PROFILE_FIELD = Field(
    description="요청에 실려 온 `vehicle_profile_id`를 그대로 돌려준다.", examples=[3]
)
_EFFECTIVE_PROFILE_FIELD = Field(
    description="실제로 조회에 쓴 프로필. 대체가 일어났다면 `0`이다.", examples=[0]
)
_PROFILE_FALLBACK_FIELD = Field(
    description=(
        "대체 조회 여부. `true`면 이 점수는 요청한 차량 기준이 아니다."
    ),
    examples=[True],
)


# 모델 docstring은 그대로 OpenAPI에 실리는데, Swagger UI의 스키마 설명 칸이 여러
# 줄 문단을 제대로 감싸지 못해 중간이 잘려 보인다. 그래서 docstring은 한 줄로 두고
# 자세한 내용은 필드 설명으로 내린다 -- 코드 독자를 위한 배경은 이런 주석에 남긴다.
#
# `source`가 `standard`가 되는 경우는 둘이다: 구간에 zone이 매핑되지 않았거나,
# 그 zone의 날씨를 아직 수집하지 못했을 때.
class ComfortScore(BaseModel):
    """한 구간 x 차량 프로필의 최신 승차감 점수."""

    segment_id: str = Field(description="LION 도로 구간 식별자.", examples=["0048146"])
    vehicle_profile_id: int = Field(
        description="이 점수를 계산한 차량 프로필. 대체가 일어났다면 대체된 쪽이다.",
        examples=[0],
    )
    comfort_score: float = Field(
        description="종합 승차감 점수. 0~100이고 높을수록 편안하다.", examples=[78.42]
    )
    vertical_score: float
    longitudinal_score: float
    lateral_score: float
    confidence_score: float = Field(
        description="점수를 얼마나 믿을 수 있는지를 0~1로 나타낸 값. 표본이 적으면 낮다.",
        examples=[0.94],
    )
    sample_count: int
    data_period_start: datetime
    standard_score_as_of: datetime
    standard_score_version: str
    weather_time: datetime | None = Field(
        description="반영한 날씨 관측 시각(UTC). `source`가 `standard`면 항상 `null`이다."
    )
    weather_rule_version: str | None
    calculated_at: datetime
    source: Literal["current", "standard"] = Field(
        description=(
            "`current`는 날씨가 반영된 점수, `standard`는 날씨를 반영할 수 없어 대신 "
            "응답한 날씨 미보정 점수다."
        ),
        examples=["current"],
    )


class ComfortScoreResponse(ComfortScore):
    """단건 조회 응답 — 점수에 어떤 차량 프로필로 조회했는지를 함께 담는다."""

    requested_vehicle_profile_id: int = _REQUESTED_PROFILE_FIELD
    effective_vehicle_profile_id: int = _EFFECTIVE_PROFILE_FIELD
    vehicle_profile_fallback: bool = _PROFILE_FALLBACK_FIELD


class ComfortScoreBatchRequest(BaseModel):
    """경로 조회는 차량 하나를 기준으로 하므로 프로필은 요청당 하나만 받는다."""

    vehicle_profile_id: int = Field(ge=0, description=_VEHICLE_PROFILE_ID_DESCRIPTION)
    segment_ids: list[SegmentId] = Field(
        min_length=1,
        max_length=MAX_COMFORT_SCORE_BATCH_ITEMS,
        description=(
            "조회할 구간 목록. 같은 id가 여러 번 와도 한 번만 조회하며, 첫 등장 "
            "순서가 응답 순서가 된다."
        ),
        examples=[["0048146", "0036273"]],
    )


# 일부 구간이 비어 있는 것은 오류가 아니다 -- current에도 standard에도 행이 없는
# 구간은 애초에 점수가 계산된 적이 없다.
class ComfortScoreBatchResponse(BaseModel):
    """조회된 점수와, 점수가 없어 조회되지 않은 구간을 구분해 담는다."""

    requested_vehicle_profile_id: int = _REQUESTED_PROFILE_FIELD
    effective_vehicle_profile_id: int = _EFFECTIVE_PROFILE_FIELD
    vehicle_profile_fallback: bool = _PROFILE_FALLBACK_FIELD
    scores: list[ComfortScore] = Field(
        description="점수를 찾은 구간. 요청의 중복 제거 후 순서를 그대로 따른다."
    )
    not_found_segment_ids: list[str] = Field(
        description=(
            "점수가 없어 `scores`에 담기지 않은 구간. 오류가 아니라 아직 점수가 "
            "계산된 적 없는 구간이다."
        ),
        examples=[["9999999"]],
    )


# 길이나 예상 소요 시간은 받지 않는다 -- 이 API는 경로를 고르는 데 필요한 승차감만
# 계산하고, 거리·시간 비교는 호출자가 자기 값으로 한다.
class RouteCandidate(BaseModel):
    """내비게이션이 이미 만들어 둔 후보 경로 하나."""

    route_id: RouteId = Field(
        description=(
            "호출자가 붙이는 후보 경로 식별자. 응답에서 결과를 되짚는 키이므로 "
            "한 요청 안에서 유일해야 한다."
        ),
        examples=["route-a"],
    )
    segment_ids: list[SegmentId] = Field(
        min_length=1,
        max_length=MAX_ROUTE_SEGMENTS,
        description=(
            "경로가 지나는 구간을 주행 순서대로. 같은 구간을 두 번 지나면 두 번 "
            "넣는다 — 그만큼 실제로 주행하므로 점수에도 두 번 반영된다."
        ),
        examples=[["0038892", "0038913", "0038915"]],
    )


class RouteEvaluationRequest(BaseModel):
    """후보 경로 여러 개를 차량 프로필 하나 기준으로 함께 비교한다."""

    vehicle_profile_id: int = Field(ge=0, description=_VEHICLE_PROFILE_ID_DESCRIPTION)
    routes: list[RouteCandidate] = Field(
        min_length=1,
        max_length=MAX_ROUTES_PER_REQUEST,
        description=(
            f"비교할 후보 경로. 중복을 제거한 구간 수가 {MAX_ROUTE_SEGMENTS}개를 "
            "넘으면 422로 거절한다."
        ),
    )

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


# 최종 점수만으로는 '전반적으로 무난한 경로'와 '평균은 높지만 불편한 구간이 섞인
# 경로'가 구분되지 않으므로 두 중간값을 함께 실어 보낸다.
class RouteComfortScore(BaseModel):
    """후보 경로 하나의 점수와, 그 점수를 만든 두 중간값."""

    route_id: str = Field(description="요청에 실려 온 `route_id`.", examples=["route-a"])
    comfort_score: float = Field(
        description=(
            "경로 최종 점수. `average_comfort_score`와 "
            "`worst_quartile_comfort_score`를 가중 평균한 값이고, 이 값으로 정렬한다."
        ),
        examples=[56.10],
    )
    average_comfort_score: float = Field(
        description="경로 위 모든 구간 점수의 평균.", examples=[63.0]
    )
    worst_quartile_comfort_score: float = Field(
        description=(
            "점수가 낮은 쪽 일부 구간만 평균한 값. 평균은 높지만 불편한 구간이 "
            "섞인 경로를 가려내기 위한 것이다."
        ),
        examples=[40.0],
    )


# 순위 필드는 두지 않는다 -- 배열 순서와 같은 말이라 서로 어긋날 여지만 생긴다.
#
# 요청한 차량 프로필이 활성이 아니면 차량 무관 sentinel로 내려가 응답하므로(#272),
# 어느 프로필로 계산한 점수인지를 함께 싣는다. 이것이 없으면 호출자는 다른 차량의
# 점수를 자기 차량 점수로 오해한다.
class RouteEvaluationResponse(BaseModel):
    """경로별 점수와 추천 경로."""

    requested_vehicle_profile_id: int = _REQUESTED_PROFILE_FIELD
    effective_vehicle_profile_id: int = _EFFECTIVE_PROFILE_FIELD
    vehicle_profile_fallback: bool = _PROFILE_FALLBACK_FIELD
    recommended_route_id: str = Field(
        description="가장 점수가 높은 경로. `routes`의 첫 원소와 항상 같다.",
        examples=["route-a"],
    )
    routes: list[RouteComfortScore] = Field(
        description=(
            "`comfort_score` 내림차순. 점수가 같으면 요청에 실려 온 순서를 유지한다."
        )
    )
