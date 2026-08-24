"""Turn road segments and their scores into GeoJSON for a web map.

UI 프레임워크에 의존하지 않는다 -- 예전에는 이 로직이 map_view.py 안에서 Folium
객체 생성과 섞여 있었지만, 지도를 그리는 쪽은 브라우저(Leaflet)이고 여기서는
FeatureCollection만 만들면 된다.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from dashboard.road_geometry import RoadSegment
from dashboard.serving_api_client import ComfortScore

GREEN = "green"
YELLOW = "yellow"
RED = "red"
GRAY = "gray"

# ~11cm at NYC's latitude -- visually identical to full float64 precision but
# meaningfully shorter once serialized across up to MAX_RENDERED_SEGMENTS
# features per render (#421).
COORDINATE_PRECISION = 6


@dataclass(frozen=True, slots=True)
class ScoredRoadSegment:
    road_segment: RoadSegment
    score: ComfortScore | None

    @property
    def color(self) -> str:
        value = self.score.comfort_score if self.score is not None else None
        return comfort_score_color(value)


def comfort_score_color(score: float | None) -> str:
    if score is None:
        return GRAY
    if score >= 80:
        return GREEN
    if score >= 60:
        return YELLOW
    return RED


def join_road_segments_with_scores(
    road_segments: Sequence[RoadSegment],
    scores: Mapping[str, ComfortScore],
) -> list[ScoredRoadSegment]:
    """Keep every road segment and attach its score by canonical segment_id."""
    return [
        ScoredRoadSegment(
            road_segment=road_segment,
            score=scores.get(road_segment.segment_id),
        )
        for road_segment in road_segments
    ]


def build_feature_collection(segments: Sequence[ScoredRoadSegment]) -> dict[str, Any]:
    return {
        "type": "FeatureCollection",
        "features": [_feature(segment) for segment in segments],
    }


def _rounded_coordinates(coordinates: Any) -> Any:
    if isinstance(coordinates, (int, float)):
        return round(coordinates, COORDINATE_PRECISION)
    return [_rounded_coordinates(value) for value in coordinates]


def _feature(segment: ScoredRoadSegment) -> dict[str, Any]:
    score = segment.score
    geometry = segment.road_segment.geometry
    return {
        "type": "Feature",
        "geometry": {
            "type": geometry["type"],
            "coordinates": _rounded_coordinates(geometry["coordinates"]),
        },
        "properties": {
            "segment_id": segment.road_segment.segment_id,
            "street_name": segment.road_segment.street_name or "N/A",
            "comfort_score": (
                f"{score.comfort_score:.2f}" if score is not None else "N/A"
            ),
            "confidence_score": (
                f"{score.confidence_score:.2f}" if score is not None else "N/A"
            ),
            "source": score.source if score is not None else "N/A",
            "weather_time": (
                score.weather_time
                if score is not None and score.weather_time is not None
                else "N/A"
            ),
            "color": segment.color,
        },
    }
