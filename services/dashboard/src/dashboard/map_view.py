"""Join road geometry with scores and render a Folium map."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import folium

from dashboard.config import NYC_MAP_CENTER, NYC_MAP_ZOOM
from dashboard.road_geometry import RoadSegment
from dashboard.serving_api_client import ComfortScore
from dashboard.zone_master import Borough

GREEN = "green"
YELLOW = "yellow"
RED = "red"
GRAY = "gray"


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


def build_map(
    segments: Sequence[ScoredRoadSegment],
    center: tuple[float, float] = NYC_MAP_CENTER,
    zoom: int = NYC_MAP_ZOOM,
) -> folium.Map:
    fmap = folium.Map(location=center, zoom_start=zoom, control_scale=True)
    # GeoJsonTooltip reads its field names off the first feature, so an empty
    # collection makes it assert that every field is missing. Nothing would be
    # drawn anyway -- the viewport can legitimately contain no segments, and the
    # first render always does, since it happens before the viewport is known.
    if not segments:
        fmap.get_root().html.add_child(folium.Element(_legend_html()))
        return fmap
    folium.GeoJson(
        _feature_collection(segments),
        name="NYC road comfort score",
        style_function=lambda feature: {
            "color": feature["properties"]["color"],
            "weight": 4,
            "opacity": 0.8,
        },
        highlight_function=lambda _feature: {"weight": 7, "opacity": 1.0},
        tooltip=folium.GeoJsonTooltip(
            fields=[
                "segment_id",
                "street_name",
                "comfort_score",
                "confidence_score",
                "source",
                "weather_time",
            ],
            aliases=[
                "segment_id",
                "street_name",
                "comfort_score",
                "confidence_score",
                "source",
                "weather_time",
            ],
            sticky=False,
        ),
    ).add_to(fmap)
    fmap.get_root().html.add_child(folium.Element(_legend_html()))
    return fmap


BOROUGH_TOOLTIP_FIELD = "borough"


def build_borough_map(
    boroughs: Sequence[Borough],
    center: tuple[float, float] = NYC_MAP_CENTER,
    zoom: int = NYC_MAP_ZOOM,
) -> folium.Map:
    """Overview map: one clickable outline per borough, no road segments.

    Six polygons instead of the whole road network, so the first render costs
    almost nothing and the user picks a borough before any segment is drawn.
    """
    fmap = folium.Map(location=center, zoom_start=zoom, control_scale=True)
    if not boroughs:
        return fmap
    folium.GeoJson(
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": borough.geometry,
                    "properties": {BOROUGH_TOOLTIP_FIELD: borough.name},
                }
                for borough in boroughs
            ],
        },
        name="NYC boroughs",
        style_function=lambda _feature: {
            "color": "#3d6fb4",
            "weight": 2,
            "fillColor": "#3d6fb4",
            "fillOpacity": 0.15,
        },
        highlight_function=lambda _feature: {"fillOpacity": 0.35},
        tooltip=folium.GeoJsonTooltip(
            fields=[BOROUGH_TOOLTIP_FIELD],
            aliases=[BOROUGH_TOOLTIP_FIELD],
            sticky=False,
        ),
    ).add_to(fmap)
    return fmap


def _feature_collection(segments: Sequence[ScoredRoadSegment]) -> dict[str, Any]:
    return {
        "type": "FeatureCollection",
        "features": [_feature(segment) for segment in segments],
    }


def _feature(segment: ScoredRoadSegment) -> dict[str, Any]:
    score = segment.score
    return {
        "type": "Feature",
        "geometry": segment.road_segment.geometry,
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


def _legend_html() -> str:
    return """
    <div style="position: fixed; bottom: 28px; left: 28px; z-index: 9999;
        background: white; border: 1px solid #bbb; border-radius: 6px;
        padding: 10px 12px; font-size: 13px; line-height: 1.6;">
      <strong>Comfort Score</strong><br>
      <span style="color: green;">&#9632;</span> 80 or higher<br>
      <span style="color: #d4b000;">&#9632;</span> 60 to 79.99<br>
      <span style="color: red;">&#9632;</span> below 60<br>
      <span style="color: gray;">&#9632;</span> unavailable
    </div>
    """
