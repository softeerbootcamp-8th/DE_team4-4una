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


def build_base_map(
    boroughs: Sequence[Borough] = (),
    selected_borough: str | None = None,
) -> folium.Map:
    """Draw the stable part of the map: borough outlines and the legend.

    Never carries road segments -- those go into a separate FeatureGroup added
    via st_folium's feature_group_to_add so pan/zoom only updates that layer
    instead of rebuilding this one (#421). Always created at the default
    center/zoom regardless of the current viewport, for the same reason: the
    live center/zoom are passed to st_folium directly rather than baked in here.

    The outlines stay on the map after a borough is picked so that zooming out
    and clicking another one is possible; the selected borough is drawn without
    a fill so clicks reach the segments inside it.
    """
    fmap = folium.Map(location=NYC_MAP_CENTER, zoom_start=NYC_MAP_ZOOM, control_scale=True)
    _add_borough_layers(fmap, boroughs, selected_borough)
    fmap.get_root().html.add_child(folium.Element(_legend_html()))
    return fmap


def build_segment_feature_group(
    segments: Sequence[ScoredRoadSegment],
) -> folium.FeatureGroup:
    """Road segments as a FeatureGroup meant for st_folium's feature_group_to_add.

    Kept separate from the base map so pan/zoom only replaces this layer (#421).
    """
    group = folium.FeatureGroup(name="NYC road comfort score")

    # GeoJsonTooltip reads its field names off the first feature, so an empty
    # collection makes it assert that every field is missing. Nothing would be
    # drawn anyway -- the viewport can legitimately contain no segments, and the
    # first render always does, since it happens before the viewport is known.
    if not segments:
        return group
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
    ).add_to(group)
    return group


BOROUGH_TOOLTIP_FIELD = "borough"


def _borough_features(boroughs: Sequence[Borough]) -> dict[str, Any]:
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": borough.geometry,
                "properties": {BOROUGH_TOOLTIP_FIELD: borough.name},
            }
            for borough in boroughs
        ],
    }


def _add_borough_layers(
    fmap: folium.Map,
    boroughs: Sequence[Borough],
    selected_borough: str | None,
) -> None:
    selectable = [b for b in boroughs if b.name != selected_borough]
    if selectable:
        folium.GeoJson(
            _borough_features(selectable),
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

    current = [b for b in boroughs if b.name == selected_borough]
    if current:
        # No fill and no tooltip: a filled polygon would swallow every click
        # meant for the segments drawn inside it.
        folium.GeoJson(
            _borough_features(current),
            name="Selected borough",
            style_function=lambda _feature: {
                "color": "#3d6fb4",
                "weight": 2,
                "fill": False,
            },
        ).add_to(fmap)


# ~11cm at NYC's latitude -- visually identical to full float64 precision but
# meaningfully shorter once serialized across up to MAX_RENDERED_SEGMENTS
# features per render (#421).
_COORDINATE_PRECISION = 6


def _rounded_coordinates(coordinates: Any) -> Any:
    if isinstance(coordinates, (int, float)):
        return round(coordinates, _COORDINATE_PRECISION)
    return [_rounded_coordinates(value) for value in coordinates]


def _feature_collection(segments: Sequence[ScoredRoadSegment]) -> dict[str, Any]:
    return {
        "type": "FeatureCollection",
        "features": [_feature(segment) for segment in segments],
    }


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
