import inspect

import folium
import pytest
from dashboard.config import NYC_MAP_CENTER, NYC_MAP_ZOOM
from dashboard.map_view import (
    GRAY,
    GREEN,
    RED,
    YELLOW,
    ScoredRoadSegment,
    build_base_map,
    build_segment_feature_group,
    comfort_score_color,
    join_road_segments_with_scores,
)
from dashboard.road_geometry import RoadSegment
from dashboard.serving_api_client import ComfortScore
from dashboard.zone_master import Borough


@pytest.mark.parametrize(
    ("score", "expected"),
    [
        (100.0, GREEN),
        (80.0, GREEN),
        (79.999, YELLOW),
        (60.0, YELLOW),
        (59.999, RED),
        (0.0, RED),
        (None, GRAY),
    ],
)
def test_comfort_score_color(score: float | None, expected: str) -> None:
    assert comfort_score_color(score) == expected


def test_join_keeps_segments_without_api_score_and_colors_them_gray() -> None:
    segments = [
        RoadSegment(
            segment_id="1",
            street_name="First Avenue",
            geometry={"type": "LineString", "coordinates": [[-74.0, 40.7]] * 2},
        ),
        RoadSegment(
            segment_id="2",
            street_name="Second Avenue",
            geometry={"type": "LineString", "coordinates": [[-73.9, 40.8]] * 2},
        ),
    ]
    scores = {
        "1": ComfortScore(
            segment_id="1",
            comfort_score=82.0,
            confidence_score=0.9,
            source="current",
            weather_time="2026-08-24T00:00:00Z",
        )
    }

    joined = join_road_segments_with_scores(segments, scores)

    assert [segment.road_segment.segment_id for segment in joined] == ["1", "2"]
    assert joined[0].score == scores["1"]
    assert joined[0].color == GREEN
    assert joined[1].score is None
    assert joined[1].color == GRAY


def _scored_segment(segment_id: str, comfort_score: float | None) -> ScoredRoadSegment:
    road_segment = RoadSegment(
        segment_id=segment_id,
        street_name="Test Street",
        geometry={
            "type": "LineString",
            "coordinates": [[-74.0, 40.7], [-74.01, 40.71]],
        },
    )
    score = (
        None
        if comfort_score is None
        else ComfortScore(
            segment_id=segment_id,
            comfort_score=comfort_score,
            confidence_score=0.5,
            source="current",
            weather_time="2026-08-24T00:00:00Z",
        )
    )
    return ScoredRoadSegment(road_segment=road_segment, score=score)


def _borough(name: str) -> Borough:
    return Borough(
        name=name,
        geometry={
            "type": "Polygon",
            "coordinates": [[[-74.02, 40.70], [-74.02, 40.72], [-74.0, 40.72], [-74.0, 40.70], [-74.02, 40.70]]],
        },
        bounds=(-74.02, 40.70, -74.0, 40.72),
    )


class TestBuildBaseMap:
    def test_builds_without_boroughs_or_a_selection(self) -> None:
        build_base_map()

    def test_never_takes_a_center_or_zoom_argument(self) -> None:
        # pan/zoom 값을 base map 생성에 흘려보내면 base map 자체가 매번 새로
        # 만들어진다 -- 이 이슈가 없애려는 재생성 비용이 그대로 남는다(#421).
        parameters = inspect.signature(build_base_map).parameters
        assert "center" not in parameters
        assert "zoom" not in parameters

    def test_uses_the_default_center_and_zoom(self) -> None:
        fmap = build_base_map()
        assert fmap.location == list(NYC_MAP_CENTER)
        assert fmap.options["zoom"] == NYC_MAP_ZOOM

    def test_legend_is_present_exactly_once(self) -> None:
        html = build_base_map().get_root().render()
        assert html.count("Comfort Score") == 1

    def test_never_contains_road_segment_data(self) -> None:
        html = build_base_map(
            boroughs=[_borough("Manhattan")], selected_borough="Manhattan"
        ).get_root().render()
        assert "comfort_score" not in html
        assert "NYC road comfort score" not in html

    def test_draws_selectable_and_selected_borough_layers(self) -> None:
        html = build_base_map(
            boroughs=[_borough("Manhattan"), _borough("Brooklyn")],
            selected_borough="Manhattan",
        ).get_root().render()
        # The non-selected borough keeps its fill (clickable, selectable)...
        assert '"fillOpacity": 0.15' in html
        # ...while the selected one has none, so clicks reach segments inside it.
        assert '"fill": false' in html


class TestBuildSegmentFeatureGroup:
    def test_empty_segment_list_does_not_raise(self) -> None:
        group = build_segment_feature_group([])
        assert isinstance(group, folium.FeatureGroup)

    def test_contains_road_geojson_for_the_given_segments(self) -> None:
        group = build_segment_feature_group([_scored_segment("1", 90.0)])
        fmap = folium.Map(location=NYC_MAP_CENTER, zoom_start=NYC_MAP_ZOOM)
        group.add_to(fmap)
        html = fmap.get_root().render()
        assert '"segment_id": "1"' in html
        assert "-74.01" in html

    def test_preserves_the_tooltip_fields(self) -> None:
        group = build_segment_feature_group([_scored_segment("42", 55.0)])
        fmap = folium.Map(location=NYC_MAP_CENTER, zoom_start=NYC_MAP_ZOOM)
        group.add_to(fmap)
        html = fmap.get_root().render()
        for field in (
            "segment_id",
            "street_name",
            "comfort_score",
            "confidence_score",
            "source",
            "weather_time",
        ):
            assert field in html

    def test_rounds_coordinates_to_cut_geojson_payload_size(self) -> None:
        # 좌표 정밀도를 낮춰 직렬화/전송 크기를 줄인다 -- 시각적으로는 구분이
        # 안 되는 수준(~11cm)이라 지도에 그려지는 결과는 그대로다(#421).
        road_segment = RoadSegment(
            segment_id="1",
            street_name="Test Street",
            geometry={
                "type": "LineString",
                "coordinates": [[-74.00000012345, 40.70000098765]],
            },
        )
        segment = ScoredRoadSegment(road_segment=road_segment, score=None)
        group = build_segment_feature_group([segment])
        fmap = folium.Map(location=NYC_MAP_CENTER, zoom_start=NYC_MAP_ZOOM)
        group.add_to(fmap)
        html = fmap.get_root().render()
        assert "-74.00000012345" not in html
        assert "-74.0" in html

    def test_preserves_the_comfort_score_color_rules(self) -> None:
        segments = [
            _scored_segment("green", 90.0),
            _scored_segment("yellow", 65.0),
            _scored_segment("red", 10.0),
            _scored_segment("gray", None),
        ]
        group = build_segment_feature_group(segments)
        fmap = folium.Map(location=NYC_MAP_CENTER, zoom_start=NYC_MAP_ZOOM)
        group.add_to(fmap)
        html = fmap.get_root().render()
        assert '"color": "green"' in html
        assert '"color": "yellow"' in html
        assert '"color": "red"' in html
        assert '"color": "gray"' in html
