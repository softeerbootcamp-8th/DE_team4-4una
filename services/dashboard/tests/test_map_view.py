import pytest
from dashboard.map_view import (
    GRAY,
    GREEN,
    RED,
    YELLOW,
    build_map,
    comfort_score_color,
    join_road_segments_with_scores,
)
from dashboard.road_geometry import RoadSegment
from dashboard.serving_api_client import ComfortScore


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


def test_build_map_renders_without_segments() -> None:
    """The first render happens before the viewport is known, so an empty
    segment list must not raise: GeoJsonTooltip asserts its field names against
    the first feature and finds none in an empty collection."""
    build_map([])
