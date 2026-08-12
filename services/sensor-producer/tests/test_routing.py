from datetime import UTC, datetime

from sensor_producer.domain import RoadSegment
from sensor_producer.routing import RoadRouter
from shapely.geometry import LineString


def segment(
    segment_id: str,
    from_node: str,
    to_node: str,
    direction: str,
    coordinates: list[tuple[float, float]],
) -> RoadSegment:
    return RoadSegment(
        segment_id=segment_id,
        from_node_id=from_node,
        to_node_id=to_node,
        traffic_direction=direction,
        street_name="TEST STREET",
        geometry=LineString(coordinates),
        length_m=100.0,
        posted_speed_mph=25.0,
        curve_radius_m=None,
    )


def test_router_respects_lion_traffic_direction() -> None:
    router = RoadRouter(
        [
            segment("s1", "n1", "n2", "W", [(-74.0, 40.0), (-73.999, 40.0)]),
            segment("s2", "n2", "n3", "T", [(-73.999, 40.0), (-73.998, 40.0)]),
        ]
    )

    forward = router.shortest_path("n1", "n3")
    backward = router.shortest_path("n3", "n1")

    assert [edge.segment.segment_id for edge in forward] == ["s1", "s2"]
    assert backward == ()


def test_route_plan_is_stamped_at_request_time() -> None:
    road = segment("s1", "n1", "n2", "T", [(-74.0, 40.0), (-73.999, 40.0)])
    router = RoadRouter([road])
    planned_at = datetime(2024, 2, 1, 10, tzinfo=UTC)

    plan = router._to_plan(
        "trip-1",
        planned_at,
        "n1",
        "n2",
        router.shortest_path("n1", "n2"),
    )

    assert plan.planned_at == planned_at
    assert plan.segment_ids == ("s1",)
