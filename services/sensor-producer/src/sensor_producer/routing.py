"""Deterministic shortest-path routing over canonical LION segments."""

from __future__ import annotations

import hashlib
import heapq
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime

from shapely.geometry import Point
from shapely.strtree import STRtree

from sensor_producer.domain import RoadSegment, RouteLeg, RoutePlan
from sensor_producer.errors import TripInfeasibleError, TripSkipReason
from sensor_producer.geo import reversed_line

METERS_PER_MILE = 1609.344
# 존별 후보를 제한해 최악의 경우 경로 탐색을 400회로 제한한다.
MAX_ROUTE_CANDIDATES_PER_ZONE = 20
# TLC 기록 거리 대비 15% 이내면 시뮬레이션에 사용할 경로로 채택한다.
ACCEPTABLE_DISTANCE_ERROR_RATIO = 0.15


@dataclass(frozen=True, slots=True)
class GraphEdge:
    destination: str
    segment: RoadSegment
    reverse: bool


class RoadRouter:
    def __init__(self, segments: list[RoadSegment]):
        self.adjacency: dict[str, list[GraphEdge]] = defaultdict(list)
        self.node_coordinates: dict[str, tuple[float, float]] = {}
        self.segments_by_id = {segment.segment_id: segment for segment in segments}
        if len(self.segments_by_id) != len(segments):
            raise ValueError("road segments must have unique segment_id values")
        for segment in segments:
            coordinates = list(segment.geometry.coords)
            self.node_coordinates.setdefault(segment.from_node_id, coordinates[0])
            self.node_coordinates.setdefault(segment.to_node_id, coordinates[-1])
            if segment.traffic_direction in {"W", "T"}:
                self.adjacency[segment.from_node_id].append(
                    GraphEdge(segment.to_node_id, segment, False)
                )
            if segment.traffic_direction in {"A", "T"}:
                self.adjacency[segment.to_node_id].append(
                    GraphEdge(segment.from_node_id, segment, True)
                )
        for edges in self.adjacency.values():
            edges.sort(key=lambda edge: (edge.segment.segment_id, edge.destination))
        self._routable_node_ids = tuple(sorted(self.adjacency))
        self._node_points = [
            Point(self.node_coordinates[node]) for node in self._routable_node_ids
        ]
        self._node_tree = STRtree(self._node_points)

    def zone_anchor_nodes(self, polygon: object) -> tuple[str, ...]:
        """택시존을 대표할 서로 떨어진 LION 노드를 결정론적으로 고른다"""
        nodes = self._nodes_inside(polygon)
        if not nodes:
            return ()
        representative = polygon.representative_point()  # type: ignore[union-attr]
        primary = min(
            nodes,
            key=lambda node: (
                representative.distance(Point(self.node_coordinates[node])),
                node,
            ),
        )
        remaining = [node for node in nodes if node != primary]
        if not remaining:
            return (primary,)
        primary_point = Point(self.node_coordinates[primary])
        secondary = max(
            remaining,
            key=lambda node: (
                primary_point.distance(Point(self.node_coordinates[node])),
                node,
            ),
        )
        return primary, secondary

    def plan_for_zones(
        self,
        trip_id: str,
        planned_at: datetime,
        pickup_zone: object,
        dropoff_zone: object,
        target_distance_m: float | None = None,
    ) -> RoutePlan:
        if target_distance_m is not None and target_distance_m <= 0:
            raise ValueError("target route distance must be positive")
        pickup_nodes = self._nodes_inside(pickup_zone)
        dropoff_nodes = self._nodes_inside(dropoff_zone)
        if not pickup_nodes:
            raise TripInfeasibleError(
                TripSkipReason.PICKUP_ZONE_NO_ROUTABLE_NODES,
                "pickup taxi zone has no routable LION nodes",
            )
        if not dropoff_nodes:
            raise TripInfeasibleError(
                TripSkipReason.DROPOFF_ZONE_NO_ROUTABLE_NODES,
                "drop-off taxi zone has no routable LION nodes",
            )

        seed = int.from_bytes(hashlib.sha256(trip_id.encode()).digest()[:8], "big")
        pickup_offset = seed % len(pickup_nodes)
        dropoff_offset = (seed // max(1, len(pickup_nodes))) % len(dropoff_nodes)
        pickup_order = pickup_nodes[pickup_offset:] + pickup_nodes[:pickup_offset]
        dropoff_order = dropoff_nodes[dropoff_offset:] + dropoff_nodes[:dropoff_offset]

        # TLC 주행거리와 가장 가까운 결정론적 경로를 유지한다.
        best: tuple[float, str, str, tuple[GraphEdge, ...]] | None = None
        candidate_ends = dropoff_order[:MAX_ROUTE_CANDIDATES_PER_ZONE]
        for start_node in pickup_order[:MAX_ROUTE_CANDIDATES_PER_ZONE]:
            # 출발 노드마다 Dijkstra를 한 번만 실행해 모든 도착 후보 경로를 얻는다
            paths = self.shortest_paths(
                start_node,
                {end_node for end_node in candidate_ends if end_node != start_node},
            )
            for end_node in candidate_ends:
                edges = paths.get(end_node, ())
                if not edges:
                    continue
                if target_distance_m is None:
                    return self._to_plan(trip_id, planned_at, start_node, end_node, edges)
                route_length_m = sum(edge.segment.length_m for edge in edges)
                error_ratio = abs(route_length_m - target_distance_m) / target_distance_m
                candidate = (error_ratio, start_node, end_node, edges)
                if best is None or candidate[:3] < best[:3]:
                    best = candidate
                if error_ratio <= ACCEPTABLE_DISTANCE_ERROR_RATIO:
                    break
            if best is not None and best[0] <= ACCEPTABLE_DISTANCE_ERROR_RATIO:
                break
        if best is not None:
            _, start_node, end_node, edges = best
            return self._to_plan(trip_id, planned_at, start_node, end_node, edges)
        raise TripInfeasibleError(
            TripSkipReason.NO_DIRECTED_ROUTE,
            "no directed LION route found between deterministic zone points",
        )

    def shortest_path(self, start_node: str, end_node: str) -> tuple[GraphEdge, ...]:
        return self.shortest_paths(start_node, {end_node}).get(end_node, ())

    def shortest_paths(
        self,
        start_node: str,
        end_nodes: set[str],
    ) -> dict[str, tuple[GraphEdge, ...]]:
        if not end_nodes:
            return {}
        queue: list[tuple[float, str]] = [(0.0, start_node)]
        distances = {start_node: 0.0}
        previous: dict[str, tuple[str, GraphEdge]] = {}
        remaining = set(end_nodes)
        while queue:
            distance, node = heapq.heappop(queue)
            if distance != distances.get(node):
                continue
            remaining.discard(node)
            if not remaining:
                break
            for edge in self.adjacency.get(node, []):
                candidate = distance + edge.segment.length_m
                if candidate < distances.get(edge.destination, float("inf")):
                    distances[edge.destination] = candidate
                    previous[edge.destination] = (node, edge)
                    heapq.heappush(queue, (candidate, edge.destination))
        routes: dict[str, tuple[GraphEdge, ...]] = {}
        for end_node in end_nodes:
            if end_node not in previous:
                continue
            route: list[GraphEdge] = []
            current = end_node
            while current != start_node:
                current, edge = previous[current]
                route.append(edge)
            route.reverse()
            routes[end_node] = tuple(route)
        return routes

    def plan_from_segments(
        self,
        trip_id: str,
        planned_at: datetime,
        start_node: str,
        end_node: str,
        segment_ids: tuple[str, ...],
        reverse_flags: tuple[bool, ...],
    ) -> RoutePlan:
        if not segment_ids or len(segment_ids) != len(reverse_flags):
            raise ValueError("prepared route segments and directions must align")

        edges: list[GraphEdge] = []
        current_node = start_node
        for segment_id, reverse in zip(segment_ids, reverse_flags, strict=True):
            try:
                segment = self.segments_by_id[segment_id]
            except KeyError as error:
                raise ValueError(
                    f"prepared route references unknown segment {segment_id}"
                ) from error
            origin = segment.to_node_id if reverse else segment.from_node_id
            destination = segment.from_node_id if reverse else segment.to_node_id
            if origin != current_node:
                raise ValueError("prepared route segments are not contiguous")
            edges.append(GraphEdge(destination, segment, reverse))
            current_node = destination
        if current_node != end_node:
            raise ValueError("prepared route does not end at end_node_id")
        return self._to_plan(
            trip_id,
            planned_at,
            start_node,
            end_node,
            tuple(edges),
        )

    def _nodes_inside(self, polygon: object) -> list[str]:
        indices = self._node_tree.query(polygon, predicate="covers")
        return sorted(self._routable_node_ids[int(index)] for index in indices)

    @staticmethod
    def _to_plan(
        trip_id: str,
        planned_at: datetime,
        start_node: str,
        end_node: str,
        edges: tuple[GraphEdge, ...],
    ) -> RoutePlan:
        legs = tuple(
            RouteLeg(
                segment_id=edge.segment.segment_id,
                geometry=(
                    reversed_line(edge.segment.geometry)
                    if edge.reverse
                    else edge.segment.geometry
                ),
                length_m=edge.segment.length_m,
                posted_speed_mph=edge.segment.posted_speed_mph,
                curve_radius_m=edge.segment.curve_radius_m,
                pavement_rating=edge.segment.pavement_rating,
                hump_distances_m=tuple(
                    edge.segment.length_m * (1 - fraction if edge.reverse else fraction)
                    for fraction in edge.segment.hump_fractions
                ),
            )
            for edge in edges
        )
        return RoutePlan(
            trip_id=trip_id,
            planned_at=planned_at,
            start_node_id=start_node,
            end_node_id=end_node,
            legs=legs,
            total_length_m=sum(leg.length_m for leg in legs),
        )
