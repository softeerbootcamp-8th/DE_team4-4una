"""Deterministic shortest-path routing over canonical LION segments."""

from __future__ import annotations

import hashlib
import heapq
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime

from shapely.geometry import Point

from sensor_producer.domain import RoadSegment, RouteLeg, RoutePlan
from sensor_producer.geo import reversed_line


@dataclass(frozen=True, slots=True)
class GraphEdge:
    destination: str
    segment: RoadSegment
    reverse: bool


class RoadRouter:
    def __init__(self, segments: list[RoadSegment]):
        self.adjacency: dict[str, list[GraphEdge]] = defaultdict(list)
        self.node_coordinates: dict[str, tuple[float, float]] = {}
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

    def plan_for_zones(
        self,
        trip_id: str,
        planned_at: datetime,
        pickup_zone: object,
        dropoff_zone: object,
    ) -> RoutePlan:
        pickup_nodes = self._nodes_inside(pickup_zone)
        dropoff_nodes = self._nodes_inside(dropoff_zone)
        if not pickup_nodes or not dropoff_nodes:
            raise ValueError("taxi zone has no routable LION nodes in the downloaded environment")

        seed = int.from_bytes(hashlib.sha256(trip_id.encode()).digest()[:8], "big")
        pickup_offset = seed % len(pickup_nodes)
        dropoff_offset = (seed // max(1, len(pickup_nodes))) % len(dropoff_nodes)
        pickup_order = pickup_nodes[pickup_offset:] + pickup_nodes[:pickup_offset]
        dropoff_order = dropoff_nodes[dropoff_offset:] + dropoff_nodes[:dropoff_offset]

        for start_node in pickup_order[:30]:
            for end_node in dropoff_order[:30]:
                if start_node == end_node:
                    continue
                edges = self.shortest_path(start_node, end_node)
                if edges:
                    return self._to_plan(trip_id, planned_at, start_node, end_node, edges)
        raise ValueError("no directed LION route found between deterministic zone points")

    def shortest_path(self, start_node: str, end_node: str) -> tuple[GraphEdge, ...]:
        queue: list[tuple[float, str]] = [(0.0, start_node)]
        distances = {start_node: 0.0}
        previous: dict[str, tuple[str, GraphEdge]] = {}
        while queue:
            distance, node = heapq.heappop(queue)
            if node == end_node:
                break
            if distance != distances.get(node):
                continue
            for edge in self.adjacency.get(node, []):
                candidate = distance + edge.segment.length_m
                if candidate < distances.get(edge.destination, float("inf")):
                    distances[edge.destination] = candidate
                    previous[edge.destination] = (node, edge)
                    heapq.heappush(queue, (candidate, edge.destination))
        if end_node not in previous:
            return ()
        route: list[GraphEdge] = []
        current = end_node
        while current != start_node:
            current, edge = previous[current]
            route.append(edge)
        route.reverse()
        return tuple(route)

    def _nodes_inside(self, polygon: object) -> list[str]:
        return sorted(
            node_id
            for node_id, coordinate in self.node_coordinates.items()
            if polygon.covers(Point(coordinate))  # type: ignore[union-attr]
            and node_id in self.adjacency
        )

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
