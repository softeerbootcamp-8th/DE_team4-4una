"""Viewport queries over the road snapshot, independent of any UI framework.

app.py(Streamlit)가 들고 있던 데이터 로직을 그대로 옮긴 것이다. Streamlit의
@st.cache_data / @st.cache_resource가 하던 일은 이 객체가 프로세스 메모리에
스냅샷을 들고 있는 것으로 대체한다 -- 그래서 uvicorn worker는 1개여야 한다.
worker를 늘리면 프로세스마다 segment와 STRtree를 통째로 중복해서 들고, 각자
따로 콜드 스타트를 한다.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from shapely.geometry import box
from shapely.strtree import STRtree

from dashboard.config import (
    MAX_RENDERED_SEGMENTS,
    ROAD_SEGMENT_CACHE_TTL_SECONDS,
    SCORE_CACHE_TTL_SECONDS,
    DashboardConfig,
)
from dashboard.geojson import build_feature_collection, join_road_segments_with_scores
from dashboard.road_geometry import RoadSegment, load_road_segments
from dashboard.serving_api_client import ComfortScore, fetch_comfort_scores
from dashboard.zone_master import (
    Borough,
    borough_outlines,
    load_zone_master,
    zone_boroughs,
)

# (min_lon, min_lat, max_lon, max_lat) per segment, and (south, west, north,
# east) for a viewport -- the two orders differ because the first follows
# GeoJSON coordinate order and the second follows Leaflet's bounds payload.
SegmentBounds = tuple[float, float, float, float]
Viewport = tuple[float, float, float, float]

# score 캐시가 무한히 자라지 않게 하는 상한. 스냅샷 전체(16만 대)보다 넉넉하지만,
# vehicle profile이 여러 개 섞여도 프로세스를 먹어치우지는 않을 정도로 잡았다.
SCORE_CACHE_MAX_ENTRIES = 200_000


class UnknownBoroughError(ValueError):
    """Raised when a borough name is not in the loaded zone reference."""


@dataclass(frozen=True, slots=True)
class BoroughSummary:
    name: str
    center: tuple[float, float]
    bounds: tuple[float, float, float, float]
    segment_count: int
    geometry: dict[str, Any]


@dataclass(frozen=True, slots=True)
class Bootstrap:
    total_segment_count: int
    max_rendered_segments: int
    boroughs: tuple[BoroughSummary, ...]


@dataclass(frozen=True, slots=True)
class ViewportSegments:
    in_viewport_count: int
    rendered_count: int
    truncated: bool
    requested_vehicle_profile_id: int
    effective_vehicle_profile_id: int
    vehicle_profile_fallback: bool
    features: dict[str, Any]


@dataclass(frozen=True, slots=True)
class _CachedScore:
    expires_at: float
    # None이면 "serving API가 이 segment를 못 찾았다"는 뜻이다. 못 찾은 것도
    # 캐시해야 회색으로 남을 segment를 pan할 때마다 다시 물어보지 않는다.
    score: ComfortScore | None


@dataclass(frozen=True, slots=True)
class _Snapshot:
    segments: tuple[RoadSegment, ...]
    spatial_index: STRtree
    boroughs: tuple[Borough, ...]
    # borough 이름 -> segment 위치. 뷰포트 조회 결과가 이 borough 소속인지
    # O(1)로 확인하기 위한 것이다(#421).
    borough_indices: Mapping[str, frozenset[int]]
    expires_at: float


def geometry_bounds(geometry: Mapping[str, Any]) -> SegmentBounds:
    coordinates = geometry["coordinates"]
    points = (
        coordinates
        if geometry["type"] == "LineString"
        else [point for line in coordinates for point in line]
    )
    longitudes = [point[0] for point in points]
    latitudes = [point[1] for point in points]
    return min(longitudes), min(latitudes), max(longitudes), max(latitudes)


def group_indices_by_borough(
    segments: Sequence[RoadSegment],
    boroughs: Mapping[int, str],
) -> dict[str, frozenset[int]]:
    """모든 segment를 한 번만 순회해 borough 이름 -> segment 위치로 묶는다(#414)."""
    grouped: dict[str, list[int]] = {}
    for index, segment in enumerate(segments):
        if segment.location_id is None:
            continue
        borough = boroughs.get(segment.location_id)
        if borough is not None:
            grouped.setdefault(borough, []).append(index)
    return {name: frozenset(indices) for name, indices in grouped.items()}


def visible_segments(
    road_segments: Sequence[RoadSegment],
    spatial_index: STRtree,
    candidate_set: frozenset[int] | None,
    viewport: Viewport | None,
    max_rendered: int,
) -> tuple[list[RoadSegment], int]:
    """그릴 segment이자 score를 조회할 segment(#421 후속으로 R-tree 사용).

    공간 인덱스로 뷰포트와 겹치는 segment를 먼저 찾는다 -- 도시 전체 기준으로
    O(log N + k)이고 borough 크기와 무관하다. 그 결과가 candidate_set(현재
    borough)에 속하는지는 frozenset 조회라 O(1)이다. candidate_set이 None이면
    전체 스냅샷 모드라는 뜻이라 걸러내지 않는다.

    상한을 적용하기 전에 스냅샷 순서로 정렬한다. STRtree의 내부 순회 순서는
    보장되지 않아서, 살짝만 pan해도 잘려나가는 1000개가 통째로 바뀌며 지도가
    깜빡였다. 스냅샷 인덱스는 뷰포트와 무관하게 고정이라 정렬해두면 겹치는
    영역의 segment는 계속 같은 것이 선택된다.

    viewport를 아직 모르면 ([], 0)을 반환한다. 두 번째 값은 상한 적용 전
    교차 개수로, "N개 더 있음, 확대하라" 안내에 쓴다.
    """
    if viewport is None:
        return [], 0
    south, west, north, east = viewport
    query_result = spatial_index.query(box(west, south, east, north), predicate="intersects")
    if candidate_set is None:
        matched = sorted(int(index) for index in query_result)
    else:
        matched = sorted(int(index) for index in query_result if int(index) in candidate_set)
    visible = [road_segments[index] for index in matched[:max_rendered]]
    return visible, len(matched)


class DashboardService:
    """S3 스냅샷과 Serving API 앞에 놓인, 요청 하나를 처리하는 계층."""

    def __init__(
        self,
        config: DashboardConfig,
        *,
        now: Callable[[], float] = time.monotonic,
    ) -> None:
        self._config = config
        self._now = now
        self._snapshot: _Snapshot | None = None
        # 스냅샷 로딩 중에는 다른 요청도 막힌다. 첫 요청이 느려지는 대신 동시에
        # 들어온 요청들이 S3를 중복해서 읽지 않는다.
        self._snapshot_lock = threading.Lock()
        self._score_cache: dict[tuple[int, str], _CachedScore] = {}
        self._score_lock = threading.Lock()
        # requested profile -> (effective profile, fallback 여부). 전부 캐시
        # 히트라 이번 요청에서 serving API를 부르지 않아도 응답에 실어야 한다.
        self._profile_metadata: dict[int, tuple[int, bool]] = {}

    def bootstrap(self) -> Bootstrap:
        snapshot = self._ensure_snapshot()
        return Bootstrap(
            total_segment_count=len(snapshot.segments),
            max_rendered_segments=MAX_RENDERED_SEGMENTS,
            boroughs=tuple(
                BoroughSummary(
                    name=borough.name,
                    center=borough.center,
                    bounds=borough.bounds,
                    segment_count=len(snapshot.borough_indices.get(borough.name, ())),
                    geometry=borough.geometry,
                )
                for borough in snapshot.boroughs
            ),
        )

    def get_segments_in_viewport(
        self,
        borough: str | None,
        south: float,
        west: float,
        north: float,
        east: float,
    ) -> ViewportSegments:
        snapshot = self._ensure_snapshot()
        candidate_set = self._candidate_set(snapshot, borough)
        segments, in_viewport_count = visible_segments(
            snapshot.segments,
            snapshot.spatial_index,
            candidate_set,
            (south, west, north, east),
            MAX_RENDERED_SEGMENTS,
        )
        scores, effective_profile_id, fallback = self._scores_for(segments)
        return ViewportSegments(
            in_viewport_count=in_viewport_count,
            rendered_count=len(segments),
            truncated=in_viewport_count > len(segments),
            requested_vehicle_profile_id=self._config.vehicle_profile_id,
            effective_vehicle_profile_id=effective_profile_id,
            vehicle_profile_fallback=fallback,
            features=build_feature_collection(
                join_road_segments_with_scores(segments, scores)
            ),
        )

    def _candidate_set(
        self,
        snapshot: _Snapshot,
        borough: str | None,
    ) -> frozenset[int] | None:
        """현재 선택 기준으로 그릴 수 있는 segment index 집합(#421).

        None이면 "전체가 후보"라는 뜻이라 뷰포트 쿼리 결과를 걸러낼 필요가 없다
        (zone master가 없어 borough 개념 자체가 없는 경우).
        """
        if borough is not None:
            if borough not in snapshot.borough_indices:
                raise UnknownBoroughError(borough)
            return snapshot.borough_indices[borough]
        if snapshot.boroughs:
            # Outlines only: drawing the whole network is what made the map unusable.
            return frozenset()
        return None

    def _ensure_snapshot(self) -> _Snapshot:
        snapshot = self._snapshot
        if snapshot is not None and snapshot.expires_at > self._now():
            return snapshot
        with self._snapshot_lock:
            # 락을 기다리는 사이 다른 스레드가 이미 읽어왔을 수 있다.
            snapshot = self._snapshot
            if snapshot is not None and snapshot.expires_at > self._now():
                return snapshot
            snapshot = self._load_snapshot()
            self._snapshot = snapshot
            return snapshot

    def _load_snapshot(self) -> _Snapshot:
        config = self._config
        segments = load_road_segments(config.road_segment_s3_uri, config.aws_region)
        spatial_index = STRtree(
            [box(*geometry_bounds(segment.geometry)) for segment in segments]
        )

        boroughs: tuple[Borough, ...] = ()
        borough_indices: dict[str, frozenset[int]] = {}
        if config.zone_master_s3_uri:
            # outline과 zone 조회가 다운로드 하나를 나눠 쓴다.
            zone_master = load_zone_master(config.zone_master_s3_uri, config.aws_region)
            boroughs = tuple(borough_outlines(zone_master))
            borough_indices = group_indices_by_borough(segments, zone_boroughs(zone_master))

        return _Snapshot(
            segments=tuple(segments),
            spatial_index=spatial_index,
            boroughs=boroughs,
            borough_indices=borough_indices,
            expires_at=self._now() + ROAD_SEGMENT_CACHE_TTL_SECONDS,
        )

    def _scores_for(
        self,
        segments: Sequence[RoadSegment],
    ) -> tuple[dict[str, ComfortScore], int, bool]:
        """segment_id 단위로 캐시한다 -- pan 하면 새로 보이는 것만 조회하면 된다.

        예전에는 보이는 segment_id 튜플 전체가 캐시 키였다. 조금만 움직여도 키가
        달라져 1000건을 통째로 다시 조회했다(#414).
        """
        profile_id = self._config.vehicle_profile_id
        now = self._now()
        scores: dict[str, ComfortScore] = {}
        missing: list[str] = []

        with self._score_lock:
            for segment in segments:
                entry = self._score_cache.get((profile_id, segment.segment_id))
                if entry is None or entry.expires_at <= now:
                    missing.append(segment.segment_id)
                elif entry.score is not None:
                    scores[segment.segment_id] = entry.score
            effective_profile_id, fallback = self._profile_metadata.get(
                profile_id, (profile_id, False)
            )

        if not missing:
            return scores, effective_profile_id, fallback

        result = fetch_comfort_scores(
            endpoint=self._config.batch_endpoint,
            vehicle_profile_id=profile_id,
            segment_ids=missing,
            batch_size=self._config.batch_chunk_size,
            timeout_seconds=self._config.request_timeout_seconds,
        )
        expires_at = self._now() + SCORE_CACHE_TTL_SECONDS
        with self._score_lock:
            self._prune_locked(self._now())
            for segment_id, score in result.scores.items():
                self._score_cache[(profile_id, segment_id)] = _CachedScore(expires_at, score)
            for segment_id in result.not_found_segment_ids:
                self._score_cache[(profile_id, segment_id)] = _CachedScore(expires_at, None)
            self._profile_metadata[profile_id] = (
                result.effective_vehicle_profile_id,
                result.vehicle_profile_fallback,
            )

        scores.update(result.scores)
        return (
            scores,
            result.effective_vehicle_profile_id,
            result.vehicle_profile_fallback,
        )

    def _prune_locked(self, now: float) -> None:
        """상한을 넘었을 때만 만료된 항목을 걷어낸다 -- 평소에는 순회 비용을 안 낸다."""
        if len(self._score_cache) < SCORE_CACHE_MAX_ENTRIES:
            return
        self._score_cache = {
            key: entry
            for key, entry in self._score_cache.items()
            if entry.expires_at > now
        }
        # 전부 살아 있어서 줄지 않았다면 통째로 버린다. 다시 조회하면 되고,
        # 여기까지 왔다는 것은 정상적인 뷰포트 사용 범위를 넘었다는 뜻이다.
        if len(self._score_cache) >= SCORE_CACHE_MAX_ENTRIES:
            self._score_cache = {}
