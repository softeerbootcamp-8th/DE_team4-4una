"""Borough 단위 segment 조회, UI 프레임워크에 의존하지 않는다.

한 borough를 고르면 그 안의 segment를 전부 만들어 gzip으로 눌러 캐시한다. 지도를
움직이는 동안에는 서버를 부르지 않으므로, 요청은 borough를 바꿀 때만 나간다.

스냅샷(segment 목록, borough 인덱스)과 이 캐시를 프로세스 메모리에
들고 있어서 uvicorn worker는 1개여야 한다. worker를 늘리면 worker마다 통째로
중복해 올리고 각자 따로 콜드 스타트를 한다.
"""

from __future__ import annotations

import gzip
import json
import logging
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from dashboard.config import (
    PAYLOAD_CACHE_MAX_ENTRIES,
    ROAD_SEGMENT_CACHE_TTL_SECONDS,
    SCORE_CACHE_TTL_SECONDS,
    SNAPSHOT_FALLBACK_MAX_SEGMENTS,
    VEHICLE_PROFILES,
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

logger = logging.getLogger(__name__)

# 만료된 borough를 다시 만들어두기 위해 백그라운드 스레드가 도는 주기. TTL보다
# 훨씬 짧게 잡아 만료 직후 채워지게 하되, 캐시가 살아 있으면 dict 조회만 하고
# 넘어가므로 이 주기 자체는 거의 공짜다.
PREWARM_TICK_SECONDS = 60


# (vehicle_profile_id, borough). borough가 None이면 아직 아무것도 안 고른 상태다.
_PayloadKey = tuple[int, str | None]


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
class VehicleProfile:
    vehicle_profile_id: int
    name: str


@dataclass(frozen=True, slots=True)
class Bootstrap:
    total_segment_count: int
    boroughs: tuple[BoroughSummary, ...]
    vehicle_profiles: tuple[VehicleProfile, ...]
    default_vehicle_profile_id: int


@dataclass(frozen=True, slots=True)
class SegmentPayload:
    """gzip으로 눌러둔 GeoJSON 응답 본문과 그 메타데이터.

    dict가 아니라 눌린 bytes로 들고 있는다 -- borough 하나가 GeoJSON으로 18MB인데
    파이썬 dict로 두면 그보다 훨씬 커진다. gzip 후에는 4MB대라 borough 다섯 개를
    전부 캐시해도 20MB 남짓이다.
    """

    segment_count: int
    truncated: bool
    body: bytes
    expires_at: float


@dataclass(frozen=True, slots=True)
class _Snapshot:
    segments: tuple[RoadSegment, ...]
    boroughs: tuple[Borough, ...]
    # borough 이름 -> segment 위치.
    borough_indices: Mapping[str, frozenset[int]]
    expires_at: float


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
        # (vehicle_profile_id, borough) -> 눌러둔 응답. 최근에 쓴 것이 뒤로 가는
        # 순서를 유지해, 상한을 넘으면 앞에서부터 버린다.
        self._payloads: OrderedDict[_PayloadKey, SegmentPayload] = OrderedDict()
        self._payloads_guard = threading.Lock()
        # 조합마다 따로 잠근다. 하나를 만드는 동안(Serving API 왕복이 포함되어
        # 수 초가 걸린다) 이미 캐시된 다른 조합의 요청까지 막히면 안 된다.
        self._build_locks: dict[_PayloadKey, threading.Lock] = {}
        self._build_locks_guard = threading.Lock()
        # 사용자가 실제로 열어본 (프로필, borough). 프리워밍은 기동 직후 기본
        # 프로필로 한 바퀴만 돌고, 그 뒤로는 여기 있는 것만 갱신한다 -- 아무도
        # 안 여는 조합을 15분마다 다시 만들면 Serving API에 헛조회만 쌓인다.
        self._requested: set[tuple[int, str]] = set()
        self._requested_guard = threading.Lock()
        self._prewarmed_snapshot: _Snapshot | None = None
        self._prewarm_started = False

    def bootstrap(self) -> Bootstrap:
        snapshot = self._ensure_snapshot()
        return Bootstrap(
            total_segment_count=len(snapshot.segments),
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
            vehicle_profiles=tuple(
                VehicleProfile(vehicle_profile_id=profile_id, name=name)
                for profile_id, name in VEHICLE_PROFILES
            ),
            default_vehicle_profile_id=self._config.vehicle_profile_id,
        )

    def get_segments(
        self,
        borough: str | None,
        vehicle_profile_id: int | None = None,
    ) -> SegmentPayload:
        """borough 안의 segment 전부를 gzip된 GeoJSON 응답으로 돌려준다.

        프로필을 안 주면 배포 기본값을 쓴다. 프로필이 유효한지는 확인하지 않는다 --
        없거나 비활성인 id면 Serving API가 프로필 0으로 내려주고 응답에
        vehicle_profile_fallback으로 알려준다.
        """
        profile_id = (
            self._config.vehicle_profile_id
            if vehicle_profile_id is None
            else vehicle_profile_id
        )
        if borough is not None:
            with self._requested_guard:
                self._requested.add((profile_id, borough))
        return self._payload(borough, profile_id)

    def _payload(self, borough: str | None, profile_id: int) -> SegmentPayload:
        """살아 있는 캐시가 있으면 그대로 쓰고, 없으면 이 조합에 대해서만 잠그고
        만든다 -- 같은 조합을 동시에 요청한 두 번째 요청은 첫 번째가 만든 것을 받는다."""
        snapshot = self._ensure_snapshot()
        key = (profile_id, borough)
        cached = self._cached(key)
        if cached is not None:
            return cached

        with self._build_lock(key):
            cached = self._cached(key)
            if cached is not None:
                return cached
            payload = self._build_payload(snapshot, borough, profile_id)
            self._store(key, payload)
            return payload

    def _cached(self, key: _PayloadKey) -> SegmentPayload | None:
        with self._payloads_guard:
            payload = self._payloads.get(key)
            if payload is None or payload.expires_at <= self._now():
                return None
            self._payloads.move_to_end(key)
            return payload

    def _store(self, key: _PayloadKey, payload: SegmentPayload) -> None:
        with self._payloads_guard:
            self._payloads[key] = payload
            self._payloads.move_to_end(key)
            now = self._now()
            for expired in [k for k, v in self._payloads.items() if v.expires_at <= now]:
                del self._payloads[expired]
            # 프로필까지 곱해지면 조합이 30개까지 늘어난다. 하나가 4MB대라
            # 상한이 없으면 캐시만으로 100MB를 넘긴다.
            while len(self._payloads) > PAYLOAD_CACHE_MAX_ENTRIES:
                self._payloads.popitem(last=False)

    def start_prewarm(self) -> None:
        """borough별 응답을 미리 만들어두는 백그라운드 스레드를 띄운다.

        사용자가 outline을 보며 borough를 고르는 사이에 서버가 미리 만들어두면,
        첫 클릭이 캐시 히트가 된다.
        """
        if self._prewarm_started:
            return
        self._prewarm_started = True
        threading.Thread(
            target=self._prewarm_loop, name="dashboard-prewarm", daemon=True
        ).start()

    def _prewarm_loop(self) -> None:
        while True:
            try:
                for profile_id, borough in self._prewarm_targets():
                    self._payload(borough, profile_id)
            except Exception:
                # 스레드가 죽으면 다시 뜨지 않는다. Serving API가 잠깐 내려간
                # 정도로 프리워밍을 영구히 잃지 않도록 다음 tick에 재시도한다.
                logger.exception("prewarm tick failed")
            time.sleep(PREWARM_TICK_SECONDS)

    def _prewarm_targets(self) -> list[tuple[int, str]]:
        """이번 tick에 만들어둘 (프로필, borough).

        새 스냅샷을 처음 보는 tick에서는 **기본 프로필로만** 전부 한 바퀴 돈다 --
        배포 직후 첫 클릭이 캐시 히트가 되게 하려는 것이다. 프로필까지 곱해서
        미리 만들면 6 x 5 = 30개를 아무도 요청하기 전에 만들게 되고, Serving API
        조회량도 그만큼 곱해진다.

        그 뒤로는 실제로 열어본 조합만 갱신한다. zone master가 없어 borough가
        없으면 대상도 없다(상한이 걸린 fallback이라 미리 만들 이유가 없다).
        """
        snapshot = self._ensure_snapshot()
        if snapshot is not self._prewarmed_snapshot:
            self._prewarmed_snapshot = snapshot
            default_profile_id = self._config.vehicle_profile_id
            return [(default_profile_id, borough.name) for borough in snapshot.boroughs]
        with self._requested_guard:
            return sorted(self._requested)

    def _build_lock(self, key: _PayloadKey) -> threading.Lock:
        with self._build_locks_guard:
            return self._build_locks.setdefault(key, threading.Lock())

    def _build_payload(
        self,
        snapshot: _Snapshot,
        borough: str | None,
        profile_id: int,
    ) -> SegmentPayload:
        segments, truncated = self._segments_for(snapshot, borough)
        scores, effective_profile_id, fallback = self._scores_for(segments, profile_id)
        body = {
            "segment_count": len(segments),
            "truncated": truncated,
            "requested_vehicle_profile_id": profile_id,
            "effective_vehicle_profile_id": effective_profile_id,
            "vehicle_profile_fallback": fallback,
            "features": build_feature_collection(
                join_road_segments_with_scores(segments, scores)
            ),
        }
        return SegmentPayload(
            segment_count=len(segments),
            truncated=truncated,
            body=gzip.compress(json.dumps(body, separators=(",", ":")).encode(), 6),
            expires_at=self._now() + SCORE_CACHE_TTL_SECONDS,
        )

    def _segments_for(
        self,
        snapshot: _Snapshot,
        borough: str | None,
    ) -> tuple[list[RoadSegment], bool]:
        """그릴 segment 전부. 두 번째 값은 상한에 걸려 잘렸는지 여부다."""
        if borough is not None:
            if borough not in snapshot.borough_indices:
                raise UnknownBoroughError(borough)
            indices = sorted(snapshot.borough_indices[borough])
            return [snapshot.segments[index] for index in indices], False
        if snapshot.boroughs:
            # 아직 아무 borough도 고르지 않았다. outline만 보여주는 상태라
            # 도로는 하나도 그리지 않는다.
            return [], False
        # zone master가 없어 borough 개념 자체가 없는 배포. 기준이 스냅샷
        # 전체뿐이라 상한을 걸지 않으면 응답이 20MB를 넘는다.
        segments = list(snapshot.segments[:SNAPSHOT_FALLBACK_MAX_SEGMENTS])
        return segments, len(snapshot.segments) > len(segments)

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
            # 새 스냅샷의 segment는 예전 응답과 다를 수 있다.
            self._payloads.clear()
            return snapshot

    def _load_snapshot(self) -> _Snapshot:
        config = self._config
        segments = load_road_segments(config.road_segment_s3_uri, config.aws_region)

        boroughs: tuple[Borough, ...] = ()
        borough_indices: dict[str, frozenset[int]] = {}
        if config.zone_master_s3_uri:
            # outline과 zone 조회가 다운로드 하나를 나눠 쓴다.
            zone_master = load_zone_master(config.zone_master_s3_uri, config.aws_region)
            boroughs = tuple(borough_outlines(zone_master))
            borough_indices = group_indices_by_borough(segments, zone_boroughs(zone_master))

        return _Snapshot(
            segments=tuple(segments),
            boroughs=boroughs,
            borough_indices=borough_indices,
            expires_at=self._now() + ROAD_SEGMENT_CACHE_TTL_SECONDS,
        )

    def _scores_for(
        self,
        segments: Sequence[RoadSegment],
        profile_id: int,
    ) -> tuple[dict[str, ComfortScore], int, bool]:
        """borough 전체 점수를 한 번에 조회한다.

        serving API가 요청 하나에 1000건까지 받으므로 3만 건이면 30번으로 쪼개져
        나가고, fetch_comfort_scores가 그중 8개씩 병렬로 던진다.
        """
        if not segments:
            return {}, profile_id, False

        result = fetch_comfort_scores(
            endpoint=self._config.batch_endpoint,
            vehicle_profile_id=profile_id,
            segment_ids=[segment.segment_id for segment in segments],
            batch_size=self._config.batch_chunk_size,
            timeout_seconds=self._config.request_timeout_seconds,
        )
        return (
            result.scores,
            result.effective_vehicle_profile_id,
            result.vehicle_profile_fallback,
        )
