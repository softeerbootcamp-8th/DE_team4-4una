"""Map Matching: STRtree candidate search + scoring + best-candidate selection in one mapInPandas pass (#479)."""

from __future__ import annotations

import hashlib
import logging
import pickle
from collections import OrderedDict
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd
import shapely
from pyproj import Transformer
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from shapely import STRtree

from batch_jobs.map_matching.candidates import (
    SOURCE_CRS,
    TARGET_CRS,
    RoadSegmentCandidate,
    collect_road_segment_candidates,
    validate_search_radius,
)
from batch_jobs.map_matching.scoring import (
    circular_heading_diff,
    compute_forward_reverse_bearing,
    compute_match_scores,
    resolve_prematched_road_bearing,
    validate_score_weights,
)

logger = logging.getLogger(__name__)

MATCH_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("segment_id", StringType(), nullable=True),
        StructField("road_snapshot_date", DateType(), nullable=True),
        StructField("map_match_distance_m", DoubleType(), nullable=True),
        StructField("map_match_heading_diff_deg", DoubleType(), nullable=True),
        StructField("map_match_score", DoubleType(), nullable=True),
        StructField("candidate_count", LongType(), nullable=False),
        StructField("map_match_status", StringType(), nullable=False),
    ]
)

# match_batch()가 돌려주는 컬럼 중 event_id를 뺀 나머지 — 센서 배치에 덧붙일 매칭 결과다(#560).
MATCH_RESULT_FIELDS = tuple(MATCH_SCHEMA.fields[1:])

# 센서 컬럼을 통과시켜도 매칭 자체에 반드시 필요한 컬럼(#560).
PASSTHROUGH_REQUIRED_COLUMNS = ("event_id", "latitude", "longitude", "heading")

# select_best_segment()과 동일한 우선순위: match_score DESC, distance_m/heading_diff_deg/segment_id ASC, 모두 NULLS LAST.
_TIE_BREAK_COLUMNS = ["match_score", "distance_m", "heading_diff_deg", "segment_id"]
_TIE_BREAK_ASCENDING = [False, True, True, True]

# worker당 최근 snapshot 소수만 보관하는 bounded cache 크기.
_MAX_WORKER_CONTEXT_ENTRIES = 2


@dataclass(frozen=True, slots=True)
class MatchingRoadSegment:
    """Map Matching hot path 전용 최소 필드(from_node_id/to_node_id 제외, #479)."""

    segment_id: str
    geometry_wkb: bytes
    traffic_direction: str | None


@dataclass(frozen=True, slots=True)
class RoadSegmentBroadcastPayload:
    """optimized hot path가 broadcast하는 페이로드(cache_key는 driver에서 1회만 계산)."""

    records: tuple[MatchingRoadSegment, ...]
    snapshot_date: date
    cache_key: str


@dataclass(frozen=True, slots=True)
class WorkerMatchingContext:
    """worker-local로 재사용하는 특정 road snapshot의 파생 데이터(Spark 객체는 담지 않음)."""

    snapshot_date: date
    geometries: np.ndarray
    tree: STRtree
    transformer: Transformer
    forward_bearing_deg: np.ndarray
    reverse_bearing_deg: np.ndarray
    traffic_direction: np.ndarray
    segment_id: np.ndarray


def build_broadcast_payload(road_records: list[RoadSegmentCandidate]) -> RoadSegmentBroadcastPayload:
    """hot path에 필요한 최소 필드만 뽑아 payload와 cache key를 만든다(#479)."""
    snapshot_date = road_records[0].snapshot_date
    matching_records = tuple(
        MatchingRoadSegment(
            segment_id=record.segment_id,
            geometry_wkb=record.geometry_wkb,
            traffic_direction=record.traffic_direction,
        )
        for record in road_records
    )
    cache_key = _compute_cache_key(road_records, snapshot_date)
    payload = RoadSegmentBroadcastPayload(
        records=matching_records, snapshot_date=snapshot_date, cache_key=cache_key
    )

    # job당 1회만 남긴다(센서 row 수와 무관).
    logger.info(
        "prepared map matching broadcast payload snapshot_date=%s count=%d cache_key=%s "
        "bytes=%d",
        snapshot_date,
        len(matching_records),
        cache_key,
        len(pickle.dumps(payload)),
    )
    return payload


def _compute_cache_key(road_records: list[RoadSegmentCandidate], snapshot_date: date) -> str:
    # driver에서 1회만 계산: geometry_wkb/traffic_direction도 해시해야 같은 snapshot_date/segment_id인데 내용만 바뀐 데이터를 stale cache로 재사용하지 않는다.
    hasher = hashlib.sha256()
    hasher.update(snapshot_date.isoformat().encode("utf-8"))
    hasher.update(b"|")
    hasher.update(str(len(road_records)).encode("utf-8"))
    for record in road_records:
        hasher.update(b"|")
        hasher.update(record.segment_id.encode("utf-8"))
        hasher.update(b"|")
        hasher.update(record.geometry_wkb)
        hasher.update(b"|")
        hasher.update((record.traffic_direction or "").encode("utf-8"))
    return hasher.hexdigest()


def build_worker_matching_context(payload: RoadSegmentBroadcastPayload) -> WorkerMatchingContext:
    # 결과는 WorkerMatchingContextCache에 cache_key로 저장돼 같은 worker의 다음 task에서 재사용된다(#479).
    geometry_wkb = [record.geometry_wkb for record in payload.records]
    geometries = shapely.from_wkb(geometry_wkb)
    tree = STRtree(geometries)
    transformer = Transformer.from_crs(SOURCE_CRS, TARGET_CRS, always_xy=True)
    forward_bearing_deg, reverse_bearing_deg = compute_forward_reverse_bearing(geometries)
    traffic_direction = np.array(
        [record.traffic_direction for record in payload.records], dtype=object
    )
    segment_id = np.array([record.segment_id for record in payload.records], dtype=object)

    return WorkerMatchingContext(
        snapshot_date=payload.snapshot_date,
        geometries=geometries,
        tree=tree,
        transformer=transformer,
        forward_bearing_deg=forward_bearing_deg,
        reverse_bearing_deg=reverse_bearing_deg,
        traffic_direction=traffic_direction,
        segment_id=segment_id,
    )


class WorkerMatchingContextCache:
    """Python worker process 안에서 STRtree/geometry/bearing을 cache_key 기준 bounded 재사용(#479)."""

    def __init__(self, max_entries: int = _MAX_WORKER_CONTEXT_ENTRIES) -> None:
        if max_entries < 1:
            raise ValueError("max_entries must be at least 1")
        self._max_entries = max_entries
        self._entries: OrderedDict[str, WorkerMatchingContext] = OrderedDict()

    def get_or_build(self, payload: RoadSegmentBroadcastPayload) -> WorkerMatchingContext:
        cached = self._entries.get(payload.cache_key)
        if cached is not None:
            self._entries.move_to_end(payload.cache_key)
            logger.debug("map matching worker context cache hit cache_key=%s", payload.cache_key)
            return cached

        # task당 최대 1회만 남는다(row 수와 무관).
        logger.debug("map matching worker context cache miss cache_key=%s", payload.cache_key)
        context = build_worker_matching_context(payload)
        self._entries[payload.cache_key] = context
        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)
        return context

    def __len__(self) -> int:
        return len(self._entries)


# Python worker process(프로세스별 독립 모듈 전역)에서 여러 task에 걸쳐 재사용되는 캐시.
_WORKER_CONTEXT_CACHE = WorkerMatchingContextCache()


def get_worker_matching_context(payload: RoadSegmentBroadcastPayload) -> WorkerMatchingContext:
    # mapInPandas 클로저는 이 top-level 함수만 참조해야 한다: _WORKER_CONTEXT_CACHE를 직접 캡처하면 cloudpickle이 값으로 스냅샷 떠서 task마다 별개 캐시가 된다.
    return _WORKER_CONTEXT_CACHE.get_or_build(payload)


def build_passthrough_schema(sensor_schema: StructType) -> StructType:
    """센서 컬럼 뒤에 매칭 결과 컬럼을 붙인 mapInPandas 출력 스키마를 만든다(#560)."""
    sensor_names = [field.name for field in sensor_schema.fields]

    missing = [name for name in PASSTHROUGH_REQUIRED_COLUMNS if name not in sensor_names]
    if missing:
        raise ValueError(f"sensor_df is missing required columns for map matching: {missing}")

    # 이름이 겹치면 어느 쪽 값이 남는지 알 수 없어 조용히 틀린 결과가 나온다 — 먼저 실패시킨다.
    conflicting = [field.name for field in MATCH_RESULT_FIELDS if field.name in sensor_names]
    if conflicting:
        raise ValueError(f"sensor_df already has map matching result columns: {conflicting}")

    return StructType([*sensor_schema.fields, *MATCH_RESULT_FIELDS])


def attach_match_results(batch: pd.DataFrame, result: pd.DataFrame) -> pd.DataFrame:
    """원본 센서 배치 옆에 매칭 결과를 붙인다 — event_id로 되붙이는 조인을 없애기 위해서다(#560)."""
    attached = batch.reset_index(drop=True)
    # pandas는 Series 대입을 인덱스로 정렬한다. numpy 배열로 넘겨 위치 기준으로 붙인다 —
    # 인덱스가 어긋나면 행이 뒤섞인 채로 조용히 통과한다.
    for column in result.columns:
        if column != "event_id":
            attached[column] = result[column].to_numpy()
    return attached


def match_segment_candidates(
    sensor_df: DataFrame,
    road_segment_df: DataFrame,
    search_radius_m: float,
    distance_weight: float,
    heading_weight: float,
) -> DataFrame:
    """센서 컬럼을 그대로 통과시키면서 event당 매칭 결과 1행을 덧붙여 반환한다(#560).

    매칭 결과 자체는 find_segment_candidates -> score_segment_candidates ->
    select_best_segment과 동일하다. 원본 센서 컬럼을 함께 돌려주므로 호출자가 결과를
    event_id로 되붙이는 조인(Exchange 2개 + Sort 2개)을 하지 않아도 된다.
    """
    validate_search_radius(search_radius_m)
    validate_score_weights(distance_weight, heading_weight)
    # road_segment를 driver로 모으기 전에 검증한다 — 스키마 문제는 collect 비용을 치르기 전에 드러나야 한다.
    output_schema = build_passthrough_schema(sensor_df.schema)

    road_records = collect_road_segment_candidates(road_segment_df)
    payload = build_broadcast_payload(road_records)

    spark = sensor_df.sparkSession
    payload_broadcast = spark.sparkContext.broadcast(payload)

    def match_events(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        # 반드시 top-level 함수를 통해서만 캐시에 접근한다(get_worker_matching_context() 참고, #479).
        context = get_worker_matching_context(payload_broadcast.value)

        for batch in batches:
            result = match_batch(
                batch, context, search_radius_m, distance_weight, heading_weight
            )
            yield attach_match_results(batch, result)

    return sensor_df.mapInPandas(match_events, schema=output_schema)


def select_best_candidates(candidates_df: pd.DataFrame) -> pd.DataFrame:
    """position(그룹 키)별로 select_best_segment()과 동일한 tie-break로 최선의 1행을 고른다."""
    return (
        candidates_df.sort_values(
            by=_TIE_BREAK_COLUMNS,
            ascending=_TIE_BREAK_ASCENDING,
            na_position="last",
            kind="mergesort",
        )
        .drop_duplicates(subset="position", keep="first")
    )


def match_batch(
    batch: pd.DataFrame,
    context: WorkerMatchingContext,
    search_radius_m: float,
    distance_weight: float,
    heading_weight: float,
) -> pd.DataFrame:
    """배치 전체를 벡터화해 이벤트별 candidate 검색/scoring/선택을 한 번에 수행한다."""
    event_ids = batch["event_id"].to_numpy()
    latitudes = batch["latitude"].to_numpy(dtype="float64", na_value=np.nan)
    longitudes = batch["longitude"].to_numpy(dtype="float64", na_value=np.nan)
    headings = batch["heading"].to_numpy(dtype="float64", na_value=np.nan)
    snapshot_date = context.snapshot_date
    n = len(batch)

    segment_id_out = np.full(n, None, dtype=object)
    distance_out = np.full(n, np.nan, dtype="float64")
    heading_diff_out = np.full(n, np.nan, dtype="float64")
    score_out = np.full(n, np.nan, dtype="float64")
    candidate_count = np.zeros(n, dtype="int64")

    valid = (
        np.isfinite(latitudes)
        & np.isfinite(longitudes)
        & (latitudes >= -90)
        & (latitudes <= 90)
        & (longitudes >= -180)
        & (longitudes <= 180)
    )
    valid_positions = np.flatnonzero(valid)

    if valid_positions.size:
        x, y = context.transformer.transform(longitudes[valid_positions], latitudes[valid_positions])
        points = shapely.points(x, y)

        # dwithin은 GEOS가 정확한 거리로 직접 필터링해 buffer() 후 재계산보다 저렴하다(find_segment_candidates와 동일).
        local_point_index, seg_index = context.tree.query(
            points, predicate="dwithin", distance=search_radius_m
        )

        if local_point_index.size:
            positions = valid_positions[local_point_index]
            distances = shapely.distance(points[local_point_index], context.geometries[seg_index])
            candidate_headings = headings[positions]

            # WKB 재decode 없이 worker context의 forward/reverse bearing/metadata를 segment index로 인덱싱한다.
            candidate_segment_ids = context.segment_id[seg_index]
            candidate_forward_bearing = context.forward_bearing_deg[seg_index]
            candidate_reverse_bearing = context.reverse_bearing_deg[seg_index]
            candidate_traffic_direction = context.traffic_direction[seg_index]

            road_bearing = resolve_prematched_road_bearing(
                candidate_forward_bearing,
                candidate_reverse_bearing,
                candidate_traffic_direction,
                candidate_headings,
            )
            heading_diff = circular_heading_diff(candidate_headings, road_bearing)
            _, _, match_score = compute_match_scores(
                distances, heading_diff, search_radius_m, distance_weight, heading_weight
            )

            candidates_df = pd.DataFrame(
                {
                    "position": positions,
                    "segment_id": candidate_segment_ids,
                    "distance_m": distances,
                    "heading_diff_deg": heading_diff,
                    "match_score": match_score,
                }
            )

            counts = candidates_df.groupby("position").size()
            candidate_count[counts.index.to_numpy()] = counts.to_numpy()

            best = select_best_candidates(candidates_df)

            idx = best["position"].to_numpy()
            segment_id_out[idx] = best["segment_id"].to_numpy()
            distance_out[idx] = best["distance_m"].to_numpy()
            heading_diff_out[idx] = best["heading_diff_deg"].to_numpy()
            score_out[idx] = best["match_score"].to_numpy()

    status = np.where(candidate_count > 0, "matched", "unmatched")

    return pd.DataFrame(
        {
            "event_id": event_ids,
            "segment_id": segment_id_out,
            "road_snapshot_date": snapshot_date,
            "map_match_distance_m": distance_out,
            "map_match_heading_diff_deg": heading_diff_out,
            "map_match_score": score_out,
            "candidate_count": candidate_count,
            "map_match_status": status,
        }
    )
