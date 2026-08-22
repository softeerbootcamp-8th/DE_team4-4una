"""Bronze 계층 zone_weather_snapshot 소파일 정리 (#271, ADR-0009).

sensor-events는 이 job의 범위에서 제외됐다 — Spark Structured Streaming의
FileStreamSink가 쓰는 대상이라 `_spark_metadata/` 커밋 로그가 있고,
`batch_jobs.cleansing.reader.read_bronze_sensor_events`의 `spark.read.parquet()`는
그 로그에 기록된 파일만 읽는다. 원본을 지우면 로그엔 남아 있는데 파일이 없어서
읽기가 깨지고, 새로 쓴 병합 파일은 로그에 커밋된 적이 없어서 아예 안 읽힌다.
제자리 압축이 이 대상엔 근본적으로 안전하지 않다(ADR-0009 대안 참고) —
sensor-events 백로그 정리는 별도 이슈로 남는다.

압축 대상(zone_weather_snapshot)을 상위 "디렉터리"로 그룹핑한다
(`weather_date=D/weather_time=T.parquet` → 날짜 파티션별로 한 그룹). 그룹 안
오브젝트가 모두 안전 경계보다 오래됐을 때만(아직 쓰기가 진행 중일 수 있는 그룹은
건너뜀) 압축하고, 이미 목표 오브젝트 수 이하인 그룹은 스킵한다(멱등성). 병합
결과를 최종 키에 직접 쓰고 다시 읽어 row 수를 검증한 뒤에만 원본을 지운다 —
검증에 실패하면 방금 쓴 결과물을 정리하고 원본을 그대로 둔 채 hard-fail한다.
이미 압축된 결과물(`compacted-` 접두어)은 항상 병합 후보에서 제외한다 —
그래야 중단된 실행이 남긴 결과물이 다음 실행에서 원본과 함께 다시 병합돼
row가 중복되는 사고를 막는다.
"""

from __future__ import annotations

import io
import logging
import os
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
from de4_core import ObjectStore, join_uri
from de4_core.storage import ObjectMetadata

logger = logging.getLogger(__name__)

# 마지막 쓰기 이후 이만큼 지나야 "닫힌" 그룹으로 보고 압축 대상에 포함한다.
DEFAULT_SAFETY_MARGIN = timedelta(hours=1)

# 그룹의 원본 오브젝트 수가 이 값 이하로 이미 압축돼 있으면 스킵한다(멱등성).
DEFAULT_TARGET_OBJECT_COUNT = 1

DEFAULT_ZONE_WEATHER_SNAPSHOT_URI = "data/local-lake/bronze/zone_weather_snapshot"

# 압축 결과물 키 접두어. 이미 이 접두어로 시작하는 오브젝트는 병합 후보에서
# 제외한다(원본이 아니라 이 job이 만든 결과물이므로) — 중단된 실행이 남긴
# 결과물이 다음 실행에서 원본과 함께 다시 병합돼 row가 중복되는 걸 막는다.
FINAL_KEY_PREFIX = "compacted"


class BronzeCompactionRowCountMismatch(RuntimeError):
    """병합 결과의 row 수가 원본 합과 다를 때 — 원본을 보존한 채 hard-fail한다."""


@dataclass(frozen=True, slots=True)
class BronzeCompactionConfig:
    zone_weather_snapshot_uri: str
    safety_margin: timedelta
    target_object_count: int

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> BronzeCompactionConfig:
        source = env if env is not None else os.environ
        margin_minutes = source.get("BRONZE_COMPACTION_SAFETY_MARGIN_MINUTES")
        return cls(
            zone_weather_snapshot_uri=(
                source.get("BRONZE_COMPACTION_ZONE_WEATHER_SNAPSHOT_URI")
                or DEFAULT_ZONE_WEATHER_SNAPSHOT_URI
            ),
            safety_margin=(
                timedelta(minutes=int(margin_minutes))
                if margin_minutes
                else DEFAULT_SAFETY_MARGIN
            ),
            target_object_count=int(
                source.get("BRONZE_COMPACTION_TARGET_OBJECT_COUNT")
                or DEFAULT_TARGET_OBJECT_COUNT
            ),
        )


@dataclass(frozen=True, slots=True)
class CompactionGroupSummary:
    group_key: str
    source_object_count: int
    row_count: int
    final_uri: str


@dataclass(frozen=True, slots=True)
class BronzeCompactionSummary:
    root_uri: str
    compacted_groups: tuple[CompactionGroupSummary, ...]
    skipped_group_count: int


def _basename(uri: str) -> str:
    return uri.rsplit("/", 1)[-1]


def _parent_group_key(uri: str) -> str:
    """오브젝트 URI의 상위 "디렉터리"를 압축 그룹 키로 쓴다.

    zone_weather_snapshot(`weather_date=D/weather_time=T.parquet`)은 날짜
    파티션별로 한 그룹이 된다.
    """
    return uri.rsplit("/", 1)[0]


def compact_bronze_prefix(
    store: ObjectStore,
    root_uri: str,
    *,
    now: datetime,
    safety_margin: timedelta = DEFAULT_SAFETY_MARGIN,
    target_object_count: int = DEFAULT_TARGET_OBJECT_COUNT,
) -> BronzeCompactionSummary:
    cutoff = now - safety_margin
    objects = [
        obj
        for obj in store.list_objects(root_uri)
        if obj.uri.endswith(".parquet") and not _basename(obj.uri).startswith(FINAL_KEY_PREFIX)
    ]

    groups: dict[str, list[ObjectMetadata]] = {}
    for obj in objects:
        groups.setdefault(_parent_group_key(obj.uri), []).append(obj)

    compacted: list[CompactionGroupSummary] = []
    skipped = 0
    for group_key, members in groups.items():
        if len(members) <= target_object_count:
            skipped += 1
            continue
        if max(member.last_modified for member in members) >= cutoff:
            skipped += 1
            continue
        compacted.append(_compact_group(store, group_key, members))

    return BronzeCompactionSummary(
        root_uri=root_uri,
        compacted_groups=tuple(compacted),
        skipped_group_count=skipped,
    )


def _compact_group(
    store: ObjectStore, group_key: str, members: Sequence[ObjectMetadata]
) -> CompactionGroupSummary:
    source_uris = sorted(member.uri for member in members)
    tables = [pq.read_table(io.BytesIO(store.read_bytes(uri))) for uri in source_uris]
    expected_row_count = sum(table.num_rows for table in tables)
    merged = pa.concat_tables(tables)

    buffer = io.BytesIO()
    pq.write_table(merged, buffer)
    merged_bytes = buffer.getvalue()

    final_uri = join_uri(group_key, f"{FINAL_KEY_PREFIX}-{uuid.uuid4().hex}.parquet")
    store.write_bytes(final_uri, merged_bytes)

    written_row_count = pq.read_table(io.BytesIO(store.read_bytes(final_uri))).num_rows
    if written_row_count != expected_row_count:
        try:
            store.delete_objects([final_uri])
        except Exception:
            logger.exception(
                "failed to clean up invalid compaction output %s after a row count "
                "mismatch in group %s",
                final_uri,
                group_key,
            )
        raise BronzeCompactionRowCountMismatch(
            f"{group_key}: expected {expected_row_count} rows, wrote {written_row_count}"
        )

    store.delete_objects(source_uris)

    return CompactionGroupSummary(
        group_key=group_key,
        source_object_count=len(source_uris),
        row_count=written_row_count,
        final_uri=final_uri,
    )


def run_zone_weather_snapshot_compaction(
    config: BronzeCompactionConfig, now: datetime, store: ObjectStore | None = None
) -> BronzeCompactionSummary:
    active_store = store if store is not None else ObjectStore()
    return compact_bronze_prefix(
        active_store,
        config.zone_weather_snapshot_uri,
        now=now,
        safety_margin=config.safety_margin,
        target_object_count=config.target_object_count,
    )
