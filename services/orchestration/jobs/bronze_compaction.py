"""Bronze 계층(sensor-events, zone_weather_snapshot) 소파일 정리 (#271, ADR-0009).

두 대상 모두 오브젝트를 상위 "디렉터리"로 그룹핑하는 규칙 하나로 처리한다 —
`sensor-events`(파티션 없는 flat 출력)는 전체가 한 그룹, `zone_weather_snapshot`
(`weather_date=D/weather_time=T.parquet`)은 날짜 파티션별로 한 그룹이 된다. 그룹 안
오브젝트가 모두 안전 경계보다 오래됐을 때만(아직 쓰기가 진행 중일 수 있는 그룹은
건너뜀) 압축하고, 이미 목표 오브젝트 수 이하인 그룹은 스킵한다(멱등성). 병합은
임시 키에 쓰고 다시 읽어 row 수를 검증한 뒤에만 원본을 지운다 — 불일치하면 원본을
그대로 두고 hard-fail한다.
"""

from __future__ import annotations

import io
import logging
import os
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
from de4_core import ObjectStore, join_uri
from de4_core.storage import ObjectMetadata

logger = logging.getLogger(__name__)

# 마지막 쓰기 이후 이만큼 지나야 "닫힌" 그룹으로 보고 압축 대상에 포함한다.
DEFAULT_SAFETY_MARGIN = timedelta(hours=1)

# 그룹의 오브젝트 수가 이 값 이하로 이미 압축돼 있으면 스킵한다(멱등성).
DEFAULT_TARGET_OBJECT_COUNT = 1

DEFAULT_SENSOR_EVENTS_URI = "data/local-lake/bronze/sensor-events"
DEFAULT_ZONE_WEATHER_SNAPSHOT_URI = "data/local-lake/bronze/zone_weather_snapshot"

# 압축 중 임시로 쓰는 키를 원본과 구분하기 위한 접두어 — row count 검증에 쓰는
# _CorruptingObjectStore류 테스트 더블도 이 문자열로 임시 키를 식별한다.
TEMP_KEY_PREFIX = "_bronze_compaction_tmp"
FINAL_KEY_PREFIX = "compacted"


class BronzeCompactionRowCountMismatch(RuntimeError):
    """병합 결과의 row 수가 원본 합과 다를 때 — 원본을 보존한 채 hard-fail한다."""


@dataclass(frozen=True, slots=True)
class BronzeCompactionConfig:
    sensor_events_uri: str
    zone_weather_snapshot_uri: str
    safety_margin: timedelta
    target_object_count: int

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> BronzeCompactionConfig:
        source = env if env is not None else os.environ
        margin_minutes = source.get("BRONZE_COMPACTION_SAFETY_MARGIN_MINUTES")
        return cls(
            sensor_events_uri=(
                source.get("BRONZE_COMPACTION_SENSOR_EVENTS_URI") or DEFAULT_SENSOR_EVENTS_URI
            ),
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


def _parent_group_key(uri: str) -> str:
    """오브젝트 URI의 상위 "디렉터리"를 압축 그룹 키로 쓴다.

    sensor-events(파티션 없는 flat 출력)는 전체가 한 그룹, zone_weather_snapshot
    (`weather_date=D/weather_time=T.parquet`)은 날짜 파티션별로 한 그룹이 된다 — 두
    Bronze 대상 모두 소스별 특수 처리 없이 이 하나의 규칙으로 그룹핑된다.
    """
    return uri.rsplit("/", 1)[0]


def compact_bronze_prefix(
    store: ObjectStore,
    root_uri: str,
    *,
    now: datetime,
    safety_margin: timedelta = DEFAULT_SAFETY_MARGIN,
    target_object_count: int = DEFAULT_TARGET_OBJECT_COUNT,
    group_key_fn: Callable[[str], str] = _parent_group_key,
) -> BronzeCompactionSummary:
    cutoff = now - safety_margin
    objects = [obj for obj in store.list_objects(root_uri) if obj.uri.endswith(".parquet")]

    groups: dict[str, list[ObjectMetadata]] = {}
    for obj in objects:
        groups.setdefault(group_key_fn(obj.uri), []).append(obj)

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

    temp_uri = join_uri(group_key, f"{TEMP_KEY_PREFIX}-{uuid.uuid4().hex}.parquet")
    store.write_bytes(temp_uri, merged_bytes)

    written_row_count = pq.read_table(io.BytesIO(store.read_bytes(temp_uri))).num_rows
    if written_row_count != expected_row_count:
        raise BronzeCompactionRowCountMismatch(
            f"{group_key}: expected {expected_row_count} rows, wrote {written_row_count}"
        )

    store.delete_objects(source_uris)

    final_uri = join_uri(group_key, f"{FINAL_KEY_PREFIX}-{uuid.uuid4().hex}.parquet")
    store.write_bytes(final_uri, merged_bytes)
    store.delete_objects([temp_uri])

    return CompactionGroupSummary(
        group_key=group_key,
        source_object_count=len(source_uris),
        row_count=written_row_count,
        final_uri=final_uri,
    )


def run_sensor_events_compaction(
    config: BronzeCompactionConfig, now: datetime, store: ObjectStore | None = None
) -> BronzeCompactionSummary:
    active_store = store if store is not None else ObjectStore()
    return compact_bronze_prefix(
        active_store,
        config.sensor_events_uri,
        now=now,
        safety_margin=config.safety_margin,
        target_object_count=config.target_object_count,
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
