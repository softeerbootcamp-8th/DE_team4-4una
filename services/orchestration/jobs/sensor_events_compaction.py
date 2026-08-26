"""Bronze sensor-events 시간 파티션의 소파일을 안전하게 병합한다 (#585).

Structured Streaming writer가 남기는 ``_spark_metadata``는 읽거나 지우지 않는다.
이 job은 이미 닫힌 시간 파티션의 원본 Parquet만 새 파일로 바꾼다.
"""

from __future__ import annotations

import logging
import os
import tempfile
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from de4_core import ObjectStore, join_uri
from de4_core.storage import ObjectMetadata

logger = logging.getLogger(__name__)

DEFAULT_SENSOR_EVENTS_ROOT_URI = "data/local-lake/bronze/sensor-events"
DEFAULT_SAFETY_MARGIN = timedelta(minutes=120)
DEFAULT_TARGET_FILE_MB = 128
DEFAULT_MAX_GROUPS_PER_RUN = 0
FINAL_KEY_PREFIX = "compacted-"


class BronzeCompactionRowCountMismatch(RuntimeError):
    """출력 footer의 row 수가 원본 합과 다를 때 원본을 보존하고 실패한다."""


@dataclass(frozen=True, slots=True)
class SensorEventsCompactionConfig:
    root_uri: str
    safety_margin: timedelta
    target_file_mb: int
    max_groups_per_run: int

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> SensorEventsCompactionConfig:
        source = env if env is not None else os.environ
        return cls(
            root_uri=(
                source.get("SENSOR_EVENTS_COMPACTION_ROOT_URI")
                or DEFAULT_SENSOR_EVENTS_ROOT_URI
            ),
            safety_margin=timedelta(
                minutes=int(
                    source.get("SENSOR_EVENTS_COMPACTION_SAFETY_MARGIN_MINUTES")
                    or DEFAULT_SAFETY_MARGIN.total_seconds() // 60
                )
            ),
            target_file_mb=int(
                source.get("SENSOR_EVENTS_COMPACTION_TARGET_FILE_MB")
                or DEFAULT_TARGET_FILE_MB
            ),
            max_groups_per_run=int(
                source.get("SENSOR_EVENTS_COMPACTION_MAX_GROUPS_PER_RUN")
                or DEFAULT_MAX_GROUPS_PER_RUN
            ),
        )


@dataclass(frozen=True, slots=True)
class SensorEventsCompactionGroupSummary:
    group_key: str
    source_object_count: int
    output_object_count: int
    row_count: int
    output_uris: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SensorEventsCompactionSummary:
    root_uri: str
    compacted_groups: tuple[SensorEventsCompactionGroupSummary, ...]
    skipped_group_count: int


def compact_sensor_events(
    store: ObjectStore,
    config: SensorEventsCompactionConfig,
    *,
    now: datetime,
) -> SensorEventsCompactionSummary:
    """닫힌 시간 파티션을 병합하고 검증이 끝난 원본만 삭제한다."""
    if config.target_file_mb <= 0:
        raise ValueError("target_file_mb must be positive")
    if config.max_groups_per_run < 0:
        raise ValueError("max_groups_per_run must not be negative")

    cutoff = now - config.safety_margin
    groups: dict[str, list[ObjectMetadata]] = {}
    for obj in store.list_objects(config.root_uri):
        if not _is_source_candidate(obj.uri):
            continue
        groups.setdefault(_parent_group_key(obj.uri), []).append(obj)

    compacted: list[SensorEventsCompactionGroupSummary] = []
    skipped = 0
    for group_key in sorted(groups):
        members = groups[group_key]
        if len(members) < 2 or max(member.last_modified for member in members) >= cutoff:
            skipped += 1
            continue
        if config.max_groups_per_run and len(compacted) >= config.max_groups_per_run:
            skipped += 1
            continue
        compacted.append(
            _compact_group(
                store,
                group_key,
                members,
                target_bytes=config.target_file_mb * 1024 * 1024,
            )
        )

    return SensorEventsCompactionSummary(
        root_uri=config.root_uri,
        compacted_groups=tuple(compacted),
        skipped_group_count=skipped,
    )


def run_sensor_events_compaction(
    config: SensorEventsCompactionConfig,
    now: datetime,
    store: ObjectStore | None = None,
) -> SensorEventsCompactionSummary:
    return compact_sensor_events(store or ObjectStore(), config, now=now)


def _compact_group(
    store: ObjectStore,
    group_key: str,
    members: Sequence[ObjectMetadata],
    *,
    target_bytes: int,
) -> SensorEventsCompactionGroupSummary:
    source_uris = tuple(sorted(member.uri for member in members))
    expected_rows = 0
    expected_schema: pa.Schema | None = None
    output_paths: list[Path] = []
    output_uris: tuple[str, ...] = ()
    writer: pq.ParquetWriter | None = None
    current_path: Path | None = None
    current_has_rows = False

    try:
        for source_uri in source_uris:
            with store.open_reader(source_uri) as source:
                parquet_file = pq.ParquetFile(source)
                schema = parquet_file.schema_arrow
                if expected_schema is None:
                    expected_schema = schema
                elif schema != expected_schema:
                    raise ValueError(f"{group_key}: source Parquet schemas do not match")
                expected_rows += parquet_file.metadata.num_rows

                for row_group_index in range(parquet_file.num_row_groups):
                    if writer is None or (
                        current_has_rows
                        and current_path is not None
                        and current_path.stat().st_size >= target_bytes
                    ):
                        if writer is not None:
                            writer.close()
                            writer = None
                        current_path = _new_temp_path()
                        output_paths.append(current_path)
                        writer = pq.ParquetWriter(current_path, expected_schema, compression="snappy")
                        current_has_rows = False
                    writer.write_table(parquet_file.read_row_group(row_group_index))
                    current_has_rows = True
        if writer is not None:
            writer.close()
            writer = None

        output_uris = tuple(
            join_uri(group_key, f"{FINAL_KEY_PREFIX}{uuid.uuid4().hex}.parquet")
            for _ in output_paths
        )
        for path, output_uri in zip(output_paths, output_uris, strict=True):
            store.upload_file(path, output_uri)

        written_rows = sum(_footer_row_count(store, output_uri) for output_uri in output_uris)
        if written_rows != expected_rows:
            _cleanup_outputs(store, output_uris, group_key)
            raise BronzeCompactionRowCountMismatch(
                f"{group_key}: expected {expected_rows} rows, wrote {written_rows}"
            )
        store.delete_objects(source_uris)
    except Exception:
        if writer is not None:
            writer.close()
        if output_uris:
            _cleanup_outputs(store, output_uris, group_key)
        raise
    finally:
        for path in output_paths:
            path.unlink(missing_ok=True)

    return SensorEventsCompactionGroupSummary(
        group_key=group_key,
        source_object_count=len(source_uris),
        output_object_count=len(output_uris),
        row_count=written_rows,
        output_uris=output_uris,
    )


def _new_temp_path() -> Path:
    descriptor, path = tempfile.mkstemp(prefix="de4-sensor-events-", suffix=".parquet")
    os.close(descriptor)
    return Path(path)


def _footer_row_count(store: ObjectStore, uri: str) -> int:
    with store.open_reader(uri) as source:
        return pq.ParquetFile(source).metadata.num_rows


def _cleanup_outputs(store: ObjectStore, output_uris: Sequence[str], group_key: str) -> None:
    try:
        store.delete_objects(output_uris)
    except Exception:
        logger.exception("failed to clean invalid outputs for %s", group_key)


def _basename(uri: str) -> str:
    return uri.rsplit("/", 1)[-1]


def _is_source_candidate(uri: str) -> bool:
    return (
        uri.endswith(".parquet")
        and "/_spark_metadata/" not in uri
        and not _basename(uri).startswith(FINAL_KEY_PREFIX)
    )


def _parent_group_key(uri: str) -> str:
    return uri.rsplit("/", 1)[0]
