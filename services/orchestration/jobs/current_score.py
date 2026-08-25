# 최신 standard 점수에 zone별 최신 날씨를 반영해 current_segment_comfort_score를 만든다 (#216).
# context/comfort-score.md "Weather-adjusted current score" Step A~D.

from __future__ import annotations

import io
import logging
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
from de4_core import ObjectStore, join_uri, perf_phase
from psycopg2.extras import execute_values

from . import current_score_quarantine
from .road_environment import resolve_active_road_snapshot_date
from .weather_rules import (
    WEATHER_RULE_VERSION,
    WeatherRuleConfig,
    adjust_comfort_scores,
    load_weather_rule_config,
    parse_impact_signature,
)

logger = logging.getLogger(__name__)

TABLE = "current_segment_comfort_score"

DEFAULT_ROAD_SEGMENT_URI = "data/processed/road_segment"

# batch_jobs.db_lock_keys.CURRENT_SCORE_JOB_LOCK_KEY와 같은 값이어야 한다 —
# 서비스 경계 때문에 import하지 못해 그쪽 레지스트리에 예약만 해 두고 여기서 복사한다.
LOCK_KEY = 1004

# 전량 갱신은 도로망 전체 x 차량 프로필이라 한 번에 다 메모리에 올리지 않는다.
FETCH_BATCH_SIZE = 5000

_ROW_COLUMNS = (
    "segment_id",
    "vehicle_profile_id",
    "location_id",
    "standard_score_as_of",
    "weather_time",
    "data_period_start",
    "vertical_score",
    "longitudinal_score",
    "lateral_score",
    "comfort_score",
    "sample_count",
    "confidence_score",
    "standard_score_version",
    "weather_rule_version",
    "weather_impact_signature",
    "calculated_at",
)

_UPDATE_ASSIGNMENTS = ",\n    ".join(
    f"{column} = EXCLUDED.{column}"
    for column in _ROW_COLUMNS
    if column not in {"segment_id", "vehicle_profile_id"}
)

_UPSERT_SQL = f"""
INSERT INTO {TABLE} ({", ".join(_ROW_COLUMNS)})
VALUES %s
ON CONFLICT (segment_id, vehicle_profile_id) DO UPDATE SET
    {_UPDATE_ASSIGNMENTS}
"""

# standard는 (segment, 차량 프로필)당 최신 세대 1행만 담으므로(#503, migration 0012)
# 그대로 읽으면 된다. WHERE의 segment_id는 PK 선행 컬럼이라 인덱스를 탄다.
_LATEST_STANDARD_SQL = """
SELECT segment_id, vehicle_profile_id, score_as_of, data_period_start,
       vertical_score, longitudinal_score, lateral_score,
       sample_count, confidence_score, score_version
FROM standard_segment_comfort_score
{where}
"""

# 한 행이라도 다르면 그 zone은 재계산 대상이다. 부분 실패로 zone 안에 서명이 섞여도
# 자동으로 다시 걸린다. 반대로 current 행이 아직 없는 zone은 여기 걸리지 않고,
# 시간별 전량 갱신에서 처음 만들어진다.
_CHANGED_ZONES_SQL = f"""
SELECT w.location_id
FROM latest_zone_weather w
JOIN {TABLE} c ON c.location_id = w.location_id
WHERE c.weather_impact_signature IS DISTINCT FROM w.impact_signature
GROUP BY w.location_id
"""


@dataclass(frozen=True, slots=True)
class CurrentScoreJobConfig:
    road_segment_uri: str
    road_snapshot_date: date
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> CurrentScoreJobConfig:
        source = env if env is not None else os.environ
        return cls(
            road_segment_uri=source.get("CURRENT_SCORE_ROAD_SEGMENT_URI") or DEFAULT_ROAD_SEGMENT_URI,
            road_snapshot_date=_resolve_road_snapshot_date(source),
            postgres_host=_require(source, "POSTGRES_HOST"),
            postgres_port=int(_require(source, "POSTGRES_PORT")),
            postgres_db=_require(source, "POSTGRES_DB"),
            postgres_user=_require(source, "POSTGRES_USER"),
            postgres_password=_require(source, "POSTGRES_PASSWORD"),
        )


def _require(source: Mapping[str, str], key: str) -> str:
    value = source.get(key)
    if not value:
        raise ValueError(f"{key} must be set")
    return value


# road_environment_uri(#389)의 active pointer/manifest에서 최신 build의
# road_snapshot_date를 직접 읽는다(#402) — 새 road snapshot이 발행되면 사람이
# CURRENT_SCORE_ROAD_SNAPSHOT_DATE를 수동으로 갱신하지 않아도 다음 실행부터
# 자동으로 반영된다. URI가 없으면(로컬 개발 등) 기존 하드코딩 값으로 폴백한다.
def _resolve_road_snapshot_date(source: Mapping[str, str]) -> date:
    road_environment_uri = source.get("CURRENT_SCORE_ROAD_ENVIRONMENT_URI")
    if road_environment_uri:
        return resolve_active_road_snapshot_date(road_environment_uri)

    fallback = source.get("CURRENT_SCORE_ROAD_SNAPSHOT_DATE")
    if not fallback:
        raise ValueError(
            "CURRENT_SCORE_ROAD_ENVIRONMENT_URI or CURRENT_SCORE_ROAD_SNAPSHOT_DATE must be set"
        )
    logger.warning(
        "CURRENT_SCORE_ROAD_ENVIRONMENT_URI not set — falling back to the hardcoded "
        "CURRENT_SCORE_ROAD_SNAPSHOT_DATE=%s (#402)",
        fallback,
    )
    return date.fromisoformat(fallback)


@dataclass(frozen=True, slots=True)
class CurrentScoreJobSummary:
    zone_count: int
    upserted_count: int
    # zone이 배정되지 않은 segment. location_id가 NOT NULL이라 행을 만들 수 없다.
    skipped_unzoned_count: int
    quarantined_count: int


# road_segment(Parquet)에서 segment -> zone 매핑을 읽는다. 이 매핑은 Postgres에 없다.
# road_segment_uri는 항상 root(예: normalized/road_segment)를 가리키고, 실제로는
# 그 아래 {road_segment_uri}/snapshot_date=<date>/ partition만 조회한다 — 이 root
# 아래에는 여러 snapshot_date가 쌓여 있을 수 있어(#400) 매번 전체를 스캔하면
# S3에서는 비용도 크고 실수로 다른 날짜 데이터까지 섞일 위험도 있다. local
# path/file:// URI/s3:// URI 모두 de4_core.ObjectStore가 처리한다.
def load_segment_zones(
    road_segment_uri: str | Path, road_snapshot_date: date, *, store: ObjectStore | None = None
) -> dict[str, int]:
    active_store = store if store is not None else ObjectStore()
    partition_uri = join_uri(str(road_segment_uri), f"snapshot_date={road_snapshot_date.isoformat()}")
    objects = [
        obj for obj in active_store.list_objects(partition_uri) if obj.uri.endswith(".parquet")
    ]
    if not objects:
        raise ValueError(f"{partition_uri}: no parquet files found")

    tables = [
        pq.read_table(
            io.BytesIO(active_store.read_bytes(obj.uri)),
            columns=["segment_id", "snapshot_date", "location_id"],
        )
        for obj in objects
    ]
    table = pa.concat_tables(tables)
    snapshot_dates = set(table.column("snapshot_date").to_pylist())
    # 파티션 경로 이름과 파일 내부 값이 어긋나 있으면(잘못 배치된 데이터) 점수를
    # 만들기 전에 실패시킨다(batch_jobs.hourly_segment_feature_job과 같은 방침).
    if snapshot_dates != {road_snapshot_date}:
        raise ValueError(
            f"{partition_uri}: expected snapshot_date {road_snapshot_date}, "
            f"got {sorted(snapshot_dates)}"
        )
    return {
        segment_id: int(location_id)
        for segment_id, location_id in zip(
            table.column("segment_id").to_pylist(),
            table.column("location_id").to_pylist(),
            strict=True,
        )
        if location_id is not None
    }


def load_latest_zone_weather(connection) -> dict[int, tuple[datetime, str]]:
    with connection.cursor() as cursor:
        cursor.execute("SELECT location_id, weather_time, impact_signature FROM latest_zone_weather")
        return {
            int(location_id): (weather_time, signature)
            for location_id, weather_time, signature in cursor.fetchall()
        }


def find_changed_zones(connection) -> tuple[int, ...]:
    with connection.cursor() as cursor:
        cursor.execute(_CHANGED_ZONES_SQL)
        return tuple(sorted(int(row[0]) for row in cursor.fetchall()))


def run_current_score_job(
    config: CurrentScoreJobConfig,
    connection,
    *,
    changed_zones_only: bool,
    rule_config: WeatherRuleConfig | None = None,
) -> CurrentScoreJobSummary:
    """standard 점수를 날씨로 보정해 UPSERT한다.

    `changed_zones_only=True`면 15분 실행용으로 impact_signature가 달라진 zone만 다시
    만든다. False면 시간별 standard 적재 뒤 전량을 다시 만든다 — 날씨가 그대로여도
    standard 스냅샷이 새로 생겼으므로 갱신 대상이다.
    """
    rule_config = rule_config if rule_config is not None else load_weather_rule_config()
    # 17만 건 road_segment를 S3에서 읽는 구간과 Postgres UPSERT 구간을 갈라 놓는다(#461).
    # 이 job은 Spark를 안 쓰므로(ADR-0007) event log가 없고, 이 로그가 유일한 근거다.
    with perf_phase(logger, "current_score.load_segment_zones") as fields:
        segment_zones = load_segment_zones(
            config.road_segment_uri, config.road_snapshot_date
        )
        fields["rows"] = len(segment_zones)
    weather_by_zone = load_latest_zone_weather(connection)

    if changed_zones_only:
        zones = find_changed_zones(connection)
        if not zones:
            logger.info("no zone changed its weather — nothing to recompute")
            return CurrentScoreJobSummary(0, 0, 0, 0)
        target_zones: set[int] | None = set(zones)
    else:
        zones = tuple(sorted(set(segment_zones.values())))
        target_zones = None

    calculated_at = datetime.now(UTC)
    upserted = 0
    skipped = 0
    quarantined = 0
    suite = current_score_quarantine.load_expectation_suite()

    with (
        perf_phase(logger, "current_score.upsert_loop") as loop_fields,
        connection.cursor() as write_cursor,
    ):
        # 두 트리거(15분/시간별)가 겹쳐도 같은 행을 서로 덮어쓰지 않게 직렬화한다.
        write_cursor.execute("SELECT pg_advisory_lock(%s)", (LOCK_KEY,))
        try:
            for batch in _read_standard_rows(connection, segment_zones, target_zones):
                rows = []
                for record in batch:
                    location_id = segment_zones.get(record[0])
                    if location_id is None:
                        skipped += 1
                        continue
                    rows.append(
                        _build_row(
                            record, location_id, weather_by_zone.get(location_id),
                            rule_config, calculated_at,
                        )
                    )
                if rows:
                    split = current_score_quarantine.split_batch(rows, rule_config, suite)
                    if split.normal_rows:
                        execute_values(
                            write_cursor,
                            _UPSERT_SQL,
                            [
                                tuple(row[column] for column in _ROW_COLUMNS)
                                for row in split.normal_rows
                            ],
                        )
                    current_score_quarantine.insert_quarantined_rows(
                        write_cursor, split.quarantined_records
                    )
                    upserted += len(split.normal_rows)
                    quarantined += len(split.quarantined_records)
            current_score_quarantine.check_circuit_breaker(
                upserted_count=upserted, quarantined_count=quarantined
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            loop_fields["rows"] = upserted
            write_cursor.execute("SELECT pg_advisory_unlock(%s)", (LOCK_KEY,))

    logger.info(
        "current comfort score job finished zones=%d upserted=%d skipped_unzoned=%d quarantined=%d",
        len(zones), upserted, skipped, quarantined,
    )
    return CurrentScoreJobSummary(
        zone_count=len(zones),
        upserted_count=upserted,
        skipped_unzoned_count=skipped,
        quarantined_count=quarantined,
    )


def _read_standard_rows(connection, segment_zones, target_zones):
    """standard 최신 행을 batch 단위로 흘려보낸다.

    전량 갱신은 도로망 전체 x 차량 프로필이라 fetchall()로 한 번에 받지 않는다.
    """
    if target_zones is None:
        where = ""
        parameters: tuple = ()
    else:
        segment_ids = [
            segment_id
            for segment_id, location_id in segment_zones.items()
            if location_id in target_zones
        ]
        if not segment_ids:
            return
        where = "WHERE segment_id = ANY(%s::text[])"
        parameters = (segment_ids,)

    with connection.cursor() as cursor:
        cursor.execute(_LATEST_STANDARD_SQL.format(where=where), parameters)
        while batch := cursor.fetchmany(FETCH_BATCH_SIZE):
            yield batch


def _build_row(
    record: Sequence,
    location_id: int,
    weather: tuple[datetime, str] | None,
    rule_config: WeatherRuleConfig,
    calculated_at: datetime,
) -> dict:
    (
        segment_id,
        vehicle_profile_id,
        score_as_of,
        data_period_start,
        vertical_score,
        longitudinal_score,
        lateral_score,
        sample_count,
        confidence_score,
        score_version,
    ) = record

    # 아직 날씨를 못 받은 zone은 보정 없이 standard 값을 그대로 쓴다. 0006/0009의 CHECK가
    # weather_time/rule_version/impact_signature를 한 묶음으로 NULL이길 요구한다.
    if weather is None:
        conditions = frozenset()
        weather_time = None
        weather_rule_version = None
        impact_signature = None
    else:
        weather_time, impact_signature = weather
        conditions = parse_impact_signature(impact_signature)
        weather_rule_version = WEATHER_RULE_VERSION

    adjusted = adjust_comfort_scores(
        float(vertical_score), float(longitudinal_score), float(lateral_score),
        conditions, rule_config,
    )
    return {
        "segment_id": segment_id,
        "vehicle_profile_id": int(vehicle_profile_id),
        "location_id": location_id,
        "standard_score_as_of": score_as_of,
        "weather_time": weather_time,
        "data_period_start": data_period_start,
        "vertical_score": adjusted.vertical_score,
        "longitudinal_score": adjusted.longitudinal_score,
        "lateral_score": adjusted.lateral_score,
        "comfort_score": adjusted.comfort_score,
        "sample_count": int(sample_count),
        "confidence_score": float(confidence_score),
        "standard_score_version": score_version,
        "weather_rule_version": weather_rule_version,
        "weather_impact_signature": impact_signature,
        "calculated_at": calculated_at,
    }


# Airflow task가 그대로 부를 수 있는 진입점. 두 DAG가 커넥션 개폐를 각자 베끼지 않도록
# 여기서 한 번만 처리한다.
def run_from_env(*, changed_zones_only: bool) -> CurrentScoreJobSummary:
    config = CurrentScoreJobConfig.from_env()
    connection = psycopg2.connect(
        host=config.postgres_host,
        port=config.postgres_port,
        dbname=config.postgres_db,
        user=config.postgres_user,
        password=config.postgres_password,
    )
    try:
        return run_current_score_job(
            config, connection, changed_zones_only=changed_zones_only
        )
    finally:
        connection.close()
