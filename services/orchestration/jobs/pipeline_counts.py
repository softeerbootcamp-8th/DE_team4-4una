"""standard_score_pipeline/data_quality_audit 성공 알림에 넣을 처리 건수 조회 (#409).

EMR Serverless로 제출되는 task는 원격 Spark job이라 Airflow XCom으로 건수를
돌려줄 방법이 없다. 이 모듈은 각 stage가 방금 쓴 output을 orchestration
프로세스에서 직접 다시 읽어(S3/로컬 Parquet) 또는 조회해(Postgres) 건수만 센다
— services/batch-jobs는 건드리지 않는다.
"""

from __future__ import annotations

import io
import os
from dataclasses import dataclass
from datetime import datetime

import pyarrow.parquet as pq
from de4_core import ObjectStore, join_uri


@dataclass(frozen=True, slots=True)
class PostgresConfig:
    host: str
    port: str
    dbname: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> PostgresConfig:
        return cls(
            host=os.environ["POSTGRES_HOST"],
            port=os.environ["POSTGRES_PORT"],
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        )

    def as_connect_kwargs(self) -> dict[str, str]:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
        }


@dataclass(frozen=True, slots=True)
class StandardScorePipelineCounts:
    quarantine_count: int
    feature_count: int
    hourly_comfort_score_count: int
    standard_segment_comfort_score_count: int


def count_standard_score_pipeline_outputs(
    *,
    target_hour: datetime,
    as_of: datetime,
    quarantine_output_path: str,
    feature_output_path: str,
    hourly_comfort_output_path: str,
    connection,
    store: ObjectStore | None = None,
) -> StandardScorePipelineCounts:
    active_store = store if store is not None else ObjectStore()

    # batch-jobs가 실제로 쓰는 파티션 경로 규칙(#409 조사 기록 참고) — cleansing/
    # hourly_storage.py의 quarantine_hour_path(), hourly_segment_feature_storage.py의
    # hour_output_path()와 반드시 같은 형식이어야 한다.
    quarantine_partition = join_uri(
        quarantine_output_path,
        f"target_date={target_hour.date().isoformat()}",
        f"target_hour={target_hour.hour:02d}",
    )
    feature_partition = join_uri(
        feature_output_path,
        f"data_period_date={target_hour.date().isoformat()}",
        f"hour={target_hour.hour:02d}",
    )

    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM standard_segment_comfort_score WHERE score_as_of = %s",
            (as_of,),
        )
        standard_segment_comfort_score_count = cursor.fetchone()[0]

    return StandardScorePipelineCounts(
        quarantine_count=_count_parquet_rows(active_store, quarantine_partition),
        feature_count=_count_parquet_rows(active_store, feature_partition),
        hourly_comfort_score_count=_count_parquet_rows(active_store, hourly_comfort_output_path),
        standard_segment_comfort_score_count=standard_segment_comfort_score_count,
    )


def count_audit_gold_tables(*, connection) -> dict[str, int]:
    counts: dict[str, int] = {}
    with connection.cursor() as cursor:
        # 테이블명은 아래 고정된 리터럴 2개뿐이라 f-string으로 넣어도 인젝션 위험이 없다.
        for table in ("standard_segment_comfort_score", "current_segment_comfort_score"):
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = cursor.fetchone()[0]
    return counts


def _count_parquet_rows(store: ObjectStore, uri: str) -> int:
    objects = [obj for obj in store.list_objects(uri) if obj.uri.endswith(".parquet")]
    if not objects:
        return 0
    return sum(pq.read_table(io.BytesIO(store.read_bytes(obj.uri))).num_rows for obj in objects)
