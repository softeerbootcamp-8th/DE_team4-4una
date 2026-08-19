"""Resolve the (segment_id, vehicle_profile_id) universe for the standard job (#198).

standard_segment_comfort_score는 관측 여부와 무관하게 매 실행마다 모든 조합에 행을
만든다(context/comfort-score.md, "Handling a vehicle profile that never traversed a
segment"). 그래서 "이번 윈도우에 등장한 조합"이 아니라 도로망과 차량 프로필 마스터에서
universe를 따로 읽어와야 한다.

- segment 목록: 활성 environment pointer(active.json) -> manifest -> enriched_segment_reference
  artifact URI 순으로 따라가 읽는다. 경로를 직접 조립하지 않으므로 build_id가 바뀌어도
  job 설정을 고칠 필요가 없다.
- vehicle profile 목록: PostgreSQL `vehicle_profile`에서 읽는다. FK 대상이 그 테이블이라
  거기 없는 프로필로 행을 만들면 어차피 적재가 실패한다. sensor-producer의 프로필 상수를
  import 하지 않는 이유는 서비스 경계 규칙(AGENTS.md) 때문이다.
"""

from __future__ import annotations

import json
import urllib.parse

from de4_core import ObjectStore, join_uri
from pyspark.sql import DataFrame, SparkSession

from batch_jobs.comfort_score.formula import VEHICLE_AGNOSTIC_VEHICLE_PROFILE_ID

ACTIVE_POINTER_KEY = "prepared/simulation_environment/active.json"
SEGMENT_ARTIFACT_ROLE = "enriched_segment_reference"


def load_universe(
    spark: SparkSession,
    data_lake_uri: str,
    connection,
    store: ObjectStore | None = None,
) -> DataFrame:
    """실제 차량 프로필 x 전체 segment의 cross join을 반환한다.

    sentinel `vehicle_profile_id=0`은 제외한다 — vehicle-agnostic 행은 formula.py가
    segment 목록으로부터 직접 만든다.
    """
    segments = load_segment_ids(spark, data_lake_uri, store)
    profile_ids = load_vehicle_profile_ids(connection)
    profiles = spark.createDataFrame(
        [(profile_id,) for profile_id in profile_ids], "vehicle_profile_id int"
    )
    return segments.crossJoin(profiles)


def load_segment_ids(
    spark: SparkSession, data_lake_uri: str, store: ObjectStore | None = None
) -> DataFrame:
    """활성 environment의 enriched_segment_reference에서 segment_id만 읽는다."""
    artifact_uri = resolve_segment_artifact_uri(data_lake_uri, store)
    return (
        spark.read.parquet(_decode_local_file_uri(artifact_uri))
        .select("segment_id")
        .distinct()
    )


def _decode_local_file_uri(path: str) -> str:
    # de4_core.join_uri()가 로컬 file:// URI에서 '='를 %3D로 인코딩하는데 Spark는
    # 이를 못 읽어서 디코딩한다. Manifest의 artifact URI는 대부분
    # reference_date=.../build_id=... 파티션 경로라 항상 이 인코딩을 거친다.
    if path.startswith("file://"):
        return urllib.parse.unquote(path)
    return path


def resolve_segment_artifact_uri(
    data_lake_uri: str, store: ObjectStore | None = None
) -> str:
    active_store = store if store is not None else ObjectStore()
    pointer_uri = join_uri(data_lake_uri, ACTIVE_POINTER_KEY)
    pointer = json.loads(active_store.read_bytes(pointer_uri))

    manifest_uri = pointer.get("manifest_uri")
    if not manifest_uri:
        raise ValueError(f"{pointer_uri}: active pointer has no manifest_uri")

    manifest = json.loads(active_store.read_bytes(manifest_uri))
    for artifact in manifest.get("artifacts", ()):
        if artifact.get("role") == SEGMENT_ARTIFACT_ROLE:
            uri = artifact.get("uri")
            if not uri:
                raise ValueError(f"{manifest_uri}: {SEGMENT_ARTIFACT_ROLE} artifact has no uri")
            return uri
    raise ValueError(f"{manifest_uri}: no {SEGMENT_ARTIFACT_ROLE} artifact in the manifest")


def load_vehicle_profile_ids(connection) -> tuple[int, ...]:
    """`vehicle_profile`의 실제 프로필 ID를 오름차순으로 반환한다 (sentinel 0 제외)."""
    cursor = connection.cursor()
    try:
        cursor.execute(
            "SELECT vehicle_profile_id FROM vehicle_profile "
            "WHERE vehicle_profile_id <> %s ORDER BY vehicle_profile_id",
            (VEHICLE_AGNOSTIC_VEHICLE_PROFILE_ID,),
        )
        profile_ids = tuple(int(row[0]) for row in cursor.fetchall())
    finally:
        cursor.close()
    if not profile_ids:
        raise RuntimeError(
            "vehicle_profile has no real profiles — run `migrate-database` first"
        )
    return profile_ids
