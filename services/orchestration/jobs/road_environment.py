"""활성 road-environment manifest에서 road_snapshot_date를 읽는다 (#402).

hourly_segment_feature/current_score가 참조하는 road snapshot 날짜를 사람이
Airflow Variable/env var로 수동 갱신하지 않아도, road_environment_uri가 가리키는
active pointer -> manifest 체인(#389)을 그대로 따라가 최신 build의
road_snapshot_date를 읽는다. batch_jobs.comfort_score.universe가 같은 체인을
segment artifact 조회에 쓰지만, 서비스 경계 규칙(AGENTS.md) 때문에 그 코드를
import하지 않고 여기서 다시 구현한다.
"""

from __future__ import annotations

import json
from datetime import date

from de4_core import ObjectStore, join_uri
from de4_core.environment import RoadEnvironmentManifest

ACTIVE_POINTER_KEY = "prepared/simulation_environment/active.json"


def resolve_active_road_snapshot_date(
    road_environment_uri: str, store: ObjectStore | None = None
) -> date:
    active_store = store if store is not None else ObjectStore()
    pointer_uri = join_uri(road_environment_uri, ACTIVE_POINTER_KEY)
    pointer = json.loads(active_store.read_bytes(pointer_uri))

    manifest_uri = pointer.get("manifest_uri")
    if not manifest_uri:
        raise ValueError(f"{pointer_uri}: active pointer has no manifest_uri")

    manifest = RoadEnvironmentManifest.from_json(active_store.read_bytes(manifest_uri))
    return manifest.road_snapshot_date
