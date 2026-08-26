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
import re
from datetime import date

from de4_core import ObjectStore, join_uri
from de4_core.environment import RoadEnvironmentManifest

ACTIVE_POINTER_KEY = "prepared/simulation_environment/active.json"
BUILD_PREFIX = "prepared/simulation_environment/"

# build-road-environment가 쓰는 파티션 경로
# (services/batch-jobs/src/batch_jobs/pipeline.py:78-81). local 개발에서는
# ObjectStore.list_objects가 Path.as_uri()로 URI를 만들어 "="가 "%3D"로 퍼센트
# 인코딩돼 오므로(S3는 raw key) 두 형태를 모두 받는다.
_SEP = r"(?:=|%3[Dd])"
_MANIFEST_KEY = re.compile(
    rf"reference_date{_SEP}(?P<reference_date>\d{{4}}-\d{{2}}-\d{{2}})/"
    rf"build_id{_SEP}[^/]+/manifest\.json$"
)


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


def resolve_road_snapshot_date_for_month(
    road_environment_uri: str, target_month: date, store: ObjectStore | None = None
) -> date:
    """`target_month`가 속한 달의 road-environment build에서 road_snapshot_date를 읽는다 (#540).

    active pointer 하나만 보던 방식과 달리 build 파티션을 직접 훑는다 — 백필로 과거
    달을 돌릴 때 그 달에 맞는 도로 정보를 쓰기 위해서다. 같은 달에 build가 여러 개면
    가장 최신 하나, 그 달에 아무것도 없으면 그 달 **이전** 중 가장 최신을 쓴다.
    `target_month`보다 나중 달의 build는 후보에서 제외한다.
    """
    active_store = store if store is not None else ObjectStore()
    month_start = target_month.replace(day=1)
    next_month = _next_month(month_start)

    # 한 build 디렉터리에 manifest.json과 parquet 산출물이 함께 있으므로 manifest만
    # 추린다. reference_date는 경로에서 바로 읽히니 후보를 고르는 동안에는 manifest
    # 본문을 내려받지 않는다 — 이긴 것 하나만 읽는다.
    candidates = []
    for obj in active_store.list_objects(join_uri(road_environment_uri, BUILD_PREFIX)):
        match = _MANIFEST_KEY.search(obj.uri)
        if match is None:
            continue
        reference_date = date.fromisoformat(match.group("reference_date"))
        if reference_date >= next_month:
            continue
        candidates.append((reference_date, obj.last_modified, obj.uri))

    same_month = [item for item in candidates if item[0] >= month_start]
    eligible = same_month or candidates
    if not eligible:
        raise ValueError(
            f"{road_environment_uri}: no road-environment build at or before "
            f"{month_start.isoformat()}"
        )

    # build_id는 임의의 path-safe 문자열이라(pipeline.py:331) 시간순 정렬이 안 된다.
    # 같은 reference_date에 build가 여러 개면 나중에 쓰인 manifest를 최신으로 본다.
    _, _, manifest_uri = max(eligible, key=lambda item: (item[0], item[1]))
    manifest = RoadEnvironmentManifest.from_json(active_store.read_bytes(manifest_uri))
    return manifest.road_snapshot_date


def _next_month(month_start: date) -> date:
    if month_start.month == 12:
        return month_start.replace(year=month_start.year + 1, month=1)
    return month_start.replace(month=month_start.month + 1)
