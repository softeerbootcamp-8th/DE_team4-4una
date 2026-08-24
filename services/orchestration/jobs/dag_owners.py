"""DAG/task 담당자 및 심각도 레지스트리 로더 (#409).

config/dag_owners.yaml을 읽어 dag_id(+ 선택적 task_id/task_group_id) ->
owner/severity를 조회한다. weather_rules.py와 같은 dataclass + load_...
패턴을 따른다.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

VALID_SEVERITIES: tuple[str, ...] = ("critical", "high", "medium", "low")

SEVERITY_LABELS: dict[str, str] = {
    "critical": "🔴 critical",
    "high": "🟠 high",
    "medium": "🟡 medium",
    "low": "⚪ low",
}

# jobs/dag_owners.py -> jobs -> orchestration -> services -> 저장소 루트.
# 컨테이너 안에서는 config/의 마운트 위치가 다르므로(infra/compose/airflow.yaml),
# DAG_OWNERS_CONFIG_PATH 환경변수가 있으면 그쪽을 우선한다.
_DEFAULT_LOCAL_DAG_OWNERS_CONFIG_PATH = (
    Path(__file__).resolve().parents[3] / "config" / "dag_owners.yaml"
)


@dataclass(frozen=True, slots=True)
class OwnerRef:
    name: str
    email: str | None
    slack_id: str | None


@dataclass(frozen=True, slots=True)
class _DagEntry:
    owner: str
    severity: str
    tasks: dict[str, tuple[str, str]]  # task_id/task_group_id -> (owner_name, severity)


@dataclass(frozen=True, slots=True)
class DagOwnersRegistry:
    users: dict[str, OwnerRef]
    dags: dict[str, _DagEntry]

    def resolve_owner(
        self, dag_id: str, task_id: str | None = None, task_group_id: str | None = None
    ) -> OwnerRef:
        return self.users[self._resolve_owner_name(dag_id, task_id, task_group_id)]

    def resolve_severity(
        self, dag_id: str, task_id: str | None = None, task_group_id: str | None = None
    ) -> str:
        dag_entry = self._dag_entry(dag_id)
        for key in (task_id, task_group_id):
            if key is not None and key in dag_entry.tasks:
                return dag_entry.tasks[key][1]
        return dag_entry.severity

    def _resolve_owner_name(
        self, dag_id: str, task_id: str | None, task_group_id: str | None
    ) -> str:
        dag_entry = self._dag_entry(dag_id)
        for key in (task_id, task_group_id):
            if key is not None and key in dag_entry.tasks:
                return dag_entry.tasks[key][0]
        return dag_entry.owner

    def _dag_entry(self, dag_id: str) -> _DagEntry:
        if dag_id not in self.dags:
            raise KeyError(f"dag_owners.yaml에 '{dag_id}' 항목이 없습니다")
        return self.dags[dag_id]


def load_dag_owners_registry(path: Path | None = None) -> DagOwnersRegistry:
    config_path = path if path is not None else _default_dag_owners_config_path()
    document = yaml.safe_load(config_path.read_text())
    if not isinstance(document, dict):
        raise TypeError(f"{config_path}: top-level YAML document must be a mapping")

    users = {
        name: _parse_owner_ref(name, raw, config_path)
        for name, raw in document.get("users", {}).items()
    }
    dags = {
        dag_id: _parse_dag_entry(dag_id, raw, users, config_path)
        for dag_id, raw in document.get("dags", {}).items()
    }
    return DagOwnersRegistry(users=users, dags=dags)


def _default_dag_owners_config_path() -> Path:
    override = os.environ.get("DAG_OWNERS_CONFIG_PATH")
    return Path(override) if override else _DEFAULT_LOCAL_DAG_OWNERS_CONFIG_PATH


def _parse_owner_ref(name: str, raw: dict, config_path: Path) -> OwnerRef:
    if raw is None:
        raw = {}
    email = raw.get("email")
    slack_id = raw.get("slack_id")
    if not email and not slack_id:
        raise ValueError(f"{config_path}: users.{name} must have 'email' or 'slack_id'")
    return OwnerRef(name=name, email=email, slack_id=slack_id)


def _parse_dag_entry(
    dag_id: str, raw: dict, users: dict[str, OwnerRef], config_path: Path
) -> _DagEntry:
    owner = raw.get("owner")
    severity = raw.get("severity")
    _require_known_owner(dag_id, owner, users, config_path)
    _require_valid_severity(dag_id, severity, config_path)

    tasks: dict[str, tuple[str, str]] = {}
    for task_key, task_raw in raw.get("tasks", {}).items():
        if task_raw is None:
            task_raw = {}
        location = f"{dag_id}.tasks.{task_key}"
        task_owner = task_raw.get("owner")
        task_severity = task_raw.get("severity")
        _require_known_owner(location, task_owner, users, config_path)
        _require_valid_severity(location, task_severity, config_path)
        tasks[task_key] = (task_owner, task_severity)

    return _DagEntry(owner=owner, severity=severity, tasks=tasks)


def _require_known_owner(
    location: str, owner: str | None, users: dict[str, OwnerRef], config_path: Path
) -> None:
    if not owner or owner not in users:
        raise ValueError(f"{config_path}: {location}.owner must reference a name in 'users'")


def _require_valid_severity(location: str, severity: str | None, config_path: Path) -> None:
    if severity not in VALID_SEVERITIES:
        raise ValueError(
            f"{config_path}: {location}.severity must be one of {VALID_SEVERITIES}, "
            f"got {severity!r}"
        )
