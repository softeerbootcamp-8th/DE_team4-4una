# config/dag_owners.yaml의 users: 레지스트리를 재사용하되, dag_owners.py(Airflow 전용)는 import하지 않고 같은 YAML을 독립적으로 다시 읽는다(#447).

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True, slots=True)
class ServiceOwner:
    name: str
    email: str | None
    slack_id: str | None
    severity: str


@dataclass(frozen=True, slots=True)
class ServiceOwnersRegistry:
    services: dict[str, ServiceOwner]

    def resolve(self, service: str) -> ServiceOwner | None:
        return self.services.get(service)


def load_service_owners_registry(path: str | Path) -> ServiceOwnersRegistry:
    config_path = Path(path)
    document = yaml.safe_load(config_path.read_text())
    if not isinstance(document, dict):
        raise TypeError(f"{config_path}: top-level YAML document must be a mapping")

    users: dict[str, dict] = document.get("users") or {}
    services_raw: dict[str, dict] = document.get("services") or {}

    services: dict[str, ServiceOwner] = {}
    for service_name, raw in services_raw.items():
        raw = raw or {}
        owner_name = raw.get("owner")
        if not owner_name or owner_name not in users:
            raise ValueError(
                f"{config_path}: services.{service_name}.owner must reference a name in 'users'"
            )
        owner_raw = users[owner_name] or {}
        services[service_name] = ServiceOwner(
            name=owner_name,
            email=owner_raw.get("email"),
            slack_id=owner_raw.get("slack_id"),
            severity=raw.get("severity") or "medium",
        )
    return ServiceOwnersRegistry(services=services)
