from __future__ import annotations

import pytest
from ops_agent.owners import load_service_owners_registry

VALID_YAML = """
users:
  alice:
    email: alice@example.com
  bob:
    slack_id: U0456GHIJKL

services:
  stream-processor:
    owner: bob
    severity: high
  serving-api:
    owner: alice
    severity: critical
"""


class TestLoadServiceOwnersRegistry:
    def test_resolves_a_registered_service_using_the_shared_users_registry(self, tmp_path):
        path = tmp_path / "dag_owners.yaml"
        path.write_text(VALID_YAML)

        registry = load_service_owners_registry(path)
        owner = registry.resolve("stream-processor")

        assert owner is not None
        assert owner.name == "bob"
        assert owner.slack_id == "U0456GHIJKL"
        assert owner.severity == "high"

    def test_an_unregistered_service_resolves_to_none(self, tmp_path):
        path = tmp_path / "dag_owners.yaml"
        path.write_text(VALID_YAML)

        registry = load_service_owners_registry(path)

        assert registry.resolve("some-other-service") is None

    def test_an_owner_name_not_present_in_users_raises(self, tmp_path):
        path = tmp_path / "dag_owners.yaml"
        path.write_text(
            """
            users:
              alice:
                email: alice@example.com
            services:
              stream-processor:
                owner: unknown-person
                severity: high
            """
        )

        with pytest.raises(ValueError, match="services.stream-processor.owner must reference"):
            load_service_owners_registry(path)

    def test_a_file_with_no_services_key_loads_an_empty_registry(self, tmp_path):
        path = tmp_path / "dag_owners.yaml"
        path.write_text(
            """
            users:
              alice:
                email: alice@example.com
            dags:
              some_pipeline:
                owner: alice
                severity: medium
            """
        )

        registry = load_service_owners_registry(path)

        assert registry.services == {}

    def test_reads_the_repositorys_own_config_alongside_the_dag_owners_structure(self):
        # 이 로더가 dags:는 무시하고 services:/users:만 읽어야 dag_owners.py의 기존 구조와 공존한다(#447).
        from pathlib import Path

        repo_root = Path(__file__).resolve().parents[3]
        config_path = repo_root / "config" / "dag_owners.yaml"

        registry = load_service_owners_registry(config_path)
        owner = registry.resolve("stream-processor")

        assert owner is not None
        assert owner.severity == "high"
