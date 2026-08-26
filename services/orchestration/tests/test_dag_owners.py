"""dag_owners.py의 레지스트리 로드/조회 폴백(#409)을 검증한다."""

from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from jobs.dag_owners import load_dag_owners_registry

_MINIMAL_YAML = textwrap.dedent(
    """
    users:
      alice:
        email: alice@example.com
      bob:
        slack_id: U0456GHIJKL

    dags:
      standard_score_pipeline:
        owner: alice
        severity: critical
        tasks:
          emr_teardown.check_idle:
            owner: bob
            severity: high
      zone_weather_compaction:
        owner: bob
        severity: low
    """
)


def _write_config(tmp_path: Path, text: str = _MINIMAL_YAML) -> Path:
    path = tmp_path / "dag_owners.yaml"
    path.write_text(text)
    return path


def test_users_are_parsed_with_email_or_slack_id(tmp_path):
    registry = load_dag_owners_registry(_write_config(tmp_path))

    assert registry.users["alice"].email == "alice@example.com"
    assert registry.users["alice"].slack_id is None
    assert registry.users["bob"].slack_id == "U0456GHIJKL"
    assert registry.users["bob"].email is None


def test_task_level_override_wins_over_dag_default(tmp_path):
    registry = load_dag_owners_registry(_write_config(tmp_path))

    owner = registry.resolve_owner(
        "standard_score_pipeline",
        task_id="emr_teardown.check_idle",
    )
    severity = registry.resolve_severity(
        "standard_score_pipeline",
        task_id="emr_teardown.check_idle",
    )
    assert owner.name == "bob"
    assert severity == "high"


def test_unregistered_task_falls_back_to_dag_default(tmp_path):
    registry = load_dag_owners_registry(_write_config(tmp_path))

    owner = registry.resolve_owner(
        "standard_score_pipeline", task_id="compute_hourly_score"
    )
    severity = registry.resolve_severity(
        "standard_score_pipeline", task_id="compute_hourly_score"
    )
    assert owner.name == "alice"
    assert severity == "critical"


def test_task_group_id_is_used_when_task_id_has_no_override(tmp_path):
    text = _MINIMAL_YAML.replace(
        "emr_teardown.check_idle:",
        "emr_teardown:",
    )
    registry = load_dag_owners_registry(_write_config(tmp_path, text))

    owner = registry.resolve_owner(
        "standard_score_pipeline",
        task_id="emr_teardown.stop_application",
        task_group_id="emr_teardown",
    )
    assert owner.name == "bob"


def test_dag_without_task_overrides_uses_dag_default(tmp_path):
    registry = load_dag_owners_registry(_write_config(tmp_path))

    owner = registry.resolve_owner("zone_weather_compaction")
    assert owner.name == "bob"


def test_unregistered_dag_raises_key_error(tmp_path):
    registry = load_dag_owners_registry(_write_config(tmp_path))

    with pytest.raises(KeyError):
        registry.resolve_owner("unknown_dag")


def test_user_missing_email_and_slack_id_raises(tmp_path):
    text = _MINIMAL_YAML.replace("email: alice@example.com", "")
    with pytest.raises(ValueError, match="email' or 'slack_id'"):
        load_dag_owners_registry(_write_config(tmp_path, text))


def test_dag_owner_referencing_unknown_user_raises(tmp_path):
    text = _MINIMAL_YAML.replace("owner: alice", "owner: charlie", 1)
    with pytest.raises(ValueError, match="users"):
        load_dag_owners_registry(_write_config(tmp_path, text))


def test_invalid_severity_raises(tmp_path):
    text = _MINIMAL_YAML.replace("severity: critical", "severity: nonsense")
    with pytest.raises(ValueError, match="severity"):
        load_dag_owners_registry(_write_config(tmp_path, text))


def test_real_config_file_covers_all_target_dags():
    config_path = Path(__file__).resolve().parents[3] / "config" / "dag_owners.yaml"
    registry = load_dag_owners_registry(config_path)

    for dag_id in (
        "standard_score_pipeline",
        "current_score_pipeline",
        "zone_weather_pipeline",
        "data_quality_audit",
        "zone_weather_compaction",
    ):
        owner = registry.resolve_owner(dag_id)
        severity = registry.resolve_severity(dag_id)
        assert owner.name in registry.users
        assert severity in ("critical", "high", "medium", "low")
