"""Orchestration 배포가 sensor-events compaction 운영 설정을 보장하는지 검증한다."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
WORKFLOW_PATH = REPO_ROOT / ".github/workflows/deploy-orchestration.yml"


def test_deploy_reads_the_sensor_events_s3_root_from_orchestration_env() -> None:
    workflow = WORKFLOW_PATH.read_text()

    assert "vars.SENSOR_EVENTS_COMPACTION_ROOT_URI" not in workflow
    assert "--env-file '$ENV_FILE'" in workflow
    assert "s3://*/bronze/sensor-events" in workflow
    assert "actual_root" in workflow


def test_deploy_validates_the_canary_and_controls_dag_pause_state() -> None:
    workflow = WORKFLOW_PATH.read_text()

    assert "max_groups" in workflow
    assert "vars.SENSOR_EVENTS_COMPACTION_ENABLED" not in workflow
    assert "SENSOR_EVENTS_COMPACTION_ENABLED" in workflow
    assert "airflow dags unpause bronze_sensor_events_compaction" in workflow
    assert "airflow dags pause bronze_sensor_events_compaction" in workflow
