"""hourly_pipeline DAG의 구조를 docker 없이 검증하는 테스트.

실제 task 실행(batch-jobs 컨테이너 기동)은 로컬 Airflow에서 수동으로 확인하고,
여기서는 DAG가 정상 파싱되는지와 cleanse/features/scoring/publish TaskGroup의
골격이 의도대로 구성됐는지만 확인한다.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

DAG_PATH = (
    Path(__file__).resolve().parents[1] / "dags" / "hourly_pipeline.py"
)


def _load_dag_module():
    spec = importlib.util.spec_from_file_location("hourly_pipeline", DAG_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_dag_parses_with_expected_schedule():
    module = _load_dag_module()

    assert module.dag.dag_id == "hourly_pipeline"
    assert module.dag.schedule == "0 * * * *"
    assert module.dag.catchup is False


def test_cleanse_task_group_contains_only_run_cleanse():
    module = _load_dag_module()

    task_ids = {task.task_id for task in module.dag.tasks}
    assert "cleanse.run_cleanse" in task_ids


def test_scoring_task_group_contains_only_run_scoring():
    module = _load_dag_module()

    task_ids = {task.task_id for task in module.dag.tasks}
    assert "scoring.run_scoring" in task_ids


def test_features_task_group_contains_only_run_features():
    module = _load_dag_module()

    task_ids = {task.task_id for task in module.dag.tasks}
    assert "features.run_features" in task_ids


def test_run_features_invokes_feature_job_with_required_arguments():
    module = _load_dag_module()

    run_features = module.dag.get_task("features.run_features")
    command = run_features.bash_command
    assert "build-hourly-segment-features" in command
    assert "--target-hour='{{ data_interval_start.isoformat() }}'" in command
    assert "HOURLY_SEGMENT_FEATURE_ROAD_SNAPSHOT_DATE" in command
    assert "HOURLY_SEGMENT_FEATURE_VERSION" in command
    assert "--run-id='{{ run_id }}'" in command


def test_run_scoring_invokes_score_hourly_comfort_with_templated_run_id():
    module = _load_dag_module()

    run_scoring = module.dag.get_task("scoring.run_scoring")
    assert "score-hourly-comfort" in run_scoring.bash_command
    assert "--run-id={{ run_id }}" in run_scoring.bash_command


def test_publish_task_group_contains_only_run_publish():
    module = _load_dag_module()

    task_ids = {task.task_id for task in module.dag.tasks}
    assert "publish.run_publish" in task_ids


def test_run_publish_invokes_load_segment_comfort_score_with_templated_as_of():
    module = _load_dag_module()

    run_publish = module.dag.get_task("publish.run_publish")
    command = run_publish.bash_command
    assert "load-segment-comfort-score" in command
    assert "--as-of='{{ data_interval_end.isoformat() }}'" in command
    assert "SEGMENT_COMFORT_SCORE_DATA_LAKE_URI" in command
    assert "POSTGRES_HOST" in command
    assert "POSTGRES_PASSWORD" in command


def test_dag_contains_expected_pipeline_tasks_so_far():
    # 이 이슈(#176)에서 scoring >> publish 의존관계 연결까지는 하지 않으므로,
    # publish.run_publish는 이 집합에 들어가지만 순서 테스트 대상은 아니다.
    module = _load_dag_module()

    task_ids = {task.task_id for task in module.dag.tasks}
    assert task_ids == {
        "cleanse.run_cleanse",
        "features.run_features",
        "scoring.run_scoring",
        "publish.run_publish",
    }


def test_task_groups_follow_hourly_pipeline_order():
    module = _load_dag_module()

    run_cleanse = module.dag.get_task("cleanse.run_cleanse")
    run_features = module.dag.get_task("features.run_features")
    run_scoring = module.dag.get_task("scoring.run_scoring")

    assert run_cleanse.downstream_task_ids == {"features.run_features"}
    assert run_features.upstream_task_ids == {"cleanse.run_cleanse"}
    assert run_features.downstream_task_ids == {"scoring.run_scoring"}
    assert run_scoring.upstream_task_ids == {"features.run_features"}
