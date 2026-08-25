import copy

from pipeline_perf.compare import render_comparison, summarize
from scenario import build_collector


def _after(payload, factor):
    """Spark 계산 구간만 `factor`배로 바꾼 가상의 after 수집 결과."""
    changed = copy.deepcopy(payload)
    overhead = changed["dags"][0]["runs"][0]["overhead"]
    overhead["spark_app_s"] = round(overhead["spark_app_s"] * factor, 3)
    return changed


def test_summary_is_per_dag_run_so_different_run_counts_stay_comparable(lake):
    payload = build_collector(lake).collect()
    doubled = copy.deepcopy(payload)
    doubled["dags"][0]["runs"].append(copy.deepcopy(payload["dags"][0]["runs"][0]))

    assert summarize(payload)["run.duration_s"] == summarize(doubled)["run.duration_s"]
    assert summarize(doubled)["run.count"] == 2


def test_comparison_shows_delta_and_change_rate(lake):
    payload = build_collector(lake).collect()

    table = render_comparison(payload, _after(payload, 0.5))

    assert "| overhead.spark_app_s | 0:26 | 0:13 | -0:13 | -50.0% |" in table


def test_task_level_rows_are_included(lake):
    payload = build_collector(lake).collect()

    table = render_comparison(payload, payload)

    assert "task.sensor_processing.run_sensor_processing.duration_s" in table
    assert "| 4:40 | 4:40 | +0:00 | +0.0% |" in table


def test_empty_collection_compares_without_raising():
    empty = {"collected_at": "2026-08-25T00:00:00Z", "dags": []}

    assert "성능 비교" in render_comparison(empty, empty)
