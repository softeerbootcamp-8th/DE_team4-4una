"""Before/after delta table over two collected reports (#462).

최적화 전후를 같은 명령으로 비교하기 위한 절반이다. 수집 결과마다 DAG run 수가
다를 수 있으므로 모든 지표를 **DAG run 1건당 평균**으로 환산해 비교한다.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pipeline_perf import format as fmt
from pipeline_perf.render import flatten_runs

_OVERHEAD_KEYS = (
    "provisioning_s",
    "spark_boot_s",
    "spark_app_s",
    "teardown_s",
    "airflow_gap_s",
    "other_task_s",
    "unaccounted_s",
)

_SPARK_TOTAL_KEYS = (
    "task_count",
    "input_records",
    "input_bytes",
    "shuffle_read_bytes",
    "shuffle_write_bytes",
    "memory_bytes_spilled",
    "disk_bytes_spilled",
    "jvm_gc_time_ms",
    "executor_run_time_ms",
)

# 초 단위 지표(`*_s`)는 이름 규칙으로 걸러 `m:ss`로 쓰고, 나머지 단위만 여기 적는다.
_FORMATTERS = {
    "spark.input_bytes": fmt.size,
    "spark.shuffle_read_bytes": fmt.size,
    "spark.shuffle_write_bytes": fmt.size,
    "spark.memory_bytes_spilled": fmt.size,
    "spark.disk_bytes_spilled": fmt.size,
    "spark.jvm_gc_time_ms": fmt.milliseconds,
    "spark.executor_run_time_ms": fmt.milliseconds,
}


def _formatter(key: str) -> Callable[[Any], str] | None:
    explicit = _FORMATTERS.get(key)
    if explicit is not None:
        return explicit
    return fmt.duration if key.endswith("_s") else None


def summarize(payload: dict[str, Any]) -> dict[str, float | None]:
    """수집 결과 하나를 DAG run 1건당 평균 지표로 요약한다."""
    runs = flatten_runs([payload])
    if not runs:
        return {}
    count = len(runs)
    summary: dict[str, float | None] = {
        "run.count": count,
        "run.duration_s": _mean(run.get("duration_s") for run in runs),
    }
    for key in _OVERHEAD_KEYS:
        summary[f"overhead.{key}"] = _mean(
            (run.get("overhead") or {}).get(key) for run in runs
        )
    for key in _SPARK_TOTAL_KEYS:
        summary[f"spark.{key}"] = _mean(
            sum(
                ((task.get("spark") or {}).get("totals") or {}).get(key) or 0
                for task in run["tasks"]
            )
            for run in runs
        )
    summary["resource.billed_vcpu_hour"] = _mean(
        sum((task.get("emr") or {}).get("billed_vcpu_hour") or 0 for task in run["tasks"])
        for run in runs
    )
    summary["resource.billed_memory_gb_hour"] = _mean(
        sum((task.get("emr") or {}).get("billed_memory_gb_hour") or 0 for task in run["tasks"])
        for run in runs
    )
    for task_id in sorted(
        {task["task_id"] for run in runs for task in run["tasks"]}
    ):
        summary[f"task.{task_id}.duration_s"] = _mean(
            task.get("duration_s")
            for run in runs
            for task in run["tasks"]
            if task["task_id"] == task_id
        )
    return summary


def render_comparison(before: dict[str, Any], after: dict[str, Any]) -> str:
    """두 수집 결과의 델타 표를 마크다운으로 만든다."""
    before_summary = summarize(before)
    after_summary = summarize(after)
    keys = sorted(set(before_summary) | set(after_summary))
    lines = [
        "# 성능 비교 (DAG run 1건당 평균)",
        "",
        f"- before: {before.get('collected_at', '-')} (run {before_summary.get('run.count', 0):.0f}건)",
        f"- after: {after.get('collected_at', '-')} (run {after_summary.get('run.count', 0):.0f}건)",
        "",
        "| 지표 | before | after | 델타 | 변화율 |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for key in keys:
        if key == "run.count":
            continue
        old = before_summary.get(key)
        new = after_summary.get(key)
        lines.append(
            f"| {key} | {_display(key, old)} | {_display(key, new)} | {_display(key, _delta(old, new), signed=True)} | {_change_rate(old, new)} |"
        )
    return "\n".join(lines) + "\n"


def _mean(values: Any) -> float | None:
    collected = [value for value in values if value is not None]
    if not collected:
        return None
    return round(sum(collected) / len(collected), 4)


def _delta(old: float | None, new: float | None) -> float | None:
    if old is None or new is None:
        return None
    return round(new - old, 4)


def _change_rate(old: float | None, new: float | None) -> str:
    if old is None or new is None or not old:
        return "-"
    return f"{(new - old) / old * 100:+.1f}%"


def _display(key: str, value: float | None, signed: bool = False) -> str:
    if value is None:
        return "-"
    formatter = _formatter(key)
    if formatter is not None:
        text = formatter(abs(value))
        return f"{'-' if value < 0 else '+'}{text}" if signed else text
    text = fmt.number(abs(value), 1)
    return f"{'-' if value < 0 else '+'}{text}" if signed else text
