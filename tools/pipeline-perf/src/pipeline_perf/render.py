"""Renders collected raw JSON into the baseline markdown report (#462).

리포트는 8개 절로 고정한다. 마지막 절은 관찰된 사실만 나열하고 최적화 방안은 담지
않는다 — 검증되지 않은 추측이 리포트에 사실로 남는 것을 막기 위한 규칙이다(#462).
"""

from __future__ import annotations

from typing import Any

from pipeline_perf import format as fmt

_SKEW_THRESHOLD = 2.0
_GC_RATIO_THRESHOLD = 0.1
# GC 비율을 논할 가치가 있는 최소 executor 실행시간.
_MIN_RUN_TIME_MS = 1_000
_TOP_N = 10


def render(payloads: list[dict[str, Any]], title: str | None = None) -> str:
    runs = flatten_runs(payloads)
    lines = [
        f"# {title or '승차감 점수 파이프라인 성능 베이스라인'}",
        "",
        "`pipeline-perf collect`가 모은 원시 JSON을 `pipeline-perf render`가 옮긴 것이다.",
        "숫자는 모두 실제 실행에서 측정한 값이고, 8절은 관찰된 사실만 담는다 —",
        "최적화 방안은 이 리포트의 범위가 아니다(#460, #462).",
        "",
    ]
    lines += _header_block(payloads, runs)
    lines += _section_runs(runs)
    lines += _section_timeline(runs)
    lines += _section_tasks(runs)
    lines += _section_stages(runs)
    lines += _section_sql(runs)
    lines += _section_perf_log(runs)
    lines += _section_normalized(runs)
    lines += _section_observations(runs)
    return "\n".join(lines).rstrip() + "\n"


# --- 절 ------------------------------------------------------------------


def _header_block(payloads: list[dict[str, Any]], runs: list[dict[str, Any]]) -> list[str]:
    collected = sorted({payload.get("collected_at", "") for payload in payloads if payload})
    applications = sorted({payload.get("application_id") or "-" for payload in payloads})
    notes = [note for payload in payloads for note in payload.get("notes", [])]
    lines = [
        "| 항목 | 값 |",
        "| --- | --- |",
        f"| 수집 시각 | {', '.join(collected) or '-'} |",
        f"| EMR Serverless Application | {', '.join(applications)} |",
        f"| DAG run 수 | {len(runs)} |",
        f"| Spark 릴리스 | {', '.join(_spark_versions(runs)) or '-'} |",
        "",
    ]
    missing = sorted(
        {
            metric
            for run in runs
            for task in run["tasks"]
            for metric in (task.get("spark") or {}).get("missing_metrics", [])
        }
    )
    if missing:
        lines += [
            f"event log에서 확인되지 않은 필드: {', '.join(missing)}. 해당 지표는 비어 있다.",
            "",
        ]
    if notes:
        lines += ["수집 중 남은 기록:", ""] + [f"- {note}" for note in notes] + [""]
    return lines


def _section_runs(runs: list[dict[str, Any]]) -> list[str]:
    lines = [
        "## 1. 대상 실행과 데이터량",
        "",
        "라벨은 아래 절들이 이 실행을 가리킬 때 쓰는 이름이다.",
        "",
        "| 라벨 | DAG | run id | 상태 | 시작(UTC) | 소요 | Bronze 파일 | Bronze 크기 | 평균 파일 | 처리 행 수 |",
        "| --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | --- |",
    ]
    for run in runs:
        bronze = run.get("bronze_input") or {}
        lines.append(
            "| {label} | {dag} | {run_id} | {state} | {start} | {duration} | {files} | {total} | {avg} | {counts} |".format(
                label=run_label(run),
                dag=run["dag_id"],
                run_id=run["dag_run_id"],
                state=run.get("state") or "-",
                start=_short_time(run.get("start_date")),
                duration=fmt.duration(run.get("duration_s")),
                files=fmt.number(bronze.get("file_count")),
                total=fmt.size(bronze.get("total_bytes")),
                avg=fmt.size(bronze.get("avg_bytes")),
                counts=_counts_summary(run.get("processing_counts")),
            )
        )
    lines.append("")
    return lines


def _section_timeline(runs: list[dict[str, Any]]) -> list[str]:
    lines = [
        "## 2. 타임라인과 오버헤드 대 실제 계산 시간",
        "",
        "`spark_app`(Spark 애플리케이션 시작~종료)만 실제 계산이고 나머지는 오버헤드다.",
        "`unaccounted`는 DAG run 총시간에서 아래 구간의 합을 뺀 나머지 — 스케줄러가",
        "task를 집어들기 전 대기처럼 어느 구간에도 안 잡히는 시간이다. 음수면 구간이",
        "서로 겹쳤다는 뜻이다: task가 재시도되면 Airflow가 보고하는 task 소요는 마지막",
        "시도 것인데 Job Run 구간은 그와 겹치는 다른 시도의 것일 수 있다.",
        "계산 비율의 `-`는 그 실행에 EMR Job Run이 없어 Spark 구간을 재지 않았다는 뜻이다",
        "(`current_score_pipeline`은 Spark를 쓰지 않는다).",
        "",
        "| run | 총시간 | 프로비저닝 | Spark 부팅 | Spark 계산 | 정리·커밋 | Airflow gap | 기타 task | 미계상 | 계산 비율 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for run in runs:
        overhead = run.get("overhead") or {}
        lines.append(
            "| {run_id} | {total} | {prov} | {boot} | {app} | {teardown} | {gap} | {other} | {unaccounted} | {ratio} |".format(
                run_id=run_label(run),
                total=fmt.duration(overhead.get("dag_run_duration_s")),
                prov=fmt.duration(overhead.get("provisioning_s")),
                boot=fmt.duration(overhead.get("spark_boot_s")),
                app=fmt.duration(overhead.get("spark_app_s")),
                teardown=fmt.duration(overhead.get("teardown_s")),
                gap=fmt.duration(overhead.get("airflow_gap_s")),
                other=fmt.duration(overhead.get("other_task_s")),
                unaccounted=fmt.duration(overhead.get("unaccounted_s")),
                ratio=fmt.percent(overhead.get("compute_ratio")),
            )
        )
    lines.append("")
    triggers = [run for run in runs if run.get("asset_trigger")]
    if triggers:
        lines += [
            "Asset 트리거로 시작한 실행의 대기:",
            "",
            "| run | 트리거 | 트리거 시각 | 시작까지 |",
            "| --- | --- | --- | ---: |",
        ]
        for run in triggers:
            trigger = run["asset_trigger"]
            lines.append(
                "| {run_id} | {source} | {at} | {wait} |".format(
                    run_id=run_label(run),
                    source=f"{trigger.get('source_dag_id')}.{trigger.get('source_task_id')}",
                    at=_short_time(trigger.get("triggered_at")),
                    wait=fmt.duration(trigger.get("wait_to_start_s")),
                )
            )
        lines.append("")
    return lines


def _section_tasks(runs: list[dict[str, Any]]) -> list[str]:
    lines = [
        "## 3. task별 상세",
        "",
        "| run | task | 상태 | 시도 | 소요 | 프로비저닝 | Job Run 실행 | stage | task(Spark) | vCPU-h | mem GB-h | storage GB-h |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for run in runs:
        for task in run["tasks"]:
            emr = task.get("emr") or {}
            spark = task.get("spark") or {}
            lines.append(
                "| {run_id} | {task_id} | {state} | {tries} | {duration} | {prov} | {job} | {stages} | {tasks} | {vcpu} | {mem} | {storage} |".format(
                    run_id=run_label(run),
                    task_id=task["task_id"],
                    state=task.get("state") or "-",
                    tries=fmt.number(task.get("try_number")),
                    duration=fmt.duration(task.get("duration_s")),
                    prov=fmt.duration(emr.get("provisioning_wait_s")),
                    job=fmt.duration(emr.get("run_duration_s")),
                    stages=fmt.number(spark.get("stage_count")),
                    tasks=fmt.number(spark.get("task_count")),
                    vcpu=fmt.number(emr.get("billed_vcpu_hour"), 3),
                    mem=fmt.number(emr.get("billed_memory_gb_hour"), 3),
                    storage=fmt.number(emr.get("billed_storage_gb_hour"), 3),
                )
            )
    lines.append("")
    return lines


def _section_stages(runs: list[dict[str, Any]]) -> list[str]:
    stages = _all_stages(runs)
    stages.sort(key=lambda item: item["stage"].get("wall_time_ms") or 0, reverse=True)
    lines = [
        f"## 4. 느린 스테이지 top {_TOP_N}",
        "",
        "`GC 비율`은 `jvmGcTime / executorRunTime`이다. JVM GC 시간에는 같은 executor를",
        "쓰는 다른 태스크가 유발한 GC도 잡히므로 100%를 넘을 수 있다 — 짧은 태스크에서",
        "특히 그렇다.",
        "",
        "`skew`는 스테이지 안 task duration의 `max / p50`이다. `task 합`은 그 스테이지",
        "task duration의 총합으로, `wall`과 크게 벌어지면 그 차이는 계산이 아니라",
        "슬롯을 기다린 시간이다.",
        "",
        "| run/task | stage | 이름 | wall | tasks | task 합 | p50 | p95 | max | skew | shuffle R/W | spill(mem/disk) | GC 비율 |",
        "| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: |",
    ]
    for entry in stages[:_TOP_N]:
        stage = entry["stage"]
        durations = stage.get("task_duration") or {}
        lines.append(
            "| {origin} | {stage_id} | {name} | {wall} | {tasks} | {total} | {p50} | {p95} | {max} | {skew} | {shuffle} | {spill} | {gc} |".format(
                origin=entry["origin"],
                stage_id=stage["stage_id"],
                name=fmt.truncate(stage.get("name"), 46),
                wall=fmt.milliseconds(stage.get("wall_time_ms")),
                tasks=fmt.number(stage.get("task_count")),
                total=fmt.milliseconds(durations.get("sum_ms")),
                p50=fmt.milliseconds(durations.get("p50_ms")),
                p95=fmt.milliseconds(durations.get("p95_ms")),
                max=fmt.milliseconds(durations.get("max_ms")),
                skew=fmt.number(stage.get("skew_ratio"), 2),
                shuffle=f"{fmt.size(stage.get('shuffle_read_bytes'))} / {fmt.size(stage.get('shuffle_write_bytes'))}",
                spill=f"{fmt.size(stage.get('memory_bytes_spilled'))} / {fmt.size(stage.get('disk_bytes_spilled'))}",
                gc=fmt.percent(stage.get("gc_ratio")),
            )
        )
    lines.append("")
    return lines


def _section_sql(runs: list[dict[str, Any]]) -> list[str]:
    executions = []
    for run in runs:
        for task in run["tasks"]:
            spark = task.get("spark") or {}
            for execution in spark.get("sql_executions", []):
                executions.append(
                    {"origin": _origin(run, task), "execution": execution}
                )
    executions.sort(key=lambda item: item["execution"].get("duration_ms") or 0, reverse=True)
    lines = [
        f"## 5. 느린 SQL execution top {_TOP_N}",
        "",
        "| run/task | execution | 소요 | description |",
        "| --- | ---: | ---: | --- |",
    ]
    for entry in executions[:_TOP_N]:
        execution = entry["execution"]
        lines.append(
            "| {origin} | {execution_id} | {duration} | {description} |".format(
                origin=entry["origin"],
                execution_id=execution["execution_id"],
                duration=fmt.milliseconds(execution.get("duration_ms")),
                description=fmt.truncate(execution.get("description"), 60),
            )
        )
    if not executions:
        lines.append("| - | - | - | SQL execution 이벤트가 수집되지 않았다 |")
    lines.append("")
    return lines


def _section_perf_log(runs: list[dict[str, Any]]) -> list[str]:
    lines = [
        "## 6. Spark 밖 구간 (PERF 로그)",
        "",
        "`de4_core.perf_phase`가 남긴 구간이다(#461). Spark event log에 흔적이 없는",
        "psycopg2 직접 실행 구간이 여기 잡힌다. EMR Job Run은 driver의 stdout에서,",
        "Spark를 쓰지 않는 task는 Airflow task 로그에서 읽는다.",
        "",
        "| run/task | phase | 소요 | 성공 | 부가 필드 |",
        "| --- | --- | ---: | --- | --- |",
    ]
    found = False
    for run in runs:
        for task in run["tasks"]:
            perf = task.get("perf_phases") or {}
            for phase in perf.get("phases", []):
                found = True
                extras = {
                    key: value
                    for key, value in phase.items()
                    if key not in {"phase", "elapsed_s", "ok"}
                }
                lines.append(
                    "| {origin} | {phase} | {elapsed} | {ok} | {extras} |".format(
                        origin=_origin(run, task),
                        phase=phase.get("phase", "-"),
                        elapsed=fmt.duration(phase.get("elapsed_s")),
                        ok="예" if phase.get("ok") else "아니오",
                        extras=fmt.truncate(
                            ", ".join(f"{key}={value}" for key, value in sorted(extras.items())),
                            50,
                        ),
                    )
                )
    if not found:
        lines.append("| - | - | - | - | PERF 로그가 수집되지 않았다 |")
    lines.append("")
    return lines


def _section_normalized(runs: list[dict[str, Any]]) -> list[str]:
    lines = [
        "## 7. 정규화 지표와 DAG run당 원가",
        "",
        "`vCPU-초/100만건`은 billed vCPU-hour를 Spark 입력 레코드 수로 정규화한 값이다.",
        "금액은 요금표를 리포트에 박아 두지 않기 위해 싣지 않는다 — 자원 사용량만 남긴다.",
        "",
        "| run | Spark 입력 레코드 | 입력 바이트 | 레코드/초 | vCPU-h | mem GB-h | storage GB-h | vCPU-초/100만건 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for run in runs:
        metrics = _run_resource_metrics(run)
        lines.append(
            "| {run_id} | {records} | {bytes_read} | {rate} | {vcpu} | {mem} | {storage} | {normalized} |".format(
                run_id=run_label(run),
                records=fmt.number(metrics["input_records"]),
                bytes_read=fmt.size(metrics["input_bytes"]),
                rate=fmt.number(metrics["records_per_second"], 1),
                vcpu=fmt.number(metrics["vcpu_hour"], 3),
                mem=fmt.number(metrics["memory_gb_hour"], 3),
                storage=fmt.number(metrics["storage_gb_hour"], 3),
                normalized=fmt.number(metrics["vcpu_seconds_per_million_records"], 1),
            )
        )
    lines.append("")
    return lines


def _section_observations(runs: list[dict[str, Any]]) -> list[str]:
    lines = ["## 8. 관찰된 병목 후보", "", "사실만 적는다. 원인 진단과 대응은 후속 이슈에서 다룬다.", ""]
    lines += [f"- {observation}" for observation in _observations(runs)] or [
        "- 수집된 실행이 없어 관찰할 것이 없다."
    ]
    lines.append("")
    return lines


def _observations(runs: list[dict[str, Any]]) -> list[str]:
    if not runs:
        return []
    observations = []
    states: dict[str, int] = {}
    for run in runs:
        state = str(run.get("state") or "unknown")
        states[state] = states.get(state, 0) + 1
    observations.append(
        "수집한 DAG run "
        + f"{len(runs)}건의 상태는 "
        + ", ".join(f"{state} {count}건" for state, count in sorted(states.items()))
        + "이다."
    )
    # DAG를 섞으면 Spark를 안 쓰는 DAG가 계산 비율을 희석한다. DAG별로 나눠 적는다.
    for dag_id in sorted({run["dag_id"] for run in runs}):
        dag_runs = [run for run in runs if run["dag_id"] == dag_id]
        dag_durations = [
            value for run in dag_runs if (value := run["overhead"].get("dag_run_duration_s"))
        ]
        dag_total = sum(dag_durations)
        if not dag_total:
            continue
        compute = sum(run["overhead"].get("spark_app_s") or 0 for run in dag_runs)
        dag_gap = sum(run["overhead"].get("airflow_gap_s") or 0 for run in dag_runs)
        measured = any(run["overhead"].get("compute_ratio") is not None for run in dag_runs)
        share = (
            f" 그중 Spark 계산 구간은 {fmt.duration(compute)}"
            f"({fmt.percent(compute / dag_total)}), task 사이 Airflow gap은 "
            f"{fmt.duration(dag_gap)}({fmt.percent(dag_gap / dag_total)})이다."
            if measured
            else " Spark를 쓰지 않아 계산 구간을 따로 재지 않았다."
        )
        observations.append(
            f"{dag_id} {len(dag_durations)}건의 총시간 합은 {fmt.duration(dag_total)}이고,{share}"
        )
    provisioning = sum(run["overhead"].get("provisioning_s") or 0 for run in runs)
    job_runs = sum(1 for run in runs for task in run["tasks"] if task.get("emr"))
    if job_runs:
        observations.append(
            f"Job Run {job_runs}건의 프로비저닝 대기 합은 {fmt.duration(provisioning)}이고, "
            f"건당 평균은 {fmt.duration(provisioning / job_runs)}이다."
        )
    stages = _all_stages(runs)
    skewed = [
        entry
        for entry in stages
        if (entry["stage"].get("skew_ratio") or 0) >= _SKEW_THRESHOLD
        and (entry["stage"].get("task_count") or 0) > 1
    ]
    if skewed:
        worst = max(skewed, key=lambda entry: entry["stage"]["skew_ratio"])
        observations.append(
            f"task duration의 max/p50이 {_SKEW_THRESHOLD} 이상인 스테이지가 {len(skewed)}개이고, "
            f"최대는 {worst['origin']} stage {worst['stage']['stage_id']}의 "
            f"{fmt.number(worst['stage']['skew_ratio'], 2)}배다."
        )
    spilled = [
        entry
        for entry in stages
        if (entry["stage"].get("memory_bytes_spilled") or 0)
        or (entry["stage"].get("disk_bytes_spilled") or 0)
    ]
    if spilled:
        disk = sum(entry["stage"].get("disk_bytes_spilled") or 0 for entry in spilled)
        memory = sum(entry["stage"].get("memory_bytes_spilled") or 0 for entry in spilled)
        observations.append(
            f"spill이 발생한 스테이지가 {len(spilled)}개이고, 합계는 memory {fmt.size(memory)}, "
            f"disk {fmt.size(disk)}이다."
        )
    # executorRunTime이 1초도 안 되는 스테이지는 GC 비율이 쉽게 100%를 넘어 의미가 없다.
    gc_heavy = [
        entry
        for entry in stages
        if (entry["stage"].get("gc_ratio") or 0) >= _GC_RATIO_THRESHOLD
        and (entry["stage"].get("executor_run_time_ms") or 0) >= _MIN_RUN_TIME_MS
    ]
    if gc_heavy:
        worst = max(gc_heavy, key=lambda entry: entry["stage"]["gc_ratio"])
        observations.append(
            f"GC 시간이 executor 실행시간의 {fmt.percent(_GC_RATIO_THRESHOLD)} 이상인 스테이지가 "
            f"{len(gc_heavy)}개이고, 최대는 {worst['origin']} stage "
            f"{worst['stage']['stage_id']}의 {fmt.percent(worst['stage']['gc_ratio'])}다."
        )
    slowest = max(stages, key=lambda entry: entry["stage"].get("wall_time_ms") or 0, default=None)
    if slowest and slowest["stage"].get("wall_time_ms"):
        observations.append(
            f"가장 오래 걸린 스테이지는 {slowest['origin']} stage {slowest['stage']['stage_id']} "
            f"({fmt.truncate(slowest['stage'].get('name'), 40)})로 "
            f"{fmt.milliseconds(slowest['stage']['wall_time_ms'])}이 걸렸다."
        )
    utilizations = [
        ((task.get("spark") or {}).get("concurrency") or {}).get("slot_utilization")
        for run in runs
        for task in run["tasks"]
    ]
    utilizations = sorted(value for value in utilizations if value is not None)
    if utilizations:
        median = utilizations[len(utilizations) // 2]
        observations.append(
            f"Job Run {len(utilizations)}건의 태스크 점유 시간은 가용 슬롯 시간의 "
            f"{fmt.percent(utilizations[0])}~{fmt.percent(utilizations[-1])}이고, "
            f"중앙값은 {fmt.percent(median)}다."
        )
    bronze = [run["bronze_input"] for run in runs if run.get("bronze_input")]
    if bronze:
        files = sum(entry["file_count"] for entry in bronze)
        total_bytes = sum(entry["total_bytes"] for entry in bronze)
        observations.append(
            f"Bronze 입력 파티션 {len(bronze)}개의 파일 수는 {fmt.number(files)}개, 합계는 "
            f"{fmt.size(total_bytes)}이고, 파일당 평균은 "
            f"{fmt.size(round(total_bytes / files) if files else None)}이다."
        )
    return observations


# --- 공통 도우미 ----------------------------------------------------------


def flatten_runs(payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    runs = []
    for payload in payloads:
        for dag in payload.get("dags", []):
            for run in dag.get("runs", []):
                runs.append({**run, "dag_id": dag["dag_id"]})
    runs.sort(key=lambda run: (run["dag_id"], run.get("start_date") or ""))
    return runs


def _all_stages(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"origin": _origin(run, task), "stage": stage}
        for run in runs
        for task in run["tasks"]
        for stage in (task.get("spark") or {}).get("stages", [])
    ]


def run_label(run: dict[str, Any]) -> str:
    """표에서 실행을 가리키는 짧은 이름.

    `dag_run_id`를 잘라 쓰면 `scheduled__2026-08-25T0…`가 전부 같아 보여 어느 실행의
    행인지 구분이 안 된다. DAG 약칭과 data interval 시각으로 줄인다. 전체 id는
    1절 표에 그대로 있다.
    """
    dag_code = "".join(word[:1] for word in run["dag_id"].split("_"))
    # 실제 시작 시각을 쓴다. Airflow 3의 scheduled run_id는 `run_after` 기준이고
    # `data_interval_start`는 그보다 한 시간 앞이라, interval을 쓰면 라벨과 run id가
    # 어긋나 보인다.
    stamp = run.get("start_date") or run.get("logical_date") or ""
    return f"{dag_code} {stamp[5:16].replace('T', ' ')}".strip()


def _origin(run: dict[str, Any], task: dict[str, Any]) -> str:
    return f"{run_label(run)}/{task['task_id'].rsplit('.', 1)[-1]}"


def _spark_versions(runs: list[dict[str, Any]]) -> list[str]:
    return sorted(
        {
            version
            for run in runs
            for task in run["tasks"]
            if (version := (task.get("spark") or {}).get("spark_version"))
        }
    )


def _counts_summary(counts: Any) -> str:
    if not isinstance(counts, dict) or not counts:
        return "-"
    return ", ".join(f"{key}={fmt.number(value)}" for key, value in sorted(counts.items()))


def _short_time(value: str | None) -> str:
    if not value:
        return "-"
    return value.replace("T", " ")[:16]


def _run_resource_metrics(run: dict[str, Any]) -> dict[str, float | None]:
    measured_spark = any(task.get("spark") for task in run["tasks"])
    input_records = 0
    input_bytes = 0
    vcpu_hour = 0.0
    memory_gb_hour = 0.0
    storage_gb_hour = 0.0
    for task in run["tasks"]:
        totals = (task.get("spark") or {}).get("totals") or {}
        input_records += totals.get("input_records") or 0
        input_bytes += totals.get("input_bytes") or 0
        emr = task.get("emr") or {}
        vcpu_hour += emr.get("billed_vcpu_hour") or 0.0
        memory_gb_hour += emr.get("billed_memory_gb_hour") or 0.0
        storage_gb_hour += emr.get("billed_storage_gb_hour") or 0.0
    compute_seconds = (run.get("overhead") or {}).get("spark_app_s") or 0
    return {
        # Spark를 재지 않은 실행에서 0은 "0건을 읽었다"가 아니라 "재지 않았다"이다.
        "input_records": input_records if measured_spark else None,
        "input_bytes": input_bytes if measured_spark else None,
        "records_per_second": (
            round(input_records / compute_seconds, 1) if compute_seconds and input_records else None
        ),
        "vcpu_hour": round(vcpu_hour, 4) or None,
        "memory_gb_hour": round(memory_gb_hour, 4) or None,
        "storage_gb_hour": round(storage_gb_hour, 4) or None,
        "vcpu_seconds_per_million_records": (
            round(vcpu_hour * 3600 / (input_records / 1_000_000), 1) if input_records else None
        ),
    }
