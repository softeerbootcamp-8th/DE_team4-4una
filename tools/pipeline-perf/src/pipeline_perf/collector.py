"""Assembles the four collection layers into one raw report (#462).

L1 Airflow, L2 EMR Serverless, L3 Spark event log, L4 PERF 로그를 DAG run 하나
아래로 모은다. 계층별 수집기는 각자의 모듈에 있고 여기서는 조립과 파생 지표
계산만 한다. 클라이언트는 전부 주입받아 테스트에서 fake로 대체한다.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from pipeline_perf import emr as emr_module
from pipeline_perf import lake, perf_log, spark_events
from pipeline_perf.airflow import (
    AirflowClient,
    parse_timestamp,
    seconds_between,
    task_gaps,
)

SCHEMA_VERSION = 1

_EMR_OPERATOR = "EmrServerlessStartJobOperator"
_COUNTS_TASK_ID = "report_processing_counts"

# 한 번도 실행되지 않은 task의 상태. Airflow는 이런 task에도 start_date를 찍기
# 때문에(상류 실패를 전파한 시각), 시각만 보고 Job Run을 찾으면 그 시간대에 있던
# 남의 Job Run에 잘못 붙는다 — 실제 수집에서 관측한 오탐이다.
_NEVER_RAN_STATES = frozenset({"upstream_failed", "skipped", "removed", "none", "null"})


@dataclass(frozen=True, slots=True)
class CollectConfig:
    """`pipeline-perf collect` 한 번의 수집 범위.

    선택자는 셋 중 하나다. `run_ids`가 있으면 그 실행만 지목해 읽고 목록 조회를
    건너뛴다. 없으면 `since`/`until`로 자른 구간에서 최신 `last`건을 본다.
    """

    dag_ids: list[str]
    last: int = 5
    run_ids: tuple[str, ...] = ()
    since: str | None = None
    until: str | None = None
    application_id: str | None = None
    log_uri: str | None = None
    bronze_input_uri: str | None = None
    with_spark: bool = True
    counts_task_id: str = _COUNTS_TASK_ID


@dataclass
class Collector:
    airflow: AirflowClient
    emr_client: Any
    object_store: Any
    config: CollectConfig
    notes: list[str] = field(default_factory=list)

    def collect(self) -> dict[str, Any]:
        application_id = self.config.application_id or self.airflow.variable(
            "EMR_SERVERLESS_APPLICATION_ID"
        )
        # 기본값은 Job Run을 제출하는 쪽(services/orchestration/dags/emr_serverless.py)이
        # 쓰는 값과 같아야 한다 — 그쪽이 로그를 어디에 쓰는지의 원본이다.
        log_uri = self.config.log_uri or self._resolved_variable(
            "EMR_SERVERLESS_LOG_S3_URI",
            "s3://de4-observability-473551908409-ap-northeast-2-an/emr-serverless/logs/",
        )
        bronze_uri = self.config.bronze_input_uri or self.airflow.variable(
            "CLEANSING_BRONZE_INPUT_PATH"
        )
        if not application_id:
            # Airflow Variable이 `AIRFLOW_VAR_*` 환경변수로 주입돼 있으면 메타DB에
            # 없어서 REST API의 /variables가 404를 준다. 그 경우 플래그로 넘겨야 한다.
            self.notes.append(
                "EMR Serverless Application ID를 찾지 못해 L2/L3/L4를 건너뛴다. "
                "Variable이 AIRFLOW_VAR_* 환경변수로 주입된 배포라면 REST API로는 "
                "보이지 않으므로 --application-id로 직접 넘긴다."
            )

        asset_triggers = self._asset_trigger_index()
        dags = [
            self._collect_dag(dag_id, application_id, log_uri, bronze_uri, asset_triggers)
            for dag_id in self.config.dag_ids
        ]
        return {
            "schema_version": SCHEMA_VERSION,
            "collected_at": datetime.now().astimezone().isoformat(),
            "application_id": application_id,
            "log_uri": log_uri,
            "bronze_input_uri": bronze_uri,
            "with_spark": self.config.with_spark,
            "dags": dags,
            "notes": self.notes,
        }

    # --- DAG / run ------------------------------------------------------

    def _collect_dag(
        self,
        dag_id: str,
        application_id: str | None,
        log_uri: str,
        bronze_uri: str | None,
        asset_triggers: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        runs = []
        for dag_run in self._selected_dag_runs(dag_id):
            if not dag_run.get("end_date"):
                # 아직 도는 중인 실행은 구간이 반만 찍혀 있다. 베이스라인 평균에
                # 섞으면 총시간을 실제보다 짧게 만든다.
                self.notes.append(
                    f"{dag_run['dag_run_id']}: 아직 끝나지 않아 제외했다"
                    f"(state={dag_run.get('state')})."
                )
                continue
            runs.append(
                self._collect_run(
                    dag_id, dag_run, application_id, log_uri, bronze_uri, asset_triggers
                )
            )
        return {"dag_id": dag_id, "runs": runs}

    def _selected_dag_runs(self, dag_id: str) -> list[dict[str, Any]]:
        """이 DAG에서 수집할 실행 목록.

        `run_ids`를 지정하면 목록 조회 없이 그 실행만 한 건씩 읽는다. 최근 N건 안에
        남아 있지 않은 실행도 지목할 수 있고, event log를 읽는 양도 그만큼 줄어든다.
        """
        if not self.config.run_ids:
            return self.airflow.dag_runs(
                dag_id,
                self.config.last,
                since=self.config.since,
                until=self.config.until,
            )
        selected = []
        for run_id in self.config.run_ids:
            dag_run = self.airflow.dag_run(dag_id, run_id)
            if dag_run is None:
                self.notes.append(f"{dag_id}: {run_id} 실행이 없어 건너뛴다.")
                continue
            selected.append(dag_run)
        return selected

    def _collect_run(
        self,
        dag_id: str,
        dag_run: dict[str, Any],
        application_id: str | None,
        log_uri: str,
        bronze_uri: str | None,
        asset_triggers: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        run_id = dag_run["dag_run_id"]
        instances = self.airflow.task_instances(dag_id, run_id)
        tasks = [
            self._collect_task(dag_id, run_id, instance, application_id, log_uri)
            for instance in sorted(instances, key=lambda item: (item.get("start_date") or ""))
        ]
        target_hour = parse_timestamp(dag_run.get("data_interval_start"))
        bronze = None
        if bronze_uri and target_hour is not None:
            bronze = lake.describe_partition(
                self.object_store, lake.bronze_partition_uri(bronze_uri, target_hour)
            )
        trigger = asset_triggers.get(run_id)
        gaps = task_gaps(instances)
        return {
            "dag_run_id": run_id,
            "run_type": dag_run.get("run_type"),
            "state": dag_run.get("state"),
            "logical_date": dag_run.get("logical_date"),
            "data_interval_start": dag_run.get("data_interval_start"),
            "data_interval_end": dag_run.get("data_interval_end"),
            "queued_at": dag_run.get("queued_at"),
            "start_date": dag_run.get("start_date"),
            "end_date": dag_run.get("end_date"),
            "duration_s": seconds_between(dag_run.get("start_date"), dag_run.get("end_date")),
            "queued_to_start_s": seconds_between(
                dag_run.get("queued_at"), dag_run.get("start_date")
            ),
            "asset_trigger": _asset_trigger_wait(trigger, dag_run),
            "processing_counts": self.airflow.xcom_value(
                dag_id, run_id, self.config.counts_task_id, "return_value"
            ),
            "bronze_input": bronze,
            "task_gaps": gaps,
            "tasks": tasks,
            "overhead": _overhead_breakdown(dag_run, tasks, gaps),
        }

    def _collect_task(
        self,
        dag_id: str,
        run_id: str,
        instance: dict[str, Any],
        application_id: str | None,
        log_uri: str,
    ) -> dict[str, Any]:
        task_id = instance["task_id"]
        task = {
            "task_id": task_id,
            "operator": instance.get("operator"),
            "state": instance.get("state"),
            "try_number": instance.get("try_number"),
            "queued_when": instance.get("queued_when"),
            "start_date": instance.get("start_date"),
            "end_date": instance.get("end_date"),
            "duration_s": instance.get("duration"),
            "job_run_id": None,
            "job_run_id_source": None,
            "emr": None,
            "spark": None,
            "perf_phases": None,
        }
        if _never_ran(instance):
            return task
        if instance.get("operator") != _EMR_OPERATOR or not application_id:
            # Spark를 안 쓰는 task는 PERF 로그가 S3가 아니라 Airflow task 로그에만
            # 남는다(`current_score_pipeline`). 그쪽도 L4로 함께 본다.
            task["perf_phases"] = self._task_log_perf_phases(dag_id, run_id, instance)
            return task
        job_run_id, source = self._resolve_job_run_id(dag_id, run_id, instance, application_id)
        task["job_run_id"] = job_run_id
        task["job_run_id_source"] = source
        if job_run_id is None:
            self.notes.append(f"{run_id}/{task_id}: Job Run ID를 찾지 못해 L2~L4를 건너뛴다.")
            return task
        task["emr"] = emr_module.describe_job_run(self.emr_client, application_id, job_run_id)
        prefix = emr_module.job_log_prefix(log_uri, application_id, job_run_id)
        task["perf_phases"] = perf_log.collect_perf_phases(self.object_store, prefix)
        if self.config.with_spark:
            task["spark"] = spark_events.aggregate_event_log(
                self.object_store, f"{prefix}/sparklogs/"
            )
        return task

    def _task_log_perf_phases(
        self, dag_id: str, run_id: str, instance: dict[str, Any]
    ) -> dict[str, Any]:
        task_id = instance["task_id"]
        try_number = instance.get("try_number") or 1
        text = self.airflow.task_log(dag_id, run_id, task_id, try_number)
        if not text:
            return {"source_uris": [], "available": False, "phases": []}
        return {
            "source_uris": [f"airflow-task-log:{dag_id}/{run_id}/{task_id}/try={try_number}"],
            "available": True,
            "phases": list(perf_log.parse_perf_lines(text.splitlines())),
        }

    def _resolve_job_run_id(
        self,
        dag_id: str,
        run_id: str,
        instance: dict[str, Any],
        application_id: str,
    ) -> tuple[str | None, str | None]:
        """XCom을 1순위로, Job Run 이름+시각 매칭을 fallback으로 쓴다(#462)."""
        value = self.airflow.xcom_value(dag_id, run_id, instance["task_id"], "return_value")
        if isinstance(value, str) and value:
            return value, "xcom"
        started = parse_timestamp(instance.get("start_date"))
        if started is None:
            return None, None
        # Job Run name은 TaskGroup 접두사가 없는 짧은 task_id다(submit_batch_jobs_command).
        name = instance["task_id"].rsplit(".", 1)[-1]
        found = emr_module.find_job_run_id_by_name(
            self.emr_client, application_id, name, started
        )
        return (found, "name_match") if found else (None, None)

    # --- 보조 -----------------------------------------------------------

    def _resolved_variable(self, key: str, default: str) -> str:
        value = self.airflow.variable(key)
        return value or default

    def _asset_trigger_index(self) -> dict[str, dict[str, Any]]:
        """Asset 이벤트를 그 이벤트가 만든 DAG run id로 색인한다.

        Asset으로 트리거되는 DAG(current_score_pipeline)의 "트리거 -> 실행 시작"
        대기를 재려면 트리거 시각이 필요한데, 소비자 쪽 API에는 그 시각이 없다.
        생산자 DAG의 asset 이벤트에서 `created_dagruns`를 따라가 연결한다.
        """
        index: dict[str, dict[str, Any]] = {}
        for dag_id in self.config.dag_ids:
            for event in self.airflow.asset_events(dag_id):
                for created in event.get("created_dagruns", []):
                    created_run_id = created.get("run_id") or created.get("dag_run_id")
                    if created_run_id:
                        index[created_run_id] = event
        return index


def _asset_trigger_wait(
    trigger: dict[str, Any] | None, dag_run: dict[str, Any]
) -> dict[str, Any] | None:
    if trigger is None:
        return None
    return {
        "source_dag_id": trigger.get("source_dag_id"),
        "source_task_id": trigger.get("source_task_id"),
        "source_run_id": trigger.get("source_run_id"),
        "triggered_at": trigger.get("timestamp"),
        "wait_to_start_s": seconds_between(trigger.get("timestamp"), dag_run.get("start_date")),
    }


def _overhead_breakdown(
    dag_run: dict[str, Any], tasks: list[dict[str, Any]], gaps: list[dict[str, Any]]
) -> dict[str, Any]:
    """DAG run 하나를 오버헤드와 실제 계산으로 쪼갠다.

    구간 정의:
      provisioning  Job Run 생성 -> 시작 (EMR이 워커를 붙이는 시간)
      spark_boot    Job Run 시작 -> Spark 애플리케이션 시작 (컨테이너/JVM 부팅)
      spark_app     Spark 애플리케이션 시작 -> 종료 (이 안이 실제 계산)
      teardown      Spark 애플리케이션 종료 -> Job Run 종료 (커밋·정리)
      airflow_gap   task 사이 빈 시간
    Spark를 쓰지 않는 task(PythonOperator)는 duration을 그대로 `other_task`에 넣는다.
    """
    totals = {
        "provisioning_s": 0.0,
        "spark_boot_s": 0.0,
        "spark_app_s": 0.0,
        "teardown_s": 0.0,
        "airflow_gap_s": round(sum(gap["seconds"] for gap in gaps), 3),
        "other_task_s": 0.0,
    }
    for task in tasks:
        emr_facts = task.get("emr")
        if not emr_facts:
            totals["other_task_s"] += task.get("duration_s") or 0.0
            continue
        totals["provisioning_s"] += emr_facts.get("provisioning_wait_s") or 0.0
        spark = task.get("spark") or {}
        app_start = _epoch_seconds(spark.get("application_start_ms"))
        app_end = _epoch_seconds(spark.get("application_end_ms"))
        started = parse_timestamp(emr_facts.get("started_at"))
        ended = parse_timestamp(emr_facts.get("ended_at"))
        if app_start is not None and started is not None:
            totals["spark_boot_s"] += max(app_start - started.timestamp(), 0.0)
        if app_start is not None and app_end is not None:
            totals["spark_app_s"] += max(app_end - app_start, 0.0)
        if app_end is not None and ended is not None:
            totals["teardown_s"] += max(ended.timestamp() - app_end, 0.0)
        elif app_start is None:
            # event log를 못 읽은 경우 Job Run 실행시간 전부를 spark_app으로 본다.
            totals["spark_app_s"] += emr_facts.get("run_duration_s") or 0.0
    totals = {key: round(value, 3) for key, value in totals.items()}
    duration = seconds_between(dag_run.get("start_date"), dag_run.get("end_date"))
    totals["dag_run_duration_s"] = duration
    accounted = sum(
        totals[key]
        for key in (
            "provisioning_s",
            "spark_boot_s",
            "spark_app_s",
            "teardown_s",
            "airflow_gap_s",
            "other_task_s",
        )
    )
    totals["unaccounted_s"] = round(duration - accounted, 3) if duration is not None else None
    measured_spark = any(task.get("emr") for task in tasks)
    totals["compute_ratio"] = (
        round(totals["spark_app_s"] / duration, 4) if duration and measured_spark else None
    )
    return totals


def _never_ran(instance: dict[str, Any]) -> bool:
    """이 task 시도가 실제로 실행된 적이 없는지."""
    state = str(instance.get("state") or "none").lower()
    return state in _NEVER_RAN_STATES or not instance.get("try_number")


def _epoch_seconds(milliseconds: int | None) -> float | None:
    return milliseconds / 1000 if milliseconds else None
