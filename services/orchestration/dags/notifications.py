"""standard_score_pipeline 등 5개 DAG가 공유하는 Slack 알림 콜백 (#409).

on_failure_callback은 task 단위(default_args)로, on_success_callback은 DAG
단위로 배선한다. 두 콜백 모두 최상위(모듈 import 시점)에서는 yaml/slack_sdk/
de4_core를 쓰지 않는다 — dag-processor/webserver도 이 파일을 파싱하므로,
콜백 함수 본문 안에서만 지연 import해 그 프로세스들이 pyyaml/
apache-airflow-providers-slack 없이도 DAG를 파싱할 수 있게 한다(jobs.weather를
함수 안에서만 import하는 기존 관례와 동일).
"""

from __future__ import annotations

# DagRun 성공 요약에 어느 task의 XCom(return value)을 쓸지 — dag_id별 매핑.
# standard_score_pipeline/data_quality_audit의 값은 TaskGroup 접두사를 포함한
# 전체 task_id다(get_task_instance는 dotted id를 그대로 받는다). on_failure_callback도
# 이 매핑을 재사용해, 실패 시점까지 이미 성공한 상위 task가 있으면 그 처리 건수를
# 실패 알림에도 넣는다(spec §7) — 아직 안 돌았으면 조용히 생략한다.
_SUMMARY_TASK_IDS: dict[str, str] = {
    "current_score_pipeline": "run_current_score",
    "zone_weather_pipeline": "run_weather_collection",
    "bronze_compaction": "compact_zone_weather_snapshot",
    # report_processing_counts는 standard_score TaskGroup 밖(DAG 최상위)에 있으므로
    # task_id에 TaskGroup 접두사가 붙지 않는다 — dags/standard_score_pipeline.py 참고.
    "standard_score_pipeline": "report_processing_counts",
    "data_quality_audit": "report_audit_counts",
}

# 사용자가 콘솔에서 미리 만들어 둔 관측 버킷(#409) — Airflow Variable
# OBSERVABILITY_FAILED_TASKS_S3_URI로 override 가능.
_DEFAULT_FAILED_TASKS_S3_ROOT = (
    "s3://de4-observability-473551908409-ap-northeast-2-a/airflow/failed-tasks/"
)


def on_failure_callback(context: dict) -> None:
    from jobs.dag_owners import SEVERITY_LABELS, load_dag_owners_registry

    task = context["task"]
    task_instance = context["task_instance"]
    dag_id = context["dag"].dag_id
    task_id = task_instance.task_id
    task_group_id = task.task_group.group_id if task.task_group else None

    registry = load_dag_owners_registry()
    owner = registry.resolve_owner(dag_id, task_id=task_id, task_group_id=task_group_id)
    severity = registry.resolve_severity(dag_id, task_id=task_id, task_group_id=task_group_id)

    hook = _build_slack_hook()
    mention = _resolve_mention(owner, hook)
    exception = context.get("exception")
    counts = _pull_summary(context, dag_id)

    # S3에 남기는 원본은 가공 없이 이 dict 그대로 JSON 직렬화한다(#409) — 가독성은
    # 아래 Slack 메시지 쪽에서 챙긴다. 값을 지어내지 않는다: 아직 아무 상위 task도
    # 성공하지 않았으면 counts는 그대로 None이다.
    record = {
        "dag_id": dag_id,
        "task_id": task_id,
        "logical_date": str(context["logical_date"]),
        "owner": owner.name,
        "severity": severity,
        "exception": str(exception) if exception else None,
        "log_url": task_instance.log_url,
        "counts": counts,
    }
    record_url = _write_failure_record(record, context["run_id"])

    lines = [
        f"{SEVERITY_LABELS[severity]} *{dag_id}* task 실패: `{task_id}`",
        f"담당자: {mention}",
        f"처리 일자(logical_date): {record['logical_date']}",
        f"예외: {exception}" if exception else "예외 정보 없음(수동 확인 필요)",
        f"처리 건수: {counts}"
        if counts is not None
        else "처리 건수: 이 실행에서 아직 집계되지 않음",
        f"<{task_instance.log_url}|Task Instance 열기>",
        f"<{record_url}|실패 상세 기록 열기(S3)>",
    ]
    s3_logs_link = _emr_s3_logs_link(context)
    if s3_logs_link:
        lines.append(f"<{s3_logs_link}|EMR Serverless 원본 로그 열기>")

    _post_message(hook, "\n".join(lines))


def on_success_callback(context: dict) -> None:
    from jobs.dag_owners import SEVERITY_LABELS, load_dag_owners_registry

    dag_run = context["dag_run"]
    dag_id = context["dag"].dag_id

    registry = load_dag_owners_registry()
    owner = registry.resolve_owner(dag_id)
    severity = registry.resolve_severity(dag_id)

    lines = [
        f"{SEVERITY_LABELS[severity]} *{dag_id}* 성공 (담당자: {owner.name})",
        f"<{_dag_run_url(dag_run)}|DAG Run 열기>",
    ]
    summary = _pull_summary(context, dag_id)
    if summary is not None:
        lines.append(f"처리 건수: {summary}")

    hook = _build_slack_hook()
    _post_message(hook, "\n".join(lines))


def _dag_run_url(dag_run) -> str:
    # Airflow 3 콜백 컨텍스트의 dag_run은 Pydantic 모델이라 (구버전 ORM DagRun에 있던)
    # get_absolute_url()이 없다 — task_instance.log_url과 같은 방식(airflow.sdk의
    # RuntimeTaskInstance.log_url)으로 base_url을 직접 읽어 URL을 만든다(#409 로컬
    # 검증 중 AttributeError로 실제 발견).
    from urllib.parse import quote

    from airflow.configuration import conf

    base_url = conf.get("api", "base_url", fallback="http://localhost:8080/")
    return f"{base_url.rstrip('/')}/dags/{dag_run.dag_id}/runs/{quote(dag_run.run_id)}"


def _pull_summary(context: dict, dag_id: str):
    # Airflow 3 콜백 컨텍스트의 dag_run(Pydantic 모델)에는 get_task_instance()가 없다
    # (#409 로컬 검증 중 AttributeError로 실제 발견) — 대신 context["task_instance"](
    # RuntimeTaskInstance)의 xcom_pull은 task_ids로 다른 task의 XCom도 정상적으로
    # 조회한다. 이 키가 아예 없는 극단적인 경우(성공/실패한 task가 하나도 없음)에도
    # 대비한다.
    task_instance = context.get("task_instance")
    if task_instance is None:
        return None
    summary_task_id = _SUMMARY_TASK_IDS.get(dag_id)
    if summary_task_id is None:
        return None
    try:
        return task_instance.xcom_pull(task_ids=summary_task_id)
    except Exception:  # noqa: BLE001
        # 처리 건수는 부가 정보다 — XCom 조회가 실패해도(예: 콜백 실행 컨텍스트가
        # 제한적인 경우) 핵심 알림(Slack 메시지 자체)까지 막아서는 안 된다(#409
        # 로컬 검증 중 `airflow dags test`에서 SUPERVISOR_COMMS ImportError로 실제 발견).
        return None


def _emr_s3_logs_link(context: dict) -> str | None:
    # EmrServerlessStartJobOperator는 Job Run이 실패해도(대기 중 raise되기 전에)
    # 이 XCom을 남긴다 — #409 조사 기록 참고. EMR task가 아니면 그냥 None.
    from airflow.providers.amazon.aws.links.emr import EmrServerlessS3LogsLink

    task_instance = context["task_instance"]
    link_data = task_instance.xcom_pull(
        task_ids=task_instance.task_id, key=EmrServerlessS3LogsLink.key
    )
    if not link_data:
        return None
    return EmrServerlessS3LogsLink().format_link(**link_data)


def _write_failure_record(record: dict, run_id: str) -> str:
    import json

    from de4_core import ObjectStore, join_uri

    root_uri = _failed_tasks_s3_root()
    object_uri = join_uri(root_uri, record["dag_id"], record["task_id"], f"{run_id}.json")
    ObjectStore().write_bytes(
        object_uri, json.dumps(record, ensure_ascii=False).encode("utf-8")
    )
    return _s3_console_url(object_uri)


def _failed_tasks_s3_root() -> str:
    from airflow.sdk import Variable

    return Variable.get(
        "OBSERVABILITY_FAILED_TASKS_S3_URI", default=_DEFAULT_FAILED_TASKS_S3_ROOT
    )


def _s3_console_url(uri: str) -> str:
    import os

    from de4_core.storage import parse_uri

    _, bucket, key = parse_uri(uri)
    region = os.environ.get("AWS_REGION", "ap-northeast-2")
    return f"https://{region}.console.aws.amazon.com/s3/object/{bucket}?region={region}&prefix={key}"


def _build_slack_hook():
    from airflow.providers.slack.hooks.slack import SlackHook

    return SlackHook(slack_conn_id="slack_api_default")


def _resolve_mention(owner, hook) -> str:
    if owner.slack_id:
        return f"<@{owner.slack_id}>"
    response = hook.client.users_lookupByEmail(email=owner.email)
    return f"<@{response['user']['id']}>"


def _slack_channel() -> str:
    from airflow.sdk import Variable

    return Variable.get("SLACK_ALERT_CHANNEL")


def _post_message(hook, text: str) -> None:
    hook.client.chat_postMessage(channel=_slack_channel(), text=text)
