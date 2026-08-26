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
    "s3://de4-observability-473551908409-ap-northeast-2-an/airflow/failed-tasks/"
)

# EMR Serverless가 Job Run 로그를 올리는 S3 레이아웃. driver stderr에 Python 트레이스백과
# Spark 예외가 남는다 — 알림에 실리던 Airflow 래퍼 예외("Job reached failure state FAILED")로는
# 원인을 알 수 없어서 이 파일을 직접 읽는다.
# stderr는 JVM/log4j 출력, stdout은 애플리케이션 출력이 나뉘어 담긴다. 실제 실패 Job Run의
# stderr가 JVM 경고와 shutdown hook 두 줄뿐인 경우를 확인해서 둘 다 읽는다.
_EMR_DRIVER_LOG_SUFFIXES = ("SPARK_DRIVER/stderr.gz", "SPARK_DRIVER/stdout.gz")

# Job Run이 끝난 직후에는 S3 flush가 아직 안 끝나 객체가 없을 수 있다. 짧게만 기다린다 —
# 콜백이 오래 걸리면 알림 자체가 늦어진다.
# 최악의 경우에도 총 대기가 4초를 넘지 않게 한다 — 진단은 부가 정보인데 이것 때문에
# 실패 알림이 늦어지면 본말이 전도된다.
_LOG_FETCH_ATTEMPTS = 3
_LOG_FETCH_RETRY_SECONDS = 2.0

# Slack 메시지 상한(약 4000자)에 걸리지 않도록 근거 로그를 제한한다. 전문은 S3 링크로 본다.
_MAX_EVIDENCE_LINES = 5
_MAX_EVIDENCE_CHARS = 1200

# driver가 기동 시 자기 sparkSubmitParameters를 stderr에 그대로 찍는데, standard_score_pipeline이
# POSTGRES_PASSWORD를 driverEnv로 넘기므로 로그에 평문으로 남는다. 값 기반으로 지우기 위해
# 알림 시점에 조회할 Variable 이름.
_SECRET_VARIABLE_NAMES = ("POSTGRES_PASSWORD",)


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
    # EMR task면 driver 로그를 읽어 원인을 분류한다. 아니면(또는 조회 실패면) None이라
    # 아래 메시지에서 진단 줄만 빠진다.
    diagnosis = _diagnose_emr_failure(context)

    # S3에 남기는 원본은 가공 없이 이 dict 그대로 JSON 직렬화한다(#409) — 가독성은
    # 아래 Slack 메시지 쪽에서 챙긴다. 값을 지어내지 않는다: 아직 아무 상위 task도
    # 성공하지 않았으면 counts는 그대로 None이다.
    # data_interval 없이 트리거된 bare manual run은 context에 logical_date 키
    # 자체가 없다(#409 EC2 검증 중 KeyError로 실제 발견) — 없으면 None으로 남긴다.
    logical_date = context.get("logical_date")
    record = {
        "dag_id": dag_id,
        "task_id": task_id,
        "logical_date": str(logical_date) if logical_date is not None else None,
        "owner": owner.name,
        "severity": severity,
        "exception": str(exception) if exception else None,
        "log_url": task_instance.log_url,
        "counts": counts,
        # UNKNOWN_ERROR 비율을 나중에 집계해 룰을 추가할지 판단하기 위해 남긴다.
        "error_type": diagnosis["error_type"] if diagnosis else None,
        "error_evidence": diagnosis["evidence"] if diagnosis else None,
    }
    # S3 기록도 부가 정보다 — 버킷 오설정/권한 문제로 쓰기가 실패해도 Slack 알림은
    # 나가야 한다(#409 EC2 검증에서 정확히 이것 때문에 알림이 침묵했다). 실패하면
    # 링크 줄만 빠진다.
    try:
        record_url = _write_failure_record(record, context["run_id"])
    except Exception:  # noqa: BLE001
        record_url = None

    lines = [
        f"{SEVERITY_LABELS[severity]} *{dag_id}* task 실패: `{task_id}`",
        f"담당자: {mention}",
        f"처리 일자(logical_date): {record['logical_date'] or '알 수 없음(수동 트리거)'}",
        f"예외: {exception}" if exception else "예외 정보 없음(수동 확인 필요)",
        f"처리 건수: {counts}"
        if counts is not None
        else "처리 건수: 이 실행에서 아직 집계되지 않음",
        f"<{task_instance.log_url}|Task Instance 열기>",
    ]
    if diagnosis is not None:
        # 링크 줄 앞에 끼워 넣는다 — 원인이 링크보다 먼저 보여야 한다.
        lines[-1:-1] = _diagnosis_lines(diagnosis)
    if record_url:
        lines.append(f"<{record_url}|실패 상세 기록 열기(S3)>")
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


def _emr_log_link_data(context: dict) -> dict | None:
    # EmrServerlessStartJobOperator는 Job Run이 실패해도(대기 중 raise되기 전에)
    # 이 XCom을 남긴다 — #409 조사 기록 참고. EMR task가 아니면 그냥 None.
    # 콘솔 링크와 driver 로그 조회가 같은 값(log_uri/application_id/job_run_id)을 쓰므로
    # 조회를 한 곳에 둔다.
    from airflow.providers.amazon.aws.links.emr import EmrServerlessS3LogsLink

    task_instance = context["task_instance"]
    try:
        return (
            task_instance.xcom_pull(
                task_ids=task_instance.task_id, key=EmrServerlessS3LogsLink.key
            )
            or None
        )
    except Exception:  # noqa: BLE001
        return None


def _emr_s3_logs_link(context: dict) -> str | None:
    from airflow.providers.amazon.aws.links.emr import EmrServerlessS3LogsLink

    link_data = _emr_log_link_data(context)
    if not link_data:
        return None
    # 이 링크도 부가 정보다 — 포맷이 실패해도 알림을 막지 않는다(#409).
    try:
        return EmrServerlessS3LogsLink().format_link(**link_data)
    except Exception:  # noqa: BLE001
        return None


def _read_emr_driver_log(link_data: dict) -> str | None:
    """driver의 stderr와 stdout을 합쳐 돌려준다. 둘 다 못 읽으면 None."""
    import gzip

    from de4_core import ObjectStore, join_uri

    store = ObjectStore()
    parts = []
    for suffix in _EMR_DRIVER_LOG_SUFFIXES:
        uri = join_uri(
            link_data["log_uri"],
            "applications",
            link_data["application_id"],
            "jobs",
            link_data["job_run_id"],
            suffix,
        )
        raw = _read_with_retry(store, uri)
        if raw is not None:
            parts.append(gzip.decompress(raw).decode("utf-8", errors="replace"))
    return "\n".join(parts) if parts else None


def _read_with_retry(store, uri: str) -> bytes | None:
    import time

    for attempt in range(_LOG_FETCH_ATTEMPTS):
        try:
            return store.read_bytes(uri)
        except Exception:  # noqa: BLE001
            # 아직 flush 전이거나(객체 없음) 권한/네트워크 문제다.
            if attempt == _LOG_FETCH_ATTEMPTS - 1:
                return None
            time.sleep(_LOG_FETCH_RETRY_SECONDS)
    return None


def _secret_values() -> list[str]:
    import logging

    from airflow.sdk import Variable

    values = []
    for name in _SECRET_VARIABLE_NAMES:
        try:
            value = Variable.get(name, default=None)
        except Exception:
            # 값을 못 읽어도 failure_rules.mask_values의 패턴 기반 마스킹이 남는다.
            logging.getLogger(__name__).warning(
                "could not read Variable %s for log masking", name, exc_info=True
            )
            value = None
        if value:
            values.append(value)
    return values


def _diagnose_emr_failure(context: dict) -> dict | None:
    """EMR task의 driver 로그를 읽어 원인을 분류한다. EMR task가 아니거나 실패하면 None.

    진단은 부가 정보다 — 로그 조회/해제/분류 중 무엇이 실패해도 알림 자체는 나가야 한다
    (`_emr_s3_logs_link`/`_write_failure_record`와 같은 원칙).
    """
    try:
        link_data = _emr_log_link_data(context)
        if not link_data:
            return None
        log_text = _read_emr_driver_log(link_data)
        if log_text is None:
            return None

        from jobs.failure_rules import classify, extract_error_window, mask_values

        # 마스킹을 분류보다 먼저 한다 — 근거로 뽑히는 줄이 그대로 Slack과 S3로 나간다.
        window = extract_error_window(mask_values(log_text, _secret_values()))
        classification = classify(window, max_evidence=_MAX_EVIDENCE_LINES)
        rule = classification.rule
        return {
            "error_type": classification.error_type,
            "summary": rule.summary if rule else None,
            "cause": rule.cause if rule else None,
            "actions": list(rule.actions) if rule else [],
            "evidence": list(classification.evidence),
        }
    except Exception:  # noqa: BLE001
        return None


def _diagnosis_lines(diagnosis: dict) -> list[str]:
    lines = [f"오류 유형: `{diagnosis['error_type']}`"]
    if diagnosis["summary"]:
        lines.append(f"원인: {diagnosis['summary']} {diagnosis['cause']}")
    elif diagnosis["evidence"]:
        # 등록된 룰에 없는 실패다. 원인 문구를 지어내지 않고 사람이 볼 줄만 준다.
        lines.append("원인: 등록된 룰에 해당하지 않습니다 — 아래 로그와 원본을 확인해 주세요.")
    else:
        # 로그는 읽었는데 오류 흔적이 한 줄도 없다. 이것 자체가 단서다 — driver가 로그를
        # 남길 틈 없이 강제 종료됐거나(OOM kill) 실패가 executor 쪽에서 났다는 뜻이다.
        lines.append(
            "원인: driver 로그에 오류 흔적이 없습니다 — driver가 로그를 남기지 못하고 강제 "
            "종료됐거나(OOM kill) executor 쪽 실패일 수 있습니다."
        )
        lines.append(
            "확인 사항:\n• EMR 콘솔에서 Job Run의 종료 코드 확인\n"
            "• SPARK_EXECUTOR/*/stderr.gz 확인\n• driver memory/memoryOverhead 확인"
        )

    evidence = _truncate_evidence(diagnosis["evidence"])
    if evidence:
        lines.append("근거 로그:\n```\n" + "\n".join(evidence) + "\n```")
    if diagnosis["actions"]:
        lines.append("확인 사항:\n" + "\n".join(f"• {action}" for action in diagnosis["actions"]))
    return lines


def _truncate_evidence(evidence: list[str]) -> list[str]:
    kept: list[str] = []
    used = 0
    for line in evidence[:_MAX_EVIDENCE_LINES]:
        if used + len(line) > _MAX_EVIDENCE_CHARS:
            kept.append("... (이하 생략 — 원본 로그 참고)")
            break
        kept.append(line)
        used += len(line)
    return kept


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

    # infra/compose/airflow.yaml이 이 env var를 항상 선언해서, 호스트 .env에 값이
    # 없으면 docker compose가 빈 문자열로 채운다 — 그러면 Variable.get의 default가
    # 아니라 그 빈 문자열이 그대로 반환된다(#409 EC2 배포 중 parse_uri("") 에러로
    # 실제 발견). 빈 문자열도 미설정과 동일하게 취급한다.
    return (
        Variable.get("OBSERVABILITY_FAILED_TASKS_S3_URI", default=_DEFAULT_FAILED_TASKS_S3_ROOT)
        or _DEFAULT_FAILED_TASKS_S3_ROOT
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
    # 멘션은 부가 정보다 — 이메일이 Slack에 없거나(레지스트리 오타, 퇴사 등) API가
    # 실패해도 알림 자체를 막아서는 안 된다(#409). 멘션 없이 이름/이메일만 남긴다.
    try:
        response = hook.client.users_lookupByEmail(email=owner.email)
    except Exception:  # noqa: BLE001
        return f"{owner.name}({owner.email} — Slack 멘션 조회 실패)"
    return f"<@{response['user']['id']}>"


def _slack_channel() -> str:
    from airflow.sdk import Variable

    return Variable.get("SLACK_ALERT_CHANNEL")


def _post_message(hook, text: str) -> None:
    hook.client.chat_postMessage(channel=_slack_channel(), text=text)
