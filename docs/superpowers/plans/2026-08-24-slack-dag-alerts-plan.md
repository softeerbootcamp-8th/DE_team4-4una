# DAG 실행 결과 Slack 알림 + 담당자 레지스트리 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 5개 Airflow DAG(`standard_score_pipeline`, `current_score_pipeline`, `zone_weather_pipeline`, `data_quality_audit`, `bronze_compaction`)의 성공/실패를 Slack으로 알리고, 실패 시 담당자 멘션·심각도·처리 건수/일자·Task Instance URL·구조화된 실패 기록(S3) 링크를 포함한다.

**Architecture:** `config/dag_owners.yaml`(담당자·심각도 레지스트리) → `jobs/dag_owners.py`(로더/조회) → `dags/notifications.py`(공용 `on_failure_callback`/`on_success_callback`, Slack Bot Token으로 전송, 실패마다 구조화된 JSON 기록을 관측 버킷에 저장) → 5개 DAG 파일에 콜백 배선. EMR Serverless 기반 task는 `jobs/pipeline_counts.py`(orchestration 전용 count 조회)와 `dags/emr_serverless.py`의 원본 로그 S3 영구 저장으로 보완한다.

**Tech Stack:** Apache Airflow 3.3.1 (LocalExecutor), `apache-airflow-providers-slack`(`SlackHook`, Bot Token), `apache-airflow-providers-amazon`(`EmrServerlessStartJobOperator`), PyYAML, psycopg2, pyarrow, `de4_core.ObjectStore`.

**Spec:** `docs/superpowers/specs/2026-08-24-slack-dag-alerts-design.md`

## Global Constraints

- Python 3.12, `uv` 워크스페이스. 의존성 변경 후 `uv lock`/`uv sync --all-packages` 필수(AGENTS.md).
- 매 태스크 커밋 전 최소 `uv run --all-packages pytest services/orchestration`을 통과시킨다. DAG 관련 태스크를 모두 끝낸 뒤 한 번은 `uv run --all-packages ruff check .`, `uv run --all-packages pytest`(루트, 전체)를 돌린다.
- 비밀값(webhook 토큰, Slack Bot Token 등)은 코드/테스트/`.env.example`에 절대 기록하지 않는다 — 플레이스홀더만.
- `config/dag_owners.yaml`에는 실제 팀원 이메일/Slack ID를 넣지 않는다(이 리포는 공개 스켈레톤이 아니지만, 지금 단계에서는 예시 이름 `alice`/`bob`을 그대로 쓰고 실제 값은 팀이 나중에 채운다).
- 담당 범위는 `services/orchestration`, 루트 `config/`, `infra/compose/airflow.yaml`, `.env.example`이다 — `services/batch-jobs`는 건드리지 않는다(spec §4에서 명시적으로 기각).
- 새 코드의 주석은 WHY만, 비어있지 않을 때만 작성한다(기존 `jobs/weather_rules.py`, `jobs/current_score.py` 스타일을 따른다).

---

## 사전 조사로 확정한 핵심 사실 (구현 중 다시 찾지 않아도 됨)

- **EMR Serverless 로그 설정 위치**: `monitoringConfiguration.s3MonitoringConfiguration.logUri`는 `EmrServerlessStartJobOperator`의 `job_driver`가 아니라 **`configuration_overrides`**에 들어가야 한다(`is_monitoring_in_job_override()`가 `self.configuration_overrides`를 검사함, `.venv/lib/python3.12/site-packages/airflow/providers/amazon/aws/operators/emr.py:1560`).
- **EMR S3 로그 링크는 실패해도 XCom에 남는다**: `EmrServerlessStartJobOperator.execute()`는 `self.persist_links(context)`를 Job Run 완료 대기(`wait(...)`, 실패 시 여기서 raise) **이전에** 호출한다. `persist_links()`가 `configuration_overrides`에 `s3MonitoringConfiguration`이 있으면 `EmrServerlessS3LogsLink.persist(...)`를 호출해 `context["ti"].xcom_push(key="emr_serverless_s3_logs", value={...})`로 남긴다 — 즉 **Job Run이 실패해도 이 XCom은 남아있다**. `EmrServerlessS3LogsLink().format_link(**xcom_value)`를 호출하면 그대로 S3 콘솔 딥링크 문자열이 나온다(같은 파일 1567-1584행, `airflow/providers/amazon/aws/links/emr.py`). 이 덕분에 `on_failure_callback`은 EMR task인지 아닌지 분기할 필요 없이 `xcom_pull(task_ids=task_id, key=EmrServerlessS3LogsLink.key)`를 시도하고 없으면 조용히 건너뛰면 된다.
- **Airflow 3.3.1의 base_url 설정 키는 `[api] base_url`**(`webserver` 섹션이 아니다) — `.venv/lib/python3.12/site-packages/airflow/config_templates/config.yml`의 `api:` 섹션에서 확인. 환경변수는 `AIRFLOW__API__BASE_URL`.
- **Quarantine/feature 파티션 경로 규칙**(batch-jobs가 실제로 쓰는 값, `services/batch-jobs/src/batch_jobs/cleansing/hourly_storage.py:25-29`와 `hourly_segment_feature_storage.py:26-30`):
  - quarantine: `{output_root}/target_date={date}/target_hour={hour:02d}`
  - feature: `{output_root}/data_period_date={date}/hour={hour:02d}`
- **`apache-airflow-providers-slack`은 이 저장소에 아직 설치돼 있지 않다** — Task 1에서 추가한다.
- **구조화된 실패 기록(S3)은 사용자가 이미 버킷을 만들어 뒀다**: `de4-observability-473551908409-ap-northeast-2-a`, prefix `airflow/failed-tasks/`. S3 저장 형식 자체는 가공 없는 JSON이면 충분하다(사용자 확인) — 가독성은 Slack 메시지 쪽에서 챙긴다. `de4_core.ObjectStore.write_bytes(uri, bytes)`/`de4_core.storage.parse_uri(uri) -> (scheme, bucket, key)`가 이미 있어(`libs/de4-core/src/de4_core/storage.py`) 새 S3 클라이언트 코드 없이 바로 쓸 수 있다.

---

## Task 1: `apache-airflow-providers-slack` 의존성 추가

**Files:**
- Modify: `services/orchestration/pyproject.toml`
- Modify: `uv.lock` (루트, `uv add`가 자동 갱신)

**Interfaces:**
- Produces: `airflow.providers.slack.hooks.slack.SlackHook`을 이후 태스크에서 import 가능.

- [ ] **Step 1: 의존성 추가**

```bash
cd /Users/yong/PycharmProjects/DE_team4-4una
uv add --package orchestration apache-airflow-providers-slack
```

- [ ] **Step 2: 설치 확인**

```bash
uv sync --all-packages
uv run --package orchestration python -c "
from airflow.providers.slack.hooks.slack import SlackHook
print('slack_conn_id default:', SlackHook(slack_conn_id='slack_api_default').slack_conn_id)
print('has client property:', hasattr(SlackHook, 'client'))
"
```

Expected: 에러 없이 `slack_conn_id default: slack_api_default`, `has client property: True` 출력.

- [ ] **Step 3: 기존 테스트가 깨지지 않았는지 확인**

Run: `uv run --all-packages pytest services/orchestration -q`
Expected: 기존 테스트 전부 PASS.

- [ ] **Step 4: 커밋**

```bash
git add services/orchestration/pyproject.toml uv.lock
git commit -m "$(cat <<'EOF'
build: add apache-airflow-providers-slack to orchestration (#409)
EOF
)"
```

---

## Task 2: 담당자/심각도 레지스트리 — `config/dag_owners.yaml` + `jobs/dag_owners.py`

**Files:**
- Create: `config/dag_owners.yaml`
- Create: `services/orchestration/jobs/dag_owners.py`
- Test: `services/orchestration/tests/test_dag_owners.py`

**Interfaces:**
- Produces:
  - `OwnerRef(name: str, email: str | None, slack_id: str | None)`
  - `DagOwnersRegistry.resolve_owner(dag_id: str, task_id: str | None = None, task_group_id: str | None = None) -> OwnerRef`
  - `DagOwnersRegistry.resolve_severity(dag_id: str, task_id: str | None = None, task_group_id: str | None = None) -> str`
  - `load_dag_owners_registry(path: Path | None = None) -> DagOwnersRegistry`
  - `SEVERITY_LABELS: dict[str, str]` (키: `critical`/`high`/`medium`/`low`)

- [ ] **Step 1: 실패하는 테스트 작성**

`services/orchestration/tests/test_dag_owners.py`:

```python
"""dag_owners.py의 레지스트리 로드/조회 폴백(#409)을 검증한다."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

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
          sensor_processing.resolve_road_snapshot_date:
            owner: bob
            severity: high
      bronze_compaction:
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
        task_id="sensor_processing.resolve_road_snapshot_date",
    )
    severity = registry.resolve_severity(
        "standard_score_pipeline",
        task_id="sensor_processing.resolve_road_snapshot_date",
    )
    assert owner.name == "bob"
    assert severity == "high"


def test_unregistered_task_falls_back_to_dag_default(tmp_path):
    registry = load_dag_owners_registry(_write_config(tmp_path))

    owner = registry.resolve_owner(
        "standard_score_pipeline", task_id="hourly_scoring.run_hourly_scoring"
    )
    severity = registry.resolve_severity(
        "standard_score_pipeline", task_id="hourly_scoring.run_hourly_scoring"
    )
    assert owner.name == "alice"
    assert severity == "critical"


def test_task_group_id_is_used_when_task_id_has_no_override(tmp_path):
    text = _MINIMAL_YAML.replace(
        "sensor_processing.resolve_road_snapshot_date:",
        "sensor_processing:",
    )
    registry = load_dag_owners_registry(_write_config(tmp_path, text))

    owner = registry.resolve_owner(
        "standard_score_pipeline",
        task_id="sensor_processing.run_sensor_processing",
        task_group_id="sensor_processing",
    )
    assert owner.name == "bob"


def test_dag_without_task_overrides_uses_dag_default(tmp_path):
    registry = load_dag_owners_registry(_write_config(tmp_path))

    owner = registry.resolve_owner("bronze_compaction")
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
        "bronze_compaction",
    ):
        owner = registry.resolve_owner(dag_id)
        severity = registry.resolve_severity(dag_id)
        assert owner.name in registry.users
        assert severity in ("critical", "high", "medium", "low")
```

- [ ] **Step 2: 테스트 실행 → 실패 확인**

Run: `uv run --all-packages pytest services/orchestration/tests/test_dag_owners.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'jobs.dag_owners'` (또는 `config/dag_owners.yaml`이 없어서 나는 에러).

- [ ] **Step 3: `config/dag_owners.yaml` 작성**

```yaml
# DAG/task 담당자 및 심각도 레지스트리 (#409). services/orchestration/jobs/dag_owners.py가
# 읽는다. 실제 팀원 정보로 값을 채워 넣는다 — 아래는 스키마를 보여주는 예시다.

users:
  alice:
    email: alice@example.com   # slack_id가 없으면 알림 시점에 users.lookupByEmail로 조회
  bob:
    slack_id: U0456GHIJKL      # 이미 알고 있으면 email 조회를 건너뛰고 바로 멘션

dags:
  standard_score_pipeline:
    owner: alice
    severity: critical
    tasks:
      sensor_processing.resolve_road_snapshot_date:
        owner: bob
        severity: high
  current_score_pipeline:
    owner: alice
    severity: high
  zone_weather_pipeline:
    owner: bob
    severity: medium
  data_quality_audit:
    owner: alice
    severity: medium
  bronze_compaction:
    owner: bob
    severity: low
```

- [ ] **Step 4: `jobs/dag_owners.py` 작성**

```python
"""DAG/task 담당자 및 심각도 레지스트리 로더 (#409).

config/dag_owners.yaml을 읽어 dag_id(+ 선택적 task_id/task_group_id) ->
owner/severity를 조회한다. weather_rules.py와 같은 dataclass + load_...
패턴을 따른다.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

VALID_SEVERITIES: tuple[str, ...] = ("critical", "high", "medium", "low")

SEVERITY_LABELS: dict[str, str] = {
    "critical": "🔴 critical",
    "high": "🟠 high",
    "medium": "🟡 medium",
    "low": "⚪ low",
}

# jobs/dag_owners.py -> jobs -> orchestration -> services -> 저장소 루트.
# 컨테이너 안에서는 config/의 마운트 위치가 다르므로(infra/compose/airflow.yaml),
# DAG_OWNERS_CONFIG_PATH 환경변수가 있으면 그쪽을 우선한다.
_DEFAULT_LOCAL_DAG_OWNERS_CONFIG_PATH = (
    Path(__file__).resolve().parents[3] / "config" / "dag_owners.yaml"
)


@dataclass(frozen=True, slots=True)
class OwnerRef:
    name: str
    email: str | None
    slack_id: str | None


@dataclass(frozen=True, slots=True)
class _DagEntry:
    owner: str
    severity: str
    tasks: dict[str, tuple[str, str]]  # task_id/task_group_id -> (owner_name, severity)


@dataclass(frozen=True, slots=True)
class DagOwnersRegistry:
    users: dict[str, OwnerRef]
    dags: dict[str, _DagEntry]

    def resolve_owner(
        self, dag_id: str, task_id: str | None = None, task_group_id: str | None = None
    ) -> OwnerRef:
        return self.users[self._resolve_owner_name(dag_id, task_id, task_group_id)]

    def resolve_severity(
        self, dag_id: str, task_id: str | None = None, task_group_id: str | None = None
    ) -> str:
        dag_entry = self._dag_entry(dag_id)
        for key in (task_id, task_group_id):
            if key is not None and key in dag_entry.tasks:
                return dag_entry.tasks[key][1]
        return dag_entry.severity

    def _resolve_owner_name(
        self, dag_id: str, task_id: str | None, task_group_id: str | None
    ) -> str:
        dag_entry = self._dag_entry(dag_id)
        for key in (task_id, task_group_id):
            if key is not None and key in dag_entry.tasks:
                return dag_entry.tasks[key][0]
        return dag_entry.owner

    def _dag_entry(self, dag_id: str) -> _DagEntry:
        if dag_id not in self.dags:
            raise KeyError(f"dag_owners.yaml에 '{dag_id}' 항목이 없습니다")
        return self.dags[dag_id]


def load_dag_owners_registry(path: Path | None = None) -> DagOwnersRegistry:
    config_path = path if path is not None else _default_dag_owners_config_path()
    document = yaml.safe_load(config_path.read_text())
    if not isinstance(document, dict):
        raise TypeError(f"{config_path}: top-level YAML document must be a mapping")

    users = {
        name: _parse_owner_ref(name, raw, config_path)
        for name, raw in document.get("users", {}).items()
    }
    dags = {
        dag_id: _parse_dag_entry(dag_id, raw, users, config_path)
        for dag_id, raw in document.get("dags", {}).items()
    }
    return DagOwnersRegistry(users=users, dags=dags)


def _default_dag_owners_config_path() -> Path:
    override = os.environ.get("DAG_OWNERS_CONFIG_PATH")
    return Path(override) if override else _DEFAULT_LOCAL_DAG_OWNERS_CONFIG_PATH


def _parse_owner_ref(name: str, raw: dict, config_path: Path) -> OwnerRef:
    email = raw.get("email")
    slack_id = raw.get("slack_id")
    if not email and not slack_id:
        raise ValueError(f"{config_path}: users.{name} must have 'email' or 'slack_id'")
    return OwnerRef(name=name, email=email, slack_id=slack_id)


def _parse_dag_entry(
    dag_id: str, raw: dict, users: dict[str, OwnerRef], config_path: Path
) -> _DagEntry:
    owner = raw.get("owner")
    severity = raw.get("severity")
    _require_known_owner(dag_id, owner, users, config_path)
    _require_valid_severity(dag_id, severity, config_path)

    tasks: dict[str, tuple[str, str]] = {}
    for task_key, task_raw in raw.get("tasks", {}).items():
        location = f"{dag_id}.tasks.{task_key}"
        task_owner = task_raw.get("owner")
        task_severity = task_raw.get("severity")
        _require_known_owner(location, task_owner, users, config_path)
        _require_valid_severity(location, task_severity, config_path)
        tasks[task_key] = (task_owner, task_severity)

    return _DagEntry(owner=owner, severity=severity, tasks=tasks)


def _require_known_owner(
    location: str, owner: str | None, users: dict[str, OwnerRef], config_path: Path
) -> None:
    if not owner or owner not in users:
        raise ValueError(f"{config_path}: {location}.owner must reference a name in 'users'")


def _require_valid_severity(location: str, severity: str | None, config_path: Path) -> None:
    if severity not in VALID_SEVERITIES:
        raise ValueError(
            f"{config_path}: {location}.severity must be one of {VALID_SEVERITIES}, "
            f"got {severity!r}"
        )
```

- [ ] **Step 5: 테스트 실행 → 통과 확인**

Run: `uv run --all-packages pytest services/orchestration/tests/test_dag_owners.py -v`
Expected: 전부 PASS.

- [ ] **Step 6: lint**

Run: `uv run --all-packages ruff check services/orchestration/jobs/dag_owners.py services/orchestration/tests/test_dag_owners.py`
Expected: No issues.

- [ ] **Step 7: 커밋**

```bash
git add config/dag_owners.yaml services/orchestration/jobs/dag_owners.py services/orchestration/tests/test_dag_owners.py
git commit -m "$(cat <<'EOF'
feat: add DAG owner/severity registry loader (#409)
EOF
)"
```

---

## Task 3: 처리 건수 조회 — `jobs/pipeline_counts.py`

**Files:**
- Create: `services/orchestration/jobs/pipeline_counts.py`
- Test: `services/orchestration/tests/test_pipeline_counts.py`

**Interfaces:**
- Consumes: `de4_core.ObjectStore`, `de4_core.join_uri` (기존).
- Produces:
  - `PostgresConfig.from_env() -> PostgresConfig`, `PostgresConfig.as_connect_kwargs() -> dict[str, str]`
  - `StandardScorePipelineCounts(quarantine_count, feature_count, hourly_comfort_score_count, standard_segment_comfort_score_count)`
  - `count_standard_score_pipeline_outputs(*, target_hour, as_of, quarantine_output_path, feature_output_path, hourly_comfort_output_path, connection, store=None) -> StandardScorePipelineCounts`
  - `count_audit_gold_tables(*, connection) -> dict[str, int]`

- [ ] **Step 1: 실패하는 테스트 작성**

`services/orchestration/tests/test_pipeline_counts.py`:

```python
"""jobs/pipeline_counts.py의 orchestration 전용 count 조회 로직(#409)을 검증한다.

실제 S3/Postgres 없이 ObjectStore와 psycopg2 connection을 fake로 주입한다 —
실제 값 확인은 로컬 Airflow 수동 검증(README)에서 다룬다.
"""

from __future__ import annotations

import io
from dataclasses import dataclass
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq

from jobs.pipeline_counts import (
    PostgresConfig,
    count_audit_gold_tables,
    count_standard_score_pipeline_outputs,
)


@dataclass(frozen=True, slots=True)
class _FakeObject:
    uri: str


class _FakeObjectStore:
    def __init__(self, row_counts_by_uri: dict[str, int]):
        self._row_counts_by_uri = row_counts_by_uri

    def list_objects(self, uri: str):
        prefix = uri.rstrip("/") + "/"
        return [
            _FakeObject(uri=file_uri)
            for file_uri in self._row_counts_by_uri
            if file_uri.startswith(prefix)
        ]

    def read_bytes(self, uri: str) -> bytes:
        table = pa.table({"x": list(range(self._row_counts_by_uri[uri]))})
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        return buffer.getvalue()


class _FakeCursor:
    def __init__(self, result):
        self.result = result
        self.last_sql: str | None = None
        self.last_params = None

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, sql, params=None):
        self.last_sql = sql
        self.last_params = params

    def fetchone(self):
        return (self.result,)


class _FakeConnection:
    def __init__(self, result):
        self.cursor_obj = _FakeCursor(result)

    def cursor(self):
        return self.cursor_obj


def test_postgres_config_reads_from_env(monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", "db")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "de4")
    monkeypatch.setenv("POSTGRES_USER", "u")
    monkeypatch.setenv("POSTGRES_PASSWORD", "p")

    config = PostgresConfig.from_env()

    assert config.as_connect_kwargs() == {
        "host": "db",
        "port": "5432",
        "dbname": "de4",
        "user": "u",
        "password": "p",
    }


def test_counts_quarantine_feature_and_hourly_comfort_partitions():
    store = _FakeObjectStore(
        {
            "file:///lake/quarantine/target_date=2026-08-18/target_hour=09/part-0.parquet": 3,
            "file:///lake/features/data_period_date=2026-08-18/hour=09/part-0.parquet": 80,
            "file:///lake/hourly_comfort_score/part-0.parquet": 80,
        }
    )
    connection = _FakeConnection(result=100)

    counts = count_standard_score_pipeline_outputs(
        target_hour=datetime(2026, 8, 18, 9, tzinfo=UTC),
        as_of=datetime(2026, 8, 18, 10, tzinfo=UTC),
        quarantine_output_path="file:///lake/quarantine",
        feature_output_path="file:///lake/features",
        hourly_comfort_output_path="file:///lake/hourly_comfort_score",
        connection=connection,
        store=store,
    )

    assert counts.quarantine_count == 3
    assert counts.feature_count == 80
    assert counts.hourly_comfort_score_count == 80
    assert counts.standard_segment_comfort_score_count == 100


def test_empty_partition_counts_as_zero():
    connection = _FakeConnection(result=0)

    counts = count_standard_score_pipeline_outputs(
        target_hour=datetime(2026, 8, 18, 9, tzinfo=UTC),
        as_of=datetime(2026, 8, 18, 10, tzinfo=UTC),
        quarantine_output_path="file:///lake/quarantine",
        feature_output_path="file:///lake/features",
        hourly_comfort_output_path="file:///lake/hourly_comfort_score",
        connection=connection,
        store=_FakeObjectStore({}),
    )

    assert counts.quarantine_count == 0
    assert counts.feature_count == 0
    assert counts.hourly_comfort_score_count == 0


def test_standard_score_query_filters_by_as_of():
    connection = _FakeConnection(result=42)

    count_standard_score_pipeline_outputs(
        target_hour=datetime(2026, 8, 18, 9, tzinfo=UTC),
        as_of=datetime(2026, 8, 18, 10, tzinfo=UTC),
        quarantine_output_path="file:///lake/quarantine",
        feature_output_path="file:///lake/features",
        hourly_comfort_output_path="file:///lake/hourly_comfort_score",
        connection=connection,
        store=_FakeObjectStore({}),
    )

    assert connection.cursor_obj.last_params == (datetime(2026, 8, 18, 10, tzinfo=UTC),)


def test_count_audit_gold_tables_queries_both_tables():
    connection = _FakeConnection(result=7)

    counts = count_audit_gold_tables(connection=connection)

    assert counts == {
        "standard_segment_comfort_score": 7,
        "current_segment_comfort_score": 7,
    }
```

- [ ] **Step 2: 테스트 실행 → 실패 확인**

Run: `uv run --all-packages pytest services/orchestration/tests/test_pipeline_counts.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'jobs.pipeline_counts'`.

- [ ] **Step 3: `jobs/pipeline_counts.py` 작성**

```python
"""standard_score_pipeline/data_quality_audit 성공 알림에 넣을 처리 건수 조회 (#409).

EMR Serverless로 제출되는 task는 원격 Spark job이라 Airflow XCom으로 건수를
돌려줄 방법이 없다. 이 모듈은 각 stage가 방금 쓴 output을 orchestration
프로세스에서 직접 다시 읽어(S3/로컬 Parquet) 또는 조회해(Postgres) 건수만 센다
— services/batch-jobs는 건드리지 않는다.
"""

from __future__ import annotations

import io
import os
from dataclasses import dataclass
from datetime import datetime

import pyarrow.parquet as pq
from de4_core import ObjectStore, join_uri


@dataclass(frozen=True, slots=True)
class PostgresConfig:
    host: str
    port: str
    dbname: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> PostgresConfig:
        return cls(
            host=os.environ["POSTGRES_HOST"],
            port=os.environ["POSTGRES_PORT"],
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        )

    def as_connect_kwargs(self) -> dict[str, str]:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
        }


@dataclass(frozen=True, slots=True)
class StandardScorePipelineCounts:
    quarantine_count: int
    feature_count: int
    hourly_comfort_score_count: int
    standard_segment_comfort_score_count: int


def count_standard_score_pipeline_outputs(
    *,
    target_hour: datetime,
    as_of: datetime,
    quarantine_output_path: str,
    feature_output_path: str,
    hourly_comfort_output_path: str,
    connection,
    store: ObjectStore | None = None,
) -> StandardScorePipelineCounts:
    active_store = store if store is not None else ObjectStore()

    # batch-jobs가 실제로 쓰는 파티션 경로 규칙(#409 조사 기록 참고) — cleansing/
    # hourly_storage.py의 quarantine_hour_path(), hourly_segment_feature_storage.py의
    # hour_output_path()와 반드시 같은 형식이어야 한다.
    quarantine_partition = join_uri(
        quarantine_output_path,
        f"target_date={target_hour.date().isoformat()}",
        f"target_hour={target_hour.hour:02d}",
    )
    feature_partition = join_uri(
        feature_output_path,
        f"data_period_date={target_hour.date().isoformat()}",
        f"hour={target_hour.hour:02d}",
    )

    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM standard_segment_comfort_score WHERE score_as_of = %s",
            (as_of,),
        )
        standard_segment_comfort_score_count = cursor.fetchone()[0]

    return StandardScorePipelineCounts(
        quarantine_count=_count_parquet_rows(active_store, quarantine_partition),
        feature_count=_count_parquet_rows(active_store, feature_partition),
        hourly_comfort_score_count=_count_parquet_rows(active_store, hourly_comfort_output_path),
        standard_segment_comfort_score_count=standard_segment_comfort_score_count,
    )


def count_audit_gold_tables(*, connection) -> dict[str, int]:
    counts: dict[str, int] = {}
    with connection.cursor() as cursor:
        # 테이블명은 아래 고정된 리터럴 2개뿐이라 f-string으로 넣어도 인젝션 위험이 없다.
        for table in ("standard_segment_comfort_score", "current_segment_comfort_score"):
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = cursor.fetchone()[0]
    return counts


def _count_parquet_rows(store: ObjectStore, uri: str) -> int:
    objects = [obj for obj in store.list_objects(uri) if obj.uri.endswith(".parquet")]
    if not objects:
        return 0
    return sum(pq.read_table(io.BytesIO(store.read_bytes(obj.uri))).num_rows for obj in objects)
```

- [ ] **Step 4: 테스트 실행 → 통과 확인**

Run: `uv run --all-packages pytest services/orchestration/tests/test_pipeline_counts.py -v`
Expected: 전부 PASS.

- [ ] **Step 5: lint + 커밋**

```bash
uv run --all-packages ruff check services/orchestration/jobs/pipeline_counts.py services/orchestration/tests/test_pipeline_counts.py
git add services/orchestration/jobs/pipeline_counts.py services/orchestration/tests/test_pipeline_counts.py
git commit -m "$(cat <<'EOF'
feat: add orchestration-side processing-count queries for Slack summaries (#409)
EOF
)"
```

---

## Task 4: EMR Serverless Job Run 로그 S3 영구 저장

**Files:**
- Modify: `services/orchestration/dags/emr_serverless.py`
- Test: `services/orchestration/tests/test_emr_serverless_helper.py`

**Interfaces:**
- Produces: 모든 `submit_batch_jobs_command(...)` 호출이 만드는 `EmrServerlessStartJobOperator`가 `configuration_overrides["monitoringConfiguration"]["s3MonitoringConfiguration"]["logUri"]`를 갖는다. 이후 `notifications.py`(Task 6)가 이 operator의 실행 결과 XCom(`key="emr_serverless_s3_logs"`)을 읽는다.

- [ ] **Step 1: 실패하는 테스트 작성**

`services/orchestration/tests/test_emr_serverless_helper.py`에 추가(파일 끝):

```python
def test_all_job_runs_persist_logs_to_s3_monitoring_configuration():
    operator = _build_operator(task_id="run_thing", entry_point_arguments=["cmd"])

    log_uri = operator.configuration_overrides["monitoringConfiguration"][
        "s3MonitoringConfiguration"
    ]["logUri"]
    assert log_uri == (
        "{{ var.value.get('EMR_SERVERLESS_LOG_S3_URI', 's3://de4-emr-serverless-logs/') }}"
    )
```

- [ ] **Step 2: 테스트 실행 → 실패 확인**

Run: `uv run --all-packages pytest services/orchestration/tests/test_emr_serverless_helper.py -v`
Expected: FAIL with `AttributeError` 또는 `KeyError`(`configuration_overrides`가 아직 `None`).

- [ ] **Step 3: `dags/emr_serverless.py` 수정**

`_ENTRY_POINT_TEMPLATE` 정의 바로 아래에 상수 추가:

```python
# EMR Serverless Job Run의 driver/executor 로그를 영구 저장할 S3 위치(#409). 지금까지
# monitoringConfiguration이 없어 로그가 EMR 콘솔에 잠깐 노출됐다 사라졌다 — 실패
# 알림(dags/notifications.py)이 참조할 EmrServerlessS3LogsLink XCom도 이 설정이
# 있어야 채워진다.
_EMR_SERVERLESS_LOG_S3_URI_TEMPLATE = (
    "{{ var.value.get('EMR_SERVERLESS_LOG_S3_URI', 's3://de4-emr-serverless-logs/') }}"
)
```

`submit_batch_jobs_command`의 반환문을 수정:

```python
    return EmrServerlessStartJobOperator(
        task_id=task_id,
        application_id=_APPLICATION_ID_TEMPLATE,
        execution_role_arn=_EXECUTION_ROLE_ARN_TEMPLATE,
        job_driver={"sparkSubmit": spark_submit},
        # monitoringConfiguration은 job_driver가 아니라 configuration_overrides에
        # 둬야 EMR Serverless가 인식한다(#409 조사 기록 — provider의
        # is_monitoring_in_job_override()가 configuration_overrides를 검사).
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": _EMR_SERVERLESS_LOG_S3_URI_TEMPLATE}
            }
        },
        name=task_id,
        outlets=outlets or [],
    )
```

- [ ] **Step 4: 테스트 실행 → 통과 확인**

Run: `uv run --all-packages pytest services/orchestration/tests/test_emr_serverless_helper.py -v`
Expected: 전부 PASS(기존 테스트 포함).

- [ ] **Step 5: standard_score_pipeline/data_quality_audit DAG 테스트도 여전히 통과하는지 확인**

Run: `uv run --all-packages pytest services/orchestration/tests/test_standard_score_pipeline_dag.py services/orchestration/tests/test_data_quality_audit_dag.py -v`
Expected: 전부 PASS(이 두 DAG는 `submit_batch_jobs_command`를 그대로 재사용하므로 코드 변경 없이 자동 반영됨).

- [ ] **Step 6: lint + 커밋**

```bash
uv run --all-packages ruff check services/orchestration/dags/emr_serverless.py services/orchestration/tests/test_emr_serverless_helper.py
git add services/orchestration/dags/emr_serverless.py services/orchestration/tests/test_emr_serverless_helper.py
git commit -m "$(cat <<'EOF'
feat: persist EMR Serverless Job Run logs to S3 (#409)
EOF
)"
```

---

## Task 5: 공용 Slack 콜백 모듈 — `dags/notifications.py`

**Files:**
- Create: `services/orchestration/dags/notifications.py`
- Test: `services/orchestration/tests/test_notifications.py`

**Interfaces:**
- Consumes: `jobs.dag_owners.{SEVERITY_LABELS, load_dag_owners_registry}` (Task 2), `airflow.providers.amazon.aws.links.emr.EmrServerlessS3LogsLink` (Task 4가 채우는 XCom), `airflow.providers.slack.hooks.slack.SlackHook` (Task 1), `de4_core.{ObjectStore, join_uri}`(이미 존재하는 API, 신규 의존성 없음), `airflow.sdk.Variable`.
- Produces: `on_failure_callback(context: dict) -> None`, `on_success_callback(context: dict) -> None`. 5개 DAG 파일(Task 6-10)이 `from notifications import on_failure_callback, on_success_callback`으로 가져다 쓴다. `_SUMMARY_TASK_IDS`(dag_id -> XCom summary task_id) 매핑도 이 모듈이 소유하며, `on_failure_callback`도 실패 시점까지 이미 성공한 상위 task가 있으면 이 매핑으로 처리 건수를 함께 조회한다(spec §7). 실패마다 구조화된 JSON 레코드를 S3(`de4-observability-473551908409-ap-northeast-2-a/airflow/failed-tasks/`)에 쓰고, Slack 메시지 본문에 처리 건수·처리 일자·그 레코드로 가는 링크를 포함한다.

- [ ] **Step 1: 실패하는 테스트 작성**

`services/orchestration/tests/test_notifications.py`:

```python
"""dags/notifications.py의 Slack 콜백(#409)을 가짜 Airflow context로 검증한다.

실제 Airflow 실행/Slack 전송/S3 쓰기 없이, on_failure_callback/on_success_callback이
받는 context의 필요한 속성만 흉내 낸 가짜 객체를 주입한다. 실제 Airflow
context 모양과 일치하는지는 로컬 Airflow 수동 검증(README)에서 확인한다.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from unittest.mock import MagicMock

import pytest

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(DAGS_DIR))

import notifications  # noqa: E402


@dataclass
class _FakeTaskGroup:
    group_id: str | None


@dataclass
class _FakeTask:
    task_group: _FakeTaskGroup | None = None


@dataclass
class _FakeTaskInstance:
    task_id: str
    log_url: str = "http://localhost:8080/dags/x/grid?task_id=y"
    xcom_store: dict = field(default_factory=dict)

    def xcom_pull(self, task_ids=None, key="return_value"):
        return self.xcom_store.get((task_ids, key))


@dataclass
class _FakeDag:
    dag_id: str


@dataclass
class _FakeDagRun:
    task_instances: dict
    absolute_url: str = "http://localhost:8080/dags/x/grid"

    def get_absolute_url(self):
        return self.absolute_url

    def get_task_instance(self, task_id):
        return self.task_instances.get(task_id)


def _context(
    dag_id: str,
    task_instance: _FakeTaskInstance,
    *,
    task_group_id: str | None = None,
    exception=None,
    dag_run: _FakeDagRun | None = None,
) -> dict:
    return {
        "dag": _FakeDag(dag_id),
        "task": _FakeTask(
            task_group=_FakeTaskGroup(task_group_id) if task_group_id is not None else None
        ),
        "task_instance": task_instance,
        "dag_run": dag_run if dag_run is not None else _FakeDagRun(task_instances={}),
        "run_id": "manual__2026-08-24T04:17:00+00:00",
        "logical_date": "2026-08-24T04:17:00+00:00",
        "exception": exception,
    }


@pytest.fixture(autouse=True)
def _registry(monkeypatch, tmp_path):
    config_path = tmp_path / "dag_owners.yaml"
    config_path.write_text(
        """
        users:
          alice:
            email: alice@example.com
          bob:
            slack_id: U0456GHIJKL

        dags:
          bronze_compaction:
            owner: bob
            severity: low
          standard_score_pipeline:
            owner: alice
            severity: critical
            tasks:
              sensor_processing.run_sensor_processing:
                owner: bob
                severity: high
          current_score_pipeline:
            owner: alice
            severity: high
        """
    )
    monkeypatch.setenv("DAG_OWNERS_CONFIG_PATH", str(config_path))


@pytest.fixture
def _fake_slack_hook(monkeypatch):
    hook = MagicMock()
    hook.client.users_lookupByEmail.return_value = {"user": {"id": "U0999LOOKEDUP"}}
    monkeypatch.setattr(notifications, "_build_slack_hook", lambda: hook)
    monkeypatch.setenv("AIRFLOW_VAR_SLACK_ALERT_CHANNEL", "#alerts")

    class _FakeVariable:
        @staticmethod
        def get(key, default=None):
            import os

            return os.environ.get(f"AIRFLOW_VAR_{key}", default)

    monkeypatch.setattr(notifications, "_slack_channel", lambda: _FakeVariable.get("SLACK_ALERT_CHANNEL"))
    return hook


@pytest.fixture
def _fake_object_store(monkeypatch):
    # on_failure_callback이 `from de4_core import ObjectStore`를 함수 안에서 지연
    # import하므로, 실제 de4_core.ObjectStore 속성 자체를 patch해야 잡힌다.
    written: dict[str, bytes] = {}

    class _FakeObjectStore:
        def write_bytes(self, uri, value):
            written[uri] = value

    monkeypatch.setattr("de4_core.ObjectStore", _FakeObjectStore)
    # _failed_tasks_s3_root()는 airflow.sdk.Variable.get()을 호출하는데, 이건 실제
    # Airflow DB/API 연결이 필요해 단위 테스트에서 그대로 두면 실패하거나 멈춘다.
    # _slack_channel과 같은 이유로 여기서도 우회한다.
    monkeypatch.setattr(
        notifications,
        "_failed_tasks_s3_root",
        lambda: notifications._DEFAULT_FAILED_TASKS_S3_ROOT,
    )
    return written


def test_failure_callback_mentions_slack_id_owner_directly(_fake_slack_hook, _fake_object_store):
    context = _context(
        "bronze_compaction",
        _FakeTaskInstance(task_id="compact_zone_weather_snapshot"),
        exception=ValueError("boom"),
    )

    notifications.on_failure_callback(context)

    _fake_slack_hook.client.users_lookupByEmail.assert_not_called()
    text = _fake_slack_hook.client.chat_postMessage.call_args.kwargs["text"]
    assert "<@U0456GHIJKL>" in text
    assert "⚪ low" in text
    assert "compact_zone_weather_snapshot" in text
    assert "boom" in text


def test_failure_callback_resolves_email_owner_via_slack_lookup(_fake_slack_hook, _fake_object_store):
    context = _context(
        "standard_score_pipeline", _FakeTaskInstance(task_id="report_processing_counts")
    )

    notifications.on_failure_callback(context)

    _fake_slack_hook.client.users_lookupByEmail.assert_called_once_with(email="alice@example.com")
    text = _fake_slack_hook.client.chat_postMessage.call_args.kwargs["text"]
    assert "<@U0999LOOKEDUP>" in text
    assert "🔴 critical" in text


def test_failure_callback_uses_task_level_override_over_dag_default(_fake_slack_hook, _fake_object_store):
    context = _context(
        "standard_score_pipeline",
        _FakeTaskInstance(task_id="sensor_processing.run_sensor_processing"),
        task_group_id="sensor_processing",
    )

    notifications.on_failure_callback(context)

    text = _fake_slack_hook.client.chat_postMessage.call_args.kwargs["text"]
    assert "<@U0456GHIJKL>" in text  # task 오버라이드(bob)
    assert "🟠 high" in text


def test_failure_callback_includes_emr_s3_logs_link_when_xcom_present(_fake_slack_hook, _fake_object_store):
    from airflow.providers.amazon.aws.links.emr import EmrServerlessS3LogsLink

    task_instance = _FakeTaskInstance(task_id="compact_zone_weather_snapshot")
    task_instance.xcom_store[
        ("compact_zone_weather_snapshot", EmrServerlessS3LogsLink.key)
    ] = {
        "region_name": "ap-northeast-2",
        "aws_domain": "aws.amazon.com",
        "log_uri": "s3://de4-emr-serverless-logs/",
        "application_id": "app-1",
        "job_run_id": "job-1",
    }
    context = _context("bronze_compaction", task_instance)

    notifications.on_failure_callback(context)

    text = _fake_slack_hook.client.chat_postMessage.call_args.kwargs["text"]
    assert "app-1" in text
    assert "job-1" in text


def test_failure_callback_omits_emr_link_when_not_an_emr_task(_fake_slack_hook, _fake_object_store):
    context = _context("bronze_compaction", _FakeTaskInstance(task_id="compact_zone_weather_snapshot"))

    notifications.on_failure_callback(context)

    text = _fake_slack_hook.client.chat_postMessage.call_args.kwargs["text"]
    assert "EMR Serverless 원본 로그" not in text


def test_failure_callback_writes_structured_json_record_to_s3(_fake_slack_hook, _fake_object_store):
    import json

    context = _context(
        "bronze_compaction",
        _FakeTaskInstance(task_id="compact_zone_weather_snapshot"),
        exception=ValueError("boom"),
    )

    notifications.on_failure_callback(context)

    assert len(_fake_object_store) == 1
    written_uri, written_bytes = next(iter(_fake_object_store.items()))
    assert written_uri.startswith(
        "s3://de4-observability-473551908409-ap-northeast-2-a/airflow/failed-tasks/"
        "bronze_compaction/compact_zone_weather_snapshot/"
    )
    record = json.loads(written_bytes)
    assert record["dag_id"] == "bronze_compaction"
    assert record["task_id"] == "compact_zone_weather_snapshot"
    assert record["exception"] == "boom"
    assert record["owner"] == "bob"
    assert record["severity"] == "low"
    assert record["counts"] is None  # 이 DAG는 단일 task라 이번 실행에서 아무 요약도 안 남았다.

    text = _fake_slack_hook.client.chat_postMessage.call_args.kwargs["text"]
    assert "실패 상세 기록 열기" in text
    assert (
        "console.aws.amazon.com/s3/object/de4-observability-473551908409-ap-northeast-2-a"
        in text
    )


def test_failure_callback_includes_counts_when_upstream_summary_already_succeeded(
    _fake_slack_hook, _fake_object_store
):
    summary_ti = _FakeTaskInstance(task_id="report_processing_counts")
    summary_ti.xcom_store[("report_processing_counts", "return_value")] = {
        "standard_segment_comfort_score_count": 80
    }
    dag_run = _FakeDagRun(task_instances={"report_processing_counts": summary_ti})
    context = _context(
        "standard_score_pipeline",
        _FakeTaskInstance(task_id="some_later_task"),
        dag_run=dag_run,
    )

    notifications.on_failure_callback(context)

    text = _fake_slack_hook.client.chat_postMessage.call_args.kwargs["text"]
    assert "standard_segment_comfort_score_count" in text
    assert "80" in text


def test_success_callback_posts_summary_from_mapped_task(_fake_slack_hook):
    summary_ti = _FakeTaskInstance(task_id="run_current_score")
    summary_ti.xcom_store[("run_current_score", "return_value")] = {"upserted_count": 42}
    dag_run = _FakeDagRun(task_instances={"run_current_score": summary_ti})
    context = {
        "dag": _FakeDag("current_score_pipeline"),
        "dag_run": dag_run,
    }

    notifications.on_success_callback(context)

    text = _fake_slack_hook.client.chat_postMessage.call_args.kwargs["text"]
    assert "upserted_count" in text
    assert "42" in text
    assert "alice" in text


def test_success_callback_without_summary_task_still_posts(_fake_slack_hook):
    dag_run = _FakeDagRun(task_instances={})
    context = {
        "dag": _FakeDag("bronze_compaction"),
        "dag_run": dag_run,
    }

    notifications.on_success_callback(context)

    _fake_slack_hook.client.chat_postMessage.assert_called_once()
```

- [ ] **Step 2: 테스트 실행 → 실패 확인**

Run: `uv run --all-packages pytest services/orchestration/tests/test_notifications.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'notifications'`.

- [ ] **Step 3: `dags/notifications.py` 작성**

```python
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
    counts = _pull_summary(context["dag_run"], dag_id)

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
        f"<{dag_run.get_absolute_url()}|DAG Run 열기>",
    ]
    summary = _pull_summary(dag_run, dag_id)
    if summary is not None:
        lines.append(f"처리 건수: {summary}")

    hook = _build_slack_hook()
    _post_message(hook, "\n".join(lines))


def _pull_summary(dag_run, dag_id: str):
    summary_task_id = _SUMMARY_TASK_IDS.get(dag_id)
    if summary_task_id is None:
        return None
    task_instance = dag_run.get_task_instance(summary_task_id)
    if task_instance is None:
        return None
    return task_instance.xcom_pull(task_ids=summary_task_id)


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
```

- [ ] **Step 4: 테스트 실행 → 통과 확인**

Run: `uv run --all-packages pytest services/orchestration/tests/test_notifications.py -v`
Expected: 전부 PASS. 실패하면 다음을 확인한다 — `_FakeTaskInstance.xcom_pull`의 키가 `(task_ids, key)` 튜플인지 코드와 정확히 맞는지, `_pull_summary`가 `dag_run.get_task_instance(summary_task_id)`를 위치 인자로 호출하는지(테스트의 `_FakeDagRun.get_task_instance`도 위치 인자), `_fake_object_store` fixture가 `monkeypatch.setattr("de4_core.ObjectStore", ...)`로 실제 `de4_core` 모듈 속성을 patch하는지(`_write_failure_record`가 함수 안에서 `from de4_core import ObjectStore`를 매번 새로 import하므로, `notifications.ObjectStore`가 아니라 `de4_core.ObjectStore`를 patch해야 한다).

- [ ] **Step 5: lint + 커밋**

```bash
uv run --all-packages ruff check services/orchestration/dags/notifications.py services/orchestration/tests/test_notifications.py
git add services/orchestration/dags/notifications.py services/orchestration/tests/test_notifications.py
git commit -m "$(cat <<'EOF'
feat: add shared Slack failure/success notification callbacks (#409)
EOF
)"
```

---

## Task 6: `current_score_pipeline` DAG에 콜백 배선

**Files:**
- Modify: `services/orchestration/dags/current_score_pipeline.py`
- Modify: `services/orchestration/tests/test_current_score_pipeline_dag.py`

**Interfaces:**
- Consumes: `notifications.{on_failure_callback, on_success_callback}` (Task 5).

- [ ] **Step 1: 실패하는 테스트 추가**

`test_current_score_pipeline_dag.py` 끝에 추가:

```python
def test_dag_wires_shared_slack_notification_callbacks():
    import notifications

    module = _load_dag_module()

    assert module.dag.default_args["on_failure_callback"] is notifications.on_failure_callback
    assert module.dag.on_success_callback is notifications.on_success_callback
```

- [ ] **Step 2: 테스트 실행 → 실패 확인**

Run: `uv run --all-packages pytest services/orchestration/tests/test_current_score_pipeline_dag.py::test_dag_wires_shared_slack_notification_callbacks -v`
Expected: FAIL (`KeyError: 'on_failure_callback'`).

- [ ] **Step 3: `dags/current_score_pipeline.py` 수정**

import 절에 추가(`from comfort_score_assets import STANDARD_SCORE_ASSET` 다음 줄):

```python
from notifications import on_failure_callback, on_success_callback
```

`_run_current_score`가 summary를 반환하도록 수정:

```python
def _run_current_score(triggering_asset_events) -> dict:
    # dag-processor/webserver에는 마운트되지 않아 task 콜백 안에서 지연 import한다.
    from jobs.current_score import run_from_env

    changed_zones_only = _changed_zones_only(triggering_asset_events)
    summary = run_from_env(changed_zones_only=changed_zones_only)
    result = {
        "changed_zones_only": changed_zones_only,
        "zone_count": summary.zone_count,
        "upserted_count": summary.upserted_count,
        "skipped_unzoned_count": summary.skipped_unzoned_count,
        "quarantined_count": summary.quarantined_count,
    }
    print(result)
    return result
```

`DAG(...)` 선언 수정:

```python
with DAG(
    dag_id="current_score_pipeline",
    description="current_segment_comfort_score의 유일한 writer — 트리거 Asset에 따라 전량/변경-zone 모드 결정",
    schedule=AssetAny(STANDARD_SCORE_ASSET, ZONE_WEATHER_ASSET),
    start_date=datetime.datetime(2026, 8, 20, tzinfo=datetime.UTC),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=2),
        "on_failure_callback": on_failure_callback,
    },
    on_success_callback=on_success_callback,
    tags=["current-score-pipeline", "comfort-score"],
) as dag:
```

- [ ] **Step 4: 테스트 실행 → 통과 확인**

Run: `uv run --all-packages pytest services/orchestration/tests/test_current_score_pipeline_dag.py -v`
Expected: 전부 PASS.

- [ ] **Step 5: lint + 커밋**

```bash
uv run --all-packages ruff check services/orchestration/dags/current_score_pipeline.py services/orchestration/tests/test_current_score_pipeline_dag.py
git add services/orchestration/dags/current_score_pipeline.py services/orchestration/tests/test_current_score_pipeline_dag.py
git commit -m "$(cat <<'EOF'
feat: wire Slack notifications into current_score_pipeline (#409)
EOF
)"
```

---

## Task 7: `zone_weather_pipeline` DAG에 콜백 배선

**Files:**
- Modify: `services/orchestration/dags/zone_weather_pipeline.py`
- Modify: `services/orchestration/tests/test_zone_weather_pipeline_dag.py`

- [ ] **Step 1: 실패하는 테스트 추가**

`test_zone_weather_pipeline_dag.py` 끝에 추가:

```python
def test_dag_wires_shared_slack_notification_callbacks():
    import notifications

    module = _load_dag_module()

    assert module.dag.default_args["on_failure_callback"] is notifications.on_failure_callback
    assert module.dag.on_success_callback is notifications.on_success_callback
```

- [ ] **Step 2: 테스트 실행 → 실패 확인**

Run: `uv run --all-packages pytest services/orchestration/tests/test_zone_weather_pipeline_dag.py::test_dag_wires_shared_slack_notification_callbacks -v`
Expected: FAIL.

- [ ] **Step 3: `dags/zone_weather_pipeline.py` 수정**

import 절에 추가(`from assets import ZONE_WEATHER_ASSET` 다음 줄):

```python
from notifications import on_failure_callback, on_success_callback
```

`_collect_latest_zone_weather`가 summary를 반환하도록 수정:

```python
def _collect_latest_zone_weather(data_interval_end) -> dict:
    import psycopg2
    from jobs.weather import LatestZoneWeatherJobConfig, run_latest_zone_weather_job

    config = LatestZoneWeatherJobConfig.from_env()
    connection = psycopg2.connect(
        host=config.postgres_host,
        port=config.postgres_port,
        dbname=config.postgres_db,
        user=config.postgres_user,
        password=config.postgres_password,
    )
    try:
        summary = run_latest_zone_weather_job(config, data_interval_end, connection)
    finally:
        connection.close()
    result = {
        "requested_zone_count": summary.requested_zone_count,
        "collected_count": summary.collected_count,
        "failed_zone_count": summary.failed_zone_count,
        "snapshot_uri": summary.snapshot_uri,
    }
    print(result)
    return result
```

`DAG(...)` 선언 수정:

```python
with DAG(
    dag_id="zone_weather_pipeline",
    description="Open-Meteo 15분 날씨를 latest_zone_weather에 수집하고 변경 zone을 감시",
    schedule=CronDataIntervalTimetable(
        "*/15 * * * *",
        timezone=pendulum.timezone("UTC"),
    ),
    start_date=datetime.datetime(2026, 8, 19, tzinfo=datetime.UTC),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=2),
        "on_failure_callback": on_failure_callback,
    },
    on_success_callback=on_success_callback,
    tags=["zone-weather-pipeline"],
) as dag:
```

- [ ] **Step 4: 테스트 실행 → 통과 확인**

Run: `uv run --all-packages pytest services/orchestration/tests/test_zone_weather_pipeline_dag.py -v`
Expected: 전부 PASS.

- [ ] **Step 5: lint + 커밋**

```bash
uv run --all-packages ruff check services/orchestration/dags/zone_weather_pipeline.py services/orchestration/tests/test_zone_weather_pipeline_dag.py
git add services/orchestration/dags/zone_weather_pipeline.py services/orchestration/tests/test_zone_weather_pipeline_dag.py
git commit -m "$(cat <<'EOF'
feat: wire Slack notifications into zone_weather_pipeline (#409)
EOF
)"
```

---

## Task 8: `bronze_compaction` DAG에 콜백 배선

**Files:**
- Modify: `services/orchestration/dags/bronze_compaction.py`
- Modify: `services/orchestration/tests/test_bronze_compaction_dag.py`

- [ ] **Step 1: 실패하는 테스트 추가**

`test_bronze_compaction_dag.py` 끝에 추가:

```python
def test_dag_wires_shared_slack_notification_callbacks():
    import notifications

    module = _load_dag_module()

    assert module.dag.default_args["on_failure_callback"] is notifications.on_failure_callback
    assert module.dag.on_success_callback is notifications.on_success_callback
```

- [ ] **Step 2: 테스트 실행 → 실패 확인**

Run: `uv run --all-packages pytest services/orchestration/tests/test_bronze_compaction_dag.py::test_dag_wires_shared_slack_notification_callbacks -v`
Expected: FAIL.

- [ ] **Step 3: `dags/bronze_compaction.py` 수정**

`from airflow.sdk import DAG` 다음 줄에 추가:

```python
from notifications import on_failure_callback, on_success_callback
```

`_compact_zone_weather_snapshot`이 summary를 반환하도록 수정:

```python
def _compact_zone_weather_snapshot(data_interval_end) -> dict:
    from jobs.bronze_compaction import (
        BronzeCompactionConfig,
        run_zone_weather_snapshot_compaction,
    )

    config = BronzeCompactionConfig.from_env()
    summary = run_zone_weather_snapshot_compaction(config, data_interval_end)
    result = {
        "root_uri": summary.root_uri,
        "compacted_group_count": len(summary.compacted_groups),
        "skipped_group_count": summary.skipped_group_count,
    }
    print(result)
    return result
```

`DAG(...)` 선언 수정:

```python
with DAG(
    dag_id="bronze_compaction",
    description="Bronze(zone_weather_snapshot) 소파일 정리 — 매일 1회, soft fail",
    schedule="17 4 * * *",
    start_date=datetime.datetime(2026, 8, 20, tzinfo=datetime.UTC),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=5),
        "on_failure_callback": on_failure_callback,
    },
    on_success_callback=on_success_callback,
    tags=["bronze-compaction"],
) as dag:
```

- [ ] **Step 4: 테스트 실행 → 통과 확인**

`test_dag_preserves_retry_policy`가 `module.dag.tasks`를 순회하며 `task.retries`/`task.retry_delay`를 확인하는데, `default_args`에 새 키를 추가해도 값 자체는 그대로라 영향 없음을 확인한다.

Run: `uv run --all-packages pytest services/orchestration/tests/test_bronze_compaction_dag.py -v`
Expected: 전부 PASS.

- [ ] **Step 5: lint + 커밋**

```bash
uv run --all-packages ruff check services/orchestration/dags/bronze_compaction.py services/orchestration/tests/test_bronze_compaction_dag.py
git add services/orchestration/dags/bronze_compaction.py services/orchestration/tests/test_bronze_compaction_dag.py
git commit -m "$(cat <<'EOF'
feat: wire Slack notifications into bronze_compaction (#409)
EOF
)"
```

---

## Task 9: `standard_score_pipeline` DAG — `report_processing_counts` task + 콜백 배선

**Files:**
- Modify: `services/orchestration/dags/standard_score_pipeline.py`
- Modify: `services/orchestration/tests/test_standard_score_pipeline_dag.py`

**Interfaces:**
- Consumes: `jobs.pipeline_counts.{PostgresConfig, count_standard_score_pipeline_outputs}` (Task 3), `notifications.{on_failure_callback, on_success_callback}` (Task 5).
- Produces: task_id `report_processing_counts`(TaskGroup 밖, XCom return value) — `notifications._SUMMARY_TASK_IDS["standard_score_pipeline"] = "report_processing_counts"`가 참조.

- [ ] **Step 1: 실패하는 테스트 추가**

`test_standard_score_pipeline_dag.py`에서 기존 `test_dag_contains_expected_pipeline_tasks_so_far`를 다음으로 교체:

```python
def test_dag_contains_expected_pipeline_tasks_so_far():
    module = _load_dag_module()

    task_ids = {task.task_id for task in module.dag.tasks}
    assert task_ids == {
        "sensor_processing.resolve_road_snapshot_date",
        "sensor_processing.run_sensor_processing",
        "sensor_processing.validate_sensor_processing",
        "hourly_scoring.run_hourly_scoring",
        "hourly_scoring.validate_hourly_scoring",
        "standard_score.run_standard_score",
        "standard_score.validate_standard_score",
        "report_processing_counts",
    }
```

파일 끝에 추가:

```python
def test_report_processing_counts_runs_after_standard_score_group():
    module = _load_dag_module()

    task = module.dag.get_task("report_processing_counts")
    assert isinstance(task, PythonOperator)
    assert task.python_callable is module._report_processing_counts
    assert task.upstream_task_ids == {"standard_score.validate_standard_score"}


def test_report_processing_counts_templates_the_same_paths_as_upstream_tasks():
    module = _load_dag_module()

    task = module.dag.get_task("report_processing_counts")
    assert task.op_kwargs["quarantine_output_path"] == module._CLEANSING_QUARANTINE_OUTPUT_PATH
    assert task.op_kwargs["feature_output_path"] == module._HOURLY_SEGMENT_FEATURE_OUTPUT_PATH
    assert task.op_kwargs["hourly_comfort_output_path"] == module._HOURLY_COMFORT_OUTPUT_PATH
    assert task.op_kwargs["target_hour"] == "{{ data_interval_start.isoformat() }}"
    assert task.op_kwargs["as_of"] == "{{ data_interval_end.isoformat() }}"


def test_dag_wires_shared_slack_notification_callbacks():
    import notifications

    module = _load_dag_module()

    assert module.dag.default_args["on_failure_callback"] is notifications.on_failure_callback
    assert module.dag.on_success_callback is notifications.on_success_callback
```

- [ ] **Step 2: 테스트 실행 → 실패 확인**

Run: `uv run --all-packages pytest services/orchestration/tests/test_standard_score_pipeline_dag.py -v`
Expected: 여러 개 FAIL(task 없음, callback 없음).

- [ ] **Step 3: `dags/standard_score_pipeline.py` 수정**

import 절에 추가(`from emr_serverless import submit_batch_jobs_command` 다음 줄):

```python
from notifications import on_failure_callback, on_success_callback
```

`_resolve_road_snapshot_date` 함수 뒤, `with DAG(...)` 앞에 추가:

```python
def _report_processing_counts(
    target_hour: str,
    as_of: str,
    quarantine_output_path: str,
    feature_output_path: str,
    hourly_comfort_output_path: str,
) -> dict:
    import datetime as dt

    import psycopg2
    from jobs.pipeline_counts import PostgresConfig, count_standard_score_pipeline_outputs

    config = PostgresConfig.from_env()
    connection = psycopg2.connect(**config.as_connect_kwargs())
    try:
        counts = count_standard_score_pipeline_outputs(
            target_hour=dt.datetime.fromisoformat(target_hour),
            as_of=dt.datetime.fromisoformat(as_of),
            quarantine_output_path=quarantine_output_path,
            feature_output_path=feature_output_path,
            hourly_comfort_output_path=hourly_comfort_output_path,
            connection=connection,
        )
    finally:
        connection.close()

    result = {
        "quarantine_count": counts.quarantine_count,
        "feature_count": counts.feature_count,
        "hourly_comfort_score_count": counts.hourly_comfort_score_count,
        "standard_segment_comfort_score_count": counts.standard_segment_comfort_score_count,
    }
    print(result)
    return result
```

`DAG(...)` 선언에 콜백 추가:

```python
with DAG(
    dag_id="standard_score_pipeline",
    description="sensor processing -> scoring -> standard 3단계 시간배치 파이프라인",
    schedule=CronDataIntervalTimetable(
        "0 * * * *",
        timezone=pendulum.timezone("UTC"),
    ),
    start_date=datetime.datetime(2026, 8, 17, tzinfo=datetime.UTC),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=5),
        "on_failure_callback": on_failure_callback,
    },
    on_success_callback=on_success_callback,
    tags=["standard-score-pipeline", "comfort-score"],
) as dag:
```

`standard_score` TaskGroup 정의 뒤, 파일 맨 끝의 `sensor_processing >> hourly_scoring >> standard_score`를 다음으로 교체:

```python
    report_processing_counts = PythonOperator(
        task_id="report_processing_counts",
        python_callable=_report_processing_counts,
        op_kwargs={
            "target_hour": "{{ data_interval_start.isoformat() }}",
            "as_of": "{{ data_interval_end.isoformat() }}",
            "quarantine_output_path": _CLEANSING_QUARANTINE_OUTPUT_PATH,
            "feature_output_path": _HOURLY_SEGMENT_FEATURE_OUTPUT_PATH,
            "hourly_comfort_output_path": _HOURLY_COMFORT_OUTPUT_PATH,
        },
    )

    sensor_processing >> hourly_scoring >> standard_score >> report_processing_counts
```

- [ ] **Step 4: 테스트 실행 → 통과 확인**

Run: `uv run --all-packages pytest services/orchestration/tests/test_standard_score_pipeline_dag.py -v`
Expected: 전부 PASS.

- [ ] **Step 5: lint + 커밋**

```bash
uv run --all-packages ruff check services/orchestration/dags/standard_score_pipeline.py services/orchestration/tests/test_standard_score_pipeline_dag.py
git add services/orchestration/dags/standard_score_pipeline.py services/orchestration/tests/test_standard_score_pipeline_dag.py
git commit -m "$(cat <<'EOF'
feat: add processing-count reporting and Slack notifications to standard_score_pipeline (#409)
EOF
)"
```

---

## Task 10: `data_quality_audit` DAG — `report_audit_counts` task + 콜백 배선

**Files:**
- Modify: `services/orchestration/dags/data_quality_audit.py`
- Modify: `services/orchestration/tests/test_data_quality_audit_dag.py`

**Interfaces:**
- Consumes: `jobs.pipeline_counts.{PostgresConfig, count_audit_gold_tables}` (Task 3), `notifications.{on_failure_callback, on_success_callback}` (Task 5).
- Produces: task_id `report_audit_counts`(XCom return value) — `notifications._SUMMARY_TASK_IDS["data_quality_audit"] = "report_audit_counts"`가 참조.

- [ ] **Step 1: 실패하는 테스트 추가**

`test_data_quality_audit_dag.py`에서 기존 `test_dag_contains_one_task_per_gold_table`을 다음으로 교체:

```python
def test_dag_contains_one_task_per_gold_table_plus_the_count_report():
    module = _load_dag_module()

    task_ids = {task.task_id for task in module.dag.tasks}
    assert task_ids == {
        "audit_standard_segment_comfort_score",
        "audit_current_segment_comfort_score",
        "report_audit_counts",
    }
```

기존 `test_tasks_have_no_upstream_or_downstream_dependencies`를 다음으로 교체:

```python
def test_audit_tasks_are_independent_of_each_other():
    module = _load_dag_module()

    standard_task = module.dag.get_task("audit_standard_segment_comfort_score")
    current_task = module.dag.get_task("audit_current_segment_comfort_score")

    assert standard_task.upstream_task_ids == set()
    assert current_task.upstream_task_ids == set()


def test_report_audit_counts_runs_after_both_audits():
    module = _load_dag_module()

    report_task = module.dag.get_task("report_audit_counts")
    assert isinstance(report_task, PythonOperator)
    assert report_task.python_callable is module._report_audit_counts
    assert report_task.upstream_task_ids == {
        "audit_standard_segment_comfort_score",
        "audit_current_segment_comfort_score",
    }
```

`import` 절에 추가(파일 상단):

```python
from airflow.providers.standard.operators.python import PythonOperator
```

파일 끝에 추가:

```python
def test_dag_wires_shared_slack_notification_callbacks():
    import notifications

    module = _load_dag_module()

    assert module.dag.default_args["on_failure_callback"] is notifications.on_failure_callback
    assert module.dag.on_success_callback is notifications.on_success_callback
```

- [ ] **Step 2: 테스트 실행 → 실패 확인**

Run: `uv run --all-packages pytest services/orchestration/tests/test_data_quality_audit_dag.py -v`
Expected: 여러 개 FAIL.

- [ ] **Step 3: `dags/data_quality_audit.py` 수정**

import 절 수정 — 기존 4줄(`from __future__ import annotations` / `import datetime` /
`from airflow.sdk import DAG` / `from emr_serverless import submit_batch_jobs_command`)을
아래로 교체(ruff의 isort 정렬 순서를 따른다 — `airflow.providers...`가
`airflow.sdk`보다 알파벳순으로 앞이고, `notifications`는 `emr_serverless`
다음이다):

```python
from __future__ import annotations

import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from emr_serverless import submit_batch_jobs_command
from notifications import on_failure_callback, on_success_callback
```

`_audit_gold_driver_env` 함수 뒤, `with DAG(...)` 앞에 추가:

```python
def _report_audit_counts() -> dict:
    import psycopg2
    from jobs.pipeline_counts import PostgresConfig, count_audit_gold_tables

    config = PostgresConfig.from_env()
    connection = psycopg2.connect(**config.as_connect_kwargs())
    try:
        counts = count_audit_gold_tables(connection=connection)
    finally:
        connection.close()
    print(counts)
    return counts
```

`DAG(...)` 선언에 콜백 추가:

```python
with DAG(
    dag_id="data_quality_audit",
    description="Gold(standard/current_segment_comfort_score) at-rest 품질 감시 — 매일 1회, soft fail",
    schedule="0 3 * * *",
    start_date=datetime.datetime(2026, 8, 20, tzinfo=datetime.UTC),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=5),
        "on_failure_callback": on_failure_callback,
    },
    on_success_callback=on_success_callback,
    tags=["data-quality-audit", "comfort-score"],
) as dag:
```

두 audit task 정의 뒤에 추가(파일 맨 끝):

```python
    report_audit_counts = PythonOperator(
        task_id="report_audit_counts",
        python_callable=_report_audit_counts,
    )
    [
        audit_standard_segment_comfort_score,
        audit_current_segment_comfort_score,
    ] >> report_audit_counts
```

- [ ] **Step 4: 테스트 실행 → 통과 확인**

Run: `uv run --all-packages pytest services/orchestration/tests/test_data_quality_audit_dag.py -v`
Expected: 전부 PASS. `test_tasks_have_no_outlets`/`test_dag_preserves_retry_policy`는 모든 task를 순회하므로 새 task도 자동으로 포함돼 통과해야 한다(outlets 기본값 `[]`, retries/retry_delay는 default_args에서 상속).

- [ ] **Step 5: lint + 커밋**

```bash
uv run --all-packages ruff check services/orchestration/dags/data_quality_audit.py services/orchestration/tests/test_data_quality_audit_dag.py
git add services/orchestration/dags/data_quality_audit.py services/orchestration/tests/test_data_quality_audit_dag.py
git commit -m "$(cat <<'EOF'
feat: add audited-row reporting and Slack notifications to data_quality_audit (#409)
EOF
)"
```

---

## Task 11: 인프라 배선 — `infra/compose/airflow.yaml`, `.env.example`

**Files:**
- Modify: `infra/compose/airflow.yaml`
- Modify: `.env.example`

**Interfaces:**
- Produces: `airflow-scheduler`/`airflow-dag-processor` 컨테이너에 `config/`가 `${AIRFLOW_HOME}/config`로 마운트되고, `DAG_OWNERS_CONFIG_PATH`가 그 경로를 가리키며, `pyyaml`/`apache-airflow-providers-slack`이 설치된다.

- [ ] **Step 1: Task 1에서 확정된 버전 문자열을 셸 변수로 확보**

```bash
SLACK_PROVIDER_CONSTRAINT=$(
  grep -oE '"apache-airflow-providers-slack[^"]*"' services/orchestration/pyproject.toml \
    | tr -d '"'
)
echo "$SLACK_PROVIDER_CONSTRAINT"
```

Expected: 빈 문자열이 아닌, `apache-airflow-providers-slack`으로 시작하는 한 줄(예:
`apache-airflow-providers-slack>=9.1.0,<10.0.0`)이 출력된다. 이 셸 세션을
Step 3-4에서 그대로 이어서 쓴다(새 셸이면 이 Step부터 다시 실행).

- [ ] **Step 2: `infra/compose/airflow.yaml`의 `x-airflow-env`에 추가**

`AIRFLOW_VAR_GOLD_AUDIT_S3_BUCKET: ${AIRFLOW_VAR_GOLD_AUDIT_S3_BUCKET}` 다음 줄에 추가:

```yaml
    # DAG 실행 결과 Slack 알림(#409). base_url은 [api] 섹션 설정이다(Airflow 3.3.1,
    # webserver 섹션 아님) — DAG Run/Task Instance 알림 링크가 이 값을 기준으로
    # 생성된다.
    AIRFLOW__API__BASE_URL: ${AIRFLOW_API_BASE_URL:-http://localhost:8080}
    AIRFLOW_CONN_SLACK_API_DEFAULT: ${AIRFLOW_CONN_SLACK_API_DEFAULT}
    AIRFLOW_VAR_SLACK_ALERT_CHANNEL: ${AIRFLOW_VAR_SLACK_ALERT_CHANNEL}
    # EmrServerlessStartJobOperator(dags/emr_serverless.py)가 Job Run 로그를
    # 영구 저장할 S3 위치. 비어 있으면 emr_serverless.py의 기본값을 쓴다.
    AIRFLOW_VAR_EMR_SERVERLESS_LOG_S3_URI: ${AIRFLOW_VAR_EMR_SERVERLESS_LOG_S3_URI}
    # notifications.py의 on_failure_callback이 구조화된 실패 기록(JSON)을 쓰는
    # 관측 버킷. 비어 있으면 notifications.py의 기본값(사용자가 사전에 만든
    # de4-observability-473551908409-ap-northeast-2-a 버킷)을 쓴다.
    AIRFLOW_VAR_OBSERVABILITY_FAILED_TASKS_S3_URI: ${AIRFLOW_VAR_OBSERVABILITY_FAILED_TASKS_S3_URI}
    # jobs/dag_owners.py가 config/dag_owners.yaml을 읽는 경로 — 아래
    # airflow-scheduler/airflow-dag-processor volumes의 마운트 위치와 맞춘다.
    DAG_OWNERS_CONFIG_PATH: ${AIRFLOW_HOME}/config/dag_owners.yaml
```

- [ ] **Step 3: `airflow-dag-processor` 서비스에 `config/` 마운트와 신규 의존성 추가**

`airflow-dag-processor:` 블록을 다음 내용으로 교체(먼저 Edit 도구로 아래 텍스트를
그대로 넣고, `PYYAML_AND_SLACK_PLACEHOLDER` 자리만 Step 1에서 확보한
`$SLACK_PROVIDER_CONSTRAINT` 값으로 바꾼다):

```yaml
  airflow-dag-processor:
    <<: *airflow-common
    command: dag-processor
    environment:
      <<: *airflow-env
      # notifications.py의 on_success_callback(DAG 단위)이 이 프로세스에서 실행되므로
      # (Airflow의 DagFileProcessor가 DAG-level 콜백을 실행한다) 여기도 필요하다.
      _PIP_ADDITIONAL_REQUIREMENTS: "pyyaml>=6.0,<7.0 PYYAML_AND_SLACK_PLACEHOLDER datadog>=0.47.0"
    volumes:
      - ../../services/orchestration/dags:${AIRFLOW_HOME}/dags
      - ../../config:${AIRFLOW_HOME}/config:ro
    depends_on:
      airflow-init:
        condition: service_completed_successfully
```

그 다음 같은 셸 세션에서(Step 1의 `$SLACK_PROVIDER_CONSTRAINT`가 살아있어야 함)
플레이스홀더를 실제 값으로 치환한다 — 수작업 복붙 대신 `sed`로 기계적으로
바꿔 오타를 막는다:

```bash
sed -i '' "s#PYYAML_AND_SLACK_PLACEHOLDER#${SLACK_PROVIDER_CONSTRAINT}#" infra/compose/airflow.yaml
grep -n "_PIP_ADDITIONAL_REQUIREMENTS" infra/compose/airflow.yaml
```

Expected: `airflow-dag-processor`의 `_PIP_ADDITIONAL_REQUIREMENTS` 값에
`PYYAML_AND_SLACK_PLACEHOLDER`가 아니라 실제 `apache-airflow-providers-slack>=...`
constraint가 들어가 있다(macOS `sed -i ''` 문법 — Linux라면 `sed -i` 그대로).

- [ ] **Step 4: `airflow-scheduler` 서비스에 `config/` 마운트와 신규 의존성 추가**

`airflow-scheduler`의 `environment._PIP_ADDITIONAL_REQUIREMENTS` 값을 다음으로
교체(기존 값 앞에 `pyyaml>=6.0,<7.0`과 플레이스홀더를 붙인다):

```yaml
      _PIP_ADDITIONAL_REQUIREMENTS: "pyyaml>=6.0,<7.0 PYYAML_AND_SLACK_PLACEHOLDER requests>=2.32,<3.0 psycopg2-binary>=2.9,<3.0 pyarrow>=25.0.1 great-expectations>=1.21.0 pandas>=2.0.0 boto3>=1.40,<2.0 datadog>=0.47.0"
```

`airflow-scheduler`의 `volumes:` 목록 끝에 추가:

```yaml
      - ../../config:${AIRFLOW_HOME}/config:ro
```

Step 3과 같은 `sed` 명령을 다시 실행해 이번에 새로 넣은 플레이스홀더도 치환한다:

```bash
sed -i '' "s#PYYAML_AND_SLACK_PLACEHOLDER#${SLACK_PROVIDER_CONSTRAINT}#" infra/compose/airflow.yaml
grep -n "PYYAML_AND_SLACK_PLACEHOLDER" infra/compose/airflow.yaml
```

Expected: 마지막 grep이 아무것도 출력하지 않는다(치환 대상이 하나도 안 남음).

- [ ] **Step 5: `.env.example`에 플레이스홀더 추가**

`AIRFLOW_VAR_GOLD_AUDIT_S3_BUCKET=` 다음 줄에 추가:

```
# DAG 실행 결과 Slack 알림(#409). Bot Token 기반 Slack App이 필요하다(Incoming
# Webhook이 아니다 — 담당자 이메일 -> Slack 멘션 변환에 Slack Web API 호출이
# 필요해서다). 필요 스코프: chat:write, users:read.email.
# 형식: slack://:xoxb-<bot-token>@ (Airflow의 URI 기반 env-var connection 규약)
AIRFLOW_CONN_SLACK_API_DEFAULT=
AIRFLOW_VAR_SLACK_ALERT_CHANNEL=
# EMR Serverless Job Run 로그를 영구 저장할 S3 버킷. 비우면
# dags/emr_serverless.py 기본값(s3://de4-emr-serverless-logs/)을 쓴다.
AIRFLOW_VAR_EMR_SERVERLESS_LOG_S3_URI=
# on_failure_callback이 실패마다 구조화된 JSON 기록을 남기는 관측 버킷. 비우면
# dags/notifications.py 기본값(s3://de4-observability-473551908409-ap-northeast-2-a/airflow/failed-tasks/,
# 사용자가 사전에 만들어 둔 버킷)을 쓴다.
AIRFLOW_VAR_OBSERVABILITY_FAILED_TASKS_S3_URI=
# DAG Run/Task Instance Slack 알림 링크가 이 값을 기준으로 생성된다. 로컬
# 기본값은 http://localhost:8080 — 운영(EC2)에서는 실제 접속 URL로 채운다.
AIRFLOW_API_BASE_URL=
```

- [ ] **Step 6: compose 파일 문법 검증**

```bash
docker compose -f infra/compose/airflow.yaml config --quiet
```

Expected: 에러 없이 종료(0).

- [ ] **Step 7: 커밋**

```bash
git add infra/compose/airflow.yaml .env.example
git commit -m "$(cat <<'EOF'
chore: wire Slack connection, dag_owners.yaml mount, and EMR log bucket into Airflow compose (#409)
EOF
)"
```

---

## Task 12: README 문서화

**Files:**
- Modify: `services/orchestration/README.md`

- [ ] **Step 1: "준비" 절에 추가**

기존 `AIRFLOW_VAR_GOLD_AUDIT_S3_BUCKET` 항목 뒤에 추가:

```markdown
- `AIRFLOW_CONN_SLACK_API_DEFAULT`, `AIRFLOW_VAR_SLACK_ALERT_CHANNEL`,
  `AIRFLOW_VAR_EMR_SERVERLESS_LOG_S3_URI`,
  `AIRFLOW_VAR_OBSERVABILITY_FAILED_TASKS_S3_URI`, `AIRFLOW_API_BASE_URL` —
  DAG 실행 결과 Slack 알림(#409)에 필요하다.
  - Slack Bot Token 기반 App을 사전에 만들고(Incoming Webhook 아님 —
    담당자 이메일을 Slack 멘션으로 바꾸려면 Slack Web API 호출이 필요하다),
    `chat:write`, `users:read.email` 스코프를 부여한다. Bot User OAuth
    Token(`xoxb-...`)을 `AIRFLOW_CONN_SLACK_API_DEFAULT=slack://:xoxb-...@`
    형식으로 `.env`에 채운다.
  - `AIRFLOW_VAR_SLACK_ALERT_CHANNEL`은 알림을 보낼 채널(예: `#de4-alerts`).
  - `AIRFLOW_VAR_EMR_SERVERLESS_LOG_S3_URI`는 EMR Serverless Job Run의
    원본 Spark driver/executor 로그를 영구 저장할 S3 버킷(예:
    `s3://de4-emr-serverless-logs/`, 콘솔에서 사전 생성). 비우면
    `dags/emr_serverless.py`의 같은 기본값을 쓴다. EMR execution role
    (IAM)에 이 버킷에 대한 `s3:PutObject` 권한이 미리 부여돼 있어야 한다
    (다른 EMR 관련 버킷과 마찬가지로 콘솔에서 사람이 준비).
  - `AIRFLOW_VAR_OBSERVABILITY_FAILED_TASKS_S3_URI`는 `on_failure_callback`이
    실패할 때마다 남기는 구조화된 요약 기록(JSON — dag_id/task_id/처리
    일자/담당자/심각도/예외/처리 건수)을 쓸 S3 버킷이다. 기본값은 사용자가
    사전에 만들어 둔 `s3://de4-observability-473551908409-ap-northeast-2-a/airflow/failed-tasks/`.
    `airflow-scheduler`가 쓰는 AWS 자격증명(로컬은 boto3 기본 체인, 운영은
    EC2 Instance Role)에 이 버킷에 대한 `s3:PutObject` 권한이 미리 부여돼
    있어야 한다. §6의 EMR 원본 로그와는 별개다 — 이건 5개 DAG 전부에서
    남고, 사람이 Slack에서 바로 읽을 수 있게 우리가 직접 구조화한
    요약이다.
  - `AIRFLOW_API_BASE_URL`은 DAG Run/Task Instance Slack 알림 링크가 가리킬
    기준 URL이다(Airflow 3.3.1은 `[api] base_url` 설정, `webserver` 섹션이
    아니다). 로컬 기본값은 `http://localhost:8080`, 운영(EC2)에서는 실제
    접속 URL로 채운다.
```

- [ ] **Step 2: 담당자 레지스트리 절 추가**

"웹 UI" 절 앞에 새 절 추가:

```markdown
## DAG 실행 결과 Slack 알림 + 담당자 레지스트리 (#409)

`standard_score_pipeline`, `current_score_pipeline`, `zone_weather_pipeline`,
`data_quality_audit`, `bronze_compaction` 5개 DAG(`hello_world` 제외)는
`dags/notifications.py`의 공용 콜백을 쓴다. task가 재시도까지 소진하고
최종 실패하면 담당자 멘션·심각도·처리 일자·처리 건수(이미 성공한 상위
task가 있을 때만, 없으면 정직하게 "집계되지 않음")·Task Instance URL을
담아 Slack에 알리고, 같은 정보를 구조화된 JSON으로도
`AIRFLOW_VAR_OBSERVABILITY_FAILED_TASKS_S3_URI`(기본값: 사용자가 준비한
관측 버킷)에 남긴 뒤 그 링크도 함께 붙인다(EMR 기반 task라면 원본 Spark
로그 링크도 추가). DagRun이 성공하면 처리 건수 요약과 함께 1회 알린다.

담당자/심각도는 저장소 루트의 `config/dag_owners.yaml`에서 관리한다
(`jobs/dag_owners.py`가 로드). 새 DAG나 task에 담당자를 지정하려면:

1. `users`에 담당자가 없으면 추가한다 — `email`(알림 시점에
   `users.lookupByEmail`로 Slack ID를 조회) 또는 `slack_id`(이미 알면 조회
   생략) 중 하나 이상 채운다.
2. `dags.<dag_id>`에 `owner`(위 `users`의 키)와 `severity`
   (`critical`/`high`/`medium`/`low` 중 하나)를 채운다 — DAG 전체의 기본값이다.
3. 특정 task/TaskGroup만 다른 담당자·심각도를 쓰려면 `dags.<dag_id>.tasks`에
   그 task의 전체 dotted id(예: `sensor_processing.run_sensor_processing`)
   또는 TaskGroup id(예: `sensor_processing`)를 키로 추가한다. 조회 순서는
   task_id -> task_group_id -> DAG 기본값이다.
4. `owner`가 `users`에 없거나 `severity`가 유효하지 않으면 DAG 파싱
   시점이 아니라 콜백이 처음 로드를 시도할 때(다음 실행) 에러가 난다 —
   `uv run --package orchestration pytest services/orchestration/tests/test_dag_owners.py`로
   미리 검증할 수 있다.

`report_processing_counts`(`standard_score_pipeline`)와
`report_audit_counts`(`data_quality_audit`)는 EMR Serverless로 제출된
task가 방금 쓴 output을 orchestration 프로세스에서 직접 다시 세어(S3
Parquet/Postgres COUNT) 성공 알림에 넣는 전용 task다 — EMR Job Run
자체는 XCom을 만들 수 없어서다(`jobs/pipeline_counts.py`).
```

- [ ] **Step 3: "범위 밖" 절에서 항목 제거**

`## 범위 밖` 절의 첫 항목을 수정:

```markdown
## 범위 밖

- `data_quality_audit`/`standard_score_pipeline` 모두, batch-jobs 커스텀
  이미지 완성 후 실제 EMR Serverless Job Run 트리거 검증과 Airflow의 EC2
  이전(#289 후속 이슈)
- Kafka -> Bronze 오케스트레이션
- CeleryExecutor/KubernetesExecutor 등 분산 실행 지원
- CD는 EC2에서 컨테이너를 기동하고 헬스체크까지 확인한다(#315). 인증 관리자
  교체(SimpleAuthManager → FAB 등)와 RBAC 설정, RDS의 Airflow용 DB(스키마)
  실제 생성은 범위 밖이다 — 사람이 사전에 수행한다
```

(기존 "Great Expectations 검증 task, Slack 실패 알림" 줄을 지우고, 남은 항목만
유지한다 — GX 검증 task는 이미 구현돼 있었으므로 이 줄 전체가 stale했다.)

- [ ] **Step 4: 커밋**

```bash
git add services/orchestration/README.md
git commit -m "$(cat <<'EOF'
docs: document Slack alert setup and dag_owners.yaml registry (#409)
EOF
)"
```

---

## Task 13: 로컬 Airflow 수동 검증 (완료 조건)

**Files:** 없음(코드 변경 없음) — 실행/확인만.

- [ ] **Step 1: 전체 검증 명령 실행**

```bash
cd /Users/yong/PycharmProjects/DE_team4-4una
uv sync --all-packages
uv run --all-packages ruff check .
uv run --all-packages pytest
```

Expected: 전부 통과.

- [ ] **Step 2: `config/dag_owners.yaml`에 실제 Slack 채널/알림 대상 채우기**

`config/dag_owners.yaml`의 `alice`/`bob`을 실제 팀원 email 또는 slack_id로
바꾼다(사람이 직접, git에 커밋할지는 팀 판단 — 실제 이메일/Slack ID가
민감하다고 판단되면 로컬 `.env`처럼 별도 미커밋 파일로 관리하는 방안을
사용자와 상의한다).

- [ ] **Step 3: Slack App 준비 확인**

사용자에게 다음을 확인받는다(이 세션이 직접 할 수 없는 부분, spec 제외 범위):
Bot Token 발급, `chat:write`/`users:read.email` 스코프, 알림을 받을 채널에
봇 초대. `.env`의 `AIRFLOW_CONN_SLACK_API_DEFAULT`, `AIRFLOW_VAR_SLACK_ALERT_CHANNEL`,
`AIRFLOW_VAR_EMR_SERVERLESS_LOG_S3_URI`(S3 버킷 사전 생성 + EMR execution role
`s3:PutObject` 권한), `AIRFLOW_VAR_OBSERVABILITY_FAILED_TASKS_S3_URI`(사용자가
이미 만들어 둔 `de4-observability-473551908409-ap-northeast-2-a` 버킷 —
`airflow-scheduler`가 쓰는 AWS 자격증명에 `s3:PutObject` 권한 확인),
`AIRFLOW_API_BASE_URL`을 채운다.

- [ ] **Step 4: Airflow 기동 및 대상 DAG unpause**

```bash
make up-postgres
make up-airflow
docker compose -f infra/compose/airflow.yaml exec airflow-webserver \
  airflow dags unpause bronze_compaction
docker compose -f infra/compose/airflow.yaml exec airflow-webserver \
  airflow dags unpause current_score_pipeline
```

(EMR Serverless 기반 `standard_score_pipeline`/`data_quality_audit`는
README의 기존 절차대로 EMR 준비가 끝난 뒤 검증 — 이 세션에서 새로 요구하는
절차는 아니다.)

- [ ] **Step 5: 성공 알림 확인**

```bash
docker compose -f infra/compose/airflow.yaml exec airflow-webserver \
  airflow dags trigger bronze_compaction
```

Slack 채널에 `bronze_compaction` 성공 메시지(담당자, 심각도, DAG Run 링크,
처리 건수)가 도착하는지 확인한다.

- [ ] **Step 6: 실패 알림 확인**

일부러 실패시킨다(예: `bronze_compaction`이 읽는 로컬 lake 경로를 임시로
지워 task를 실패시킴). Slack에 담당자 멘션(`<@...>`), 심각도 이모지/라벨,
처리 일자, 처리 건수 줄(이 DAG는 상위 task가 없어 "집계되지 않음"으로
나오는 게 정상), Task Instance URL, "실패 상세 기록 열기(S3)" 링크가
도착하는지 확인한다. Task Instance URL은 클릭 시 실제 실행 화면으로
이동해야 한다. "실패 상세 기록 열기" 링크는 클릭 시
`de4-observability-473551908409-ap-northeast-2-a` 버킷의
`airflow/failed-tasks/bronze_compaction/compact_zone_weather_snapshot/`
아래 방금 쓰인 JSON 객체로 이동해야 하고, 그 객체를 열어 `dag_id`/
`task_id`/`exception`/`counts` 필드가 실제 실패 내용과 맞는지 확인한다.
이어서 EMR 기반 task(`standard_score_pipeline`/`data_quality_audit`,
`AIRFLOW_VAR_EMR_SERVERLESS_LOG_S3_URI`를 잘못된 값으로 바꾸는 등으로
의도적으로 실패시킴)도 같은 방식으로 확인하되, 이번엔 "EMR Serverless
원본 로그 열기" 링크도 함께 와야 하고 클릭 시 `de4-emr-serverless-logs`
버킷의 해당 Job Run 경로로 이동해야 한다.

- [ ] **Step 7: 웹 UI 접속 정보 안내**

`http://localhost:8080`, 관리자 비밀번호는:

```bash
docker compose -f infra/compose/airflow.yaml logs airflow-webserver | grep "Password for user"
```

사용자에게 이 URL과 비밀번호를 안내하고, 실행 결과를 직접 확인할 수 있게 한다
(프로젝트 메모리 규칙 — Airflow 변경 후 로컬 UI로 검증).

- [ ] **Step 8: 정리**

```bash
docker compose -f infra/compose/airflow.yaml down
docker compose -f infra/compose/postgres.yaml down
```
