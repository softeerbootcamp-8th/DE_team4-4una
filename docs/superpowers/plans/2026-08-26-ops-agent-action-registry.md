# Ops Agent 무위험 조치 2종 추가 구현 계획 (#547)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `restart_serving_api`와 `restart_node_exporter`를 자동 조치에 추가한다.

**Architecture:** alertname 하나로 조치 명세(상태 PromQL / SSH 대상 / 컨테이너 이름)를
찾는 dict를 `policy.py`에 두고, orchestrator는 그 명세가 지시하는 값으로만 동작한다.
조치 3종이 전부 "컨테이너 하나 재시작"이라 실행 함수는 하나로 합친다.

**Tech Stack:** Python 3.12, uv workspace, FastAPI, sqlite3, pytest, Grafana provisioning(YAML)

**Spec:** `docs/superpowers/specs/2026-08-26-ops-agent-visibility-and-safe-actions-design.md` §2, §3
**ADR:** `docs/adr/0013-immediate-remediation-without-slack-approval.md`

## Global Constraints

- Python 3.12. 테스트는 `uv run --package ops-agent pytest services/ops-agent/tests`.
- 모든 모듈은 `from __future__ import annotations`로 시작하고, 데이터 구조는 `@dataclass(frozen=True, slots=True)`를 쓴다.
- **`ssh.py`의 불변식:** 원격 argv는 코드에 고정된 값으로만 구성한다. **alert payload에서 온 값이 argv에 들어가면 안 된다.** 컨테이너 이름은 명세에 박힌 리터럴만 쓴다.
- ADR-0013의 저위험 다섯 조건을 만족하는 조치만 추가한다. Airflow Scheduler, Kafka, EMR, 디스크 정리는 이번에도 escalation-only.
- 필수 환경변수에 기본값을 두지 않는다. 없으면 기동 시점에 실패한다(`config._require`).
- 자명하지 않은 코드에는 **왜**를 설명하는 한국어 주석을 단다.
- 커밋 메시지는 `<type>: <subject>`, 영어 소문자 명령형. Co-author 푸터 금지.

## 설계를 줄인 근거 (읽고 시작할 것)

초안은 Task 8개였다. 아래 두 사실을 확인해 절반으로 줄였다. **이 근거를 모르면
"왜 blackbox를 안 쓰지?" 하고 다시 늘리게 된다.**

**① DB 장애와 프로세스 장애 구분에 blackbox probe가 필요 없다.** serving-api는
metrics 서버를 uvicorn과 **같은 프로세스**에서 띄운다(`services/serving-api/src/serving_api/__init__.py:21-22`).
그래서 `up{job="serving-api"}`가 이미 프로세스 생사와 정확히 일치한다.

| 상황 | `up{job="serving-api"}` | 판정 |
| --- | --- | --- |
| 정상 | 1 | 조치 없음 |
| 프로세스/컨테이너 죽음 | 0 | **재시작 대상** |
| DB만 죽음 (앱은 살아서 metrics 계속 노출) | 1 | alert 자체가 발화하지 않음 |

DB 장애가 이 alert를 발화시키지 않으므로 #512가 요구한 구분이 저절로 충족된다.
blackbox probe, `extra_hosts`, `probe_http_status_code`, `ServingApiDatabaseUnavailable`
규칙이 전부 불필요하다.

**② 라벨 매칭이 필요 없다.** 기존 `NodeDown`을 재사용하면 EC2 3대를 커버해서 `job`
라벨로 갈라야 하고, 그것 때문에 `Incident.labels` 추가와 명세 순회가 생긴다. 좁은 규칙
`SparkNodeExporterDown`을 새로 만들면 alertname만으로 대상이 정해져 그게 통째로 사라진다.
`monitoring-node`도 자동으로 제외된다 — 규칙이 spark만 보기 때문이다.

`project-node`의 exporter는 이번 범위에서 뺐다. 넣으려면 규칙 하나와 명세 한 줄이면
되지만, 이번 변경을 작게 유지한다.

## 이름 변경 요약

| 기존 | 변경 후 |
| --- | --- |
| `StreamProcessorStatus` | `ServiceStatus` |
| `StreamProcessorDiagnostics` | `ContainerDiagnostics` |
| `collect_stream_processor_diagnostics(target)` | `collect_container_diagnostics(target, container)` |
| `restart_stream_processor(target)` | `restart_container(target, container, action=...)` |

`STREAM_PROCESSOR_STATUS_QUERY` 상수는 **그대로 둔다.** spark-streaming 대시보드
패널과 문자 그대로 같아야 한다는 기존 테스트(#437)가 이 이름을 참조한다.

## 브랜치 상황

`feat/547-extend-ops-agent-remediation`에서 작업하며 `feat/546-...` 위에 쌓여 있다.
CI에 ops-agent를 등록하는 커밋(`055bf2b`)이 이미 첫 커밋으로 들어가 있다. #550이
develop에 병합되면 PR base를 `develop`으로 바꾼다.

---

### Task 1: 진단과 조치를 컨테이너 이름으로 일반화한다

**Files:**
- Modify: `services/ops-agent/src/ops_agent/diagnostics.py`
- Modify: `services/ops-agent/src/ops_agent/remediation.py`
- Modify: `services/ops-agent/src/ops_agent/notification.py` (타입 이름만)
- Modify: `services/ops-agent/src/ops_agent/orchestrator.py` (임시 기본 콜백)
- Modify: `services/ops-agent/tests/test_diagnostics.py`, `test_remediation.py`, `conftest.py`

**Interfaces:**
- Consumes: `CommandKind`, `ExecutedCommand`, `SshTarget`, `run_remote_command` (#546)
- Produces: `ContainerDiagnostics(container_status, restart_count, recent_logs, commands)`, `collect_container_diagnostics(target, container) -> ContainerDiagnostics`, `restart_container(target, container, *, action) -> RemediationResult`

- [ ] **Step 1: 실패 테스트를 쓴다**

`services/ops-agent/tests/test_remediation.py`의 `TestRestartStreamProcessor` 클래스를
아래로 교체한다. 파일 상단 헬퍼 `ssh_result`와 `TARGET`은 그대로 둔다.

```python
class TestRestartContainer:
    def test_it_restarts_the_container_it_was_given(self, monkeypatch):
        captured = {}

        def fake_run(target, argv, *, kind, **kwargs):
            captured["argv"] = argv
            return ssh_result(0, "serving-api", "", argv, kind)

        monkeypatch.setattr(remediation_module, "run_remote_command", fake_run)

        result = restart_container(TARGET, "serving-api", action="restart_serving_api")

        assert result.succeeded is True
        assert result.action == "restart_serving_api"
        assert captured["argv"] == ["docker", "restart", "serving-api"]

    def test_the_executed_command_is_marked_as_mutating(self, monkeypatch):
        monkeypatch.setattr(
            remediation_module,
            "run_remote_command",
            lambda target, argv, *, kind, **kwargs: ssh_result(0, "x", "", argv, kind),
        )

        result = restart_container(TARGET, "node-exporter", action="restart_node_exporter")

        assert result.command.kind is CommandKind.MUTATE
        assert result.command.argv == ("docker", "restart", "node-exporter")

    def test_a_failed_restart_is_reported_with_the_ssh_error(self, monkeypatch):
        monkeypatch.setattr(
            remediation_module,
            "run_remote_command",
            lambda target, argv, *, kind, **kwargs: ssh_result(
                1, "", "connection refused", argv, kind
            ),
        )

        result = restart_container(
            TARGET, "stream-processor", action="restart_stream_processor"
        )

        assert result.succeeded is False
        assert "connection refused" in result.detail
```

import를 `from ops_agent.remediation import restart_container`로 바꾼다.

`services/ops-agent/tests/test_diagnostics.py`는 import를
`from ops_agent.diagnostics import collect_container_diagnostics`로 바꾸고, 기존
호출 3곳을 `collect_container_diagnostics(TARGET, "stream-processor")`로 바꾼 뒤
아래를 추가한다.

```python
    def test_it_inspects_the_container_it_was_given(self, monkeypatch):
        seen = []
        outputs = [(0, "running|0", ""), (0, "log", "")]

        def fake_run(target, argv, *, kind, **kwargs):
            seen.append(argv)
            exit_code, stdout, stderr = outputs.pop(0)
            return ssh_result(exit_code, stdout, stderr, argv, kind)

        monkeypatch.setattr(diagnostics_module, "run_remote_command", fake_run)

        collect_container_diagnostics(TARGET, "serving-api")

        assert seen[0][-1] == "serving-api"
        assert seen[1][-1] == "serving-api"
```

- [ ] **Step 2: 실패를 확인한다**

Run: `uv run --package ops-agent pytest services/ops-agent/tests/test_remediation.py -q`
Expected: FAIL — `ImportError: cannot import name 'restart_container' from 'ops_agent.remediation'`

- [ ] **Step 3: `diagnostics.py`를 구현한다**

`STREAM_PROCESSOR_CONTAINER_NAME` 상수는 지운다 — 컨테이너 이름은 명세가 준다.

```python
# 진단은 읽기 전용 명령만 쓴다. 대상 컨테이너 이름은 policy.py의 명세가 주고 여기서 정하지 않는다.

from __future__ import annotations

from dataclasses import dataclass

from ops_agent.ssh import CommandKind, ExecutedCommand, SshTarget, run_remote_command

_LOG_TAIL_LINES = 50


@dataclass(frozen=True, slots=True)
class ContainerDiagnostics:
    container_status: str
    restart_count: int | None
    recent_logs: str
    commands: tuple[ExecutedCommand, ...]


def collect_container_diagnostics(target: SshTarget, container: str) -> ContainerDiagnostics:
    inspect = run_remote_command(
        target,
        ["docker", "inspect", "--format", "{{.State.Status}}|{{.RestartCount}}", container],
        kind=CommandKind.READ,
    )
    if not inspect.ok:
        return ContainerDiagnostics(
            container_status="not found",
            restart_count=None,
            recent_logs=inspect.stderr.strip(),
            commands=(inspect.command,),
        )

    status_field, _, restart_field = inspect.stdout.strip().partition("|")
    try:
        restart_count = int(restart_field)
    except ValueError:
        restart_count = None

    logs = run_remote_command(
        target,
        ["docker", "logs", "--tail", str(_LOG_TAIL_LINES), container],
        kind=CommandKind.READ,
    )
    return ContainerDiagnostics(
        container_status=status_field or "unknown",
        restart_count=restart_count,
        recent_logs=(logs.stdout + logs.stderr).strip(),
        commands=(inspect.command, logs.command),
    )
```

- [ ] **Step 4: `remediation.py`를 구현한다**

```python
# 조치 3종(stream-processor / serving-api / node-exporter)이 전부 "컨테이너 하나 재시작"이라 함수를 하나로 합쳤다(#547).

from __future__ import annotations

from dataclasses import dataclass

from ops_agent.ssh import CommandKind, ExecutedCommand, SshTarget, run_remote_command


@dataclass(frozen=True, slots=True)
class RemediationResult:
    action: str
    succeeded: bool
    detail: str
    command: ExecutedCommand


def restart_container(target: SshTarget, container: str, *, action: str) -> RemediationResult:
    result = run_remote_command(
        target, ["docker", "restart", container], kind=CommandKind.MUTATE
    )
    return RemediationResult(
        action=action,
        succeeded=result.ok,
        detail=(result.stdout.strip() if result.ok else result.stderr.strip()),
        command=result.command,
    )
```

- [ ] **Step 5: 이름 변경을 따라가게 한다**

`notification.py`에서 `StreamProcessorDiagnostics` → `ContainerDiagnostics`
(import 1곳, `NotificationInput.diagnostics` 타입 1곳, `build_log_reply` 시그니처 1곳).

`orchestrator.py`는 Task 4에서 명세 기반으로 바꾼다. 이 Task에서는 green 유지를 위해
기본 콜백만 임시로 감싼다.

```python
def _default_diagnose(target: SshTarget) -> ContainerDiagnostics:
    # Task 4에서 명세가 컨테이너 이름을 주게 되면 사라진다.
    return collect_container_diagnostics(target, "stream-processor")


def _default_remediate(target: SshTarget) -> RemediationResult:
    return restart_container(target, "stream-processor", action="restart_stream_processor")
```

생성자 기본값을 이 두 함수로 바꾸고, `StreamProcessorDiagnostics` import를
`ContainerDiagnostics`로 바꾼다.

`conftest.py`의 fake도 새 시그니처를 받게 한다.

```python
from ops_agent.diagnostics import ContainerDiagnostics


def fake_diagnose(_target, _container="stream-processor") -> ContainerDiagnostics:
    return ContainerDiagnostics(
        container_status="running",
        restart_count=0,
        recent_logs="fake logs",
        commands=(
            executed(("docker", "inspect", "stream-processor")),
            executed(("docker", "logs", "--tail", "50", "stream-processor")),
        ),
    )


def make_fake_remediate(*, succeeded: bool = True):
    def _remediate(_target, _container="stream-processor", *, action="restart_stream_processor"):
        return RemediationResult(
            action=action,
            succeeded=succeeded,
            detail="fake ssh output",
            command=executed(("docker", "restart", "stream-processor"), kind=CommandKind.MUTATE),
        )

    return _remediate
```

- [ ] **Step 6: 통과를 확인한다**

Run: `uv run --package ops-agent pytest services/ops-agent/tests -q`
Expected: PASS

- [ ] **Step 7: 커밋한다**

```bash
git add services/ops-agent/src services/ops-agent/tests
git commit -m "refactor: take the container name as an argument"
```

---

### Task 2: 상태 판정을 HealthCheck로 일반화한다

**Files:**
- Modify: `services/ops-agent/src/ops_agent/prometheus_client.py`
- Modify: `services/ops-agent/src/ops_agent/orchestrator.py`, `notification.py` (타입 이름)
- Modify: `services/ops-agent/tests/test_prometheus_client.py`, `conftest.py`

**Interfaces:**
- Consumes: 없음
- Produces: `HealthCheck(query, labels, healthy_code)`, `ServiceStatus(code, label, instance, healthy_code)` with `.is_healthy`, `PrometheusClient.evaluate(check) -> ServiceStatus`, `STREAM_PROCESSOR_HEALTH`

- [ ] **Step 1: 실패 테스트를 쓴다**

`services/ops-agent/tests/test_prometheus_client.py`에 추가하고 import에 `HealthCheck`를 넣는다.

```python
class TestEvaluate:
    def test_it_maps_the_value_through_the_checks_label_table(self):
        session = FakeSession(
            FakeResponse(success_payload([{"metric": {"instance": "a:1"}, "value": [0, "4"]}]))
        )
        client = PrometheusClient("http://prometheus:9090", session=session)
        check = HealthCheck(query="up", labels={0: "UP", 4: "TARGET DOWN"}, healthy_code=0)

        status = client.evaluate(check)

        assert status.code == 4
        assert status.label == "TARGET DOWN"
        assert status.is_healthy is False
        assert session.calls[0]["params"] == {"query": "up"}

    def test_no_data_is_never_reported_as_healthy(self):
        session = FakeSession(FakeResponse(success_payload([])))
        client = PrometheusClient("http://prometheus:9090", session=session)

        status = client.evaluate(HealthCheck(query="up", labels={0: "UP"}, healthy_code=0))

        assert status.code is None
        assert status.label == "NO DATA"
        assert status.is_healthy is False

    def test_the_worst_instance_wins_when_several_match(self):
        session = FakeSession(
            FakeResponse(
                success_payload(
                    [
                        {"metric": {"instance": "a:1"}, "value": [0, "1"]},
                        {"metric": {"instance": "b:1"}, "value": [0, "4"]},
                    ]
                )
            )
        )
        client = PrometheusClient("http://prometheus:9090", session=session)
        check = HealthCheck(query="up", labels={1: "STALE", 4: "TARGET DOWN"}, healthy_code=0)

        status = client.evaluate(check)

        assert status.code == 4
        assert status.instance == "b:1"

    def test_the_stream_processor_check_reuses_the_dashboard_query(self):
        from ops_agent.prometheus_client import (
            STREAM_PROCESSOR_HEALTH,
            STREAM_PROCESSOR_STATUS_QUERY,
        )

        assert STREAM_PROCESSOR_HEALTH.query == STREAM_PROCESSOR_STATUS_QUERY
        assert STREAM_PROCESSOR_HEALTH.healthy_code == 0
```

- [ ] **Step 2: 실패를 확인한다**

Run: `uv run --package ops-agent pytest services/ops-agent/tests/test_prometheus_client.py -q`
Expected: FAIL — `ImportError: cannot import name 'HealthCheck' from 'ops_agent.prometheus_client'`

- [ ] **Step 3: `prometheus_client.py`를 구현한다**

`STREAM_PROCESSOR_STATUS_QUERY`, `STREAM_PROCESSOR_STATUS_LABELS`,
`HEALTHY_STATUS_CODE`는 그대로 둔다. 아래를 추가/교체한다.

```python
@dataclass(frozen=True, slots=True)
class HealthCheck:
    """한 서비스의 상태를 판정하는 데 필요한 전부 — 조치 명세가 이걸 들고 다닌다."""

    query: str
    labels: Mapping[int, str]
    # 정상으로 볼 코드. evaluate()가 max()로 최악을 고르므로 항상 labels의 최솟값이어야 한다.
    healthy_code: int


@dataclass(frozen=True, slots=True)
class ServiceStatus:
    code: int | None
    label: str
    instance: str | None
    healthy_code: int

    @property
    def is_healthy(self) -> bool:
        return self.code == self.healthy_code


STREAM_PROCESSOR_HEALTH = HealthCheck(
    query=STREAM_PROCESSOR_STATUS_QUERY,
    labels=STREAM_PROCESSOR_STATUS_LABELS,
    healthy_code=HEALTHY_STATUS_CODE,
)
```

`stream_processor_status()`를 아래로 대체한다.

```python
    def evaluate(self, check: HealthCheck) -> ServiceStatus:
        """check가 지시하는 PromQL로 현재 상태를 판정한다. instance가 여러 개면 가장 심각한 것을 고른다."""
        results = self.instant_query(check.query)
        if not results:
            # metric 자체가 없으면 상태를 확정할 수 없으므로 "정상"으로 오판하지 않는다.
            return ServiceStatus(
                code=None, label="NO DATA", instance=None, healthy_code=check.healthy_code
            )

        worst = max(results, key=lambda result: float(result["value"][1]))
        code = int(float(worst["value"][1]))
        return ServiceStatus(
            code=code,
            label=check.labels.get(code, "UNKNOWN"),
            instance=(worst.get("metric") or {}).get("instance"),
            healthy_code=check.healthy_code,
        )
```

`from collections.abc import Mapping`을 import에 추가한다.

- [ ] **Step 4: 호출부와 fake를 맞춘다**

`orchestrator.py`의 `stream_processor_status()` 호출 2곳을
`self._prometheus.evaluate(STREAM_PROCESSOR_HEALTH)`로 바꾸고,
`StreamProcessorStatus` 타입 힌트를 `ServiceStatus`로 바꾼다(`notification.py`도 동일).

`conftest.py`:

```python
from ops_agent.prometheus_client import HealthCheck, ServiceStatus


def status(code: int, label: str, instance: str = "spark-ec2:9103") -> ServiceStatus:
    return ServiceStatus(code=code, label=label, instance=instance, healthy_code=0)


@dataclass
class FakePrometheusClient:
    """`evaluate()`가 순서대로 미리 정해둔 값을 돌려준다 — reverify(조치 전) -> reverify(조치 후) 흐름을 시뮬레이션한다."""

    statuses: list[ServiceStatus]
    calls: int = 0

    def evaluate(self, _check: HealthCheck) -> ServiceStatus:
        index = min(self.calls, len(self.statuses) - 1)
        self.calls += 1
        return self.statuses[index]
```

- [ ] **Step 5: 통과를 확인한다**

Run: `uv run --package ops-agent pytest services/ops-agent/tests -q`
Expected: PASS

- [ ] **Step 6: 커밋한다**

```bash
git add services/ops-agent/src services/ops-agent/tests
git commit -m "refactor: evaluate service health from a check spec"
```

---

### Task 3: 조치 명세와 Project EC2 SSH 대상을 추가한다

**Files:**
- Modify: `services/ops-agent/src/ops_agent/policy.py`
- Modify: `services/ops-agent/src/ops_agent/config.py`
- Modify: `services/ops-agent/tests/test_policy.py`
- Create: `services/ops-agent/tests/test_config.py`
- Modify: `infra/compose/monitoring.yaml`, `infra/monitoring/.env.example`

**Interfaces:**
- Consumes: `HealthCheck`, `STREAM_PROCESSOR_HEALTH` (Task 2)
- Produces: `ActionSpec(action, health, ssh_target_key, container)`, `ACTION_SPECS: dict[str, ActionSpec]`, `decide(incident, spec) -> PolicyDecision`, `SERVING_API_HEALTH`, `SPARK_NODE_EXPORTER_HEALTH`, `OpsAgentConfig.ssh_targets() -> dict[str, SshTarget]`

- [ ] **Step 1: 실패 테스트를 쓴다**

`services/ops-agent/tests/test_policy.py`에 추가한다. 기존 `decide(...)` 호출은
Step 4에서 인자 하나를 더 받도록 고친다.

```python
class TestActionSpecs:
    def test_every_alert_that_can_be_remediated_has_a_spec(self):
        from ops_agent.policy import ACTION_SPECS

        assert set(ACTION_SPECS) == {
            "StreamProcessorDown",
            "ServingApiDown",
            "SparkNodeExporterDown",
        }

    def test_specs_point_at_known_ssh_hosts(self):
        from ops_agent.policy import ACTION_SPECS

        assert {spec.ssh_target_key for spec in ACTION_SPECS.values()} <= {"spark", "project"}

    def test_serving_api_is_restarted_on_the_project_host(self):
        from ops_agent.policy import ACTION_SPECS

        spec = ACTION_SPECS["ServingApiDown"]

        assert spec.ssh_target_key == "project"
        assert spec.container == "serving-api"

    def test_container_names_are_literals_not_taken_from_alerts(self):
        # ssh.py의 불변식 회귀 방지 — 이름은 명세에 박힌 리터럴이어야 한다.
        from ops_agent.policy import ACTION_SPECS

        assert all(spec.container and " " not in spec.container for spec in ACTION_SPECS.values())

    def test_bigger_is_worse_in_every_health_check(self):
        # evaluate()가 max()로 최악을 고르므로 healthy_code는 항상 최솟값이어야 한다.
        from ops_agent.policy import ACTION_SPECS

        for spec in ACTION_SPECS.values():
            assert spec.health.healthy_code == min(spec.health.labels)

    def test_stream_processor_stale_has_no_spec(self):
        # 잠깐의 지연으로 컨테이너를 재시작하면 안 된다.
        from ops_agent.policy import ACTION_SPECS

        assert "StreamProcessorStale" not in ACTION_SPECS

    def test_every_spec_action_is_implemented(self):
        from ops_agent.policy import ACTION_SPECS, IMPLEMENTED_ACTIONS

        assert all(spec.action in IMPLEMENTED_ACTIONS for spec in ACTION_SPECS.values())
```

`services/ops-agent/tests/test_config.py`를 새로 만든다.

```python
from __future__ import annotations

import pytest
from ops_agent.config import OpsAgentConfig

BASE_ENV = {
    "SLACK_BOT_TOKEN": "xoxb-x",
    "SLACK_ALERT_CHANNEL": "#alerts",
    "STREAM_PROCESSOR_SSH_HOST": "spark.example",
    "STREAM_PROCESSOR_SSH_KEY_PATH": "/keys/spark.pem",
    "PROJECT_SSH_HOST": "project.example",
    "PROJECT_SSH_KEY_PATH": "/keys/project.pem",
}


class TestSshTargets:
    def test_both_hosts_are_available_under_their_spec_keys(self):
        targets = OpsAgentConfig.from_env(BASE_ENV).ssh_targets()

        assert targets["spark"].host == "spark.example"
        assert targets["project"].host == "project.example"
        assert targets["project"].key_path == "/keys/project.pem"

    def test_the_user_defaults_to_ec2_user_for_both(self):
        targets = OpsAgentConfig.from_env(BASE_ENV).ssh_targets()

        assert targets["spark"].user == "ec2-user"
        assert targets["project"].user == "ec2-user"

    @pytest.mark.parametrize("missing", ["PROJECT_SSH_HOST", "PROJECT_SSH_KEY_PATH"])
    def test_a_missing_project_setting_fails_at_startup(self, missing):
        # 조용히 기본값으로 채우면 엉뚱한 호스트에 docker restart를 쏠 수 있다.
        env = {key: value for key, value in BASE_ENV.items() if key != missing}

        with pytest.raises(ValueError, match=missing):
            OpsAgentConfig.from_env(env)
```

- [ ] **Step 2: 실패를 확인한다**

Run: `uv run --package ops-agent pytest services/ops-agent/tests/test_policy.py services/ops-agent/tests/test_config.py -q`
Expected: FAIL — `ImportError: cannot import name 'ACTION_SPECS' from 'ops_agent.policy'`

- [ ] **Step 3: `policy.py`에 명세를 넣는다**

`RemediationAction`에 `RESTART_NODE_EXPORTER = "restart_node_exporter"`를 추가하고
`IMPLEMENTED_ACTIONS`를 셋으로 넓힌다. `ALLOWED_ACTIONS_BY_ALERTNAME`은 지운다.
`ESCALATION_ONLY_ACTIONS`는 그대로 둔다.

```python
IMPLEMENTED_ACTIONS: frozenset[RemediationAction] = frozenset(
    {
        RemediationAction.RESTART_STREAM_PROCESSOR,
        RemediationAction.RESTART_SERVING_API,
        RemediationAction.RESTART_NODE_EXPORTER,
    }
)

# serving-api는 metrics 서버를 uvicorn과 같은 프로세스에서 띄운다
# (services/serving-api/src/serving_api/__init__.py:21) — 그래서 up이 곧 프로세스 생사다.
# DB만 죽으면 앱은 살아서 metrics를 계속 내보내므로 up은 1이고 이 조치는 발화하지 않는다.
# up은 1이 정상이라 그대로 쓰면 evaluate()의 max()가 최악이 아니라 최선을 고른다.
# 그래서 뒤집어 "값이 클수록 심각"하게 맞춘다.
SERVING_API_HEALTH = HealthCheck(
    query='1 - up{job="serving-api"}', labels={0: "UP", 1: "DOWN"}, healthy_code=0
)

SPARK_NODE_EXPORTER_HEALTH = HealthCheck(
    query='1 - up{job="spark-node"}', labels={0: "UP", 1: "DOWN"}, healthy_code=0
)


@dataclass(frozen=True, slots=True)
class ActionSpec:
    """alert 하나가 어떤 조치로 이어지는지의 전부 — 상태 판정, 접속 대상, 컨테이너 이름."""

    action: RemediationAction
    health: HealthCheck
    # config.ssh_targets()의 키. 설정이 없으면 orchestrator가 조치 없이 escalation한다.
    ssh_target_key: str
    container: str


# alertname -> 명세. 여기 없는 alertname은 항상 escalation-only다.
ACTION_SPECS: dict[str, ActionSpec] = {
    "StreamProcessorDown": ActionSpec(
        action=RemediationAction.RESTART_STREAM_PROCESSOR,
        health=STREAM_PROCESSOR_HEALTH,
        ssh_target_key="spark",
        container="stream-processor",
    ),
    "ServingApiDown": ActionSpec(
        action=RemediationAction.RESTART_SERVING_API,
        health=SERVING_API_HEALTH,
        ssh_target_key="project",
        container="serving-api",
    ),
    # 호스트 자체가 죽은 경우와 exporter만 죽은 경우는 SSH 성공 여부로 자연히 갈린다 —
    # 붙으면 재시작하고, 안 붙으면 escalation된다. 별도 판별 로직이 필요 없다.
    "SparkNodeExporterDown": ActionSpec(
        action=RemediationAction.RESTART_NODE_EXPORTER,
        health=SPARK_NODE_EXPORTER_HEALTH,
        ssh_target_key="spark",
        container="node-exporter",
    ),
}
```

import에 `from ops_agent.prometheus_client import STREAM_PROCESSOR_HEALTH, HealthCheck`를 추가한다
(`prometheus_client`는 `policy`를 import하지 않으므로 순환이 없다).

`decide`가 명세를 받게 바꾼다.

```python
def decide(incident: Incident, spec: ActionSpec | None) -> PolicyDecision:
    """이 incident에 자동 조치를 해도 되는지 판단한다. 실행은 하지 않는다."""
    if not incident.auto_remediate:
        return PolicyDecision(
            action=None, allowed=False, reason="alert did not opt into auto_remediate"
        )

    if spec is None:
        return PolicyDecision(
            action=None,
            allowed=False,
            reason=f"no action spec for alertname={incident.alertname!r}",
        )

    if spec.action not in IMPLEMENTED_ACTIONS:
        return PolicyDecision(
            action=spec.action,
            allowed=False,
            reason=f"action {spec.action.value!r} is allowlisted but not yet implemented",
        )

    return PolicyDecision(action=spec.action, allowed=True, reason="allowed")
```

- [ ] **Step 4: 기존 `test_policy.py` 호출을 맞춘다**

`decide(incident())` → `decide(incident(), ACTION_SPECS["StreamProcessorDown"])`.
`test_an_unregistered_alertname_is_not_allowed`와
`test_stream_processor_stale_is_never_allowed_even_if_mislabeled`는 두 번째 인자로
`None`을 넘긴다. `test_an_allowlisted_but_unimplemented_action_is_not_allowed`는
monkeypatch 대상을 바꾼다.

```python
    def test_an_action_that_is_not_implemented_is_not_allowed(self, monkeypatch):
        import ops_agent.policy as policy_module

        monkeypatch.setattr(policy_module, "IMPLEMENTED_ACTIONS", frozenset())

        decision = decide(incident(), ACTION_SPECS["StreamProcessorDown"])

        assert decision.allowed is False
        assert "not yet implemented" in decision.reason
```

- [ ] **Step 5: `config.py`에 Project EC2를 추가한다**

dataclass 필드와 `from_env` 항목을 추가한다.

```python
    project_ssh_host: str
    project_ssh_user: str
    project_ssh_key_path: str
```

```python
            project_ssh_host=_require(source, "PROJECT_SSH_HOST"),
            project_ssh_user=(source.get("PROJECT_SSH_USER") or "ec2-user"),
            project_ssh_key_path=_require(source, "PROJECT_SSH_KEY_PATH"),
```

메서드를 추가하고 `from ops_agent.ssh import SshTarget`를 import한다.

```python
    def ssh_targets(self) -> dict[str, SshTarget]:
        """ActionSpec.ssh_target_key -> 실제 접속 정보."""
        return {
            "spark": SshTarget(
                host=self.stream_processor_ssh_host,
                user=self.stream_processor_ssh_user,
                key_path=self.stream_processor_ssh_key_path,
            ),
            "project": SshTarget(
                host=self.project_ssh_host,
                user=self.project_ssh_user,
                key_path=self.project_ssh_key_path,
            ),
        }
```

- [ ] **Step 6: compose와 env 예시를 갱신한다**

`infra/compose/monitoring.yaml`의 `ops-agent` environment에 추가한다.

```yaml
      PROJECT_SSH_HOST: ${PROJECT_SSH_HOST:?PROJECT_SSH_HOST is required}
      PROJECT_SSH_USER: ${PROJECT_SSH_USER:-ec2-user}
      PROJECT_SSH_KEY_PATH: /run/ops-agent/project.pem
```

volumes에 추가한다.

```yaml
      # 커밋되지 않는 파일이다(.gitignore의 *.pem) — Monitoring EC2에 직접 준비해야 한다.
      - ../monitoring/ops-agent/project.pem:/run/ops-agent/project.pem:ro
```

`infra/monitoring/.env.example`에 추가한다.

```bash
# serving-api / node-exporter restart용 SSH 대상(Project EC2, #547).
PROJECT_SSH_HOST=
PROJECT_SSH_USER=ec2-user

# 위 host 접속용 개인키를 Monitoring EC2의 아래 경로에 직접 준비한다(커밋 안 됨).
#   infra/monitoring/ops-agent/project.pem
```

- [ ] **Step 7: 통과를 확인한다**

Run: `uv run --package ops-agent pytest services/ops-agent/tests/test_policy.py services/ops-agent/tests/test_config.py -q`
Expected: PASS

- [ ] **Step 8: 커밋한다**

```bash
git add services/ops-agent/src services/ops-agent/tests infra/
git commit -m "feat: add action specs for serving api and node exporter"
```

---

### Task 4: orchestrator를 명세로 구동하고 alert를 연결한다

**Files:**
- Modify: `services/ops-agent/src/ops_agent/orchestrator.py`, `__init__.py`
- Modify: `services/ops-agent/tests/test_orchestrator.py`, `test_app.py`
- Modify: `infra/monitoring/grafana/provisioning/alerting/rules.yaml`
- Modify: `services/ops-agent/README.md`
- Modify: `context/services.md`

**Interfaces:**
- Consumes: Task 1~3의 모든 산출물
- Produces: `OpsAgentOrchestrator(..., ssh_targets: Mapping[str, SshTarget], ...)`

- [ ] **Step 1: 실패 테스트를 쓴다**

`test_orchestrator.py` 상단에 추가한다.

```python
PROJECT_TARGET = SshTarget(host="5.6.7.8", user="ec2-user", key_path="/keys/project.pem")
```

`make_orchestrator`에서 `ssh_target=TARGET`을 지우고 인자를 추가한다.

```python
    ssh_targets=None,
    remediate=None,
):
    ...
        ssh_targets=ssh_targets or {"spark": TARGET, "project": PROJECT_TARGET},
        remediate=remediate or make_fake_remediate(),
```

테스트를 추가한다.

```python
    def test_serving_api_down_is_restarted_on_the_project_host(self, tmp_path):
        seen = {}

        def spy(target, container, *, action):
            seen["target"] = target
            seen["container"] = container
            return make_fake_remediate()(target, container, action=action)

        orchestrator, _slack = make_orchestrator(
            tmp_path=tmp_path, statuses=[down_status(), healthy_status()], remediate=spy
        )
        incident = Incident.from_grafana_alert(
            grafana_alert(alertname="ServingApiDown", service="serving-api")
        )

        outcome = orchestrator.handle(incident)

        assert outcome.remediation is not None
        assert seen["target"] == PROJECT_TARGET
        assert seen["container"] == "serving-api"

    def test_the_node_exporter_is_restarted_on_the_spark_host(self, tmp_path):
        seen = {}

        def spy(target, container, *, action):
            seen["target"] = target
            seen["container"] = container
            return make_fake_remediate()(target, container, action=action)

        orchestrator, _slack = make_orchestrator(
            tmp_path=tmp_path, statuses=[down_status(), healthy_status()], remediate=spy
        )
        incident = Incident.from_grafana_alert(
            grafana_alert(alertname="SparkNodeExporterDown", service="infrastructure")
        )

        orchestrator.handle(incident)

        assert seen["target"] == TARGET
        assert seen["container"] == "node-exporter"

    def test_an_unresolvable_ssh_target_escalates_instead_of_guessing(self, tmp_path):
        # project 설정이 없는데 serving-api를 재시작하려 들면 엉뚱한 호스트를 건드릴 수 있다.
        orchestrator, slack_client = make_orchestrator(
            tmp_path=tmp_path, statuses=[down_status()], ssh_targets={"spark": TARGET}
        )
        incident = Incident.from_grafana_alert(
            grafana_alert(alertname="ServingApiDown", service="serving-api")
        )

        outcome = orchestrator.handle(incident)

        assert outcome.remediation is None
        assert outcome.escalated is True
        assert "no ssh target" in block_text(main_messages(slack_client)[0])

    def test_an_alert_without_a_spec_is_escalated_without_touching_any_host(self, tmp_path):
        orchestrator, slack_client = make_orchestrator(
            tmp_path=tmp_path, statuses=[down_status()]
        )
        incident = Incident.from_grafana_alert(grafana_alert(alertname="SomethingNew"))

        outcome = orchestrator.handle(incident)

        assert outcome.remediation is None
        assert outcome.escalated is True
        body = block_text(main_messages(slack_client)[0])
        assert "읽기만 한 명령" not in body
```

- [ ] **Step 2: 실패를 확인한다**

Run: `uv run --package ops-agent pytest services/ops-agent/tests/test_orchestrator.py -q`
Expected: FAIL — `TypeError: __init__() got an unexpected keyword argument 'ssh_targets'`

- [ ] **Step 3: `orchestrator.py`를 명세 기반으로 바꾼다**

import를 정리한다.

```python
from collections.abc import Callable, Mapping

from ops_agent.diagnostics import ContainerDiagnostics, collect_container_diagnostics
from ops_agent.policy import ACTION_SPECS, PolicyDecision, decide
from ops_agent.prometheus_client import HealthCheck, PrometheusClient, ServiceStatus
from ops_agent.remediation import RemediationResult, restart_container
```

생성자에서 `ssh_target: SshTarget`을 `ssh_targets: Mapping[str, SshTarget]`으로 바꾸고
Task 1의 임시 `_default_diagnose`/`_default_remediate`를 지운 뒤 기본값을 바꾼다.

```python
        diagnose: Callable[..., ContainerDiagnostics] = collect_container_diagnostics,
        remediate: Callable[..., RemediationResult] = restart_container,
```

`handle()`의 재검증 앞에 명세 조회를 넣는다.

```python
        spec = ACTION_SPECS.get(incident.alertname)
        if spec is None:
            # 담당 명세가 없으면 확인할 PromQL도 진단할 컨테이너도 정해지지 않는다.
            # 넘겨짚지 않고 어떤 호스트도 건드리지 않는다.
            return self._escalate_without_spec(incident)

        status = self._prometheus.evaluate(spec.health)
```

healthy 분기와 진단 호출을 명세로 바꾼다.

```python
            diagnostics = self._diagnose(self._ssh_targets[spec.ssh_target_key], spec.container)
```

정책 판정과 SSH 대상 해결을 넣는다.

```python
        policy_decision = decide(incident, spec)
        self._log_stage(
            "policy", incident, allowed=policy_decision.allowed, reason=policy_decision.reason
        )

        target = self._ssh_targets.get(spec.ssh_target_key)
        if target is None:
            # 설정이 없는 호스트를 넘겨짚어 엉뚱한 곳에 docker restart를 쏘지 않는다.
            policy_decision = PolicyDecision(
                action=spec.action,
                allowed=False,
                reason=f"no ssh target configured for {spec.ssh_target_key!r}",
            )
            diagnostics = ContainerDiagnostics("unknown", None, "", ())
        else:
            diagnostics = self._diagnose(target, spec.container)
        self._log_diagnose(incident, diagnostics)
```

조치 실행과 복구 폴링을 명세로 바꾼다.

```python
        remediation_result = self._remediate(target, spec.container, action=spec.action.value)
        ...
        post_status = self._wait_for_recovery(spec.health)
```

`_wait_for_recovery`가 check를 받게 한다.

```python
    def _wait_for_recovery(self, check: HealthCheck) -> ServiceStatus:
        status = self._prometheus.evaluate(check)
        elapsed = 0.0
        while not status.is_healthy and elapsed < self._recovery_wait_seconds:
            self._sleep(self._recovery_poll_interval_seconds)
            elapsed += self._recovery_poll_interval_seconds
            status = self._prometheus.evaluate(check)
        return status
```

`_escalate_without_spec`을 추가한다.

```python
    def _escalate_without_spec(self, incident: Incident) -> IncidentOutcome:
        decision = decide(incident, None)
        empty = ContainerDiagnostics("unknown", None, "", ())
        status = ServiceStatus(code=None, label="NOT CHECKED", instance=None, healthy_code=0)
        self._notify(incident, status, empty, decision, remediation=None, recovered=False)
        return IncidentOutcome(
            incident=incident,
            handled=True,
            reverified_status=None,
            policy=decision,
            diagnostics=None,
            remediation=None,
            recovered=False,
            escalated=True,
            summary=f"{incident.alertname}: no action spec, escalated",
        )
```

- [ ] **Step 4: `__init__.py`와 `test_app.py`를 맞춘다**

`__init__.py`의 `ssh_target=SshTarget(...)`을 `ssh_targets=config.ssh_targets()`로 바꾸고
쓰이지 않게 된 `SshTarget` import를 지운다. `test_app.py`가 orchestrator를 만든다면
같은 방식으로 고친다.

- [ ] **Step 5: alert rule 2개를 추가한다**

`rules.yaml`에 새 그룹을 추가한다. `service: serving-api`는 기본 라우팅으로 ops-agent에
가므로 notification policy 변경이 필요 없다.

```yaml
  - orgId: 1
    name: serving-api
    folder: Ops Agent
    interval: 1m
    rules:
      # metrics 서버가 uvicorn과 같은 프로세스라 up이 곧 프로세스 생사다. DB만 죽으면
      # 앱은 살아서 metrics를 계속 내보내므로 여기 걸리지 않는다 — 재시작이 무의미한
      # 경우를 자동으로 걸러낸다.
      - uid: serving-api-down
        title: ServingApiDown
        condition: C
        data:
          - refId: A
            datasourceUid: prometheus
            relativeTimeRange: {from: 300, to: 0}
            model:
              editorMode: code
              expr: up{job="serving-api"}
              instant: true
              range: false
              refId: A
          - refId: C
            datasourceUid: __expr__
            model:
              type: threshold
              refId: C
              expression: A
              conditions:
                - evaluator: {type: lt, params: [1]}
        noDataState: NoData
        execErrState: Error
        # 배포 중 재시작을 장애로 오인하지 않게 StreamProcessorDown과 같은 값을 쓴다.
        for: 2m
        labels:
          service: serving-api
          severity: high
          auto_remediate: "true"
        annotations:
          summary: "serving-api가 2분 이상 scrape되지 않음 — 프로세스/컨테이너 문제로 보임"
```

`infrastructure` 그룹에 아래를 추가한다.

```yaml
      # NodeDown과 달리 spark 호스트만 본다 — alertname 하나로 조치 대상이 정해져야
      # ops-agent가 라벨을 보지 않아도 된다. 호스트 자체가 죽은 경우는 SSH가 실패해
      # 자연히 escalation된다.
      - uid: spark-node-exporter-down
        title: SparkNodeExporterDown
        condition: C
        data:
          - refId: A
            datasourceUid: prometheus
            relativeTimeRange: {from: 300, to: 0}
            model:
              editorMode: code
              expr: up{job="spark-node"}
              instant: true
              range: false
              refId: A
          - refId: C
            datasourceUid: __expr__
            model:
              type: threshold
              refId: C
              expression: A
              conditions:
                - evaluator: {type: lt, params: [1]}
        noDataState: NoData
        execErrState: Error
        for: 2m
        labels:
          service: spark-node
          severity: high
          auto_remediate: "true"
        annotations:
          summary: "spark EC2의 node_exporter가 2분 이상 scrape되지 않음 — exporter만 죽었으면 재시작한다"
```

**기존 `NodeDown`에서 `spark-node`를 뺀다.** 그대로 두면 같은 장애로 Slack 알림이 두 번
간다(`NodeDown` → infra-slack, `SparkNodeExporterDown` → ops-agent).

```yaml
              expr: up{job=~"project-node|monitoring-node"}
```

annotations의 summary도 "spark 호스트는 SparkNodeExporterDown이 담당한다"고 덧붙인다.

> `service: spark-node`로 둔 이유: `infrastructure`로 두면 notification-policies의
> `service: infrastructure` 매처에 걸려 `infra-slack`으로만 가고 ops-agent를 거치지
> 않는다. 라우팅 파일을 고치지 않으려고 라벨을 다르게 뒀다.

- [ ] **Step 6: 구성 파일이 파싱되는지 확인한다**

```bash
uv run --package ops-agent python -c "
import yaml
for path in [
    'infra/monitoring/grafana/provisioning/alerting/rules.yaml',
    'infra/compose/monitoring.yaml',
]:
    yaml.safe_load(open(path))
    print('OK', path)
"
```

Expected: 두 줄 모두 `OK`

- [ ] **Step 7: 문서를 갱신한다**

`services/ops-agent/README.md`의 "현재 구현된 조치는 `restart_stream_processor` 뿐입니다"
항목을 바꾼다.

```markdown
- 구현된 조치는 셋이다 — `restart_stream_processor`(Spark EC2),
  `restart_serving_api`(Project EC2), `restart_node_exporter`(Spark EC2).
  어떤 alert가 어떤 조치로 이어지는지는 `policy.py`의 `ACTION_SPECS`가 유일한 정의다.
  조치를 늘리려면 그 dict에 항목을 추가하고 `IMPLEMENTED_ACTIONS`에 등록한다.
- serving-api는 DB 장애로는 재시작되지 않는다. metrics 서버가 앱과 같은 프로세스라
  `up{job="serving-api"}`가 프로세스 생사와 일치하고, DB만 죽으면 앱은 살아 있어
  `ServingApiDown`이 발화하지 않는다.
- `project-node`/`monitoring-node`의 exporter는 자동 조치 대상이 아니다.
  `monitoring-node`는 ops-agent가 같은 호스트에 있어 재시작하려면 docker socket
  마운트가 필요한데 호스트 root 권한과 동등하다(ADR-0013).
```

환경변수 표에 세 줄을 추가한다.

```markdown
| `PROJECT_SSH_HOST` | ✅ | - | serving-api가 있는 Project EC2 |
| `PROJECT_SSH_USER` | - | `ec2-user` | |
| `PROJECT_SSH_KEY_PATH` | ✅ | - | 위 host에 접속할 개인키 경로 |
```

"실제 연결 시 사람이 해야 할 설정" 2번에 Project EC2용 키도
`infra/monitoring/ops-agent/project.pem`에 준비해야 한다고 덧붙인다.

`context/services.md`에 ops-agent 절을 신설한다(설계 문서 §7이 지적한 누락이다).

```markdown
### ops-agent

Grafana alert를 받아 Prometheus로 재검증하고, 저위험 조치만 자동 실행한 뒤 결과를
Slack으로 알린다. Monitoring EC2에서 동작하며 인바운드 요청은 Grafana webhook 하나만
받는다 — Slack은 출력 전용이다.

무엇을 자동 실행해도 되는지의 판정 기준은
[ADR-0013](../docs/adr/0013-immediate-remediation-without-slack-approval.md)에 있다.
alert가 어떤 조치로 이어지는지의 최종 정의는
`services/ops-agent/src/ops_agent/policy.py`의 `ACTION_SPECS`다.
```

- [ ] **Step 8: 전체 검증을 돌린다**

```bash
uv sync --all-packages
uv run --all-packages ruff check .
JAVA_HOME=/opt/homebrew/opt/openjdk@21 uv run --all-packages pytest
```

Expected: 모두 통과

- [ ] **Step 9: 커밋한다**

```bash
git add services/ops-agent infra/ context/
git commit -m "feat: restart serving api and node exporter automatically"
```

---

## 완료 후 확인

| #547 완료 조건 | 담당 Task |
| --- | --- |
| serving-api 프로세스만 죽으면 재시작, DB 장애면 알림만 | Task 3, 4 (`up`이 DB 장애를 애초에 거른다) |
| node-exporter만 죽으면 재시작, 호스트가 죽으면 알림만 | Task 3, 4 (SSH 실패가 자연히 가른다) |
| `monitoring-node`는 자동 조치 대상이 아니다 | Task 4 (규칙이 spark만 본다) |
| alert 라벨에 임의 문자열이 들어와도 SSH argv에 반영되지 않는다 | Task 3 (`test_container_names_are_literals_not_taken_from_alerts`) |
| Project EC2 설정이 없으면 기동 시점에 실패 | Task 3 |
| ruff / pytest 통과 | Task 4 Step 8 |

## 배포 후 사람이 확인해야 할 것

자동 테스트로 덮이지 않는다.

1. Monitoring EC2에 `infra/monitoring/ops-agent/project.pem`을 준비하고 Project EC2의
   `~/.ssh/authorized_keys`에 공개키를 등록했는지.
2. Grafana UI에서 alert rule 4개(`StreamProcessorDown`, `StreamProcessorStale`,
   `ServingApiDown`, `SparkNodeExporterDown`)가 보이고, `NodeDown`이 더 이상
   spark-node를 잡지 않는지. 로컬 UI 주소와 admin 계정을 사용자에게 알려 직접 확인하게 한다.
3. Slack에 실제로 Block Kit 메시지가 의도대로 렌더링되는지(#546에서도 미검증으로 남았다).
