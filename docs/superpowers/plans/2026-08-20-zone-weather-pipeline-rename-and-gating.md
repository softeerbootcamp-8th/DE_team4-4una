# Zone Weather Pipeline Rename and Gating Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rename `weather_pipeline` to `zone_weather_pipeline`, remove its current-score
recompute task, and make it publish an `Asset` event only when `find_changed_zones()`
finds a changed zone — so a later DAG (`current_score_pipeline`, issue #231) can be the
single writer of `current_segment_comfort_score`.

**Architecture:** ADR-0007 splits the comfort-score current-writer into three DAGs to
remove a stale-overwrite race. This plan implements the second of those DAGs:
`zone_weather_pipeline` becomes collection + change-detection only. A new
`ShortCircuitOperator` (`detect_changed_zones`) reuses the existing
`jobs.current_score.find_changed_zones()` unchanged. Because Airflow registers a
task's declared `outlets` unconditionally whenever that *specific* task instance
reaches `SUCCESS` — and `ShortCircuitOperator` itself always succeeds, only skipping
its downstream tasks when the callable returns falsy (verified by reading
`airflow/models/taskinstance.py::register_asset_changes_in_db` and
`airflow/providers/standard/operators/python.py::ShortCircuitOperator.execute` in the
installed `apache-airflow==3.3.1`) — the outlet cannot live on `detect_changed_zones`
itself, or it would fire on every 15-minute tick regardless of whether anything
changed. A trivial downstream `EmptyOperator` (`publish_zone_weather_asset`) carries
`outlets=[ZONE_WEATHER_ASSET]` instead: it only runs (and only then registers the
asset event) when `detect_changed_zones` does not short-circuit it.

`ZONE_WEATHER_ASSET` is defined in a new `dags/assets.py` module (not inside `jobs/`)
because Airflow's `DagFileProcessor`/`DagBag` adds the DAG bundle root
(`services/orchestration/dags` in local Compose) to `sys.path` for every file it
parses, while `jobs/` is only mounted into the `airflow-scheduler` container's
`PYTHONPATH` (see `infra/compose/airflow.yaml:75-79,90-91`) — `airflow-dag-processor`
and `airflow-webserver` never see it. Since `ZONE_WEATHER_ASSET` must be importable at
DAG **parse** time (it is passed to `outlets=[...]` at module top level, not inside a
deferred function body like the `jobs.*` calls), it has to sit somewhere every
Airflow component can reach when parsing: the dags folder itself.

**Tech Stack:** Python 3.12, Apache Airflow 3.3.1 (`airflow.sdk.Asset`,
`airflow.providers.standard.operators.python.ShortCircuitOperator`,
`airflow.providers.standard.operators.empty.EmptyOperator`), pytest, uv workspace.

**Spec:**
- GitHub issue #230: https://github.com/softeerbootcamp-8th/DE_team4-4una/issues/230
- `docs/adr/0007-split-comfort-score-pipeline-into-three-dags.md` (accepted architecture
  decision this issue implements one third of)

## Global Constraints

- Airflow is pinned to 3.3.1; `Asset`, `AssetAny`, and `outlets` are imported from
  `airflow.sdk` only (ADR-0007).
- Do not change `jobs/current_score.py::find_changed_zones()` or its SQL — reuse it
  as-is (issue #230 exclusion scope).
- Do not touch `dags/hourly_pipeline.py` or create `dags/current_score_pipeline.py` —
  those belong to separate sub-issues (#229, #231) and are out of scope for this
  branch.
- No new third-party dependency: `ShortCircuitOperator` and `EmptyOperator` ship in
  `apache-airflow`'s `standard` provider, already a declared dependency.
- Follow `docs/code-style.md` (PEP 8, 99-char lines, Korean comments at the same
  density as the surrounding file) and `CONTRIBUTING.md`'s commit convention
  (`<type>: <lowercase-imperative-subject>`, no trailing period, one logical change
  per commit).
- All files in this plan live under `services/orchestration/` (single service; no
  cross-service approval needed per AGENTS.md).

---

## File Structure

- **Create** `services/orchestration/dags/assets.py` — the cross-DAG `Asset` registry
  ADR-0007 calls "공용 Asset 모듈". Holds `ZONE_WEATHER_ASSET` now;
  `standard_score_pipeline` (#229) will add `STANDARD_SCORE_ASSET` here later.
- **Create** `services/orchestration/tests/test_assets.py` — shape test for the new
  module, independent of any one DAG.
- **Rename** `services/orchestration/dags/weather_pipeline.py` →
  `services/orchestration/dags/zone_weather_pipeline.py`, then rewrite: drop
  `run_changed_zone_recompute`/`_recompute_changed_zone_scores`, add
  `detect_changed_zones` (`ShortCircuitOperator`) and `publish_zone_weather_asset`
  (`EmptyOperator`, carries the outlet).
- **Rename** `services/orchestration/tests/test_weather_pipeline_dag.py` →
  `services/orchestration/tests/test_zone_weather_pipeline_dag.py`, then rewrite for
  the new task graph.

## Follow-up outside this plan (main session, not a subagent task)

After these tasks are green, `context/architecture.md`'s prose ("The same DAG then
loads `standard_segment_comfort_score` and refreshes every
`current_segment_comfort_score` row, while a separate 15-minute DAG collects...")
becomes inaccurate the moment this merges — the weather DAG no longer refreshes
`current` itself. Run the `update-project-context` skill to decide how much of that
drift to fix now versus once #229/#231 also land (the diagram depicts the *final*
3-DAG shape, which won't fully exist until then). Do not hand-edit the diagram as
part of this plan's tasks.

---

### Task 1: Shared Asset module

**Files:**
- Create: `services/orchestration/dags/assets.py`
- Test: `services/orchestration/tests/test_assets.py`

**Interfaces:**
- Produces: `ZONE_WEATHER_ASSET: airflow.sdk.Asset` with `.name ==
  "zone_weather_changed"` and `.uri == "postgres://latest_zone_weather/"` (Airflow
  normalizes a bare-authority URI by appending `/` — verified against the installed
  `airflow.sdk.Asset` constructor). Task 3's `zone_weather_pipeline.py` imports this
  name via `from assets import ZONE_WEATHER_ASSET`.

- [ ] **Step 1: Write the failing test**

Create `services/orchestration/tests/test_assets.py`:

```python
# dags/assets.py 테스트(#230) — 여러 DAG가 공유하는 Asset 정의가 기대한 모양인지만
# 확인한다. 이 모듈은 각 DAG 파일이 파싱 시점에 import하므로, 여기서 깨지면 모든
# DAG 파싱이 실패한다.

from __future__ import annotations

import sys
from pathlib import Path

from airflow.sdk import Asset

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from assets import ZONE_WEATHER_ASSET


def test_zone_weather_asset_is_an_asset():
    assert isinstance(ZONE_WEATHER_ASSET, Asset)


def test_zone_weather_asset_identifies_the_latest_zone_weather_table():
    assert ZONE_WEATHER_ASSET.name == "zone_weather_changed"
    assert ZONE_WEATHER_ASSET.uri == "postgres://latest_zone_weather/"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --package orchestration pytest services/orchestration/tests/test_assets.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'assets'`

- [ ] **Step 3: Write the module**

Create `services/orchestration/dags/assets.py`:

```python
"""ADR-0007에 따라 여러 DAG가 공유하는 Airflow Asset 정의 모음.

standard_score_pipeline(#229)과 zone_weather_pipeline(#230)이 각자 만든 Asset을
발행하면, current_score_pipeline(#231)이 `AssetAny(STANDARD_SCORE_ASSET,
ZONE_WEATHER_ASSET)`으로 두 producer 중 하나만 트리거되어도 깨어난다.

Airflow는 DAG 번들 경로(로컬 배포에서는 services/orchestration/dags) 전체를
sys.path에 등록해 준다(airflow.dag_processing.dagbag.DagBag). 그래서 이 파일은
jobs/ 패키지처럼 별도 PYTHONPATH 배선 없이 각 DAG 파일에서 `from assets import
...`로 바로 가져다 쓸 수 있다 — jobs/는 scheduler 컨테이너에만 마운트되어
dag-processor/webserver의 파싱 시점에는 보이지 않는다(infra/compose/airflow.yaml
참고). 이 모듈은 각 DAG 파일이 파싱 시점(모듈 최상단)에 그대로 가져다 쓸 수
있어야 하므로, Airflow SDK 이외의 무거운 의존성을 두지 않는다.
"""

from __future__ import annotations

from airflow.sdk import Asset

# zone_weather_pipeline(#230)이 detect_changed_zones를 통과했을 때(변경된 zone이
# 있을 때)만 발행한다.
ZONE_WEATHER_ASSET = Asset(name="zone_weather_changed", uri="postgres://latest_zone_weather/")
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run --package orchestration pytest services/orchestration/tests/test_assets.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add services/orchestration/dags/assets.py services/orchestration/tests/test_assets.py
git commit -m "refactor: add shared assets module for cross-dag asset triggers"
```

---

### Task 2: Rename the DAG and test files (no behavior change)

**Files:**
- Rename: `services/orchestration/dags/weather_pipeline.py` →
  `services/orchestration/dags/zone_weather_pipeline.py`
- Rename: `services/orchestration/tests/test_weather_pipeline_dag.py` →
  `services/orchestration/tests/test_zone_weather_pipeline_dag.py`

**Interfaces:**
- Consumes: none (pure rename; content is untouched in this task).
- Produces: same `dag_id="weather_pipeline"` and task graph as before, just at the
  new file paths — Task 3 changes the content.

- [ ] **Step 1: Rename both files with git mv**

```bash
git mv services/orchestration/dags/weather_pipeline.py \
       services/orchestration/dags/zone_weather_pipeline.py
git mv services/orchestration/tests/test_weather_pipeline_dag.py \
       services/orchestration/tests/test_zone_weather_pipeline_dag.py
```

- [ ] **Step 2: Update the test file's module path constant only**

In `services/orchestration/tests/test_zone_weather_pipeline_dag.py`, change line 14
from:

```python
DAG_PATH = Path(__file__).resolve().parents[1] / "dags" / "weather_pipeline.py"
```

to:

```python
DAG_PATH = Path(__file__).resolve().parents[1] / "dags" / "zone_weather_pipeline.py"
```

Leave every other line (module name `"weather_pipeline"` in
`spec_from_file_location`, all assertions) untouched for now — Task 3 rewrites this
file's content wholesale.

- [ ] **Step 3: Run the test to verify it still passes unchanged**

Run: `uv run --package orchestration pytest services/orchestration/tests/test_zone_weather_pipeline_dag.py -v`
Expected: PASS (6 passed) — renaming the file changed nothing behaviorally yet.

- [ ] **Step 4: Commit**

```bash
git add services/orchestration/dags/zone_weather_pipeline.py \
        services/orchestration/tests/test_zone_weather_pipeline_dag.py
git commit -m "refactor: rename weather_pipeline dag files to zone_weather_pipeline"
```

---

### Task 3: Gate the current-score trigger on changed zones

**Files:**
- Modify: `services/orchestration/dags/zone_weather_pipeline.py`
- Modify: `services/orchestration/tests/test_zone_weather_pipeline_dag.py`

**Interfaces:**
- Consumes: `ZONE_WEATHER_ASSET` from Task 1's `assets.py`
  (`from assets import ZONE_WEATHER_ASSET`); `jobs.current_score.find_changed_zones`
  and `jobs.weather.LatestZoneWeatherJobConfig` (both already exist, unchanged).
- Produces: DAG `zone_weather_pipeline` with tasks `run_weather_collection` →
  `detect_changed_zones` (`ShortCircuitOperator`, callable `_has_changed_zones`) →
  `publish_zone_weather_asset` (`EmptyOperator`, `outlets=[ZONE_WEATHER_ASSET]`).

- [ ] **Step 1: Rewrite the test file for the new task graph (red)**

Replace the full content of
`services/orchestration/tests/test_zone_weather_pipeline_dag.py` with:

```python
# zone_weather_pipeline DAG 구조 검증(실제 task 실행은 로컬에서 수동 확인,
# test_hourly_pipeline_dag.py와 동일 방식). #230: weather_pipeline에서 이름을
# 바꾸고, current 재계산 태스크를 ShortCircuit 게이팅 + Asset 발행으로 교체했다.

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.timetables.base import TimeRestriction
from airflow.timetables.interval import CronDataIntervalTimetable

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
DAG_PATH = DAGS_DIR / "zone_weather_pipeline.py"

# zone_weather_pipeline.py가 모듈 최상단에서 `from assets import ZONE_WEATHER_ASSET`을
# 쓴다. 실제 Airflow는 dags_folder 전체를 sys.path에 등록해 주지만
# (airflow.dag_processing.dagbag), spec_from_file_location으로 파일 하나만 직접
# 로드하는 이 테스트에서는 그 동작을 흉내내야 한다.
sys.path.insert(0, str(DAGS_DIR))


def _load_dag_module():
    spec = importlib.util.spec_from_file_location("zone_weather_pipeline", DAG_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_dag_parses_with_expected_schedule():
    module = _load_dag_module()

    assert module.dag.dag_id == "zone_weather_pipeline"
    assert isinstance(module.dag.timetable, CronDataIntervalTimetable)
    assert module.dag.timetable.summary == "*/15 * * * *"
    assert module.dag.catchup is False
    # 느려진 옛 실행이 새 실행과 겹쳐 latest_zone_weather를 역전시키지 않도록 한다.
    assert module.dag.max_active_runs == 1


def test_dag_uses_a_15_minute_utc_data_interval():
    module = _load_dag_module()

    run_info = module.dag.timetable.next_dagrun_info(
        last_automated_data_interval=None,
        restriction=TimeRestriction(
            earliest=pendulum.datetime(2026, 8, 19, 10, 0, tz="UTC"),
            latest=pendulum.datetime(2026, 8, 19, 10, 30, tz="UTC"),
            catchup=True,
        ),
    )

    assert run_info is not None
    assert run_info.data_interval.start == pendulum.datetime(2026, 8, 19, 10, 0, tz="UTC")
    assert run_info.data_interval.end == pendulum.datetime(2026, 8, 19, 10, 15, tz="UTC")


def test_dag_has_no_current_score_recompute_task():
    module = _load_dag_module()

    task_ids = {task.task_id for task in module.dag.tasks}
    assert task_ids == {
        "run_weather_collection",
        "detect_changed_zones",
        "publish_zone_weather_asset",
    }


def test_dag_collects_then_gates_on_changed_zones():
    module = _load_dag_module()

    collection = module.dag.get_task("run_weather_collection")
    detect = module.dag.get_task("detect_changed_zones")
    publish = module.dag.get_task("publish_zone_weather_asset")

    # 수집이 끝난 뒤에 비교해야 새 impact_signature가 이미 저장된 상태가 된다.
    assert collection.downstream_task_ids == {"detect_changed_zones"}
    assert detect.downstream_task_ids == {"publish_zone_weather_asset"}

    assert isinstance(detect, ShortCircuitOperator)
    assert detect.python_callable is module._has_changed_zones

    assert isinstance(publish, EmptyOperator)


def test_only_the_gated_publish_task_declares_the_zone_weather_outlet():
    module = _load_dag_module()

    detect = module.dag.get_task("detect_changed_zones")
    publish = module.dag.get_task("publish_zone_weather_asset")

    # detect_changed_zones는 조건이 False여도 자기 자신은 SUCCESS로 끝나 하위
    # task만 SKIPPED시킨다. outlets를 여기 두면 변경 zone이 없는 tick에도 매번
    # 이벤트가 발행되므로, outlets는 detect_changed_zones가 SKIPPED시킬 수 있는
    # publish_zone_weather_asset에만 있어야 한다.
    assert detect.outlets == []
    assert publish.outlets == [module.ZONE_WEATHER_ASSET]


def test_run_weather_collection_is_a_python_task_calling_the_collector():
    module = _load_dag_module()

    task = module.dag.get_task("run_weather_collection")
    assert isinstance(task, PythonOperator)
    assert task.python_callable is module._collect_latest_zone_weather


def test_collector_declares_data_interval_end_so_airflow_injects_it():
    import inspect

    module = _load_dag_module()

    parameters = inspect.signature(module._collect_latest_zone_weather).parameters
    assert "data_interval_end" in parameters


def test_retries_faster_than_the_hourly_pipeline():
    module = _load_dag_module()

    # 15분 주기라 hourly_pipeline의 5분 재시도 간격은 너무 길다(주기의 1/3).
    assert module.dag.default_args["retries"] == 2
    assert module.dag.default_args["retry_delay"] == pendulum.duration(minutes=2)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --package orchestration pytest services/orchestration/tests/test_zone_weather_pipeline_dag.py -v`
Expected: FAIL — `test_dag_has_no_current_score_recompute_task`,
`test_dag_collects_then_gates_on_changed_zones`,
`test_only_the_gated_publish_task_declares_the_zone_weather_outlet` fail (old DAG
still has `run_changed_zone_recompute` and no `assets` import); the schedule/catchup
tests still pass since the schedule is unchanged.

- [ ] **Step 3: Rewrite the DAG file (green)**

Replace the full content of `services/orchestration/dags/zone_weather_pipeline.py`
with:

```python
# 15분마다 Open-Meteo 날씨를 수집하고, 재연산이 필요한 zone이 있는지 감시하는 DAG
# (#207 수집, #230 zone_weather_pipeline로 개편 + 게이팅). current 재계산은 이 DAG의
# 책임이 아니다 — current_score_pipeline(#231, ADR-0007)이 이 DAG가 발행하는
# ZONE_WEATHER_ASSET 이벤트를 받아 변경된 zone만 다시 계산한다.
# jobs.weather(orchestration의 lightweight Python job, #209)를 PythonOperator로 직접 실행한다
# — batch-jobs/EMR과 달리 Spark가 필요 없어 docker-outside-of-docker 없이 이 컨테이너에서 바로 돈다.

from __future__ import annotations

import datetime

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sdk import DAG
from airflow.timetables.interval import CronDataIntervalTimetable

from assets import ZONE_WEATHER_ASSET


def _collect_latest_zone_weather(data_interval_end) -> None:
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
    print(
        {
            "requested_zone_count": summary.requested_zone_count,
            "collected_count": summary.collected_count,
            "failed_zone_count": summary.failed_zone_count,
            "snapshot_uri": summary.snapshot_uri,
        }
    )


def _has_changed_zones() -> bool:
    # jobs/current_score.py의 기존 find_changed_zones()를 그대로 재사용한다 — 판정
    # 로직을 여기서 새로 만들지 않는다(#230). 수집 태스크 바로 다음이라, 비교 시점에는
    # impact_signature가 이미 새로 갱신되어 있다.
    import psycopg2
    from jobs.current_score import find_changed_zones
    from jobs.weather import LatestZoneWeatherJobConfig

    config = LatestZoneWeatherJobConfig.from_env()
    connection = psycopg2.connect(
        host=config.postgres_host,
        port=config.postgres_port,
        dbname=config.postgres_db,
        user=config.postgres_user,
        password=config.postgres_password,
    )
    try:
        zones = find_changed_zones(connection)
    finally:
        connection.close()
    print({"changed_zone_count": len(zones)})
    return bool(zones)


with DAG(
    dag_id="zone_weather_pipeline",
    description="Open-Meteo 15분 날씨를 latest_zone_weather에 수집하고 변경 zone을 감시",
    schedule=CronDataIntervalTimetable(
        "*/15 * * * *",
        timezone=pendulum.timezone("UTC"),
    ),
    start_date=datetime.datetime(2026, 8, 19, tzinfo=datetime.UTC),
    catchup=False,
    # 느려진 옛 실행과 새 실행이 겹쳐 latest_zone_weather를 역전시키는 걸 막는다(jobs/weather.py의 WHERE와 이중 방어).
    max_active_runs=1,
    default_args={
        # 15분 주기라 hourly_pipeline의 5분은 너무 길다. retry_delay는 재시도 대기 시간일 뿐 실행 시간 제한이 아니다.
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=2),
    },
    tags=["zone-weather-pipeline"],
) as dag:
    run_weather_collection = PythonOperator(
        task_id="run_weather_collection",
        python_callable=_collect_latest_zone_weather,
    )

    detect_changed_zones = ShortCircuitOperator(
        task_id="detect_changed_zones",
        python_callable=_has_changed_zones,
    )

    # ShortCircuitOperator는 조건이 False여도 자기 자신은 SUCCESS로 끝나고 하위
    # task만 SKIPPED로 건너뛴다. Airflow는 TaskInstance가 SUCCESS로 끝나는 순간
    # 그 task에 선언된 outlets를 무조건 이벤트로 등록하므로(models/taskinstance.py
    # register_asset_changes_in_db), outlets를 detect_changed_zones에 직접 붙이면
    # 변경 zone이 없는 tick에도 매번 이벤트가 발행된다. 그래서 발행 전용 하위 task를
    # 두고 거기에만 outlets를 붙인다 — 이 task는 변경 zone이 없으면 SKIPPED로 끝나
    # 이벤트가 등록되지 않고, 있을 때만 SUCCESS로 끝나 이벤트를 등록한다.
    publish_zone_weather_asset = EmptyOperator(
        task_id="publish_zone_weather_asset",
        outlets=[ZONE_WEATHER_ASSET],
    )

    run_weather_collection >> detect_changed_zones >> publish_zone_weather_asset
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run --package orchestration pytest services/orchestration/tests/test_zone_weather_pipeline_dag.py -v`
Expected: PASS (8 passed)

- [ ] **Step 5: Commit**

```bash
git add services/orchestration/dags/zone_weather_pipeline.py \
        services/orchestration/tests/test_zone_weather_pipeline_dag.py
git commit -m "refactor: gate zone weather current-score trigger on changed zones"
```

---

### Task 4: Workspace-wide verification

**Files:** none (verification only).

- [ ] **Step 1: Sync and lint the whole workspace**

```bash
uv sync --all-packages
uv run --all-packages ruff check .
```

Expected: both succeed with no errors.

- [ ] **Step 2: Run the full test suite**

```bash
uv run --all-packages pytest
```

Expected: all tests pass, including the new/renamed files from Tasks 1–3 and every
other service's existing suite (this task must not touch anything outside
`services/orchestration`, so nothing else should change).

- [ ] **Step 3: Bring up the local Airflow UI and confirm the DAG parses**

Follow the project convention of verifying Airflow changes in the local web UI (start
the Compose stack per `infra/compose/airflow.yaml` / `services/orchestration/README.md`,
then confirm `zone_weather_pipeline` appears with the expected three tasks and no
import errors). Report the local URL and admin credentials to the user so they can
verify themselves.

## Self-Review Notes

- **Spec coverage:** issue #230's 작업 범위 items are covered — file/dag_id rename
  (Task 2), `run_changed_zone_recompute`/`_recompute_changed_zone_scores` removal
  (Task 3), `find_changed_zones()` reuse via `ShortCircuitOperator` (Task 3),
  `ZONE_WEATHER_ASSET` definition in a shared module (Task 1), outlet wired to fire
  only when the short-circuit passes (Task 3, with the asset-registration semantics
  verified against the installed Airflow source rather than assumed), and the test
  rename plus new structural/gating tests (Tasks 2–3). Excluded scope
  (`current_score_pipeline`, `find_changed_zones()` SQL changes) is untouched.
- **Type consistency:** `_has_changed_zones` (Task 3's DAG file) matches
  `module._has_changed_zones` (Task 3's test file); `ZONE_WEATHER_ASSET` (Task 1)
  matches the import in Task 3's DAG file and the `module.ZONE_WEATHER_ASSET`
  reference in Task 3's test file.
- **Placeholder scan:** none found — every step has literal file content or exact
  commands.
