---
owner: data-engineering
status: proposed
last_reviewed: 2026-08-26
---

# Service Map

All listed packages exist as independent `uv` workspace members. The table
describes their proposed ownership boundaries; implementation maturity varies
and does not by itself settle those boundaries.

| Workspace member | Proposed responsibility | Primary inputs | Primary outputs |
| --- | --- | --- | --- |
| `libs/de4-core` | Shared IDs, enums, event contracts, dataset contracts, configuration primitives | Accepted cross-service designs | Importable versioned contracts |
| `services/batch-jobs` | Download, snapshot, validate, and normalize reference data; run Spark map-matching and monthly score jobs if no separate Spark package is added | NYC/OSM reference sources, S3 Bronze | Road environment, Silver matches, monthly Gold dataset |
| `services/sensor-producer` | Read one local monthly HVFHV Parquet, route every valid trip, and replay deterministic synthetic vehicle observations with `trip_seq` in wall-clock time | Road environment, monthly HVFHV Parquet, vehicle profiles | Kafka dispatch and Bronze-shape sensor events without `segment_id` |
| `services/stream-processor` | Validate and persist Kafka sensor records without changing their raw meaning | Kafka events, shared contracts | Immutable S3 Bronze `sensor_event` records |
| `services/serving-api` | Return the latest available segment x vehicle-type score and provenance, and rank caller-supplied candidate routes by the comfort of their segments | Serving store | HTTP API responses |
| `services/orchestration` | Coordinate monthly reference jobs, replay runs, score jobs, and publication | Schedules and run configuration | Workflow state and run metadata |
| `services/dashboard` | Visualize road coverage and latest comfort scores on an interactive NYC map | S3 `road_segment` snapshot and Serving API | Human-facing map view |
| `tools/pipeline-perf` | Collect pipeline performance facts from Airflow, EMR Serverless, Spark event logs, and PERF logs, and render baseline/comparison reports | Airflow REST API v2, EMR Serverless API, S3 observability logs | Raw collection JSON and markdown reports under `docs/perf/` |

## Offline tools

`tools/*` members are workspace members for development convenience only. They
are never deployed and no runtime service imports them. `tools/pipeline-perf`
is the first such member; it reads observability data after the fact and writes
reports for humans.

## Current `batch-jobs` packaging

`services/batch-jobs` is implemented beyond its original skeleton. Importable
code is contained by the `batch_jobs` namespace, including the `cleansing`,
`comfort_score`, `map_matching`, `road_segment`, and `sensor_features`
subpackages. Default YAML configuration and executable PostgreSQL migrations
are package resources under
`services/batch-jobs/src/batch_jobs/resources/`, so they remain available from
an installed wheel rather than depending on the repository working directory.

The service exposes Gold calculation/publication and database migration
commands. [ADR-0003](../docs/adr/0003-gold-publication-owned-by-batch-jobs.md)
settles OQ-040 by confirming this is the accepted boundary: `services/batch-jobs`
owns Gold publication and serving-database migrations, and the former
`services/gold-loader` skeleton has been removed.

## Current `serving-api` scope

`services/serving-api` reads the serving store and nothing else. Issue #269
added `POST /api/v1/routes/evaluate`, which scores candidate routes, and that
is deliberately not route planning: the caller supplies the routes and their
segment order, and the service only aggregates the segment scores it already
serves. It holds no road graph and performs no routing, so the boundary
against `services/batch-jobs` (which owns the road environment) is unchanged.

## Current `dashboard` scope

Issue #376 implemented the first dashboard as a Streamlit/Folium application;
issue #435 replaced it with a FastAPI backend serving a React frontend. The
browser talks only to this backend, and S3/serving-API access happens
server-side. It reads the versioned `road_segment` geometry snapshot
directly from S3, but reads
comfort scores only through the serving API batch endpoint. It does not connect
to PostgreSQL, calculate scores, or reproduce the serving API's
current-to-standard fallback policy.

## Current `ops-agent` scope

Grafana alert를 받아 Prometheus로 재검증하고, 저위험 조치만 자동 실행한 뒤 결과를
Slack으로 알린다. Monitoring EC2에서 동작하며 인바운드 요청은 Grafana webhook 하나만
받는다 — Slack은 출력 전용이다.

무엇을 자동 실행해도 되는지의 판정 기준은
[ADR-0013](../docs/adr/0013-immediate-remediation-without-slack-approval.md)에 있다.
어떤 alert가 어떤 조치로 이어지는지의 최종 정의는
`services/ops-agent/src/ops_agent/policy.py`의 `ACTION_SPECS`이며, 이 문서는 그
목록을 복제하지 않는다.

## Boundary rules

- A service must not import another service package.
- Cross-service records and enums must be promoted to `de4-core`.
- Services communicate through versioned datasets, Kafka records, APIs, or
  orchestration interfaces.
- Environment-specific endpoints and credentials belong in configuration, not
  shared model definitions.
- A new deployable component should only be introduced if none of the existing
  service boundaries can own it cleanly.

## Ownership questions

The following boundaries require an explicit decision before implementation:

- Whether the Spark monthly aggregation remains in `batch-jobs` or becomes its
  own workspace member.
- Whether Spark GPS-to-LION map matching lives in `batch-jobs` or a separate
  workspace member. It must produce `sensor_events_matched` before Gold scoring.
- Whether the dashboard is required for the first end-to-end milestone.
