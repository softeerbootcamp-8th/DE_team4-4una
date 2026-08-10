---
owner: data-engineering
status: proposed
last_reviewed: 2026-08-10
---

# Service Map

All listed packages exist as independent `uv` workspace members, but currently
contain skeleton entry points only.

| Workspace member | Proposed responsibility | Primary inputs | Primary outputs |
| --- | --- | --- | --- |
| `libs/de4-core` | Shared IDs, enums, event contracts, dataset contracts, configuration primitives | Accepted cross-service designs | Importable versioned contracts |
| `services/batch-jobs` | Download, snapshot, validate, and normalize reference data; prepare deterministic TLC sample; run Spark map-matching and monthly score jobs if no separate Spark package is added | NYC/OSM reference sources, HVFHV data, S3 Bronze | Road environment, trip sample, Silver matches, monthly Gold dataset |
| `services/sensor-producer` | Route trips and replay deterministic synthetic vehicle observations with `trip_seq` in wall-clock time | Road environment, trip sample, vehicle profiles | Kafka dispatch and Bronze-shape sensor events without `segment_id` |
| `services/stream-processor` | Validate and persist Kafka sensor records without changing their raw meaning | Kafka events, shared contracts | Immutable S3 Bronze `sensor_event` records |
| `services/gold-loader` | Load a completed monthly gold snapshot into the serving store idempotently | Gold score dataset | Serving records and load audit |
| `services/serving-api` | Return the latest available segment x vehicle-type score and provenance | Serving store | HTTP API responses |
| `services/orchestration` | Coordinate monthly reference jobs, replay runs, score jobs, and publication | Schedules and run configuration | Workflow state and run metadata |
| `services/dashboard` | Visualize coverage, latest scores, pipeline status, and possibly simulated movement | Serving API and operational metadata | Human-facing views |

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
- Whether trip sampling belongs to the monthly orchestration workflow or to a
  simulation-specific preparation command.
- Whether the dashboard is required for the first end-to-end milestone.
