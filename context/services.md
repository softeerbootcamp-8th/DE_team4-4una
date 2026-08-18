---
owner: data-engineering
status: proposed
last_reviewed: 2026-08-18
---

# Service Map

All listed packages exist as independent `uv` workspace members. The table
describes their proposed ownership boundaries; implementation maturity varies
and does not by itself settle those boundaries.

| Workspace member | Proposed responsibility | Primary inputs | Primary outputs |
| --- | --- | --- | --- |
| `libs/de4-core` | Shared IDs, enums, event contracts, dataset contracts, configuration primitives | Accepted cross-service designs | Importable versioned contracts |
| `services/batch-jobs` | Download, snapshot, validate, and normalize reference data; prepare deterministic TLC sample; run Spark map-matching and monthly score jobs if no separate Spark package is added | NYC/OSM reference sources, HVFHV data, S3 Bronze | Road environment, trip sample, Silver matches, monthly Gold dataset |
| `services/sensor-producer` | Resolve a verified file/S3 environment manifest, route trips, and replay deterministic synthetic vehicle observations with `trip_seq` in wall-clock time | Prepared road environment, trip sample, vehicle profiles | Kafka dispatch and Bronze-shape sensor events without `segment_id` |
| `services/stream-processor` | Validate and persist Kafka sensor records without changing their raw meaning | Kafka events, shared contracts | Immutable S3 Bronze `sensor_event` records |
| `services/gold-loader` | Load a completed monthly gold snapshot into the serving store idempotently | Gold score dataset | Serving records and load audit |
| `services/serving-api` | Return the latest available segment x vehicle-type score and provenance | Serving store | HTTP API responses |
| `services/orchestration` | Coordinate monthly reference jobs, replay runs, score jobs, and publication | Schedules and run configuration | Workflow state and run metadata |
| `services/dashboard` | Visualize coverage, latest scores, pipeline status, and possibly simulated movement | Serving API and operational metadata | Human-facing views |

## Current `batch-jobs` packaging

`services/batch-jobs` is implemented beyond its original skeleton. Importable
code is contained by the `batch_jobs` namespace, including the `cleansing`,
`comfort_score`, `map_matching`, `road_segment`, and `sensor_features`
subpackages. Default YAML configuration and executable PostgreSQL migrations
are package resources under
`services/batch-jobs/src/batch_jobs/resources/`, so they remain available from
an installed wheel rather than depending on the repository working directory.

The service currently exposes Gold calculation/publication and database
migration commands even though the target boundary above assigns Gold loading
to `services/gold-loader`, whose entry point remains a skeleton. OQ-040 records
the required ownership decision; this document does not resolve it implicitly.

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
- Whether serving-store migrations and Gold publication remain in `batch-jobs`
  or move to `gold-loader` (OQ-040).
