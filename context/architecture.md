---
owner: data-engineering
status: proposed
last_reviewed: 2026-08-22
---

# Target Architecture

## Current repository state

The repository contains Python 3.12 `uv` workspace members for the shared
library and seven services. Implementation is incremental rather than
skeleton-only: `services/batch-jobs` currently provides reference-environment,
sensor-cleansing and hourly-feature processing in one Spark execution,
hourly-scoring, Gold-publication, and database-migration commands. Its
importable modules, default YAML configuration, and SQL migrations are packaged
under the single `batch_jobs` namespace.

The data flow below reflects the target architecture as decided in
[ADR-0003](../docs/adr/0003-gold-publication-owned-by-batch-jobs.md): Gold
score publication and serving-database migrations remain owned by
`services/batch-jobs`. `services/gold-loader` has been removed; it no longer
exists as a workspace member.

## Target local-first flow

```mermaid
flowchart LR
    R[Monthly reference sources] --> B[batch-jobs]
    B --> RS[(Versioned road environment)]

    T[TLC HVFHV source day] --> B
    B --> TS[(Deterministic trip sample)]

    RS --> P[sensor-producer]
    TS --> P
    V[Vehicle profiles] --> P
    P --> K[Kafka]

    K --> SP[stream-processor]
    SP --> BR[(S3 Bronze: sensor_event)]
    BR --> MM[Spark GPS-to-LION matching]
    RS --> MM
    MM --> SI[(Silver: sensor_events_matched)]
    SI --> S[Spark monthly aggregation]
    RS --> S
    S --> GO[(Gold comfort scores)]

    GO --> DB[(Serving store)]
    DB --> API[serving-api]
    API --> D[dashboard / clients]

    O[orchestration] -. schedules and observes .-> B
    O -. schedules and observes .-> S
    O -. coordinates replay .-> P
```

## Architectural principles

- **Determinism:** source snapshots, configuration, seeds, and algorithm versions
  must be recorded with every run.
- **Stable road identity:** the LION `SegmentID` is canonical from Silver map
  matching through aggregation, storage, and the API. Bronze sensor events retain
  only GPS coordinates.
- **Raw-data retention:** retain raw simulated measurements so scoring algorithms
  can be changed without repeating the wall-clock replay.
- **Contract ownership:** shared records and cross-service identifiers belong in
  `libs/de4-core`.
- **Local-first boundaries:** use interfaces that can map to managed AWS services,
  without requiring AWS for development.
- **Observable runs:** every dataset and score should include a run ID, source
  period, creation time, and contract or algorithm version.

## Logical data layers

| Layer | Content | Mutation policy |
| --- | --- | --- |
| Source snapshot | Unmodified downloaded source files and metadata | Immutable |
| Raw/bronze | Kafka simulation events persisted to S3 as received | Append-only |
| Validated/silver | One retained row per Bronze event with GPS-to-LION match and versioned event flags | Rebuildable |
| Gold | Monthly road segment x vehicle-type comfort score | Versioned replacement |
| Serving | Latest and optionally historical gold records | Loaded idempotently |

S3 is the target Bronze store. The file/table format, local development access
pattern, serving database, and remaining AWS service mapping are open decisions.
They should be recorded in ADRs rather than assumed from this diagram.

## Important boundaries

### Monthly environment build

Reference sources are normalized into a versioned, routable segment network.
Spatial joins attach pavement and speed-hump attributes to the canonical segment.
Taxi-zone geometry is retained to choose valid deterministic endpoints.

### Wall-clock replay

The producer reads the pinned trip sample and road environment, chooses or loads
a vehicle profile, computes a route, and emits time-ordered observations. It must
be restartable without producing logically different events.

The EC2 runtime can resolve the active S3 environment pointer or an immutable
manifest and verifies its prepared road and taxi-zone artifacts before routing.
The bounded TLC input remains local until the separate S3 trip-reader work lands.

### Stream collection

Kafka decouples replay from persistence. The stream processor validates shared
contracts, handles duplicates according to the accepted idempotency key, and
writes raw events to S3 without discarding variables that may be useful to later
scoring experiments.

### Hourly cleansing-to-feature execution

The implemented local hourly batch path uses one Spark application boundary for
cleansing and feature calculation:

```mermaid
flowchart LR
    BR[(S3 Bronze: sensor_event)] --> T1[Hourly cleansing]
    T1 --> Q[(Cleansing quarantine)]
    T1 -. typed in-memory DataFrame .-> T2[Map matching and feature calculation]
    RS[(Versioned road environment)] --> T2
    T2 --> HF[(Silver: hourly_segment_features)]
```

T1 reads every Bronze hour touched by T2's lookback and lookahead window,
validates and deduplicates each hour, and passes accepted rows directly to T2 in
the same Spark session. There is no persisted `processed_sensor_event` boundary.
Only the target-hour quarantine and `hourly_segment_features` are stored. The
Airflow DAG invokes the combined command once in its `sensor_processing`
TaskGroup, then continues to scoring and publication.

`standard_score_pipeline`(renamed from `hourly_pipeline`, issue #229) loads
`standard_segment_comfort_score` from `hourly_comfort_score` and stops there — it
does not write `current_segment_comfort_score`. A separate 15-minute
`zone_weather_pipeline` (renamed from `weather_pipeline`, issue #230) collects
Open-Meteo weather into `latest_zone_weather` and, when its changed-zone gate
(reusing `find_changed_zones()`) finds zones whose weather actually changed,
publishes `ZONE_WEATHER_ASSET` (issues #207, #216, #230).

Writing `current_segment_comfort_score` from two independently scheduled DAGs
was a known race (issue #228) — `zone_weather_pipeline`'s changed-zone read
could race a full standard-driven refresh, letting a stale weather snapshot
overwrite a fresher one. ADR-0007 (accepted) moves that write into a single
DAG, `current_score_pipeline` (issue #231), scheduled by
`AssetAny(STANDARD_SCORE_ASSET, ZONE_WEATHER_ASSET)` so either producer DAG
wakes it, with `max_active_runs=1` serializing writes. It inspects
`context["triggering_asset_events"]` to pick the recompute mode: a full
refresh when `STANDARD_SCORE_ASSET` triggered, changed-zones-only when only
`ZONE_WEATHER_ASSET` did:

```mermaid
flowchart LR
    HC[(Silver: hourly_comfort_score)] --> ST[Standard score load]
    ST --> SS[(Gold: standard_segment_comfort_score)]
    ST -. outlet .-> SA{{STANDARD_SCORE_ASSET}}
    W[Open-Meteo] --> WC[Weather collection]
    WC --> LZ[(latest_zone_weather)]
    LZ --> DZ[Changed-zone gate]
    DZ -. outlet, only if changed .-> ZA{{ZONE_WEATHER_ASSET}}
    SA --> CS[current_score_pipeline: run_current_score]
    ZA --> CS
    CS --> CU[(Gold: current_segment_comfort_score)]
```

Weather collection and the changed-zone gate need no Spark, so both run as
Python tasks inside the Airflow scheduler rather than as separate containers —
`current_score_pipeline`'s single task does too.

### Silver map matching

Spark preserves a one-to-one Bronze-to-Silver row relationship while matching
GPS coordinates against a pinned LION snapshot. Match status and diagnostic
fields are retained even when `segment_id` is null. Threshold-derived braking,
acceleration, and discomfort flags first appear here with `scoring_version`.

### Monthly score publication

A Spark job derives stable features and a versioned score. Publication must be
atomic at the score-period level so the API never serves a partially loaded
monthly result.

### Bronze compaction

Bronze `zone_weather_snapshot` accumulates small files from its 15-minute writer. An
independent, low-frequency `bronze_compaction` DAG (no outlets, does not block or gate
other DAGs) merges same-partition objects once they are old enough that no further
writes are expected, verifying row counts before discarding the originals.
`sensor-events` was dropped from this DAG's scope after discovering that in-place
compaction of a Structured Streaming FileStreamSink directory is unsafe (the sink's
`_spark_metadata/` commit log would go stale); its backlog cleanup is deferred to a
follow-up issue. See [ADR-0009](../docs/adr/0009-bronze-compaction-dag.md).
