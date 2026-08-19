---
owner: data-engineering
status: proposed
last_reviewed: 2026-08-18
---

# Data Lineage

## Reference-data lineage

```text
Pavement ratings ----\
Speed humps ----------+--> immutable source snapshots
OSM traffic signals --+--> validation and spatial normalization
LION -----------------+--> `road_segment`
Taxi zones -----------/--> `enriched_segment_reference`
```

The LION-based normalized road environment is the shared spatial foundation for endpoint
selection, routing, simulated road effects, monthly grouping, and API identity.
The transformation must preserve source IDs so results can be audited even after
the canonical identifier is selected.

## Simulation lineage

```text
HVFHV source snapshot
  -> eligible rows
  -> deterministic ~1000-trip sample
  -> deterministic pickup/drop-off road points
  -> canonical-segment routes
  -> vehicle-profile assignment
  -> dispatch and configured-frequency sensor events
  -> Kafka
  -> immutable S3 Bronze `sensor_event` records
```

Rejected trips are lineage outputs. Each rejection should retain the source row
identity, stage, reason code, and run ID.

## Score lineage

The target lineage remains:

```text
S3 Bronze `sensor_event`
  -> contract validation and sequence-aware deduplication
  -> GPS-to-LION matching with one retained Silver row per Bronze row
  -> Silver `sensor_events_matched`
  -> monthly feature aggregation
  -> versioned comfort formula
  -> gold segment x vehicle-type dataset
  -> idempotent serving-store load
  -> latest-score API response
```

The implemented local hourly path is orchestrated by `hourly_pipeline` in the
following dependency order:

```text
Bronze `sensor_event`
  -> hourly contract validation and cleansing
  -> Silver `processed_sensor_event` + cleansing quarantine
  -> hourly GPS-to-road-segment matching and segment x vehicle-profile features
  -> hourly comfort scoring + rejected output
  -> Gold window aggregation
  -> idempotent PostgreSQL upsert
```

The Airflow data interval is an explicit UTC hourly interval. Cleansing and
feature generation use its start as the target hour, while publication uses the
exclusive interval end as `as_of`. This keeps the just-completed hour inside the
Gold aggregation window.

## Required traceability

Starting with an API record, an agent or contributor should be able to identify:

1. The gold record and calculation run.
2. The algorithm, feature, and contract versions.
3. The included traversal and raw-event partitions.
4. The simulation runs and vehicle profiles.
5. The trip and road-environment snapshots.
6. Checksums and versions for original source files.

## Partitioning candidates

Physical choices remain open, but likely access patterns should be considered:

- Raw events: event date, simulation run, and possibly vehicle type.
- Traversals: score/source period and canonical segment partition strategy.
- Gold scores: score period and algorithm version.

Do not finalize partition columns until expected file volumes and query patterns
are measured. Avoid very high-cardinality partitions such as one directory per
trip or road segment.
