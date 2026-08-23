---
owner: data-engineering
status: proposed
last_reviewed: 2026-08-23
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
  -> complete valid-row set in deterministic request/source-row order
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

The implemented `batch-jobs` hourly path now follows this dependency order:

```text
Bronze `sensor_event`
  -> hourly contract validation and cleansing
     -> target-hour cleansing quarantine (persisted)
     -> accepted typed events (execution-local DataFrame)
        -> hourly GPS-to-road-segment matching
        -> segment x vehicle-profile features (persisted)
  -> hourly comfort scoring + rejected output
  -> Gold window aggregation
  -> S3 Gold `standard_segment_comfort_score` snapshot
  -> idempotent PostgreSQL serving-store upsert
```

T1 and T2 run in the same Spark session. T1 cleanses every whole Bronze hour
overlapping T2's exact lookback/lookahead interval, while only the requested
target hour's quarantine is replaced. T2 filters the passed DataFrame to its
exact event-time interval before feature calculation. No cleansed-event dataset
is written to and read back from S3.

The Airflow `sensor_processing` task invokes the combined command with the data
interval start as the target hour. The DAG then runs scoring and publication in
order, preserving the exclusive interval end as publication `as_of`. It then
loads the standard score with the same `as_of` and refreshes every current-score
row. A separate 15-minute DAG collects zone weather and refreshes only the
segments whose zone changed:

```text
Silver `hourly_comfort_score`
  -> 168-hour rollup -> Gold `standard_segment_comfort_score`
     + `latest_zone_weather` (Open-Meteo, every 15 minutes)
     -> weather adjustment -> Gold `current_segment_comfort_score`
```

Segments with no taxi zone stop at `standard_segment_comfort_score`; the current
table requires a zone.

## Required traceability

Starting with an API record, an agent or contributor should be able to identify:

1. The gold record and calculation run.
2. The algorithm, feature, and contract versions.
3. The included traversal and raw-event partitions.
4. The simulation runs and vehicle profiles.
5. The trip and road-environment snapshots.
6. Checksums and versions for original source files.

## Physical partitioning

The implemented Stream Processor partitions Bronze sensor events by UTC sensor
measurement time:

- Raw events: `event_date=YYYY-MM-DD/hour=HH`. Malformed payloads without a
  usable `event_time` use the Kafka record timestamp so they remain available
  to the hourly quarantine path.
- Each Bronze output path has one durable Structured Streaming checkpoint.
  Replacing the checkpoint while reusing the output path can collide with the
  file sink's `_spark_metadata` batch IDs and advance Kafka offsets without
  creating Parquet files.

The following physical choices remain open and should follow measured access
patterns:

- Traversals: score/source period and canonical segment partition strategy.
- Gold scores: score period and algorithm version.

Do not finalize partition columns until expected file volumes and query patterns
are measured. Avoid very high-cardinality partitions such as one directory per
trip or road segment.
