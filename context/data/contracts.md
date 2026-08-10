---
owner: data-engineering
status: draft-contract
last_reviewed: 2026-08-10
future_canonical_path: libs/de4-core/src/de4_core/contracts/
---

# Draft Logical Data Contracts

These are cross-service semantics and proposed records around the supplied table
designs. See [Table Schema Catalog](schema-catalog.md) for the current field-level
tables and [Data Quality Rules](quality-rules.md) for their invariants. When
implemented, exact types, constraints, enums, and serialization versions must
live in `libs/de4-core`.

## Shared identifiers and time fields

| Field | Meaning |
| --- | --- |
| `_run_id` | Simulation, ingestion, or ETL execution according to the owning table |
| `trip_id` | Stable project identifier derived from the source row when necessary |
| `trip_seq` | Zero-based deterministic sensor-sample order within a trip |
| `vehicle_profile_id` | FK to the not-yet-supplied `vehicle_profile` contract |
| `segment_id` | Canonical LION `SegmentID`; intentionally absent from Bronze sensor events |
| `event_time` | Simulated sensor measurement time |
| `_ingested_at` | Wall-clock time at which a record entered a Bronze/source layer |
| `_processed_at` | Wall-clock time at which a Silver record completed processing |

## Road segment reference

**Grain:** one canonical LION segment per snapshot date.

**Confirmed key:** `(segment_id, snapshot_date)`.

The normalized `road_segment` and `enriched_segment_reference` field contracts
are listed in the schema catalog. Pavement, hump, and signal mappings belong in
the enriched reference rather than the base LION table.

## Trip simulation plan

**Grain:** one selected source trip assigned to one simulated vehicle profile and
one route.

**Candidate key:** `(simulation_run_id, trip_id)`.

Candidate fields:

- source row identity and source snapshot ID
- pickup and drop-off LocationIDs
- source request, pickup, and drop-off timestamps
- deterministic pickup and drop-off road points
- ordered canonical route segment IDs
- source and modeled duration and distance
- vehicle type and vehicle-profile version
- endpoint, routing, and motion-model versions
- sampling rank and seed material
- plan status or rejection reason

## Dispatch event

**Grain:** one selected trip becoming eligible for simulation.

Candidate fields:

- simulation run and trip IDs
- source event time and simulation offset
- pickup and drop-off zone IDs
- pickup and drop-off road segment IDs
- vehicle type
- route ID or route version
- event-contract version

## Vehicle observation event

**Grain:** one simulated sensor measurement at the configured sampling frequency
while the passenger is in the vehicle.

**Primary key:** `event_id`.

**Proposed deterministic idempotency key:** `(trip_id, trip_seq)`.

Candidate fields:

- `event_id`, `vehicle_id`, `vehicle_profile_id`, `trip_id`, and `trip_seq`
- `event_time`, latitude, and longitude
- speed in meters per second
- heading and longitudinal, lateral, and vertical acceleration
- jerk, with its axis and unit still to be confirmed
- `_ingested_at` and `_run_id`

Bronze does not carry `segment_id`, road attributes, or threshold-derived event
flags. Those appear in `sensor_events_matched` after Spark map matching.

Every numeric field requires an explicit unit. Optional values need reasoned null
semantics rather than default zeroes.

## Validated segment traversal

**Grain:** one trip and vehicle traversing one canonical segment, allowing a
traversal sequence number if a route visits a segment more than once.

Candidate key:
`(simulation_run_id, trip_id, vehicle_type, traversal_sequence)`.

Candidate fields include entry/exit offsets, observed seconds and meters,
acceleration and jerk summaries, bump counts, road features, validation status,
and source raw-event locations.

## Monthly comfort score

**Grain:** one `segment_id x vehicle_profile_id x score_month`.

**Confirmed key:** `(segment_id, vehicle_profile_id, score_month)`.

The supplied schema includes:

- a 0-100 final score plus vertical, longitudinal, lateral, and pavement scores
- average speed, P95 vertical acceleration, and P95 jerk
- braking, acceleration, discomfort-event, and sample counts
- an explicit driving-data period and enriched-reference date
- confidence, score version, and calculation time

## Minimum API response

Candidate response fields:

- canonical LION segment ID
- vehicle profile ID and a displayable vehicle type from its profile
- latest available score and score scale
- score period
- algorithm version
- calculated and published timestamps
- coverage summary and data-availability status

The endpoint path, transport schema, error format, and missing-score behavior are
open. They should be captured in OpenAPI once accepted.
