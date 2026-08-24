---
owner: data-engineering
status: draft-contract
last_reviewed: 2026-08-23
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
| `event_time` | Timezone-aware UTC logical measurement time using the Trip's actual dispatch UTC date, TLC clock time, and any intra-Trip source-day offset |
| `_ingested_at` | Wall-clock time at which Spark loaded a record into Bronze |
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
- physical source-row number and trip-identity algorithm version
- plan status or rejection reason

## Dispatch event

**Grain:** one valid trip row from the configured monthly source file becoming
eligible for simulation.

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
- heading, signed front-wheel `steering_angle` in degrees, and longitudinal,
  lateral, and vertical acceleration
- longitudinal `jerk_x`, lateral `jerk_y`, and vertical `jerk_z` in m/s³
- legacy `jerk`, retained as an exact alias of `jerk_x`
- non-negative RMS-like `steering_vibration` amplitude in m/s²
- `_run_id`; Spark adds `_ingested_at` when it writes the Bronze record

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

## Route evaluation request and response

**Implemented** by `POST /api/v1/routes/evaluate` in `services/serving-api`
(issue #269). The executable contract is
`services/serving-api/src/serving_api/schemas.py`
(`RouteEvaluationRequest`, `RouteEvaluationResponse`), which is authoritative
for field names and types; FastAPI publishes it as OpenAPI at `/openapi.json`.
This section records the grain and the rules that the schema alone does not
express.

**Request grain:** one `vehicle_profile_id` plus a list of candidate routes,
each a `route_id` and its `segment_ids` in traversal order. Distance and
duration are deliberately not accepted — the caller already has them and
compares them itself; this endpoint contributes only comfort.

**Response grain:** one row per requested route, carrying the route score and
the two intermediate values that produced it (`average_comfort_score`,
`worst_quartile_comfort_score`), plus `recommended_route_id` and the vehicle
profile the scores were actually computed for.

Rules the implementation fixes:

- A requested `vehicle_profile_id` that is absent from `vehicle_profile` or not
  `is_active` is resolved to the vehicle-agnostic sentinel `0` rather than
  failing the request (issue #272), and the response reports
  `requested_vehicle_profile_id`, `effective_vehicle_profile_id`, and
  `vehicle_profile_fallback` so the caller cannot mistake an all-vehicle score
  for their own vehicle's. The substitution is also logged at WARNING, since a
  200 response would otherwise hide a caller sending a bad id repeatedly. The
  sentinel itself is not looked up — migration `0003` guarantees the row.

  All three serving endpoints resolve the profile this way and report it with
  the same three field names, so a caller does not check for a substitution
  differently per endpoint. On the point lookup those fields sit alongside the
  score's own `vehicle_profile_id`, which describes the returned row rather
  than the request, and therefore always equals the effective profile.

- Routes are returned sorted by `comfort_score` descending, and
  `recommended_route_id` is the first of them. There is no `rank` field — it
  would restate the array order and could contradict it. Tied routes keep
  their request order.
- Segment scores are read through the same current-then-standard fallback as
  the point lookup, for the deduplicated union of every candidate's segments at
  once. Score reads stay at two round trips at most (one per table) no matter
  how many candidates are compared, and segments shared between candidates are
  read once. A non-sentinel profile adds one further round trip for the
  `vehicle_profile` check above.
- A segment with no score in either table fails the request with `404` (see
  `context/comfort-score.md`, "Route comfort score").
- Bounds are per request, not per route: a capped number of candidate routes
  and a capped number of distinct segments. The segment cap is set
  independently from the comfort-scores batch-read endpoint's own limit
  (issue #414) — the two happened to share one constant before that issue,
  but no longer do. See `services/serving-api/src/serving_api/config.py` for
  the current values.
- Scores are rounded to two decimal places before serialization.
