---
owner: project-team
status: draft
last_reviewed: 2026-08-26
---

# Project Definition

## Product statement

Build a runnable, local-first data-engineering prototype that provides the latest
available comfort score for a New York City road segment and vehicle type. The
prototype must be designed so its components can later be migrated to AWS.

The intended primary consumer is a route-recommendation developer who needs a
standardized segment-level comfort signal in addition to distance and travel
time.

## Confirmed requirements

### Output

- The serving unit is **road segment x vehicle type**.
- The API returns the latest available comfort score for that unit.
- Standard scores are recalculated hourly over a rolling 168-hour window; a
  weather-adjusted current score refreshes when the standard score or zone
  weather changes.
- The canonical road identifier is the LION `SegmentID`, represented internally
  as `segment_id`.
- The published comfort-score range is 0 through 100. The direction and formula
  still require an accepted definition.

### Vehicle types

Vehicle types are defined by body type x size class rather than manufacturer
or model (accepted 2026-08-18, issue #170 — this supersedes the earlier
Genesis/Grandeur/Avante/EV5 set; see resolved `OQ-013`/`OQ-014`/`OQ-015` in
`open-questions.md`). The canonical set is:

| `vehicle_profile_id` | Profile name | Body type | Size class |
| --- | --- | --- | --- |
| 1 | `VP_SEDAN_COMPACT` | SEDAN | COMPACT |
| 2 | `VP_SEDAN_LARGE` | SEDAN | LARGE |
| 3 | `VP_SUV_COMPACT` | SUV | COMPACT |
| 4 | `VP_SUV_LARGE` | SUV | LARGE |
| 5 | `VP_MPV_LARGE` | MPV | LARGE |

`vehicle_profile_id = 0` (`ALL_VEHICLES`) is a vehicle-agnostic sentinel, not
a vehicle type — see `OQ-038`. See `context/data/schema-catalog.md`
(`vehicle_profile`) for the full column contract and
`services/batch-jobs/src/batch_jobs/resources/migrations/0005_define_vehicle_profiles.sql`
for the executable seed.

Each vehicle profile has deterministic response coefficients that approximate
differences in vertical, longitudinal, lateral, damping, and
steering-vibration behavior (`sensor_producer.domain.VehicleProfile`), not a
direct mass/wheelbase/suspension model.

### Monthly reference data

- NYC Street Pavement Ratings
- NYC speed-hump locations
- LION road-segment geometry
- TLC taxi-zone lookup and geometry
- OSM traffic-signal nodes from a versioned Overpass extract

These sources form the simulated road environment and are refreshed through a
monthly batch pipeline.

### Simulation input and replay

- Use NYC TLC High Volume For-Hire Vehicle trip records.
- Treat every valid completed trip in one pinned monthly Parquet as the eligible
  replay population, then select whole Trips deterministically so the projected
  10 Hz output stays near or below 10 million events per hour.
- Use the source request timestamp for dispatch ordering and wall-clock gaps when
  it is present; the passenger-motion simulation starts at pickup. Start at the
  source replay interval's matching New York weekday and clock time, and map that
  source anchor to one current UTC run anchor for publication.
- Simulate only the occupied passenger journey from pickup to drop-off.
- Because records identify taxi zones rather than exact coordinates, choose
  deterministic valid road points inside the pickup and drop-off zones.
- Rotate the complete valid-row set from the selected source anchor, then apply
  the versioned deterministic Trip sample in request-time and source-row order.
- Replay dispatch gaps and movement in wall-clock time: one simulated second is
  one real second.
- Find a reasonable road route between the chosen points.
- Prefer routing directly over the canonical segment network so simulation and
  API road identifiers agree.

### Processing

- Publish simulated driving measurements through Kafka.
- Persist immutable Bronze sensor events in S3 without a `segment_id`.
- Use Spark to map sensor GPS points to versioned LION segments and retain one
  Silver row for every Bronze row, including unsuccessful matches.
- Use Spark to calculate hourly segment x vehicle-type comfort scores,
  aggregate them into a rolling 168-hour standard score, and derive a
  weather-adjusted current score.
- The score formula is specified and implemented per
  `context/comfort-score.md` but remains proposed pending formal acceptance
  (OQ-006). The pipeline retains the most relevant simulated variables so
  formulas can be re-evaluated without rerunning every trip.

## Initial API capability

The minimum product behavior is a point lookup using a LION segment ID
and supported vehicle type. The response should identify the score, score period,
calculation version, and enough provenance to distinguish current data from
stale or unavailable data.

The endpoint path, score scale, database, and behavior for missing combinations
remain open.

### Candidate-route evaluation

Because the intended consumer is a route-recommendation developer, the point
lookup alone leaves the comparison work to the caller. `services/serving-api`
therefore also accepts several already-planned candidate routes and ranks them
by the comfort of the segments they traverse (issue #269,
`POST /api/v1/routes/evaluate`). The service does not plan routes, and it does
not consider distance or duration — the caller weighs those against the comfort
score itself.

The aggregation from segment scores to one route score, and its provisional
parameters, are documented in `context/comfort-score.md` ("Route comfort
score"); the request and response grain is in `context/data/contracts.md`.

## Scope boundaries

### In scope

- A runnable local end-to-end demonstration
- Monthly ingestion of road reference datasets
- Deterministic, hourly-budgeted FHV Trip replay from a full monthly file
- Synthetic vehicle-motion and comfort-related events at a configurable sampling
  frequency
- Kafka transport and lake persistence
- Spark-based hourly aggregation into a rolling 168-hour standard score
- Latest-score API and a supporting dashboard if time permits
- An architecture with a documented AWS migration path

### Not currently required

- A production navigation product
- Real vehicle telemetry
- Driver travel before passenger pickup
- A scientifically or clinically validated comfort metric
- Live traffic reconstruction unless later accepted as an input
- Full NYC production scale for the first demonstration
- Automatic AWS deployment in the first local prototype

## Success conditions for the prototype

The first end-to-end milestone is successful when the team can:

1. Rebuild a versioned road environment from pinned source snapshots.
2. Reproduce the same eligible-trip order, endpoints, routes, and sensor events
   from the same immutable input file and simulation configuration.
3. Observe selected trip events flowing through Kafka into the local data lake.
4. Run a Spark job that produces one versioned result for each observed
   segment x vehicle-type combination.
5. Query the latest available result through the serving API.
6. Trace an API result back to its calculation run and source snapshot versions.
