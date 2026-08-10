---
owner: project-team
status: draft
last_reviewed: 2026-08-10
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
- Scores are recalculated monthly.
- The canonical road identifier is the LION `SegmentID`, represented internally
  as `segment_id`.
- The published comfort-score range is 0 through 100. The direction and formula
  still require an accepted definition.

### Vehicle types

The initial requested vehicle set is:

- Genesis, with the exact model still open
- Hyundai Grandeur
- Hyundai Avante
- EV5, with manufacturer and exact model designation to be confirmed

Each vehicle type has deterministic parameters representing properties that
affect ride comfort, such as suspension behavior, wheelbase, mass, and tires.
Exact parameters are not yet confirmed.

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
- Treat every selected completed trip as a dispatch event.
- Use the source request timestamp for dispatch ordering when it is present; the
  passenger-motion simulation starts at pickup.
- Simulate only the occupied passenger journey from pickup to drop-off.
- Because records identify taxi zones rather than exact coordinates, choose
  deterministic valid road points inside the pickup and drop-off zones.
- Select a deterministic sample of approximately 1,000 trips from one source day.
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
- Use Spark to calculate monthly segment x vehicle-type comfort scores.
- The score formula is not yet defined. The first prototype should retain the
  most relevant simulated variables so formulas can be evaluated without
  rerunning every trip.

## Initial API capability

The minimum product behavior is a point lookup using a LION segment ID
and supported vehicle type. The response should identify the score, score period,
calculation version, and enough provenance to distinguish current data from
stale or unavailable data.

The endpoint path, score scale, database, and behavior for missing combinations
remain open.

## Scope boundaries

### In scope

- A runnable local end-to-end demonstration
- Monthly ingestion of road reference datasets
- Deterministic FHV trip selection and replay
- Synthetic vehicle-motion and comfort-related events at a configurable sampling
  frequency
- Kafka transport and lake persistence
- Spark-based monthly aggregation
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
2. Reproduce the same trip sample, endpoints, routes, and sensor events from the
   same inputs and simulation configuration.
3. Observe selected trip events flowing through Kafka into the local data lake.
4. Run a Spark job that produces one versioned monthly result for each observed
   segment x vehicle-type combination.
5. Query the latest available result through the serving API.
6. Trace an API result back to its calculation run and source snapshot versions.
