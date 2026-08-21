---
owner: data-engineering
status: draft-contract
last_reviewed: 2026-08-20
---

# Data Quality and Idempotency Rules

## Bronze sensor invariants

- Bronze sensor records are immutable after S3 ingestion.
- `trip_seq` starts at zero and increases by one per expected sensor sample.
- At sampling frequency `n` Hz, the nominal interval is `1000 / n` milliseconds.
  Examples: 1 Hz = 1000 ms, 10 Hz = 100 ms, 50 Hz = 20 ms, and 100 Hz = 10 ms.
- The selected production sampling frequency remains open.
- `event_id` is the declared primary key.
- `(trip_id, trip_seq)` is the deterministic replay, ordering, and deduplication
  key, provided `trip_id` uniquely identifies a vehicle-specific simulation.
- Replaying the same logical sample must reproduce its `trip_id` and `trip_seq`.
- `steering_angle` is finite and between -35 and 35 degrees inclusive.
- `steering_vibration` is finite and non-negative, with m/s² as its unit.
- Downstream windows order samples by `trip_seq`; `event_time` alone is not a
  deterministic ordering key when timestamps tie or events arrive late.

For a complete trip with a known manifest:

```text
COUNT(*) = expected_sample_count
MAX(trip_seq) + 1 = COUNT(*)
trip_seq contains every integer from 0 through expected_sample_count - 1
```

These rules detect missing samples independently from clock jitter or processing
delay.

## Sequence-aware calculations

A derivative such as jerk is valid only across consecutive samples. Apply the
same rule independently to the x, y, and z axes. Proposed Silver behavior for
the longitudinal axis:

```sql
CASE
    WHEN trip_seq - LAG(trip_seq) OVER trip_window = 1
    THEN (accel_x - LAG(accel_x) OVER trip_window) / (dt_ms / 1000.0)
    ELSE NULL
END
```

This proposed null behavior conflicts with the supplied non-null Silver
`jerk`, `jerk_x`, `jerk_y`, and `jerk_z` schema. The implementation must wait
for the nullability or rejection policy to be accepted. Bronze has no missing
samples while it is generated; its first sample uses zero for every jerk axis
because no prior observation exists.

## Bronze-to-Silver conservation

As of issue #205, cleansing and map matching run as one in-memory Spark
execution (`sensor_processing`, no persisted intermediate
`sensor_events_matched` table — see the T1→T2 cleansed sensor-event contract
in `schema-catalog.md`). Only two artifacts survive that execution:
the cleansing quarantine partition and `hourly_segment_features`. The
conservation invariant is reframed around those two:

- Every Bronze `sensor_event` is either accepted into a `hourly_segment_features`
  row's `sample_count` or quarantined with a measurable `reject_reason` — none
  are silently dropped.
- **Quarantine rate**: `quarantined_count / (quarantined_count +
  SUM(hourly_segment_features.sample_count))` for the target hour. A rate
  above 5% fails the Airflow task. Implemented as a GX Expectation
  (`ExpectColumnValuesToBeBetween` on a one-row `quarantine_rate` DataFrame)
  in `batch_jobs.sensor_processing_validation` (issue #220, ADR-0004), run as
  the `validate_sensor_processing` task right after `run_sensor_processing`.
- **`hourly_segment_features` magnitude ranges**: every RMS/P95 acceleration,
  jerk, and steering-rate/vibration column, plus `avg_speed_mps`, must be
  non-negative when present (they are physical magnitudes/absolute values, so
  a negative value indicates a computation bug). Also a GX Expectation Suite
  (`resources/expectations/hourly_segment_features_suite.json`), checked by
  the same `validate_sensor_processing` task. Schema, non-null required
  columns, PK uniqueness, and the `data_period_start`/`data_period_end`
  one-hour span remain hard invariants enforced inline by
  `sensor_features.aggregation.validate_hourly_segment_features` at write
  time (ADR-0004: hard invariants stay in code, not GX).

## Map-matching quality

- `segment_id` is null unless a match is accepted.
- `road_snapshot_date` records which LION version was used even if no match is
  found.
- Preserve match status, distance, method, heading difference, and candidate
  count.
- Reprocessing against a different road snapshot or algorithm version must not
  mutate Bronze.

## Reference-data quality

- Pavement `system_rating = 0` means not rated, not a measured worst rating.
- Raw numeric-looking strings remain unchanged in the source layer; parsed values
  belong in normalized or enriched tables.
- `enriched_segment_reference` exposes separate quality flags for pavement,
  speed-hump, and traffic-signal mappings.
- A correction rebuilt for the same `(segment_id, reference_date)` updates
  `updated_at`. The source and ETL run must remain traceable.

## Hourly comfort score quality (Silver3)

`hourly_comfort_score` is the `run_hourly_scoring` output: a full recompute of
every historical hour from `hourly_segment_features`, overwritten in place on
each run (no hour partitioning, unlike `hourly_segment_features`). Validation
therefore checks the current output in full rather than a single-hour slice.

- **Directional score ranges**: `vertical_score`, `longitudinal_score`, and
  `lateral_score` must fall between 0 and 100 inclusive. Implemented as a GX
  Expectation Suite (`resources/expectations/hourly_comfort_score_suite.json`).
- **`scoring_version` format**: must be SemVer (`MAJOR.MINOR.PATCH`), matching
  `resources/hourly_comfort.yaml`'s documented constraint that
  `comfort_score/loader.py::_select_latest_scoring_version` compares versions as
  a dot-separated integer array. Same suite as above.
- **Zero-sample rate**: the fraction of rows with `sample_count = 0` must stay
  at or below 5% (provisional threshold, mirrors the `sensor_processing`
  quarantine-rate precedent — revisit once a real distribution is observed).
  Implemented as a GX Expectation on a one-row `zero_sample_rate` DataFrame
  (`resources/expectations/hourly_comfort_score_zero_sample_rate_suite.json`).
- All of the above run in `batch_jobs.hourly_scoring_validation` (issue #249,
  ADR-0004), as the `validate_hourly_scoring` task right after
  `run_hourly_scoring`. Schema and required-column invariants remain hard
  invariants enforced by `HOURLY_COMFORT_SCORE_SCHEMA` at write time (ADR-0004:
  hard invariants stay in code, not GX).

## Gold quality

- Enforce `0 <= comfort_score <= 100`. Implemented as a GX Expectation on
  `comfort_score`, `vertical_score`, `longitudinal_score`, and
  `lateral_score` (all 0–100), plus a `score_version` SemVer format check
  (`resources/expectations/standard_segment_comfort_score_suite.json`), run by
  `batch_jobs.standard_score_validation` (issue #249, ADR-0004) via
  `SqlAlchemyExecutionEngine` against Postgres directly (ADR-0004: Gold is the
  SqlAlchemy path, not Spark). Scoped to the current run's `score_as_of = as_of`
  rows only (in-flight, not the full table) as the `validate_standard_score`
  task right after `run_standard_score`.
- Persist `score_version`, the exact driving-data period, the reference date,
  `sample_count`, and `confidence_score` with every result.
- Component score ranges and confidence-score semantics need explicit contracts
  beyond the 0–100 bound above.
- A result that fails the accepted minimum coverage rule must not appear as an
  ordinary high-confidence score. **Open**: the exact coverage rule (a
  `confidence_score` or `sample_count` threshold) is not yet defined, so it is
  not yet implemented as a GX Expectation.
