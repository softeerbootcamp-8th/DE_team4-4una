---
owner: data-engineering
status: draft-contract
last_reviewed: 2026-08-11
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

- Every Bronze `sensor_event` produces exactly one `sensor_events_matched` row.
- Do not delete unmatched, ambiguous, late, duplicated, or otherwise invalid
  observations during map matching.
- Preserve a status and reason that make each non-success state measurable.
- The expected count invariant is:
  `COUNT(bronze sensor_event) = COUNT(silver sensor_events_matched)` after
  applying the accepted deduplication boundary consistently.
- An unmatched rate above 5% fails the Airflow task. The denominator and
  treatment of ambiguous records still require exact definitions.

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

## Gold quality

- Enforce `0 <= comfort_score <= 100`.
- Persist `score_version`, the exact driving-data period, the reference date,
  `sample_count`, and `confidence_score` with every result.
- Component score ranges and confidence-score semantics need explicit contracts.
- A result that fails the accepted minimum coverage rule must not appear as an
  ordinary high-confidence score.
