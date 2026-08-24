---
owner: data-engineering
status: draft-contract
last_reviewed: 2026-08-24
---

# Data Quality and Idempotency Rules

## Bronze sensor invariants

- Bronze sensor records are immutable after S3 ingestion.
- `trip_seq` starts at zero and increases by one per expected sensor sample.
- At sampling frequency `n` Hz, the nominal interval is `1000 / n` milliseconds.
  Examples: 1 Hz = 1000 ms, 10 Hz = 100 ms, 50 Hz = 20 ms, and 100 Hz = 10 ms.
- The demonstration sampling frequency is 10 Hz; lower frequencies remain valid
  only for explicit tests and experiments.
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

## Zone weather quality (Silver/Serving)

`latest_zone_weather` holds one row per TLC taxi zone — `run_weather_collection`
UPSERTs it every 15 minutes from Open-Meteo (`jobs.weather`), and a failed
zone's row is left untouched rather than overwritten with bad data (see
`zone_weather_pipeline` in `services/orchestration/README.md`). Validation
therefore scopes to `weather_time = target_time` — exactly the rows this
run's UPSERT actually wrote or refreshed, excluding both failed zones (never
UPSERTed) and stale-skipped zones (blocked by the anti-regression `WHERE
weather_time <= EXCLUDED.weather_time` clause).

- **Observation ranges**: `temperature_2m_c` in `[-60, 60]` (°C); `precipitation_mm`,
  `rain_mm`, `snowfall_cm`, `visibility_m`, `wind_speed_10m_mps`, and
  `wind_gusts_10m_mps` all non-negative (physical magnitudes); `weather_code`
  in `[0, 99]` (WMO code range).
- **`weather_state` enum**: must be one of `snow`, `rain`, `fog`, `high_wind`,
  `dry` — `jobs.weather.classify_weather_state`'s output set.
- **`impact_signature` format**: `<semver>|<sorted conditions or clear>`
  (`jobs.weather_rules.format_impact_signature`'s exact shape) — rejects
  malformed, unsorted, or duplicated condition lists.
- **Freshness**: `fetched_at - weather_time` must fall within `[-60, 1800]`
  seconds (provisional — sized for the DAG's 2-retry/2-minute-delay policy
  plus scheduler lag).
- Implemented as **inline Python/SQL**, not a GX Expectation Suite —
  `orchestration.jobs.weather_validation` (issue #250), run as the
  `validate_weather_collection` task right after `run_weather_collection`
  and before `detect_changed_zones` (so bad data can't be misread as a
  changed zone). See the ADR-0004 amendment note for why this one path
  deviates from GX. Schema/PK invariants remain the writer's responsibility
  (`jobs.weather.upsert_latest_zone_weather`) at write time (ADR-0004: hard
  invariants stay in code, not GX).

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

### Row-level in-flight quarantine (current score, implemented, #251)

Unlike the other Gold in-flight checks above, `current_segment_comfort_score`
is validated in-memory, row by row, immediately before each UPSERT rather than
as a separate post-step task — the table is UPSERT-only with a single latest
row per `(segment_id, vehicle_profile_id)`, so a bad value written first would
have no prior value to roll back to (ADR-0008).

- **Checks**: `comfort_score`, `vertical_score`, `longitudinal_score`, and
  `lateral_score` each in `[0, 100]`; `confidence_score` in `[0, 1]`;
  `sample_count >= 0`; the directional weighted-sum identity (`comfort_score`
  equals the weighted sum of the three directional scores, skipped when
  `low_visibility` is active, per `current_score_quarantine.compute_identity_diff`);
  and `standard_score_as_of` NOT NULL (the exact freshness threshold beyond
  non-null remains open — see `OQ-042`).
- Implemented as a GX Expectation Suite
  (`resources/expectations/current_segment_comfort_score_quarantine_suite.json`)
  run by `orchestration.jobs.current_score_quarantine` (issue #251, ADR-0008)
  via `PandasExecutionEngine` against the in-memory batch, called from
  `run_current_score_job` in `jobs/current_score.py` right before the UPSERT
  (not `SqlAlchemyExecutionEngine` against Postgres like the other Gold checks,
  since validating a live UPSERT-only table would mean checking a value only
  after it already overwrote the previous one).
- **Row-level split**: rows that pass validation are UPSERTed as usual; rows
  that fail are INSERTed into the new `current_segment_comfort_score_quarantine`
  table (see `schema-catalog.md`) in the same transaction, so normal rows keep
  serving even when some rows in the batch are bad.
- **Circuit breaker**: `current_score_quarantine.check_circuit_breaker` raises
  `CurrentScoreCircuitBreakerTripped`, hard-failing the Airflow task and rolling
  back the whole transaction (both the UPSERTs and the quarantine inserts),
  when a run's normal-row count is 0 while rows were processed, or its
  quarantine rate exceeds 25% (`DEFAULT_MAX_QUARANTINE_RATE`).
- The `weather_time`/`weather_rule_version`/`weather_impact_signature`
  NULL-triplet constraint is deliberately **not** in this suite — it is
  already a hard invariant enforced twice over, by `_build_row` in code and by
  DB `CHECK` constraints (migrations `0006`/`0009`), consistent with the
  ADR-0004 principle that hard invariants stay in code/DB, not GX.

### Gold at-rest audit (implemented, #253)

`standard_segment_comfort_score` and `current_segment_comfort_score` are
audited in full once a day by the independent `data_quality_audit` DAG
(`0 3 * * *`, soft fail — a failing task signals via a red Airflow task and
a Great Expectations Data Docs report in S3, but blocks no other DAG).
Implemented as `batch_jobs.gold_audit_validation` (GX `SqlAlchemyExecutionEngine`
against Postgres, ADR-0004):

- **Range**: `comfort_score`/`vertical_score`/`longitudinal_score`/
  `lateral_score` must each be in `[0, 100]`, checked across every row in
  the table (not just the latest run).
- **Freshness**: the newest row (`score_as_of` for
  `standard_segment_comfort_score`, `calculated_at` for
  `current_segment_comfort_score`, since the latter has no `score_as_of`
  column) must be no older than 10800 seconds (3 hours).
- **`vehicle_profile_id` referential integrity**: zero rows may reference a
  `vehicle_profile_id` absent from `vehicle_profile`.
  `standard_segment_comfort_score` already enforces this with a database
  `FOREIGN KEY` (migration `0006`), so this check is a no-op safety net
  there; `current_segment_comfort_score.vehicle_profile_id` has no such FK,
  so this is the only place that violation would be caught.

Schema/PK/required-column invariants remain the writers' responsibility
(`standard_writer.py`, `jobs/current_score.py`) at write time — this audit
does not duplicate them (ADR-0004: hard invariants stay in code, not GX).
