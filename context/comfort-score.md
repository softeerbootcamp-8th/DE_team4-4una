---
owner: analytics-team
status: proposed
last_reviewed: 2026-08-20
---

# Comfort Score Design

## Current state

The Gold contract fixes the final comfort-score range at 0 through 100. Issue
#102 proposes a concrete direction, formula, weights, and minimum-coverage
policy to answer OQ-006, but this remains **Proposed**, not **Accepted** —
formal acceptance and the final `k` value are separate follow-up work (see
"Open items" below and `context/open-questions.md`).

Issue #193 splits the single Gold output into three contracts (schemas in
`context/data/schema-catalog.md`), so that weather can be layered onto the
sensor-based score without recomputing it from raw hours on every 15-minute
weather refresh:

- **`standard_segment_comfort_score`** — the weather-unadjusted score below,
  computed hourly from `hourly_comfort_score` and kept as one row per scoring
  hour (a time series).
- **`latest_zone_weather`** — each zone's latest Open-Meteo observation,
  refreshed every 15 minutes, independent of any segment. Originally
  designed as a `zone_weather_snapshot` history table (one row per zone per
  15-minute tick); #209 changed it to one row per zone, UPSERTed in place,
  since nothing downstream needed the history.
- **`current_segment_comfort_score`** — the single current-state row per
  segment x vehicle profile, combining the latest standard snapshot with the
  latest applicable weather (see "Weather-adjusted current score" below).

This document defines the schema-independent formulas and rules; issue #193
covers the contract and processing rules only — no code changes. See
"Migration order" below for how the existing single-table design is replaced.

## Measurement groups to preserve

| Group | Candidate variables | Comfort rationale |
| --- | --- | --- |
| Vertical motion | vertical acceleration, vertical jerk, bump impulse, repeated oscillation | Captures pavement and hump response |
| Longitudinal motion | acceleration, deceleration, longitudinal jerk | Captures harsh launch and braking |
| Lateral motion | lateral acceleration, lateral jerk, yaw rate | Captures turns and lane-direction changes |
| Road context | pavement rating, hump indicator, road class, curvature | Explains why motion was generated |
| Vehicle response | suspension proxies, mass, wheelbase, tire proxy | Differentiates vehicle types under the same road input |
| Exposure | seconds and meters observed, trip count, traversal count | Prevents low-coverage scores from appearing equally reliable |

The vertical/longitudinal/lateral groups above are the ones realized as
`hourly_comfort_score.vertical_score` / `.longitudinal_score` / `.lateral_score`
in Silver, which are the direct inputs to the Gold formula below.

## Standard score calculation (Segment x vehicle profile)

Grain: one `(segment_id, vehicle_profile_id, score_as_of)` row, rolled up
from every qualifying hour of `hourly_comfort_score` inside the scoring
window (a rolling 168-hour / 1-week window), computed on each scheduled
standard run and appended to `standard_segment_comfort_score` as that run's
snapshot (issue #193; see "Column calculation mapping" under
`standard_segment_comfort_score` in `context/data/schema-catalog.md`).
`score_as_of` is the run's fixed schedule time; it is stored separately from
the `data_period_start`/`data_period_end` window actually rolled up into the
score, so a run with zero qualifying hours (`N = 0`) still gets a real
`score_as_of`-keyed row. Both period columns are `NOT NULL`: when `N = 0`
there is no qualifying hour to roll up, so the standard job fills them with
the batch run's own window `[as_of - window_hours, as_of)` (issue #198).
This weather-unadjusted score is the input to the weather-adjusted
current score below — it is never itself weather-adjusted.

The three directional scores stored on `standard_segment_comfort_score`
(`vertical_score`, `longitudinal_score`, `lateral_score`) are produced by
applying Steps 2-5 below to each direction separately. Every step is linear,
so this does not change `comfort_score`: combining the three shrunk
directional scores with the Step 1 weights gives exactly the same value as
shrinking the already-combined `c_h`. That identity holds because the
qualifying-hour set `H` is shared across directions — Step 2 filters on
`trip_count`, which is direction-independent.

### Step 1 - Combine the three directional scores into one hourly score

```
c_h = 0.5 * vertical_score_h + 0.3 * longitudinal_score_h + 0.2 * lateral_score_h
```

Rear-seat passengers feel vertical bump most strongly, so it gets half the
weight; longitudinal (braking/acceleration) and lateral (cornering) split the
rest 3:2. `c_h` is one hour's directional-combined score for one segment and
one vehicle profile. These weights are **Proposed**, not yet **Accepted**
(OQ-006).

### Step 2 - Keep only hours with enough traffic

```
H_{s,p} = { h : T_h >= T_min }
N = |H_{s,p}|
```

- `T_h` is the vehicle traversal count recorded for hour `h`. This is
  `hourly_segment_features.trip_count`, **not**
  `hourly_comfort_score.sample_count` (which counts raw sensor events, not
  vehicles) - see OQ-039 for whether the Gold job joins
  `hourly_segment_features` for this or `hourly_comfort_score` grows its own
  traffic-count column.
- `T_min` is the minimum-traffic threshold below which an hour is dropped as
  unreliable.
- Over a 168-hour window, `N` ranges from 0 (no qualifying hour at all) to
  168 (traffic every hour).

### Step 3 - Average the qualifying hours

```
c_obs = (1 / N) * sum(c_h for h in H_{s,p})
```

A plain average, with no additional per-hour weighting. Hourly scores are
already traffic-normalized per vehicle at Silver time (Step 1 computes a
per-vehicle average, not a per-vehicle-count sum), so a busy rush-hour and a
quiet 3am hour are treated as equally informative samples once both clear the
`T_min` filter.

### Step 4 - Shrink toward the population mean

```
ComfortScore_{s,p} = (N * c_obs + k * mu_p) / (N + k)
```

- `mu_p` is the population mean for vehicle profile `p`: the plain average of
  every qualifying hourly `c_h` for profile `p`, pooled across **every**
  segment in the same scoring window (not a per-segment average of averages -
  every qualifying hour counts once, regardless of which segment it belongs
  to). It is the value a segment falls back to when it has no evidence of its
  own.
- `k` is the shrinkage strength, in units of "hours." Recommended estimator:
  `k = within-segment hourly variance / between-segment variance` (an
  empirical-Bayes / random-effects variance ratio), computed from realized
  data once enough of it has accumulated. The final numeric value is
  intentionally **out of scope for issue #102** (see "Open items").
- As `N` grows, `ComfortScore` converges to the plain observed average
  `c_obs`; as `N` shrinks toward 0, it converges to `mu_p`.

### Step 5 - Report a confidence alongside the score

```
Confidence_{s,p} = N / (N + k)
```

0 means the score is effectively borrowed from the population mean; 1 means
it is fully evidence-based.

## Vehicle-agnostic per-segment score

Issue #102 also asks for a `comfort_score` per segment with no vehicle-profile
split. This reuses Steps 2-5 unchanged, after pooling the vehicle-profile
dimension out of Step 1's output:

```
c_h,s = sum_p(T_h,p * c_h,p) / sum_p(T_h,p)
```

For hour `h` on segment `s`, if more than one vehicle profile traversed it,
blend their per-profile `c_h,p` values weighted by each profile's traffic
count `T_h,p`. An hour then counts once toward `N_s` regardless of how many
profiles contributed to it. Apply Step 2's filter to `T_h = sum_p(T_h,p)`,
then Steps 3-5 exactly as above, using a global `mu` (the same pooling as
`mu_p`, but across every vehicle profile) in place of `mu_p`.

Where this vehicle-agnostic score is physically stored is resolved
(OQ-038, accepted 2026-08-16): a sentinel `vehicle_profile_id = 0` row inside
`segment_comfort_score`, in the same grain as the per-profile rows.
`vehicle_profile_id = 0` represents the all-vehicle aggregate; real vehicle
profiles are numbered from 1. `services/batch-jobs/src/batch_jobs/comfort_score/formula.py`
(issue #127) produces both grains as rows of one Spark DataFrame using this
convention, rather than two separate DataFrames or a dedicated column. The
same sentinel-row convention carries forward unchanged into
`standard_segment_comfort_score` and `current_segment_comfort_score` under
issue #193.

## Handling a vehicle profile that never traversed a segment

No special-case logic is needed. If a `(segment_id, vehicle_profile_id)` pair
has zero qualifying hours, `N = 0` and Step 4 reduces exactly to:

```
ComfortScore_{s,p} = mu_p
Confidence_{s,p} = 0
```

The formula already treats "no evidence" as "trust the population mean
completely." The remaining decision is operational, not mathematical:
whether the standard job materializes a row for every `(segment_id,
vehicle_profile_id)` combination in the routing network on every scheduled
run (so this fallback is visible as a real, if low-confidence, row) or only
for combinations already present in `hourly_comfort_score`.

**Resolved for issue #193:** the standard job materializes a row for every
`(segment_id, vehicle_profile_id)` combination on every scheduled run,
regardless of `N`. This is exactly why `standard_segment_comfort_score`'s
primary key uses `score_as_of` (the run's fixed schedule time) rather than
`data_period_end` (the rolled-up, `N = 0`-nullable data window) — every
scheduled run gets a row keyed by when it ran, whether or not it found
qualifying data (`context/data/schema-catalog.md`).

## Weather-adjusted current score (Segment x vehicle profile)

Grain: one `(segment_id, vehicle_profile_id)` row — the segment's current
(latest) state, always derived from its latest
`standard_segment_comfort_score` snapshot plus its zone's `latest_zone_weather`
row (issue #193; schema in `context/data/schema-catalog.md`).

### Step A - Detect whether the zone's weather changed

`latest_zone_weather` holds only one row per zone (#209), so the previous
observation is gone the moment the collector UPSERTs. The comparison therefore
happens on the consumer side: the recompute job stores the signature it last
acted on in `current_segment_comfort_score.weather_impact_signature` (issue
#216) and compares that against the zone's current `impact_signature`. This
survives a missed tick or a retry, because the decision depends only on
current state:

- Unchanged `impact_signature` -> no recompute; every segment in that zone
  keeps its existing `current_segment_comfort_score` row untouched.
- Changed `impact_signature` -> recompute every segment in that zone (Steps
  B-D below) and UPSERT.

This is the only role the previous weather observation plays: it is a change
trigger, not a scoring input. Step B always reads the current weather
observation directly, never a delta or trend from the prior one.

`impact_signature` is the set of active Step B conditions, not a hash of the raw
observation (`build_impact_signature` in
`services/orchestration/jobs/weather_rules.py`). It is a version-tagged string
listing them in sorted order — `1.0.0|ice,snow`, or `1.0.0|clear` when none
apply. A raw-value key would differ on nearly every 15-minute tick and make the
trigger meaningless; this one changes only when a condition turns on or off.
Because `WEATHER_RULE_VERSION` is part of it, changing the rules forces exactly
one full recompute.

### Step B - Apply a weather adjustment per direction

Starting from the segment's latest `standard_segment_comfort_score`
(`vertical_score`, `longitudinal_score`, `lateral_score`), apply a
weather-derived adjustment per direction, using the zone's current
`latest_zone_weather` fields:

| Weather condition | Adjusted direction(s) |
| --- | --- |
| Rain / freezing conditions | Longitudinal score |
| High wind / gusts | Lateral score |
| Snowfall | Vertical score and longitudinal score |
| Low visibility | Final `comfort_score` only (not the three directional scores) |

Each condition is detected independently, so they combine — snow and high wind
can both be active, and snow alone moves two directions. Detection is
implemented in `services/orchestration/jobs/weather_rules.py`; every numeric
parameter lives in its `resources/weather_rules.yaml` as
`{value, provisional}`, all provisional until real observations accumulate.
`WEATHER_RULE_VERSION` tags the rule set and is what
`current_segment_comfort_score.weather_rule_version` records.

Conditions are intentionally on/off rather than graded. Intensity levels would
mean more thresholds than there is data to calibrate; they can be added later
behind a version bump.

| Condition | Detected when (provisional) | Deduction |
| --- | --- | --- |
| `rain` | >= 0.5 mm per hour, or a WMO rain code | longitudinal -6 |
| `ice` | a freezing-precipitation code (56/57/66/67), or <= 0.5 °C with liquid precipitation | longitudinal -18 |
| `snow` | >= 0.2 cm per hour, or a WMO snow code | vertical -5, longitudinal -10 |
| `wind` | gusts >= 15 m/s | lateral -6 |
| `low_visibility` | <= 1000 m, or a fog code (45/48) | final `comfort_score` -4 |

Rain and snow arrive as 15-minute sums, so they are compared as hourly rates
(x4). Deductions are point subtractions on the 0-100 scale, matching the
additive `clamp(standard + adjustment, 0-100)` shape. Ice is the largest
because it is the one condition where braking authority is gone.

When several conditions hit the same direction the deduction is their
**maximum, not their sum** — freezing rain activates `rain` and `ice`, sleet
activates `rain` and `snow`, and neither should be charged twice.

Temperature is the one field where a missing value is not read as absent:
treating `None` as 0 °C would classify every gap as freezing.

### Step C - Combine into a final current score

Combine the weather-adjusted `vertical_score` / `longitudinal_score` /
`lateral_score` into `comfort_score` using the same directional weights as
the standard score (Step 1 above), then apply the low-visibility adjustment
from Step B to the combined result. Each directional score is clamped to
0-100 after its deduction, and the combined score is clamped again after the
visibility deduction (`adjust_comfort_scores` in `weather_rules.py`).

So on `current_segment_comfort_score`, `comfort_score` is **not** the weighted
sum of the three stored directional scores while `low_visibility` is active,
since that deduction applies only to the combined value. The identity does hold
on `standard_segment_comfort_score`, so a data-quality check must not assume it
on both tables.

The three weights now exist in two places — `batch-jobs`'s
`resources/comfort_score.yaml` and `orchestration`'s `weather_rules.yaml` —
because Step C must reuse them and the service-boundary rule forbids importing
`batch_jobs`. They are kept equal by convention plus a sum-to-1.0 check.
Promoting them to `libs/de4-core` would remove the drift risk but also changes
`batch-jobs`, so it needs approval (the same unguarded drift as OQ-028's
`vehicle_profile`).

### Step D - UPSERT

Write the result to `current_segment_comfort_score`, upserting the single row
for `(segment_id, vehicle_profile_id)` with `standard_score_as_of`,
`weather_time`, `weather_rule_version`, and `weather_impact_signature` all
pointing at the exact standard and weather snapshots used
(`services/orchestration/jobs/current_score.py`, issue #216).

Two operational rules the implementation fixes:

- A segment whose `road_segment.location_id` is null gets **no row at all** —
  `current_segment_comfort_score.location_id` is `NOT NULL`, so there is no zone
  to attach. Those segments still have `standard_segment_comfort_score` rows, so
  the serving layer must treat "standard exists, current missing" as a real case
  rather than an error. 9 of 7,013 segments in the current smoke slice are in
  this state.
- A zone with no `latest_zone_weather` row yet is written **unadjusted**, with
  `weather_time` / `weather_rule_version` / `weather_impact_signature` all null,
  as the table's CHECK constraints require.

## Principles

- The x-axis is longitudinal, the y-axis is lateral, and the z-axis is
  vertical (mirrors `sensor_event.accel_x`/`accel_y`/`accel_z`, OQ-026).
- The previous weather observation is used only for change detection (Step A
  above), never as a scoring input.
- `current_segment_comfort_score` is always recomputed from the segment's
  standard score — it is never adjusted incrementally from its own previous
  value.
- If a zone's weather has not changed, `current_segment_comfort_score` is not
  updated for that zone's segments.
- If Open-Meteo is unavailable, existing `current_segment_comfort_score` rows
  are left unchanged (no partial or null-weather overwrite).
- If a zone's latest weather observation is older than an allowed freshness
  threshold, FastAPI falls back to serving the latest
  `standard_segment_comfort_score` for that segment instead of the stale
  `current_segment_comfort_score` value. The freshness threshold itself is
  out of scope here.
- The direction mapping in Step B is fixed here. The concrete thresholds and
  score-deduction coefficients are implemented in
  `services/orchestration/jobs/weather_rules.py` and its
  `resources/weather_rules.yaml`, versioned by `WEATHER_RULE_VERSION`, and are
  all still provisional until real observations and labels exist.

## Parameter and formula management

- **Numeric parameters** (`vertical_weight`, `longitudinal_weight`,
  `lateral_weight`, `min_traffic_threshold` / `T_min`, `shrinkage_k` / `k`) are
  never hardcoded. They live in
  `services/batch-jobs/src/batch_jobs/resources/comfort_score.yaml`,
  each entry shaped as `{value, provisional}`, loaded through
  `services/batch-jobs/src/batch_jobs/comfort_score/config.py` into a frozen
  `ComfortScoreConfig` dataclass - the same convention already used by
  `map_matching/config.py` and `sensor_features/config.py`.
- **The formula's shape** (not just its constants) is implemented as a
  versioned, pure Python function in
  `services/batch-jobs/src/batch_jobs/comfort_score/formula.py` (added in the follow-up
  "데이터 연산" sub-issue), tagged with a `SCORE_VERSION` constant matching the
  `scoring_version` / `score_version` schema columns. A change to the
  formula's structure - not just a constant - requires bumping this version,
  per the "Requirements for an accepted formula" section below.
- Spark SQL was considered, per issue #102's original completion criteria, and
  rejected: this repository has no other SQL-managed transform, and every
  other Silver/Gold stage is a tested Python function operating on Spark
  DataFrames. Keeping this consistent avoids introducing a second execution
  path for one formula. See the decision comment on issue #102 for the record.

## Requirements for an accepted formula

- Higher and lower values within the confirmed 0-100 range must have an
  unambiguous documented meaning.
- Every input must have a defined unit and expected range.
- Feature weights and normalization baselines must be versioned.
- The formula must be deterministic for the same validated inputs.
- Missing features must follow an explicit policy (see "Handling a vehicle
  profile that never traversed a segment" above).
- Scores with insufficient coverage must be marked or withheld (the
  `Confidence` output above is how this is surfaced, not a hard withholding
  rule).
- Component values should remain available for debugging and comparison.
- Changes to score semantics require a new algorithm version and backfill
  plan.

## Migration order (`segment_comfort_score` -> standard/current split)

Issue #193 defines the schema and processing rules only; no code changes are
part of it. The follow-up implementation work proceeds in this order, so that
`segment_comfort_score` and its existing Gold writer keep serving reads until
the new path is proven end to end:

1. Create the new tables (`standard_segment_comfort_score`,
   `latest_zone_weather`, `current_segment_comfort_score`) via migration,
   alongside the existing `segment_comfort_score`.
2. Implement standard score calculation and hourly append/update into
   `standard_segment_comfort_score`. **Done** (issue #198):
   `batch_jobs.comfort_score.standard_job`, run through the
   `load-standard-segment-comfort-score` command.
3. Implement Open-Meteo weather collection into `latest_zone_weather`,
   reading each zone's query point from `zone_master.representative_latitude`/
   `.representative_longitude` (schema-catalog.md).
4. Implement current score calculation ("Weather-adjusted current score"
   above) and UPSERT into `current_segment_comfort_score`. The rules
   themselves (bucket classification, `impact_signature`, per-direction
   deductions, Step C combination) are implemented in
   `services/orchestration/jobs/weather_rules.py`; what remains is the job
   that reads the two tables, applies them, and UPSERTs. **Done** (issue #216):
   `jobs/current_score.py`, with migration `0009` adding the
   `weather_impact_signature` column Step A compares against. Scheduling is
   issue #217 (step 5).
5. Wire both jobs into Airflow (hourly standard run, 15-minute weather run).
6. Switch the FastAPI serving layer from `segment_comfort_score` to
   `current_segment_comfort_score` (with the standard-fallback rule above).
7. Remove `segment_comfort_score` and its Gold writer.

Steps 2-7 are each their own follow-up issue and are out of scope for #193.

## Open items

These are not resolved by this document; see `context/open-questions.md` for
the full record:

- **OQ-006**: formal acceptance of the direction, weights, and `T_min`/`k`
  proposed here.
- **OQ-039**: source of the traffic count `T_h` used for the minimum-traffic
  filter.
- The final numeric value of `k` (to be computed from real data once enough
  has accumulated - explicitly out of scope for issue #102).
- **Whether the three directional weights move to `libs/de4-core`** instead of
  being duplicated between `batch-jobs` and `orchestration` (see Step C).
- The stale-weather policy conflict: this document says FastAPI falls back to
  the standard score at read time, while the v4 architecture diagram shows a
  neutral adjustment written by the pipeline. Both are defensible; only one
  should be built. The freshness threshold itself is also still undecided.

## Evaluation strategy

Until real comfort labels exist, evaluate the synthetic model with invariants and
scenario comparisons:

- A smooth segment should score more comfortable than an otherwise equivalent
  severely degraded segment.
- Adding a hump should not improve comfort under the same traversal conditions.
- More abrupt braking or steering should not improve comfort.
- A vehicle profile configured as more compliant should respond consistently
  across repeated scenarios.
- Re-running identical inputs should produce identical component values and
  scores.

These checks validate internal consistency, not real-world accuracy. Real-world
calibration would require external measurements or rider labels and is outside
the confirmed prototype scope.
