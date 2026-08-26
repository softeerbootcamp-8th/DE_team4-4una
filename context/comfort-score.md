---
owner: analytics-team
status: proposed
last_reviewed: 2026-08-26
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
  since nothing downstream needed the history. #222 persists that history
  again, but as a Parquet lake artifact decoupled from serving, not a
  PostgreSQL table — see `zone_weather_snapshot` in schema-catalog.md.
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

### Silver hourly scoring calibration (issue #544)

Each directional score is `100 * (1 - weighted_penalty)`, where every feature's
penalty is `clamp((value - comfortable) / (uncomfortable - comfortable), 0, 1)`
scaled per speed band. The `comfortable`/`uncomfortable` anchor per feature and
`scoring_version` live in
`services/batch-jobs/src/batch_jobs/resources/hourly_comfort.yaml` (each entry
carries an inline comment with its data basis), not in this document — this
section only records *that* and *why* they moved.

The original anchors (`scoring_version: 1.0.0`) were picked before any real
traffic existed and turned out far wider than the real feature distribution,
so almost every row's penalty rounded to ~0 and `standard_segment_comfort_score.comfort_score`
clustered at 90+ (measured 2026-08-26: 96.03% at 80+, only 0.10% below 60).
`scoring_version: 1.1.0` recalibrates every `hourly_comfort.yaml` anchor
against real `hourly_segment_features` from 2026-08-19 through 2026-08-26
(1,331,614 rows), using a one-off analysis command
(`batch-jobs analyze-hourly-feature-distribution`, in
`services/batch-jobs/src/batch_jobs/comfort_calibration.py` — not part of any
DAG) to pull P50/P75/P90/P95/P97/P99/P99.5 per feature. The default rule is
`comfortable ≈ P50`, `uncomfortable ≈ P95`, with named exceptions in the YAML
for features whose real distribution is heavily zero-inflated (longitudinal
`_x` features, nonzero in only ~2.6-3.0% of rows — flagged as worth a separate
data-quality look, since this issue only recalibrates scoring and does not
touch feature computation) or apparently ceiling-capped
(`p95_abs_accel_y`/`p95_abs_jerk_y`, whose top percentiles pin at a round
number below their declared max) or event-rate features so rare that even
P99.5 is still zero (`hard_brake_rate`/`hard_accel_rate`/`sharp_steer_rate`,
anchored instead on the real observed max).

This is a values-only change — the formula shape, the five-input
`vertical_score`/`longitudinal_score`/`lateral_score` component weights, and
the Gold-layer 0.5/0.3/0.2 combination and shrinkage (`k`) are all unchanged
and out of this issue's scope. Whether 1.1.0 fully resolves the 90+ clustering
at the Gold layer still needs to be confirmed by re-running scoring against
real data and re-checking the bucket percentages (see the CLI's `README.md`
usage section for the verification query); a further nonlinear-penalty
adjustment (`penalty = min(1, alpha * ratio ** gamma)`, config-driven) is a
deliberately deferred follow-up if anchor recalibration alone turns out not to
be enough.

## Standard score calculation (Segment x vehicle profile)

Grain: one `(segment_id, vehicle_profile_id)` row, rolled up from every hour
with observed traffic in `hourly_comfort_score` inside the scoring window (a
rolling 168-hour / 1-week window), computed on each scheduled standard run and
upserted into `standard_segment_comfort_score` in place, so that table always
holds the latest run's snapshot and nothing older (issues #193 and #503; see
"Column calculation mapping" under `standard_segment_comfort_score` in
`context/data/schema-catalog.md`). Each run's snapshot is also written to S3
Gold under its own `score_as_of`, and that is where the history lives.
`score_as_of` is the run's fixed schedule time; it is stored separately from
the `data_period_start`/`data_period_end` window actually rolled up into the
score, so a run with zero effective observation hours (`N_eff = 0`) still gets
a real `score_as_of`-keyed row. Both period columns are `NOT NULL`: when
`N_eff = 0` there is no hour with evidence to roll up, so the standard job
fills them with the batch run's own window `[as_of - window_hours, as_of)`
(issue #198).
This weather-unadjusted score is the input to the weather-adjusted
current score below — it is never itself weather-adjusted.

The three directional scores stored on `standard_segment_comfort_score`
(`vertical_score`, `longitudinal_score`, `lateral_score`) are produced by
applying Steps 2-5 below to each direction separately. Every step is linear,
so this does not change `comfort_score`: combining the three shrunk
directional scores with the Step 1 weights gives exactly the same value as
shrinking the already-combined `c_h`. That identity holds because the
evidence weight `e_h` is shared across directions — Step 2 weights on
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

### Step 2 - Weight each hour by how much traffic it saw

```
e_h = min(1, T_h / evidence_saturation_trip_count)
N_eff = sum(e_h for h in the window)
```

- `T_h` is the vehicle traversal count recorded for hour `h`. This is
  `hourly_segment_features.trip_count`, **not**
  `hourly_comfort_score.sample_count` (which counts raw sensor events, not
  vehicles) - see OQ-039 for whether the Gold job joins
  `hourly_segment_features` for this or `hourly_comfort_score` grows its own
  traffic-count column.
- `evidence_saturation_trip_count` is the traffic count at which an hour's
  evidence weight saturates at 1.0 (a full hour's worth of evidence); fewer
  traversals count for less, proportionally, rather than being dropped.
  `T_h = 0` gives `e_h = 0` — an hour with no traffic contributes nothing.
- `N_eff`, the effective observation-hour count, replaces the old hard-cutoff
  hour count `N`. It is a real number, not an integer, and over a 168-hour
  window it ranges from 0 (no traffic at all) up to 168 (every hour
  saturated).
- **Issue #566 replaced a hard cutoff** (`H_{s,p} = { h : T_h >= T_min }`,
  `N = |H_{s,p}|` — an hour below `T_min` contributed nothing at all) with
  this continuous weight, because the hard cutoff discarded real observations:
  with `T_min = 5`, an hour with 1-4 vehicle traversals counted exactly the
  same as an hour with zero. Measured against real data, this pushed
  `confidence_score` to 0 for about 97.8% of `standard_segment_comfort_score`
  rows even though a sizeable share of those had some observed traffic. The
  formula's shape changed, so `SCORE_VERSION` moved from `1.0.0` to `2.0.0`.

### Step 3 - Average the hours, weighted by evidence

```
c_obs = sum(e_h * c_h for h in the window) / sum(e_h for h in the window)
      = sum(e_h * c_h) / N_eff
```

Hours with more evidence (closer to or past `evidence_saturation_trip_count`)
pull the average more than hours with only a trickle of traffic, instead of
every qualifying hour counting equally once it cleared a cutoff. Hourly scores
are already traffic-normalized per vehicle at Silver time (Step 1 computes a
per-vehicle average, not a per-vehicle-count sum), so this weighting is about
*how much to trust* an hour's `c_h`, not renormalizing it.

### Step 4 - Shrink toward the population mean

```
ComfortScore_{s,p} = (N_eff * c_obs + k * mu_p) / (N_eff + k)
```

- `mu_p` is the population mean for vehicle profile `p`: the same
  evidence-weighted average as Step 3 (`sum(e_h * c_h) / sum(e_h)`), pooled
  across **every** segment in the same scoring window (not a per-segment
  average of averages - every hour's evidence counts once, regardless of
  which segment it belongs to). It is the value a segment falls back to when
  it has no evidence of its own. The vehicle-agnostic global `mu` uses the
  identical weighting, pooled across every vehicle profile too — per-segment
  observed, per-profile population, and global population all share one
  evidence definition (#566).
- `k` is the shrinkage strength, in units of "hours." Recommended estimator:
  `k = within-segment hourly variance / between-segment variance` (an
  empirical-Bayes / random-effects variance ratio), computed from realized
  data once enough of it has accumulated. The final numeric value is
  intentionally **out of scope for issue #102** (see "Open items").
- As `N_eff` grows, `ComfortScore` converges to the evidence-weighted observed
  average `c_obs`; as `N_eff` shrinks toward 0, it converges to `mu_p`.

### Step 5 - Report a confidence alongside the score

```
Confidence_{s,p} = N_eff / (N_eff + k)
```

0 means the score is effectively borrowed from the population mean; 1 means
it is fully evidence-based. `N_eff` being continuous (not an hour count) means
confidence now grows smoothly with observed traffic instead of jumping from 0
to a nonzero floor the instant an hour crosses `T_min`.

## Vehicle-agnostic per-segment score

Issue #102 also asks for a `comfort_score` per segment with no vehicle-profile
split. This reuses Steps 2-5 unchanged, after pooling the vehicle-profile
dimension out of Step 1's output:

```
c_h,s = sum_p(T_h,p * c_h,p) / sum_p(T_h,p)
```

For hour `h` on segment `s`, if more than one vehicle profile traversed it,
blend their per-profile `c_h,p` values weighted by each profile's traffic
count `T_h,p`. Apply Step 2's evidence weight to `T_h = sum_p(T_h,p)` (the
pooled traffic across every profile that hour), then Steps 3-5 exactly as
above, using a global `mu` (the same evidence-weighted pooling as `mu_p`, but
across every vehicle profile) in place of `mu_p`.

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
has zero traffic-having hours in the window (`T_h = 0` every hour, so
`N_eff = 0`), Step 4 reduces exactly to:

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
regardless of `N_eff`. This is exactly why the row records `score_as_of` (the
run's fixed schedule time) rather than leaning on `data_period_end` (the
rolled-up, `N_eff = 0`-nullable data window) — every scheduled run stamps its
row with when it ran, whether or not it found qualifying data
(`context/data/schema-catalog.md`). `score_as_of` was also the third
primary-key column until issue #503 dropped it; the column and its meaning are
unchanged, only the key is narrower.

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

## Route comfort score (candidate route)

Everything above scores one segment. Issue #269 adds a second aggregation, one
level up: reducing the segment scores along a candidate route to a single
comparable number, so a caller can rank routes it has already planned. This is
a **read-time** aggregation in `services/serving-api`, not a stored dataset —
nothing is written, and no new grain enters the Gold contracts.

For a route whose segments carry scores `CS_1 .. CS_N`, in traversal order:

1. `AvgComfort` is the mean of all `N` values. A segment traversed twice
   contributes twice, because the vehicle drives it twice.
2. `WorstComfort` is the mean of the lowest `max(1, ceil(N x worst_ratio))`
   values.
3. The route score is
   `average_weight x AvgComfort + worst_quartile_weight x WorstComfort`.

The mean alone cannot separate a uniformly decent route from one carrying a
short severely uncomfortable stretch; the single worst segment lets one value
decide the ranking. Averaging the worst tail sits between those two.

**Status: Implemented, with provisional parameters.** The aggregation is
implemented as a pure function in
`services/serving-api/src/serving_api/route_comfort.py` and served by
`POST /api/v1/routes/evaluate` (see `context/data/contracts.md`). The three
numbers — `average_weight` 0.7, `worst_quartile_weight` 0.3, `worst_ratio`
0.25 — are an MVP starting point, not a validated weighting, so they follow
the same rule as the segment-level parameters below: they are configuration
(`RouteComfortConfig` in `serving_api/config.py`, overridable through
`SERVING_API_ROUTE_AVERAGE_WEIGHT`, `SERVING_API_ROUTE_WORST_QUARTILE_WEIGHT`,
and `SERVING_API_ROUTE_WORST_RATIO`), not constants inside the formula, and
they can be retuned without accepting them as a settled decision.

Two rules the implementation fixes:

- The two weights must sum to 1, checked when the configuration is built, so
  the route score stays on the same 0-100 scale as the segment score.
- A requested segment with no score in either `current_segment_comfort_score`
  or `standard_segment_comfort_score` fails the whole request rather than being
  dropped from the average. Standard scores are materialized for the full
  segment x profile universe (see "Handling a vehicle profile that never
  traversed a segment" above), so a missing score means the caller sent a
  segment outside the road universe. Averaging what remains would answer for a
  shorter route than the one asked about.

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
  `lateral_weight`, `evidence_saturation_trip_count`, `shrinkage_k` / `k`) are
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
3. Implement Open-Meteo weather collection into `latest_zone_weather`.
   Open-Meteo is queried at 20 weather-region points, not at all 263 zone
   points, and each region's observation is spread over its member zones, so
   the table still holds one row per zone. Query points come from
   `weather_region_master.representative_latitude`/`.representative_longitude`
   (schema-catalog.md), which is built offline from
   `zone_master.representative_latitude`/`.representative_longitude`.
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
   **Done** (issue #226): the repository reads `current_segment_comfort_score`
   and falls back to the latest `standard_segment_comfort_score` row when the
   segment has none — either because it belongs to no taxi zone, or because its
   zone has no weather observation yet. The response carries `source`
   (`current` / `standard`) so a client can tell whether weather was applied;
   `weather_time` and `weather_rule_version` are null on the fallback path.
   The staleness-based fallback (serving standard when the zone's weather is
   older than a freshness threshold) is still open — see "Open items".
7. Remove `segment_comfort_score` and its Gold writer. **Done** (issue #227):
   migration `0010` drops the table and its staging copy, `gold_job`/`gold_writer`
   and the `load-segment-comfort-score` command are gone.
8. Split `current_segment_comfort_score` writes into their own DAG so the
   hourly and 15-minute producers stop writing it directly (ADR-0007, issues
   #229/#230/#231). **Done**: `standard_score_pipeline` runs
   `scoring → standard_score` and stops there; `zone_weather_pipeline` collects
   weather and gates on changed zones; both publish an Airflow Asset
   (`STANDARD_SCORE_ASSET`/`ZONE_WEATHER_ASSET`) instead of writing
   `current_segment_comfort_score` themselves. The new `current_score_pipeline`
   DAG is that table's sole writer, scheduled by `AssetAny(...)` with
   `max_active_runs=1`, and picks full vs. changed-zone recompute from which
   Asset triggered it. Verified end to end against a local fixture in issue
   #245 (including that two producers triggering while `current_score_pipeline`
   is busy correctly queue and get consumed together by one DagRun, preferring
   the full recompute) — see
   `docs/adr/0007-split-comfort-score-pipeline-into-three-dags.md`.

Steps 2-8 are each their own follow-up issue and are out of scope for #193.
All eight steps are now complete; the standard/current split is the only Gold
path in the repository.

## Open items

These are not resolved by this document; see `context/open-questions.md` for
the full record:

- **OQ-006**: formal acceptance of the direction, weights, and
  `evidence_saturation_trip_count`/`k` proposed here.
- **OQ-039**: source of the traffic count `T_h` used for the evidence weight.
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
