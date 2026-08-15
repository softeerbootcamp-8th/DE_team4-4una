---
owner: analytics-team
status: proposed
last_reviewed: 2026-08-15
---

# Comfort Score Design

## Current state

The Gold contract fixes the final comfort-score range at 0 through 100. Issue
#102 proposes a concrete direction, formula, weights, and minimum-coverage
policy to answer OQ-006, but this remains **Proposed**, not **Accepted** —
formal acceptance and the final `k` value are separate follow-up work (see
"Open items" below and `context/open-questions.md`).

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

## Gold aggregation formula (Segment x vehicle profile)

Grain: one `(segment_id, vehicle_profile_id)` pair, rolled up from every
qualifying hour of `hourly_comfort_score` inside the scoring window (the
worked examples in this document use a rolling 168-hour / 1-week window).

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

Where this vehicle-agnostic score is physically stored - a sentinel
`vehicle_profile_id` row inside `segment_comfort_score`, a dedicated column,
or a separate table - is **not decided by this document**; see OQ-038.

## Handling a vehicle profile that never traversed a segment

No special-case logic is needed. If a `(segment_id, vehicle_profile_id)` pair
has zero qualifying hours, `N = 0` and Step 4 reduces exactly to:

```
ComfortScore_{s,p} = mu_p
Confidence_{s,p} = 0
```

The formula already treats "no evidence" as "trust the population mean
completely." The only remaining decision is operational, not mathematical:
whether the Gold job materializes a row for every `(segment_id,
vehicle_profile_id)` combination in the routing network (so this fallback is
visible as a real, if low-confidence, row) or only for combinations already
present in `hourly_comfort_score`. That choice belongs to the follow-up "데이터
연산" sub-issue, not to this document.

## Parameter and formula management

- **Numeric parameters** (`vertical_weight`, `longitudinal_weight`,
  `lateral_weight`, `min_traffic_threshold` / `T_min`, `shrinkage_k` / `k`) are
  never hardcoded. They live in `services/batch-jobs/config/comfort_score.yaml`,
  each entry shaped as `{value, provisional}`, loaded through
  `services/batch-jobs/src/comfort_score/config.py` into a frozen
  `ComfortScoreConfig` dataclass - the same convention already used by
  `map_matching/config.py` and `sensor_features/config.py`.
- **The formula's shape** (not just its constants) is implemented as a
  versioned, pure Python function in
  `services/batch-jobs/src/comfort_score/formula.py` (added in the follow-up
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

## Open items

These are not resolved by this document; see `context/open-questions.md` for
the full record:

- **OQ-006**: formal acceptance of the direction, weights, and `T_min`/`k`
  proposed here.
- **OQ-038**: physical representation of the vehicle-agnostic per-segment
  score.
- **OQ-039**: source of the traffic count `T_h` used for the minimum-traffic
  filter.
- The final numeric value of `k` (to be computed from real data once enough
  has accumulated - explicitly out of scope for issue #102).

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
