---
owner: analytics-team
status: proposed
last_reviewed: 2026-08-10
---

# Comfort Score Design

## Current state

The Gold contract fixes the final comfort-score range at 0 through 100, but the
score direction, formula, weights, normalization, and minimum coverage are not
yet accepted. The first implementation must therefore preserve interpretable
measurements and derived features rather than embedding an undocumented score
directly in the simulator.

## Measurement groups to preserve

| Group | Candidate variables | Comfort rationale |
| --- | --- | --- |
| Vertical motion | vertical acceleration, vertical jerk, bump impulse, repeated oscillation | Captures pavement and hump response |
| Longitudinal motion | acceleration, deceleration, longitudinal jerk | Captures harsh launch and braking |
| Lateral motion | lateral acceleration, lateral jerk, yaw rate | Captures turns and lane-direction changes |
| Road context | pavement rating, hump indicator, road class, curvature | Explains why motion was generated |
| Vehicle response | suspension proxies, mass, wheelbase, tire proxy | Differentiates vehicle types under the same road input |
| Exposure | seconds and meters observed, trip count, traversal count | Prevents low-coverage scores from appearing equally reliable |

## Proposed aggregation stages

1. **Per-event validation:** enforce units, ranges, timestamps, and segment keys.
2. **Per-traversal features:** calculate magnitudes such as RMS acceleration,
   peak jerk, bump count, and exposure for one vehicle crossing one segment.
3. **Monthly robust aggregation:** combine traversals by score period, canonical
   segment, and vehicle type using documented robust statistics.
4. **Score mapping:** transform normalized feature components to the accepted
   score range using a versioned formula.
5. **Publication:** emit score, components, coverage, algorithm version, and
   provenance together.

The supplied Gold table publishes `vertical_score`, `longitudinal_score`,
`lateral_score`, and `pavement_score`, plus average speed, P95 vertical
acceleration, P95 jerk, discomfort counts, sample count, and confidence score.
Its grain is LION segment x vehicle profile x score month.

A design note mentions jerk as a PDI component with weight `0.25`, but no complete
PDI definition or approved formula was supplied. Treat that weight as unconfirmed
until the scoring decision is accepted.

## Requirements for an accepted formula

- Higher and lower values within the confirmed 0-100 range must have an
  unambiguous documented meaning.
- Every input must have a defined unit and expected range.
- Feature weights and normalization baselines must be versioned.
- The formula must be deterministic for the same validated inputs.
- Missing features must follow an explicit policy.
- Scores with insufficient coverage must be marked or withheld.
- Component values should remain available for debugging and comparison.
- Changes to score semantics require a new algorithm version and backfill plan.

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
