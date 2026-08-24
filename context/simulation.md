---
owner: simulation-team
status: implemented-prototype
last_reviewed: 2026-08-24
---

# Deterministic Trip Simulation

## Purpose

Convert a deterministic, capacity-bounded sample of every valid completed HVFHV
trip in one pinned monthly Parquet into a reproducible wall-clock stream of
dispatch and vehicle sensor observations. The
simulation creates engineering data for pipeline development; it does not claim
to reconstruct the vehicles' actual routes or physical motion.

## Reproducibility contract

A simulation run should record at least:

- simulation run ID
- immutable source dataset identifier or file path
- trip eligibility, ordering, and identity algorithm version
- road-environment snapshot ID
- endpoint-selection algorithm version and seed
- routing algorithm and graph version
- vehicle-profile set and, when the mix mode is used, the vehicle-mix name,
  mix version, and assignment seed
- motion-model version
- sensor sampling frequency
- hourly event target and Trip-sampling policy version
- event-contract version
- source and UTC wall-clock anchors, replay start time, and time-scale value

The same inputs and logical seeds must produce the same eligible trips, endpoints,
routes, segment traversal order, and synthetic measurements. Event timestamps may
differ between runs because the selected source clock is mapped to the current UTC
run anchor, while source gaps and Trip-relative offsets remain stable.

## Implemented prototype pipeline

At startup, the local producer opens three explicit files: one monthly TLC HVFHV
Parquet, one prepared `simulation_road_environment` Parquet, and one prepared
`taxi_zone` Parquet. It does not resolve S3 URIs, active pointers, manifests, or
AWS credentials. The trip reader filters invalid rows across the entire file,
orders the remaining rows in DuckDB, and yields `TripRecord` objects in bounded
batches rather than building a Python list.

1. Validate every row in the configured monthly HVFHV Parquet and remove rows that
   cannot support a passenger-trip simulation.
2. Sort by request timestamp and the immutable file's physical Parquet row number.
   The row number also participates in the versioned `trip_id`, so rewriting the
   input file in place is outside the reproducibility contract.
3. Project the full 10 Hz workload into source event-hour buckets. Apply one base
   Trip-sampling ratio to preserve quiet-hour sparsity and lower that ratio for busy
   hours whose expected output would exceed the configured hourly target. A stable
   hash of policy version, seed, and `trip_id` selects whole Trips, so selected
   `trip_seq` values remain continuous.
4. Convert the UTC run anchor to `America/New_York`, choose the source replay
   interval's matching weekday occurrence and local clock time, and rotate the
   sorted monthly input from that point. Rows before the point follow after the
   actual first-to-last request interval. This avoids treating a previous-month
   dispatch request for a month-start pickup as the source month.
5. Map the selected source anchor to one current UTC run anchor. Request, pickup,
   drop-off, and sensor timestamps retain their source-relative offsets, including
   midnight crossings.
6. Find valid canonical-road points inside pickup and drop-off taxi zones using a
   stable spatial ordering plus a deterministic seed derived from the trip ID.
7. Route between endpoints over the canonical road graph.
8. Assign one vehicle profile per trip. Two exclusive modes exist: a fixed profile
   for every trip (`--vehicle-profile-id`, the default) or a deterministic draw from
   a configured share table (`--vehicle-mix`). The draw is
   `sha256("vehicle-mix:{seed}:{trip_id}")` mapped onto the cumulative shares, so it
   is reproducible rather than random. The mix name is deliberately excluded from the
   hash input: adjusting one share then moves only the trips near the shifted
   boundary instead of reshuffling every assignment.
9. Convert route geometry and the pickup-to-drop-off duration into
   configured-frequency passenger-journey progress and motion states.
10. Apply road attributes, humps, turns, speed changes, and vehicle parameters to
   generate comfort-related measurements.
11. Replay inter-dispatch gaps and movement at `time_scale = 1.0`.
12. Assign zero-based `trip_seq` values and publish keyed, versioned events to
    Kafka.

## Candidate simulation inputs per sample

### Route and road state

- canonical segment ID and progress along the segment
- segment length, road class, direction, and geometry curvature
- pavement rating or normalized pavement-condition feature
- speed-hump proximity and hump traversal phase
- intersection or turn proximity
- assumed segment speed limit or modeled target speed, if available

### Vehicle profile

- mass
- wheelbase and track width
- suspension stiffness and damping proxies
- tire sidewall or tire-compliance proxy
- wheel diameter
- center-of-mass height proxy
- powertrain or regenerative-braking behavior where relevant

These are model parameters, not claims about real vehicle specifications, until
their sources and units are accepted.

### Generated motion measurements

- speed
- longitudinal acceleration and jerk
- lateral acceleration and jerk
- vertical acceleration and jerk
- signed front-wheel steering angle
- steering-wheel vibration amplitude
- yaw rate or heading change
- pitch and roll proxies
- braking and acceleration state
- bump or hump impact magnitude

The Bronze `sensor_event` contract retains speed, heading, three acceleration
axes, the corresponding three jerk axes, `steering_angle`, and
`steering_vibration`. The legacy `jerk` field remains an exact alias of
`jerk_x`. Other candidate measurements above require a contract change before
publication.

## Sensor sequence and map matching

The demonstration frequency is 10 Hz, so the producer emits one sample every
100 ms. It is configurable for test and experiment runs. Every trip uses
`trip_seq = 0, 1, 2, ...` independent of clock jitter.

The default hourly target is 10 million events. `hourly-trip-budget-v1` samples
whole Trips rather than individual events. It first computes a month-wide base
ratio and then caps busy source hours; quiet hours are not upsampled to fill the
budget. A Trip spanning several hours uses the strictest ratio among those hours.
The target is an expected capacity bound, not an exact row-count guarantee.

Bronze sensor events carry GPS coordinates but no `segment_id`. Even though the
simulator uses a LION route internally, Spark performs the authoritative
GPS-to-LION match later and writes the result to `sensor_events_matched`. This
keeps Bronze immutable when road snapshots or matching rules change.

## Time behavior

The confirmed replay scale is one simulated second per real second. To make tests
practical, the implementation may expose an injectable clock or faster test mode,
but production demonstration defaults must preserve the confirmed scale.

The coordinator interleaves overlapping trips in one time-ordered priority queue
and preserves idle gaps. It consumes input in non-decreasing `request_datetime`
order and keeps only the next pending dispatch plus the next sample from each
active trip in memory. `time_scale = 0` is an explicit no-wait verification
mode; no implicit idle-gap cap is applied.

Scheduling time and published event time are deliberately separate. Queue actions
remain on the rotated source TLC timeline so gaps are preserved, while published
timestamps remain independent of the historical calendar year. Route `planned_at`
and sensor `event_time` use timestamp policy
`current-ny-clock-to-run-utc-anchor-v2`: the source replay interval's matching New
York weekday and clock time maps to one UTC run anchor, and every Trip retains its
offset from that shared source anchor. The run summary records both anchors, the
sampling plan, and the execution start.

## Prototype signal model

The implementation uses deterministic synthetic SI measurements: speed in m/s,
all three acceleration axes in m/s², and `jerk_x`, `jerk_y`, and `jerk_z` in
m/s³. Each jerk axis is the discrete derivative of its published acceleration
axis across consecutive samples; the first sample is zero because no preceding
measurement exists. `jerk` is emitted with the same value as `jerk_x`. A
smoothstep speed trajectory spans each routed distance over the TLC
pickup-to-drop-off duration. Pavement rating changes deterministic vertical
vibration amplitude, while a mapped hump adds a localized Gaussian impact and a
damped oscillation scaled by speed and vehicle profile. Lateral acceleration is
derived from heading change and bounded to 4 m/s² to avoid discontinuities in
simplified source geometry producing impossible spikes.

`steering_angle` is a signed front-wheel angle in degrees derived from speed and
heading change with a simplified bicycle model using a representative 2.8 m
wheelbase. It is bounded to -35 through 35 degrees; positive values represent
right turns and negative values represent left turns. Near-zero speed is
reported as zero because a heading-derived estimate is unstable there.

### Vehicle response

`damping_factor` deliberately runs opposite to the response factors: it appears as
`exp(-distance * damping_factor)`, so a larger value settles motion faster and a
lower value means the body keeps swaying. It reaches the signal through two paths.

- A low-frequency body-sway term added to pavement roughness, with amplitude
  `baseline_damping / damping_factor`. `VP_SEDAN_LARGE` normalises this to 1.0. Sway
  uses a much longer wavelength (0.35 rad/m) than the wheel-level roughness term
  (1.7 rad/m).
- Residual ringing after a mapped speed hump, present only once the vehicle has
  passed the hump.

The sway term exists because `damping_factor` would otherwise be nearly inert. In the
checked-in smoke run only 0.34% of samples were near a mapped hump (162 of 46,960)
while 87% carried a pavement rating, so a hump-only coefficient would not
differentiate vehicles on ordinary road. Approximating *persistence* as *amplitude*
is a deliberate simplification: a steady-state sinusoid has no decay time, and a true
second-order response would require carrying state between samples. These values are
not damping ratios.

`steering_vibration` is a non-negative RMS-like amplitude in m/s² rather than a
steering angle. It combines road-induced vertical acceleration and lateral
steering activity, attenuates road vibration near zero speed, and applies a
deterministic high-frequency carrier plus the vehicle profile's synthetic
steering response. It is intended as an explainable pipeline feature, not a
calibrated steering-column dynamics model.

These rules create useful pipeline and scoring features but are not a calibrated
reconstruction of the source taxi's actual dynamics.

## Failure and restart behavior

The supplied schema declares `event_id` as the primary key. The producer stores
it as a deterministic UUID string derived from run, trip, vehicle profile, and
sequence. `(trip_id, trip_seq)` is the ordering and replay key, which requires
that **one `trip_id` identify exactly one vehicle**. Per-trip profile assignment
upholds this; replaying the same trip under several profiles would not, and is
therefore not supported. Such a replay would interleave two vehicles inside one Kafka
partition (the message key is `trip_id`) and, worse, inside the Silver per-trip
windows in `sensor_features/events.py` and `sensor_features/steering.py`, which
partition by `trip_id` alone - corrupting jerk and steering-episode features without
raising an error. Supporting it would require changing the message key to
`{trip_id}:{vehicle_profile_id}` and those window keys together.

Known trip-level feasibility failures do not abort the complete replay. Missing
taxi zones, zones without routable LION nodes, disconnected directed routes,
infeasible speed profiles, and empty sensor streams are logged with `trip_id`
and a bounded reason code. The run summary records attempted, planned, and
skipped trips, the skip ratio, and counts by reason. An optional maximum skip
ratio fails the command only after completed events are flushed and the summary
is written. Kafka publishing failures and unexpected exceptions still abort the
run.

Persisting each skipped trip as a row-level rejection dataset remains a target,
not current repository behavior. Skipped trips must not disappear silently or
receive fabricated cross-zone straight-line routes.
