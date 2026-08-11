---
owner: simulation-team
status: implemented-prototype
last_reviewed: 2026-08-11
---

# Deterministic Trip Simulation

## Purpose

Convert approximately 1,000 completed HVFHV trips from one source day into a
reproducible wall-clock stream of dispatch and vehicle sensor observations. The
simulation creates engineering data for pipeline development; it does not claim
to reconstruct the vehicles' actual routes or physical motion.

## Reproducibility contract

A simulation run should record at least:

- simulation run ID
- source dataset identifier and source date
- trip-sampling algorithm version and seed
- road-environment snapshot ID
- endpoint-selection algorithm version and seed
- routing algorithm and graph version
- vehicle-profile version
- motion-model version
- sensor sampling frequency
- event-contract version
- replay start time and time-scale value

The same inputs and logical seeds must produce the same sampled trips, endpoints,
routes, segment traversal order, and synthetic measurements. Wall-clock event
timestamps may differ between runs, so events should also carry deterministic
simulation offsets.

## Implemented prototype pipeline

1. Validate the chosen HVFHV source day and remove rows that cannot support a
   passenger-trip simulation.
2. Sort using a documented stable ordering and select approximately 1,000 rows
   with a seeded deterministic procedure.
3. Use the source request time as the dispatch time and use pickup time as the
   beginning of passenger motion. If the selected source schema does not provide
   request time, require an accepted fallback policy rather than silently
   substituting a different timestamp.
4. Find valid canonical-road points inside pickup and drop-off taxi zones using a
   stable spatial ordering plus a deterministic seed derived from the trip ID.
5. Route between endpoints over the canonical road graph.
6. Assign a vehicle type using a configured deterministic strategy.
7. Convert route geometry and the pickup-to-drop-off duration into
   configured-frequency passenger-journey progress and motion states.
8. Apply road attributes, humps, turns, speed changes, and vehicle parameters to
   generate comfort-related measurements.
9. Replay inter-dispatch gaps and movement at `time_scale = 1.0`.
10. Assign zero-based `trip_seq` values and publish keyed, versioned events to
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
- yaw rate or heading change
- pitch and roll proxies
- braking and acceleration state
- bump or hump impact magnitude

The Bronze `sensor_event` contract retains speed, heading, three acceleration
axes, and the corresponding three jerk axes. The legacy `jerk` field remains an
exact alias of `jerk_x`. Other candidate measurements above require a contract
change before publication.

## Sensor sequence and map matching

The demonstration frequency is 10 Hz, so the producer emits one sample every
100 ms. It is configurable for test and experiment runs. Every trip uses
`trip_seq = 0, 1, 2, ...` independent of clock jitter.

Bronze sensor events carry GPS coordinates but no `segment_id`. Even though the
simulator uses a LION route internally, Spark performs the authoritative
GPS-to-LION match later and writes the result to `sensor_events_matched`. This
keeps Bronze immutable when road snapshots or matching rules change.

## Time behavior

The confirmed replay scale is one simulated second per real second. To make tests
practical, the implementation may expose an injectable clock or faster test mode,
but production demonstration defaults must preserve the confirmed scale.

The coordinator interleaves overlapping trips in one time-ordered priority queue
and preserves idle gaps. It lazily keeps only the next sample from each active
trip in memory. `time_scale = 0` is an explicit no-wait verification mode; no
implicit idle-gap cap is applied.

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

These rules create useful pipeline and scoring features but are not a calibrated
reconstruction of the source taxi's actual dynamics.

## Failure and restart behavior

The supplied schema declares `event_id` as the primary key. The producer stores
it as a deterministic UUID string derived from run, trip, vehicle profile, and
sequence. `(trip_id, trip_seq)` remains the ordering and replay key when each trip
has one assigned profile; multi-profile trip identity is still open.

Trips without valid endpoints or routes should be written to a rejection dataset
with a reason code. They should not disappear silently or receive fabricated
cross-zone straight-line routes.
