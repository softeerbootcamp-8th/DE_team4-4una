# Sensor producer

This service turns actual NYC TLC HVFHV trip rows into deterministic, Bronze-shaped
vehicle sensor events. It calculates a directed LION route when the passenger
request occurs and starts publishing 10 Hz movement at pickup time. The default
replay scale is real time: one simulated second takes one wall-clock second.

The motion model is intentionally a data-engineering fixture, not a calibrated
vehicle-dynamics model. Street Pavement Ratings control the vertical vibration
amplitude, mapped speed humps add a localized impact and damped response, and the
assigned vehicle profile scales those responses.

## Vehicle assignment

One profile is assigned per trip at dispatch time, in one of two exclusive modes:

```bash
--vehicle-profile-id 3        # 모든 trip에 한 프로필 고정 (기본값: 1)
--vehicle-mix nyc-hvfhv-v1    # trip마다 결정론적으로 배정
```

The mix draw is `sha256("vehicle-mix:{seed}:{trip_id}")` mapped onto cumulative
shares - deterministic, not random. Adjusting one share moves only the trips near the
shifted boundary, so two runs stay comparable. `run_summary.json` records the mode,
mix name, mix version, seed, configured shares, and the realised per-profile trip
counts.

One `trip_id` always identifies exactly one vehicle. Replaying a trip under several
profiles is not supported: the Kafka message key and the Silver per-trip feature
windows both key on `trip_id` alone.

Profile definitions and their response factors live in `vehicle_profile`
(`context/data/schema-catalog.md`, migration `0005_define_vehicle_profiles.sql`) and
are mirrored in `domain.py::VEHICLE_PROFILES`.

## Vertical response and damping

`damping` runs opposite to the response factors: it is applied as
`exp(-distance * damping)`, so a **larger** value settles motion faster. A low value
means a long-lasting sway, which is why `VP_MPV_LARGE` pairs the smallest vertical
response with the smallest damping.

Because `damping` would otherwise only matter next to a mapped speed hump (0.34% of
samples in the checked-in smoke run, against 87% carrying a pavement rating), it also
drives a low-frequency body-sway component added to pavement roughness, normalised so
`VP_SEDAN_LARGE` scales it by 1.0. Persistence is approximated as amplitude; these are
not damping ratios.

`longitudinal_response` is 1.00 for every profile - acceleration and braking are
treated as driver behaviour, so `accel_x` and `jerk_x` do not vary by profile.

## Quick start

The replay process runs locally and reads three prepared local Parquet files:

- one TLC HVFHV monthly source file;
- the `simulation_road_environment` artifact (published as
  `road_environment.parquet`), containing the routable LION network
  enriched with pavement ratings and speed humps;
- the `taxi_zone` artifact (published as `taxi_zones.parquet`), containing TLC
  taxi-zone geometries.

The two environment files are outputs of `batch-jobs build-road-environment`.
Copy one immutable build to the local machine before starting the replay. The
producer does not resolve S3 URIs, active pointers, manifests, or AWS credentials.

From the repository root, install the workspace:

```bash
uv sync --all-packages
```

Start Kafka and replay in wall-clock time:

```bash
docker compose -f infra/compose/kafka.yaml up -d
uv run --package sensor-producer sensor-producer run \
  --trips-path data/tlc/fhvhv_tripdata_2024-02.parquet \
  --road-environment-path data/environment/road_environment.parquet \
  --taxi-zone-path data/environment/taxi_zones.parquet \
  --publisher kafka \
  --bootstrap-servers localhost:9092 \
  --topic sensor-events \
  --run-id nyc-202402-v1 \
  --sample-hz 10 \
  --hourly-event-target 10000000 \
  --time-scale 1
```

To publish from the local machine to the EC2 Kafka broker, replace the bootstrap
address with the broker's external listener, for example
`--bootstrap-servers <EC2_PUBLIC_HOST>:9094`. Keep the broker Security Group
restricted to the producer machine's public IP.

Use `--time-scale 0` to remove waits during automated smoke tests. Use
`--publisher jsonl --output <path>` to inspect the exact Kafka value payload
without a broker. Known trip-level feasibility failures are skipped so another
trip can continue. The warning log includes `trip_id` and a bounded reason code,
and `run_summary.json` includes attempted, planned, and skipped trip counts plus
the per-reason totals.

Set `--max-trip-skip-ratio <0..1>` when a run should exit unsuccessfully after
the replay if its skipped-trip ratio is too high. The option is disabled by
default. The producer still flushes published events and writes the run summary
before enforcing the threshold.

DuckDB reads every valid row from the configured monthly TLC Parquet to estimate the
full 10 Hz workload. The default policy uses a stable `trip_id` hash to select whole
Trips under an expected 10-million-event hourly budget. Busy hours receive a lower
selection ratio, while quiet hours are not filled artificially. Because sampling is
Trip-level, selected trips retain contiguous `trip_seq` values and 100 ms intervals.

At startup, the current UTC anchor is converted to `America/New_York`. Replay
rotates from the source replay interval's matching weekday occurrence and local
clock time. That source anchor maps to one current UTC wall anchor, so all dispatch
and event gaps remain unchanged while published `event_time` stays on the current
UTC timeline.

## Timing and delivery contracts

- Dispatch actions are ordered and scheduled by the source `request_datetime`;
  route planning happens in that action.
- Sensor `event_time` maps the selected TLC source clock onto one current UTC run
  anchor, starts at the mapped pickup, ends at the mapped drop-off, and uses
  zero-based contiguous `trip_seq` values.
- Kafka values follow `de4_core.SensorEvent`; the message key is `trip_id`, so
  one trip stays ordered within one partition.
- Kafka assigns the record timestamp independently from the logical sensor
  `event_time`. Spark adds `_ingested_at` only when the record enters Bronze.
- Bronze contains GPS but intentionally omits `segment_id`. The later Spark job
  remains the authoritative GPS-to-LION map matcher.
- `event_id` is a deterministic UUID string based on run, trip, vehicle profile,
  and sequence. Producer retries therefore retain a stable logical identity.

## Environment approximation

- `road_segment.segment_id` (from the canonical Parquet, ultimately sourced
  from LION `SegmentID`) is the routing identifier. `traffic_direction`
  (LION `TrafDir`) controls the directed graph and shortest route length is
  the Dijkstra cost.
- Pickup and drop-off road nodes are deterministically selected from LION nodes
  covered by the TLC taxi-zone polygons. Candidate routes are ranked against the
  source `trip_miles` so zone-level coordinate synthesis does not create a
  clearly inconsistent route length.
- Pavement sections are matched by normalized street name and nearest geometry
  within approximately 39 metres. Rating `0` is treated as unavailable.
- Each speed-hump source section is matched the same way. Its reported hump count
  is distributed evenly along the nearest LION segment because the source
  geometry identifies the containing road section rather than each hump point.
- Speed follows smooth acceleration, cruise, and deceleration phases over the
  routed distance while applying each segment's posted speed limit.
  Signals are deterministic SI measurements: speed in m/s, acceleration in m/s²,
  heading in degrees, and three-axis jerk in m/s³. `steering_angle` is a
  signed front-wheel angle in degrees, bounded to -35 through 35; positive is a
  right turn. The legacy `jerk` field is identical to longitudinal `jerk_x`.
- `steering_vibration` is a non-negative RMS-like steering-wheel acceleration
  amplitude in m/s². It approximates vibration transferred from vertical road
  motion plus lateral steering activity, fades toward zero at low speed, and is
  scaled by the vehicle profile's `steering_vibration_response`. It is not a
  calibrated steering column measurement.
- Residual ringing after a speed hump appears only once the vehicle has passed the
  hump, and decays at a rate set by the profile's `damping`.

The checked-in execution evidence and exact input checksums are in
[`context/runs/2026-08-10-nyc-sensor-smoke.md`](../../context/runs/2026-08-10-nyc-sensor-smoke.md).
