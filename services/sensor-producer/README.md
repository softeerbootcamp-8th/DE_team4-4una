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

From the repository root, install the workspace and fetch a deterministic sample
from official NYC sources:

```bash
uv sync --all-packages
uv run --package sensor-producer sensor-producer fetch-nyc-sample \
  --output-dir data/nyc-sensor \
  --source-date 2024-02-01 \
  --zone-id 181 \
  --max-trips 1000
```

The bounded sample uses trips whose pickup and drop-off are both inside the
selected taxi zone. This keeps the reference-data download and local routing
graph small while still using real TLC rows.

The local sample bundle uses fixed filenames: `lion.geojson`,
`pavement.geojson`, `speed_humps.geojson`, `taxi_zones.zip`, `trips.json`, and
`manifest.json`. The original monthly HVFHV source follows
`fhvhv_tripdata_YYYY-MM.parquet`; `trips.json` is the bounded replay input derived
from that Parquet file.

The producer no longer parses `lion.geojson` directly (#225) — it routes over
the canonical `road_segment` Parquet instead: a single-`snapshot_date`
Parquet file (`segment_id`, `from_node_id`, `to_node_id`, `traffic_direction`,
`street_name`, `geometry_wkb` in EPSG:32118, `length_m`, `posted_speed_mph`,
`curve_radius_m`), the same contract Transform 2's
`cleanse-sensor-events --road-segment-path` reads
(`batch_jobs.road_segment.persist.write_road_segment_snapshot`'s
`<dir>/snapshot_date=<date>/data.parquet` layout). Point `--road-segment-path`
at that exact file — the same one given to Transform 2 — so both stages route
against the identical road reference.

> Producing this file for local dev currently has to be done by hand (see
> `data/processed/road_segment/snapshot_date=.../data.parquet` under
> "통합 테스트" in `services/orchestration/README.md`): `build-road-environment`
> only publishes the versioned `normalized/road_segment/snapshot_date=.../
> build_id=.../part-00000.parquet` layout today, nothing yet copies that into
> this simple single-file shape. That gap is unrelated to #225 and out of
> scope here.

Start Kafka and replay in wall-clock time:

```bash
docker compose -f infra/compose/kafka.yaml up -d
uv run --package sensor-producer sensor-producer run \
  --input-dir data/nyc-sensor \
  --road-segment-path data/processed/road_segment/snapshot_date=2026-08-19/data.parquet \
  --publisher kafka \
  --bootstrap-servers localhost:9092 \
  --topic sensor-events \
  --run-id nyc-20240201-v1 \
  --sample-hz 10 \
  --time-scale 1
```

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

## S3 road environment

On EC2, the producer can follow the monthly batch job's active environment pointer
or pin one immutable manifest. Attach an instance role with read access to the
pointer, manifest, and referenced artifacts; never put AWS keys in the image.

```bash
docker build -f services/sensor-producer/Dockerfile -t de4-sensor-producer .
docker run --rm -v "$PWD/data/nyc-sensor:/data:ro" \
  -e AWS_REGION=us-east-1 \
  -e KAFKA_BOOTSTRAP_SERVERS=broker:9092 \
  -e SENSOR_ENVIRONMENT_POINTER_URI=s3://de4-lake/prepared/simulation_environment/active.json \
  de4-sensor-producer run \
    --trips-uri s3://de4-lake/raw/tlc/fhvhv_tripdata_2024-02.parquet \
    --source-date 2024-02-01 \
    --publisher kafka
```

The producer verifies the manifest and both runtime Parquet checksums before routing,
then caches them under `SENSOR_CACHE_DIR`. Use `--environment-manifest-uri` to pin a
specific build. `--trips-uri` accepts one exact `file://` or `s3://` Parquet object;
it never discovers a prefix or selects a month automatically. S3 input is downloaded
through the EC2 IAM role and reused from the local cache on later runs.

`--source-date` selects one NYC source day. DuckDB reads only the timestamps, pickup
and drop-off zone IDs, trip distance, and Parquet row number needed by the simulator;
it applies validity filters, deterministic ordering, and `--max-trips` before yielding
rows in batches. The physical row number is the final ordering tie-breaker and part of
the deterministic `trip_id`, so the input object must be treated as immutable. The
legacy `--trips-path <trips.json>` input remains available for local fixtures.

## Timing and delivery contracts

- Dispatch actions are ordered by `request_datetime`; route planning happens in
  that action.
- Sensor event time starts at `pickup_datetime`, ends at `dropoff_datetime`, and
  uses zero-based contiguous `trip_seq` values.
- Kafka values follow `de4_core.SensorEvent`; the message key is `trip_id`, so
  one trip stays ordered within one partition.
- Kafka assigns the record timestamp independently from the historical TLC
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
