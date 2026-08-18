# Sensor producer

This service turns actual NYC TLC HVFHV trip rows into deterministic, Bronze-shaped
vehicle sensor events. It calculates a directed LION route when the passenger
request occurs and starts publishing 10 Hz movement at pickup time. The default
replay scale is real time: one simulated second takes one wall-clock second.

The motion model is intentionally a data-engineering fixture, not a calibrated
vehicle-dynamics model. Street Pavement Ratings control the vertical vibration
amplitude, mapped speed humps add a localized impact and damped response, and the
selected synthetic vehicle profile scales those responses.

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

Start Kafka and replay in wall-clock time:

```bash
docker compose -f infra/compose/kafka.yaml up -d
uv run --package sensor-producer sensor-producer run \
  --input-dir data/nyc-sensor \
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

## Prepared environment and EC2 container

The monthly batch job publishes an active pointer and immutable environment
manifest. The producer can resolve either URI through the same `file://` or
`s3://` boundary, verify the manifest and Parquet checksums, and cache the
artifacts before routing starts:

```bash
uv run --package sensor-producer sensor-producer run \
  --environment-pointer-uri s3://de4-lake/prepared/simulation_environment/active.json \
  --trips-path data/nyc-sensor/trips.json \
  --publisher kafka
```

On EC2, attach a least-privilege instance profile with read access to the
pointer, manifest, and referenced artifacts. Boto3 then uses the standard AWS
credential chain; do not pass or bake access keys into the image.

Build the production image from the repository root and mount the current
bounded trip input until the S3 TLC reader is implemented separately:

```bash
docker build -f services/sensor-producer/Dockerfile -t de4-sensor-producer .
docker run --rm \
  -v "$PWD/data/nyc-sensor:/data:ro" \
  -e AWS_REGION=us-east-1 \
  -e KAFKA_BOOTSTRAP_SERVERS=broker:9092 \
  de4-sensor-producer run \
  --environment-pointer-uri s3://de4-lake/prepared/simulation_environment/active.json \
  --trips-path /data/trips.json \
  --publisher kafka
```

`--environment-manifest-uri` pins one build instead of following `active.json`.
`SENSOR_CACHE_DIR` controls the verified local artifact cache. The legacy
`--input-dir` mode remains available for the checked-in local sample workflow.

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

- LION `SegmentID` is the canonical routing identifier. `TrafDir` controls the
  directed graph and shortest route length is the Dijkstra cost.
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
  scaled by the synthetic vehicle profile. It is not a calibrated steering
  column measurement.

The checked-in execution evidence and exact input checksums are in
[`context/runs/2026-08-10-nyc-sensor-smoke.md`](../../context/runs/2026-08-10-nyc-sensor-smoke.md).
