---
owner: simulation-team
status: verified
executed_at: 2026-08-10
---

# NYC sensor-producer smoke test

This run proves that actual NYC inputs can be routed and converted to the agreed
Bronze event shape, then delivered to a local Kafka broker. It is a bounded
functional test, not a physical-model validation.

## Run configuration

| Setting | Value |
| --- | --- |
| Source day | 2024-02-01 |
| TLC taxi zone | 181, Park Slope |
| Trips fetched / replayed | 10 / first 6 |
| Vehicle profile | 1, synthetic Genesis |
| Sampling | 10 Hz |
| Replay scale | 0 for accelerated verification; production default is 1 |
| Run ID | `nyc-actual-20240201-v3` |
| Kafka image | `apache/kafka:4.3.1` |
| Kafka topic | `sensor-events-nyc-20240201-v3` |

The first six source rows were used because this deterministic prefix includes a
route that crosses speed-hump-enriched LION segments.

## Actual NYC inputs used

| Source | Official endpoint | Selected records | Local bytes | SHA-256 |
| --- | --- | ---: | ---: | --- |
| TLC HVFHV February 2024 Parquet | `https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-02.parquet` | 10 trip rows | 4,502 | `0b401a0b75438710471e77c6520aaaa3564dab588c7462c3ebdf8cf6b005a22c` |
| TLC taxi-zone geometry | `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip` | 263 zones | 1,022,574 | `f6d711917bb4340f8f644d5366c51665489eb2d426dd1a4a55677721ae5adf17` |
| NYC LION 26B bbox extract | `https://services5.arcgis.com/GfwWNkhOj9bNBqoJ/arcgis/rest/services/LION/FeatureServer/0/query` | 3,411 features | 1,485,981 | `865cb0bbd5d55fc991095d1301f1ecb8b50832d21ba8d056e0d53f3b61d08001` |
| NYC Street Pavement Ratings bbox extract | `https://data.cityofnewyork.us/resource/6yyb-pb25.geojson` | 8,004 features | 4,062,332 | `f4f292dd5304f6a4a8453286eefc37d6e4d92adf808631f7db9945ce1fb179e1` |
| NYC VZV Speed Humps bbox extract | `https://data.cityofnewyork.us/resource/jknp-skuy.geojson` | 136 features | 52,357 | `940628e70058ee18b15f0baf35d2685fdd0cebcd6c8c2533937e6b6c20fbef7c` |

The WGS84 bounding box was
`[-74.00092689899985, 40.65501328800008, -73.96471164100001, 40.68885661500006]`.
The environment loader retained 2,792 routable LION segments; 1,822 received a
pavement rating and 113 received one or more mapped hump positions.

## TLC rows replayed

All six rows are Uber (`HV0003`), dispatch base `B03404`, and zone 181 to zone
181. Source timestamps below are New York local time.

| Trip ID | Request | Pickup | Drop-off | Miles | Seconds |
| --- | --- | --- | --- | ---: | ---: |
| `c5fd42c4505b1844f84f3932` | 00:06:16 | 00:09:05 | 00:20:40 | 2.08 | 695 |
| `170b97f1958927f6715e36a6` | 00:06:43 | 00:09:35 | 00:17:01 | 1.73 | 446 |
| `cb58b6c7650939c9c2eb0ccd` | 00:12:54 | 00:15:01 | 00:21:38 | 1.27 | 397 |
| `c6c379146665a5fbd35c8cd8` | 00:14:19 | 00:16:27 | 00:19:54 | 0.62 | 207 |
| `992f18800c372eda4baadecf` | 00:37:42 | 00:39:37 | 00:44:43 | 1.21 | 306 |
| `7da34dc2a7e433af0d5facbd` | 00:54:08 | 00:56:19 | 01:00:13 | 0.83 | 234 |

## Execution and verification

```bash
docker compose -f infra/compose/kafka.yaml up -d
uv run --package sensor-producer sensor-producer run \
  --input-dir data/nyc-smoke \
  --publisher kafka \
  --topic sensor-events-nyc-20240201-v3 \
  --run-id nyc-actual-20240201-v3 \
  --sample-hz 10 \
  --time-scale 0 \
  --max-trips 6
```

| Check | Result |
| --- | ---: |
| Routes planned | 6 |
| Unique LION segments traversed | 93 |
| Kafka end offset / consumed records | 22,856 / 22,856 |
| Trips with contiguous zero-based sequence | 6 of 6 |
| Pavement-informed samples | 20,228 |
| Samples within 2 m of a mapped hump | 12 |
| Maximum absolute lateral acceleration | 4.0 m/s² |
| Maximum absolute vertical acceleration | 2.183 m/s² |
| Bronze payload contains `segment_id` | No, as designed |

The producer assigns the Kafka record timestamp from `_ingested_at` while
retaining the historical passenger-drive timestamp in `event_time`. This was
explicitly verified because using the 2024 timestamp as the Kafka record time
would make records immediately eligible for retention deletion in a 2026 test.
