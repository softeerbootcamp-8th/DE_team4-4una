---
owner: data-engineering
status: draft-contract
last_reviewed: 2026-08-25
---

# Table Schema Catalog

This catalog records the table designs supplied by the project team. It is the
field-level context for implementation until executable schemas are added to
`libs/de4-core`. Physical names marked **TBD** were not included in the supplied
design and must not be invented silently.

## Catalog

| Contract ID | Physical table | Layer or role | Grain |
| --- | --- | --- | --- |
| `hvfhv_trip_input` | TBD | Source/Bronze input | One completed HVFHV trip |
| `street_pavement_rating` | TBD | Source/Bronze reference | One source-defined pavement section observation |
| `speed_hump_reference` | TBD | Source/Bronze reference | One speed-hump road-section record |
| `osm_traffic_signal` | TBD | Source/Bronze reference | One OSM traffic-signal node |
| `taxi_zone_lookup` | `taxi_zone_lookup` | Reference dimension | One TLC taxi zone |
| `vehicle_profile` | `vehicle_profile` | Bronze reference (PostgreSQL) | One vehicle profile |
| `sensor_event` | `sensor_event` | Bronze on S3 | One vehicle sensor measurement (at-least-once; duplicates possible) |
| `road_segment` | `road_segment` | Normalized reference | One LION segment per snapshot date |
| `enriched_segment_reference` | `enriched_segment_reference` | Silver reference (PostgreSQL) | One segment per integrated reference date |
| `PROCESSED_SENSOR_EVENT_SCHEMA` | — | Execution-local T1→T2 DataFrame | One accepted cleansed Bronze `sensor_event` row |
| `hourly_segment_features` | `hourly_segment_features` | Silver | One hour x segment x vehicle profile feature row |
| `hourly_comfort_score` | `hourly_comfort_score` | Silver | One hour x segment x vehicle profile comfort score |
| `standard_segment_comfort_score` | TBD | Gold (PostgreSQL) | One segment x vehicle profile x standard scoring hour |
| `latest_zone_weather` | TBD | Silver/Serving (PostgreSQL) | One TLC taxi zone's latest weather observation |
| `zone_weather_snapshot` | `zone_weather_snapshot` | Bronze (local Parquet, `data/local-lake`) | One TLC taxi zone x 15-minute weather observation |
| `current_segment_comfort_score` | TBD | Gold/Serving (PostgreSQL) | One segment x vehicle profile's current state |
| `current_segment_comfort_score_quarantine` | `current_segment_comfort_score_quarantine` | Gold/Serving reject log (PostgreSQL) | One row-level GX validation rejection from a `current_score` run |
| `zone_master` | `zone_master` | Reference dimension (zone-profile pipeline) | One TLC taxi zone |
| `zone_profile_features` | `zone_profile_features` | Silver (zone-profile pipeline) | One TLC taxi zone |
| `zone_scores` | `zone_scores` | Gold (zone-profile pipeline) | One TLC taxi zone |
| `od_priority` | `od_priority` | Gold (zone-profile pipeline) | One selected pickup/dropoff zone pair (top 1000 by `priority_score`) |

## `hvfhv_trip_input`

The physical table name is not yet confirmed. `yyyymm` is encoded in the storage
path using `YYYYMM` format.

| Attribute | Column | Source column | Type | Nullable | Description |
| --- | --- | --- | --- | --- | --- |
| HVFHS license | `hvfhs_license_num` | `hvfhs_license_num` | STRING | N | Provider license such as HV0003 (Uber) or HV0005 (Lyft) |
| Dispatching base | `dispatching_base_num` | `dispatching_base_num` | STRING | N | Base that dispatched the vehicle |
| Originating base | `originating_base_num` | `originating_base_num` | STRING | Y | Base at which the request originated |
| Request time | `request_datetime` | `request_datetime` | TIMESTAMP | N | Passenger request time and replay dispatch time |
| On-scene time | `on_scene_datetime` | `on_scene_datetime` | TIMESTAMP | Y | Driver arrival at pickup location |
| Pickup time | `pickup_datetime` | `pickup_datetime` | TIMESTAMP | N | Passenger journey start |
| Drop-off time | `dropoff_datetime` | `dropoff_datetime` | TIMESTAMP | N | Passenger journey end |
| Pickup zone | `pu_location_id` | `PULocationID` | INTEGER | N | FK to `taxi_zone_lookup.location_id` |
| Drop-off zone | `do_location_id` | `DOLocationID` | INTEGER | N | FK to `taxi_zone_lookup.location_id` |
| Trip distance | `trip_miles` | `trip_miles` | DOUBLE | N | Miles |
| Trip duration | `trip_time` | `trip_time` | BIGINT | N | Seconds |
| Base fare | `base_passenger_fare` | `base_passenger_fare` | DOUBLE | N | Excludes tolls, tips, and taxes |
| Tolls | `tolls` | `tolls` | DOUBLE | N | Toll amount |
| Black Car Fund | `bcf` | `bcf` | DOUBLE | N | Black Car Fund surcharge |
| Sales tax | `sales_tax` | `sales_tax` | DOUBLE | N | Sales tax amount |
| Congestion surcharge | `congestion_surcharge` | `congestion_surcharge` | DOUBLE | N | Congestion surcharge amount |
| Airport fee | `airport_fee` | `airport_fee` | DOUBLE | N | Airport fee amount |
| Tips | `tips` | `tips` | DOUBLE | N | Tip amount |
| Driver pay | `driver_pay` | `driver_pay` | DOUBLE | N | Driver payment amount |
| Shared request | `shared_request_flag` | `shared_request_flag` | STRING | N | `Y` or `N` |
| Shared match | `shared_match_flag` | `shared_match_flag` | STRING | N | `Y` or `N` |
| Access-A-Ride | `access_a_ride_flag` | `access_a_ride_flag` | STRING | N | `Y` or `N` |
| WAV request | `wav_request_flag` | `wav_request_flag` | STRING | N | `Y` or `N` |
| WAV match | `wav_match_flag` | `wav_match_flag` | STRING | N | `Y` or `N` |
| Ingested time | `_ingested_at` | Derived | TIMESTAMP | N | Pipeline load time |
| Source file | `_source_file` | Derived | STRING | N | Input Parquet filename |
| Data month | `yyyymm` | Derived | STRING | N | Partition value in `YYYYMM` format |

## `street_pavement_rating`

The physical table name is not yet confirmed. Raw strings are deliberately
preserved where the source needs parsing or contains formatting artifacts.

| Attribute | Column | Source column | Type | Nullable | Description |
| --- | --- | --- | --- | --- | --- |
| Road geometry | `the_geom` | `the_geom` | STRING | N | `MULTILINESTRING` WKT |
| OFT code | `oft_code` | `OFTCode` | STRING | N | 17-18 characters; not unique |
| Borough | `borough_name` | `BoroughName` | STRING | Y | Source currently includes 58 null records |
| On street | `on_street_name` | `OnStreetName` | STRING | N | Rated street name |
| From street | `from_street_name` | `FromStreetName` | STRING | N | Starting cross street |
| To street | `to_street_name` | `ToStreetName` | STRING | N | Ending cross street |
| Multi-pass | `is_multi_pass` | `IsMultiPass` | STRING | N | `0` or `1` |
| Direction | `direction` | `Direction` | STRING | Y | Only present for multi-pass sections; approximately 91% null |
| Road type | `road_type` | `Road_Type` | STRING | Y | Main, Service, UnderPass, or OverPass |
| Pavement rating | `system_rating` | `SystemRating` | STRING | N | String value `0.00`-`10.00`; zero means not rated |
| Non-rating reason | `non_rating_reason` | `NonRatingReason` | STRING | Y | For example Duplicate or Construction |
| Inspection time | `inspection_time` | `InspectionTime` | STRING | N | Source format `MM/DD/YYYY HH:MM:SS AM` |
| Section length | `location_geometry_st_length` | `LocationGeometry.STLength` | STRING | N | Feet; may contain thousands separators |
| Ingested time | `_ingested_at` | Derived | TIMESTAMP | N | Pipeline load time |
| Source file | `_source_file` | Derived | STRING | N | Input filename |
| Snapshot date | `snapshot_date` | Derived from filename | STRING | N | `YYYYMMDD`, for example `20260803` |

## `speed_hump_reference`

The physical table name and the source unit of `shape_length` are not yet
confirmed.

| Attribute | Column | Source column | Type | Nullable | Description |
| --- | --- | --- | --- | --- | --- |
| Geometry | `geometry` | `the_geom` | GEOMETRY / STRING | Y | `MULTILINESTRING` for the road section containing humps |
| Object ID | `object_id` | `OBJECTID` | BIGINT | N | Source record identifier |
| On street | `on_street` | `on_street` | STRING | N | Street containing the humps |
| From street | `from_street` | `from_stree` | STRING | N | Starting cross street; source field is truncated |
| To street | `to_street` | `to_street` | STRING | Y | Ending cross street |
| Hump count | `hump_count` | `humps` | INTEGER | N | Installed hump count on the source section |
| Installation date | `installation_date` | `date_insta` | DATE | N | Installation date |
| Shape length | `shape_length` | `Shape_STLe` | DOUBLE | N | Source spatial-object length |

## `osm_traffic_signal`

This source adds traffic signals to the reference environment. The current
extract contains only nodes whose `highway` value is `traffic_signals`.

| Attribute | Column | Source path | Type | Nullable | Description |
| --- | --- | --- | --- | --- | --- |
| OSM node ID | `osm_id` | `properties.@id` | STRING | N | Form `node/42421728` |
| Longitude | `lon` | `geometry.coordinates[0]` | DOUBLE | N | EPSG:4326 |
| Latitude | `lat` | `geometry.coordinates[1]` | DOUBLE | N | EPSG:4326 |
| Highway feature | `highway` | `properties.highway` | STRING | N | All current records are `traffic_signals` |
| Signal type | `traffic_signals` | `properties.traffic_signals` | STRING | Y | For example signal or blinker; populated for about 63.2% |
| Signal direction | `traffic_signals_direction` | `properties.traffic_signals:direction` | STRING | Y | forward, backward, or both; populated for about 21.4% |
| Raw tags | `tags_raw` | `properties` | STRING (JSON) | N | Preserve every non-promoted source tag |
| Ingested time | `_ingested_at` | Derived | TIMESTAMP | N | Pipeline load time |
| Source file | `_source_file` | Derived | STRING | N | `export.geojson` |
| Extracted time | `overpass_timestamp` | File-header `timestamp` | TIMESTAMP | N | Example `2026-08-03T09:02:51Z` |
| Snapshot date | `snapshot_date` | Derived from `overpass_timestamp` | STRING | N | `YYYYMMDD`, for example `20260803` |

## `taxi_zone_lookup`

| Attribute | Column | Source column | Type | Nullable | Description |
| --- | --- | --- | --- | --- | --- |
| Zone ID | `location_id` | `LocationID` | INTEGER | N | TLC zone code, 1-265 |
| Borough | `borough` | `Borough` | STRING | Y | Seven source categories including Manhattan and Queens |
| Zone name | `zone_name` | `Zone` | STRING | Y | Display name; not unique |
| Service zone | `service_zone` | `service_zone` | STRING | Y | Four source categories such as Yellow Zone and Boro Zone |
| Ingested time | `_ingested_at` | Derived | TIMESTAMP | N | Pipeline load time |
| Source file | `_source_file` | Derived | STRING | N | Input CSV filename |

The required taxi-zone geometry contract has not yet been supplied. The lookup
alone cannot support deterministic road-point selection.

## `vehicle_profile`

**Layer:** Bronze reference. **Storage:** PostgreSQL. **Grain:** one vehicle profile.

Profiles are classified by body type x size class rather than manufacturer or
model (accepted 2026-08-18, issue #170; supersedes the earlier
Genesis/Grandeur/Avante/EV5 set — see `OQ-013`/`OQ-014`/`OQ-015`). The
executable seed is
`services/batch-jobs/src/batch_jobs/resources/migrations/0005_define_vehicle_profiles.sql`
(on top of the `0001`/`0003` table and sentinel-row migrations):

| `vehicle_profile_id` | `profile_name` | `body_type` | `size_class` |
| --- | --- | --- | --- |
| 0 | `ALL_VEHICLES` | `ALL` | `ALL` |
| 1 | `VP_SEDAN_COMPACT` | `SEDAN` | `COMPACT` |
| 2 | `VP_SEDAN_LARGE` | `SEDAN` | `LARGE` |
| 3 | `VP_SUV_COMPACT` | `SUV` | `COMPACT` |
| 4 | `VP_SUV_LARGE` | `SUV` | `LARGE` |
| 5 | `VP_MPV_LARGE` | `MPV` | `LARGE` |

`vehicle_profile_id = 0` is the vehicle-agnostic sentinel (`OQ-038`), not a
vehicle type; its response/damping factors are neutral (`1.0`). `manufacturer`,
`model_name`, `mass_kg`, `wheelbase_mm`, and `suspension_type` were dropped —
classification is body type x size class, and physical differences are
captured directly as response factors instead of derived from vehicle specs.
`services/sensor-producer/src/sensor_producer/domain.py::VEHICLE_PROFILES`
must hold the same 5 profiles' `vertical_response`/`longitudinal_response`/
`lateral_response`/`damping`/`steering_vibration_response` values as this
table's matching factor columns — kept in sync by convention only (no
runtime read from Postgres; see `OQ-028`).

| Attribute | Column | Type | Nullable | Key | Description |
| --- | --- | --- | --- | --- | --- |
| Vehicle profile ID | `vehicle_profile_id` | INTEGER | N | PK | Unique vehicle profile identifier |
| Profile name | `profile_name` | STRING | N | UK | For example `VP_SEDAN_COMPACT` |
| Body type | `body_type` | STRING | N |  | `SEDAN`, `SUV`, `MPV`, or `ALL` for the sentinel |
| Size class | `size_class` | STRING | N |  | `COMPACT`, `LARGE`, or `ALL` for the sentinel |
| Vertical response factor | `vertical_response_factor` | DOUBLE | N |  | Multiplier on vertical shock/acceleration response; mirrors `VehicleProfile.vertical_response` |
| Longitudinal response factor | `longitudinal_response_factor` | DOUBLE | N |  | Multiplier on acceleration/braking response; mirrors `VehicleProfile.longitudinal_response` |
| Lateral response factor | `lateral_response_factor` | DOUBLE | N |  | Multiplier on cornering/lateral response; mirrors `VehicleProfile.lateral_response` |
| Damping factor | `damping_factor` | DOUBLE | N |  | How quickly post-shock vibration decays; mirrors `VehicleProfile.damping` |
| Steering vibration factor | `steering_vibration_factor` | DOUBLE | N |  | Multiplier on vibration transmitted to the steering wheel; mirrors `VehicleProfile.steering_vibration_response` |
| Active flag | `is_active` | BOOLEAN | N |  | Whether the profile is currently in use |
| Created time | `created_at` | TIMESTAMPTZ | N |  | Profile creation time |
| Updated time | `updated_at` | TIMESTAMPTZ | N |  | Last modification time |

## `sensor_event`

**Layer:** Bronze. **Storage:** S3. **Grain:** one sensor measurement. Delivery
is at-least-once: no data is lost, but duplicates are possible.

`segment_id` is intentionally absent. T1 validates the Bronze event and passes
accepted rows directly to T2, where GPS-to-LION matching occurs in the same
Spark execution. No intermediate cleansed-event table is persisted. `vehicle_id`
and the legacy `jerk` alias from the earlier draft have been dropped from this
version.

| Attribute | Column | Source | Type | Nullable | Key | Description |
| --- | --- | --- | --- | --- | --- | --- |
| Sensor event ID | `event_id` | Generated | STRING/UUID | N | PK | Deterministic UUID-formatted event identifier |
| Vehicle profile | `vehicle_profile_id` | Generated | INTEGER | N | FK | References `vehicle_profile` |
| Trip ID | `trip_id` | Generated | STRING | N |  | Simulated journey identifier |
| Trip sequence | `trip_seq` | Generated | BIGINT | N |  | Zero-based sample order within a trip |
| Event time | `event_time` | Generated | TIMESTAMP | N |  | Sensor measurement time |
| Event date | `event_date` | Derived from `event_time` | DATE | N | Partition key | UTC S3 daily partitioning and period queries |
| Event hour | `hour` | Derived from `event_time` | STRING | N | Partition key | UTC hour (`00` through `23`) |
| Latitude | `latitude` | Generated | DOUBLE | N |  | GPS latitude |
| Longitude | `longitude` | Generated | DOUBLE | N |  | GPS longitude |
| Speed | `speed_mps` | Generated | DOUBLE | N |  | Meters per second |
| Heading | `heading` | Generated | DOUBLE | Y |  | Direction from 0 through 360 degrees |
| Steering angle | `steering_angle` | Generated | DOUBLE | N |  | Signed front-wheel angle in degrees, -35 through 35; positive is right |
| Longitudinal acceleration | `accel_x` | Generated | DOUBLE | Y |  | Forward/backward acceleration in m/s² |
| Lateral acceleration | `accel_y` | Generated | DOUBLE | Y |  | Side-to-side acceleration in m/s² |
| Vertical acceleration | `accel_z` | Generated | DOUBLE | N |  | Vertical vibration or impact in m/s² |
| Longitudinal jerk | `jerk_x` | Generated | DOUBLE | Y |  | Change in `accel_x` per second in m/s³; hard-accel/brake characteristic |
| Lateral jerk | `jerk_y` | Generated | DOUBLE | Y |  | Change in `accel_y` per second in m/s³; turning/lane-change characteristic |
| Vertical jerk | `jerk_z` | Generated | DOUBLE | Y |  | Change in `accel_z` per second in m/s³; speed-hump/pavement-impact characteristic |
| Steering vibration | `steering_vibration` | Generated | DOUBLE | Y |  | Steering-wheel vibration amplitude in m/s² |
| Steering angle | `steering_angle` | Generated | DOUBLE | Y |  | Steering-wheel angle in degrees; left (−) / right (+) |
| Ingested time | `_ingested_at` | Derived | TIMESTAMP | N |  | Bronze load time |
| Run ID | `_run_id` | Derived | STRING | N |  | Simulation and ingestion run identifier |

The Stream Processor stores Kafka envelopes under
`event_date=YYYY-MM-DD/hour=HH/`. These physical partition columns remain
outside the sensor `value` JSON and are reconstructed by Spark when the
partitioned dataset is read. An unparseable payload falls back to its Kafka
record timestamp so the raw record remains available for cleansing quarantine.

`event_id` is the declared primary key. `(trip_id, trip_seq)` is the proposed
deterministic idempotency and ordering key, subject to the multi-vehicle trip-ID
decision in `open-questions.md`.

## `road_segment`

**Grain:** one LION segment per snapshot date.

**Primary key:** `(segment_id, snapshot_date)`.

Built end to end by `batch_jobs.road_segment.build.build_road_segments()`
(transform -> geometry -> validate -> taxi-zone assignment) and persisted by
`batch_jobs.road_segment.persist.write_road_segment_snapshot()`. Vehicle-segment
selection requires `FeatureTyp`/`Status`/`RW_TYPE`/`TrafDir` to all pass (see
`batch_jobs.road_segment.transform.is_vehicle_segment`), which is stricter than
the LION default and intentionally excludes non-vehicle roadway types (e.g.
`RW_TYPE=12`, ferry routes) and non-constructed segments (`Status != "2"`).

| Attribute | Column | Source column | Type | Nullable | Key | Description |
| --- | --- | --- | --- | --- | --- | --- |
| LION segment ID | `segment_id` | `SegmentID` | STRING | N | PK | Canonical road segment ID |
| Snapshot date | `snapshot_date` | Derived | DATE | N | PK | LION snapshot date |
| Street name | `street_name` | `Street` | STRING | Y |  | Road name |
| From node | `from_node_id` | `NodeIDFrom` | STRING | N |  | Starting node |
| To node | `to_node_id` | `NodeIDTo` | STRING | N |  | Ending node |
| Traffic direction | `traffic_direction` | `TrafDir` | STRING | Y |  | Permitted vehicle direction |
| Segment type | `segment_type` | `SegmentTyp` | STRING | Y |  | LION segment type |
| Feature type | `feature_type` | `FeatureTyp` | STRING | Y |  | LION feature type |
| Roadway type | `roadway_type` | `RW_TYPE` | STRING | Y |  | LION roadway-type code used to filter vehicle segments |
| Roadbed layer | `roadbed_layer` | `RB_Layer` | STRING | Y |  | Grade-separated roadbed discriminator |
| From-node level | `from_node_level` | `NodeLevelF` | STRING | Y |  | Starting grade level |
| To-node level | `to_node_level` | `NodeLevelT` | STRING | Y |  | Ending grade level |
| Posted speed | `posted_speed_mph` | `POSTED_SPEED` | INTEGER | Y |  | Miles per hour |
| Curve flag | `curve_flag` | `CurveFlag` | STRING | Y |  | Whether the segment is curved |
| Curve radius | `curve_radius_m` | Converted from `Radius` (feet) | DOUBLE | Y |  | Meters |
| Length | `length_m` | Converted from `Shape__Length` (feet) | DOUBLE | N |  | Meters |
| Geometry | `geometry_wkb` | `SHAPE` | GEOMETRY (WKB) | N |  | LineString in `EPSG:32118` (NY State Plane, meters) — map matching relies on this being pre-projected, not `EPSG:4326` |
| Source version | `source_version` | Derived | STRING | N |  | LION source snapshot/release identifier |
| Ingested time | `ingested_at` | Derived | TIMESTAMPTZ | N |  | Pipeline load time (UTC) |
| Taxi zone | `location_id` | Spatial mapping (in `EPSG:32118`) | INTEGER | Y | FK | TLC taxi zone containing the segment |

`simulation_road_environment` and `taxi_zone` (the sensor-simulator-facing
artifacts also published by `build-road-environment`) keep `from_node_id`/
`to_node_id` as `BIGINT` and geometry as `EPSG:4326` WKT/WKB — those are
reprojected back from the `EPSG:32118` `road_segment` records specifically for
simulator consumption and are not affected by the above.

## `enriched_segment_reference`

**Layer:** Silver. **Storage:** PostgreSQL. **Grain:** one road segment per
integrated `reference_date`.

**Primary key:** `(segment_id, reference_date)`.

| Attribute | Column | Type | Nullable | Key | Description |
| --- | --- | --- | --- | --- | --- |
| Road segment | `segment_id` | STRING | N | PK, FK | LION-based canonical segment ID |
| Reference date | `reference_date` | DATE | N | PK | Integrated reference-data effective date |
| LION snapshot date | `road_snapshot_date` | DATE | N | FK | `road_segment` version used |
| Pavement rating | `pavement_rating` | DOUBLE | Y |  | Parsed Street Pavement Rating value |
| Pavement condition | `pavement_condition` | STRING | Y |  | Normalized category such as Good, Fair, or Poor |
| Pavement rating date | `pavement_rating_date` | DATE | Y |  | Actual pavement inspection date |
| Speed-hump count | `speed_hump_count` | INTEGER | N |  | Humps mapped to the segment |
| Traffic-signal count | `traffic_signal_count` | INTEGER | N |  | OSM traffic signals mapped to the segment |
| Curve flag | `curve_flag` | STRING | Y |  | Copied from LION |
| Curve radius | `curve_radius` | DOUBLE | Y |  | Copied from LION |
| Posted speed | `posted_speed_mph` | INTEGER | Y |  | Segment speed limit |
| Road length | `length_m` | DOUBLE | N |  | Meters |
| Pavement quality | `pavement_quality_flag` | STRING | N |  | Mapping success, estimate, or missing state |
| Hump quality | `hump_quality_flag` | STRING | N |  | Hump spatial-mapping state |
| Signal quality | `signal_quality_flag` | STRING | N |  | Signal spatial-mapping state |
| Updated time | `updated_at` | TIMESTAMP | N |  | Time this integrated record was last rebuilt or updated |

A corrected rebuild for the same `reference_date` updates the existing primary-key
row and advances `updated_at`; it does not create a new effective date merely
because processing occurred later. Audit history requirements remain open.

## T1→T2 cleansed sensor-event contract

**Role:** execution-local Spark DataFrame. **Storage:** not persisted. **Grain:**
one accepted, deduplicated Bronze `sensor_event` row.

T1 parses and validates `event_time`, applies cleansing and deduplication rules,
and passes the typed DataFrame directly to T2 in the same Spark session. Neither
`event_date` nor `event_hour` is part of the contract because the DataFrame is
not written as an hourly partitioned dataset. The handoff retains the original
Bronze JSON in `_raw_record` only long enough for T2 to preserve it if map
matching fails. Cleansing failures and `MAP_MATCH_FAILED` rows are stored in the
same target-hour `sensor_event_quarantine`; matched rows are consumed immediately
by hourly feature calculation.

The executable field and type definition is
`services/batch-jobs/src/batch_jobs/schemas.py::PROCESSED_SENSOR_EVENT_SCHEMA`.
It is authoritative for this internal handoff and should not be duplicated in
this catalog.

## `hourly_segment_features`

**Layer:** Silver. **Grain:** one row per hour x LION segment x vehicle
profile — sensor events aggregated into comfort-score input features.

**Primary key:** `(data_period_start, segment_id, vehicle_profile_id)`.

| Attribute | Column | Type | Nullable | Key | Description |
| --- | --- | --- | --- | --- | --- |
| Road segment | `segment_id` | STRING | N | PK, FK | LION-based road segment |
| Vehicle profile | `vehicle_profile_id` | INTEGER | N | PK, FK | References `vehicle_profile` |
| Data-period start | `data_period_start` | TIMESTAMP | N | PK | Start of the one-hour window used for the score |
| Data-period end | `data_period_end` | TIMESTAMP | N |  | End of the one-hour window used for the score |
| LION snapshot date | `road_snapshot_date` | DATE | N | FK | `road_segment` version used for map matching |
| Average speed | `avg_speed_mps` | DOUBLE | Y |  | Average speed during the hour |
| Longitudinal acceleration RMS | `rms_accel_x` | DOUBLE | Y |  | RMS of longitudinal acceleration; sustained accel/brake intensity |
| Lateral acceleration RMS | `rms_accel_y` | DOUBLE | Y |  | RMS of lateral acceleration; sustained side-to-side intensity |
| Vertical acceleration RMS | `rms_accel_z` | DOUBLE | Y |  | RMS of vertical acceleration; sustained road-vibration intensity |
| Longitudinal acceleration P95 | `p95_abs_accel_x` | DOUBLE | Y |  | 95th percentile of absolute longitudinal acceleration |
| Lateral acceleration P95 | `p95_abs_accel_y` | DOUBLE | Y |  | 95th percentile of absolute lateral acceleration |
| Vertical acceleration P95 | `p95_abs_accel_z` | DOUBLE | Y |  | 95th percentile of absolute vertical acceleration; recurring strong-impact level |
| Longitudinal jerk RMS | `rms_jerk_x` | DOUBLE | Y |  | RMS of longitudinal jerk |
| Lateral jerk RMS | `rms_jerk_y` | DOUBLE | Y |  | RMS of lateral jerk |
| Vertical jerk RMS | `rms_jerk_z` | DOUBLE | Y |  | RMS of vertical jerk |
| Longitudinal jerk P95 | `p95_abs_jerk_x` | DOUBLE | Y |  | 95th percentile of absolute longitudinal jerk |
| Lateral jerk P95 | `p95_abs_jerk_y` | DOUBLE | Y |  | 95th percentile of absolute lateral jerk |
| Vertical jerk P95 | `p95_abs_jerk_z` | DOUBLE | Y |  | 95th percentile of absolute vertical jerk |
| Hard-brake count | `hard_brake_count` | INTEGER | N |  | Hard-braking events judged during the hour |
| Hard-acceleration count | `hard_accel_count` | INTEGER | N |  | Hard-acceleration events judged during the hour |
| Sharp-steer count | `sharp_steer_count` | INTEGER | N |  | Sharp-steering events by steering angle/rate thresholds |
| Steering-reversal count | `steer_reversal_count` | INTEGER | N |  | Meaningful `steering_angle` direction reversals |
| Steering-rate RMS | `rms_steering_rate` | DOUBLE | Y |  | RMS of `steering_angle` rate of change; expresses steering abruptness |
| Steering-vibration RMS | `rms_steering_vibration` | DOUBLE | Y |  | Steering-system vibration intensity during the hour |
| Sensor sample count | `sample_count` | BIGINT | N |  | Sensor events used to compute the features |
| Trip count | `trip_count` | BIGINT | N |  | Distinct trips passing through this segment/hour; feature-reliability signal |
| Feature rule version | `feature_version` | STRING | N |  | Hard-brake/accel/steer threshold and feature-calculation rule version |
| Processed time | `_processed_at` | TIMESTAMP | N |  | Feature generation completion time |
| Run ID | `_run_id` | STRING | N |  | Spark/Airflow run identifier |

## `hourly_comfort_score`

**Layer:** Silver. **Grain:** one row per hour x LION segment x vehicle
profile.

**Primary key:** `(segment_id, vehicle_profile_id, data_period_start)`.
Whether `scoring_version` also belongs in the primary key is an open question.

This table carries only the three directional scores, not a combined
per-hour `comfort_score`. The Gold job derives the combined hourly score
`c_h` itself from `vertical_score` / `longitudinal_score` / `lateral_score`
(`comfort-score.md` Step 1); the combined score is only ever persisted at
the Gold layer, as `segment_comfort_score.comfort_score`.

| Attribute | Column | Type | Nullable | Key | Description |
| --- | --- | --- | --- | --- | --- |
| Road segment | `segment_id` | STRING | N | PK, FK | LION-based road segment |
| Vehicle profile | `vehicle_profile_id` | INTEGER | N | PK, FK | References `vehicle_profile` |
| Data-period start | `data_period_start` | TIMESTAMP | N | PK | Start of the one-hour scoring window |
| Data-period end | `data_period_end` | TIMESTAMP | N |  | End of the one-hour scoring window |
| LION snapshot date | `road_snapshot_date` | DATE | N | FK | `road_segment` version used for map matching |
| Vertical comfort score | `vertical_score` | DOUBLE | N |  | Computed from vertical acceleration/jerk |
| Longitudinal comfort score | `longitudinal_score` | DOUBLE | N |  | Computed from accel/brake and longitudinal jerk |
| Lateral comfort score | `lateral_score` | DOUBLE | N |  | Computed from lateral acceleration/jerk and steering characteristics |
| Scoring version | `scoring_version` | STRING | N | Possible PK (open) | Score formula, weighting, and rule version |
| Sensor sample count | `sample_count` | BIGINT | N |  | Sensor events used to compute the score |
| Trip count | `trip_count` | BIGINT | N |  | Distinct trips used for the hourly score and downstream Gold coverage |
| Run ID | `_run_id` | STRING | N |  | Spark/Airflow ETL run identifier |
| Processed time | `_processed_at` | TIMESTAMP | N |  | Processing completion time |

## `segment_comfort_score` (removed)

**Status:** removed in issue #227 (migration `0010`). It was the single Gold
table before issue #193 split it into `standard_segment_comfort_score`,
`latest_zone_weather`, and `current_segment_comfort_score`. Its writer
(`gold_job`/`gold_writer`) and the `load-segment-comfort-score` command are
gone as well. Kept here as a pointer only — historical column definitions are
in the git history of this file.

## `standard_segment_comfort_score`

**Status:** proposed (issue #193); replaces `segment_comfort_score` per the
migration order in `context/comfort-score.md`. **Layer:** Gold. **Storage:**
PostgreSQL.

**Grain:** one LION segment x vehicle profile x standard scoring run — the
weather-unadjusted comfort score for that run's trailing 168-hour window,
rolled up from `hourly_comfort_score` using the same formula as
`segment_comfort_score` (`context/comfort-score.md`, "Standard score
calculation").

**Primary key:** `(segment_id, vehicle_profile_id, score_as_of)`.
`score_as_of` is the standard job's scheduled run time (for example, the top
of the hour) — a fixed schedule identifier, not derived from the data. This is
deliberately kept separate from `data_period_start`/`data_period_end`, which
describe the actual qualifying-hour data rolled up into the score.
`segment_comfort_score` conflated these two concepts in a single
`data_period_end` column, so a run that found no qualifying data had no
distinct identity of its own (see the "Column calculation mapping" note on
`segment_comfort_score` above); `score_as_of` gives every scheduled run a key
regardless of how much data it found.

**Storage policy:** append a new row per `(segment_id, vehicle_profile_id,
score_as_of)` as new scheduled runs occur; a re-run for a `score_as_of` that
already exists updates that same row in place (idempotent), it does not
insert a duplicate. A row is written for **every** `(segment_id,
vehicle_profile_id)` combination on every scheduled run, even when `N = 0`
(no qualifying hour) — resolved for issue #193, see
`context/comfort-score.md` ("Handling a vehicle profile that never
traversed a segment").

| Attribute | Column | Type | Nullable | Key | Description |
| --- | --- | --- | --- | --- | --- |
| Road segment | `segment_id` | STRING | N | PK, FK | References `road_segment.segment_id` |
| Vehicle profile | `vehicle_profile_id` | INTEGER | N | PK, FK | References `vehicle_profile.vehicle_profile_id`; sentinel `0` is the vehicle-agnostic row (OQ-038) |
| Score as-of | `score_as_of` | TIMESTAMP | N | PK | The standard job's scheduled run time; identifies which run produced this row, independent of how much qualifying data existed for it |
| Data-period start | `data_period_start` | TIMESTAMP | N |  | Start of the trailing window actually used, `MIN(hourly_comfort_score.data_period_start)` over the qualifying hours in `H_{s,p}`; when `N = 0` there is no qualifying hour to roll up, so the standard job fills the batch run's own window bound `as_of - window_hours` instead (issue #198) |
| Data-period end | `data_period_end` | TIMESTAMP | N |  | End of the trailing window actually used, `MAX(hourly_comfort_score.data_period_end)` over the qualifying hours in `H_{s,p}`; filled with `as_of` when `N = 0`, for the same reason as `data_period_start` |
| Vertical comfort score | `vertical_score` | DOUBLE | N |  | Weather-unadjusted vertical directional score for this window |
| Longitudinal comfort score | `longitudinal_score` | DOUBLE | N |  | Weather-unadjusted longitudinal directional score for this window |
| Lateral comfort score | `lateral_score` | DOUBLE | N |  | Weather-unadjusted lateral directional score for this window |
| Comfort score | `comfort_score` | DOUBLE | N |  | Weather-unadjusted final score, 0-100 (`ComfortScore_{s,p}` in comfort-score.md) |
| Sensor sample count | `sample_count` | BIGINT | N |  | Sum of `hourly_comfort_score.sample_count` over the qualifying hours in `H_{s,p}` |
| Confidence score | `confidence_score` | DOUBLE | N |  | `Confidence_{s,p} = N / (N + k)` |
| Score version | `score_version` | STRING | N |  | Standard comfort-formula version (`SCORE_VERSION`) |
| Calculated time | `calculated_at` | TIMESTAMP | N |  | Standard job run completion time |

### Column calculation mapping

Same calculation as `segment_comfort_score`'s existing mapping above (Steps
1-5 in `context/comfort-score.md`), with two differences: `score_as_of` (the
run schedule time), not `data_period_end`, joins the primary key; and the
three directional scores are stored alongside the combined `comfort_score`.
Steps 2-5 are applied per direction and then recombined with the Step 1
weights — every step is linear, so `comfort_score` is unchanged by this and
stays equal to the weighted sum of the three stored directional scores
(issue #198). `N` is not stored; it is recoverable from `confidence_score`
and the fixed `k` as `N = k * C / (1 - C)`, and `N = 0` is identifiable as
`confidence_score = 0`. OQ-039 (traffic-count source for
the `T_min` filter) remains open and applies here exactly as it did to
`segment_comfort_score`.

## `zone_weather_snapshot`

**Status:** proposed (issue #222). **Layer:** Bronze. **Storage:** local
Parquet under `data/local-lake/bronze/zone_weather_snapshot`. S3 upload is
explicitly out of scope for #222; the config is named as a data-lake URI
so a later switch only changes the write implementation, not the config
or call sites.

**Grain:** one TLC taxi zone x 15-minute weather observation — the time
series that `latest_zone_weather` stopped keeping in #209. Reintroduced as
a decoupled lake artifact (not a PostgreSQL table) since nothing in the
serving path reads it; it exists for history/audit only.

**Primary key (concept, not DB-enforced):** `(location_id, weather_time)`.
Physically this is one Parquet file per `weather_time` tick (all zones for
that tick in one file), keyed by path:
`weather_date=YYYY-MM-DD/weather_time=YYYY-MM-DDTHH-MM-SSZ.parquet`
(`jobs.weather.write_zone_weather_snapshot`). A re-run for a `weather_time`
that was already written overwrites that same file, so retries do not
produce duplicate snapshots.

**Storage policy:** append (a new file per `weather_time`); never mutated
after being written except by an overwrite for the same `weather_time`.
Written from the same in-memory rows as the `latest_zone_weather` UPSERT
in the same job run — history is written first, then `latest_zone_weather`
is UPSERTed, so a snapshot-write failure blocks the latest-row update too
(both are retried together, idempotently, by Airflow).

Every requested zone gets a row **every run**, including zones Open-Meteo
had no matching `weather_time` for — those get `NULL` measurement/state/
signature columns and `fetch_status = 'failed'`. This is the one place a
failed zone leaves a trace; `latest_zone_weather` simply skips it and
keeps its old row (see below), so without this table a failed fetch would
be invisible. Two columns beyond `latest_zone_weather`'s set:

| Attribute | Column | Type | Nullable | Description |
| --- | --- | --- | --- | --- |
| Fetch status | `fetch_status` | STRING | N | `success` or `failed` for this zone this run |
| Error reason | `error_reason` | STRING | Y | Set only when `fetch_status = 'failed'`: "missing target_time in Open-Meteo response" (this zone's batch succeeded but had no matching timestamp) or "Open-Meteo request failed: ..." (the whole batch this zone was in failed — HTTP error or a location-count mismatch) |

## `latest_zone_weather`

**Status:** proposed (issue #193); storage model changed from a
`(location_id, weather_time)` history table to a latest-only table in #209.
**Layer:** Silver/Serving. **Storage:** PostgreSQL.

**Grain:** one TLC taxi zone's latest weather observation — not a time
series. The original design (`zone_weather_snapshot`, migration 0006) kept
one row per zone per 15-minute tick forever; #209 renamed the table and
collapsed it to one row per zone, since nothing downstream reads the
history and it makes "did this zone's weather change" a single UPSERT
instead of a query over the previous row. #222 reintroduced the history as
a separate Parquet artifact (see `zone_weather_snapshot` above) — this
table itself still keeps no history.

**Primary key:** `location_id`. Named to match `taxi_zone_lookup.location_id`
/ `zone_master.location_id` / `road_segment.location_id` (OQ-029:
`zone_master` is the canonical geometry-bearing zone reference) rather than
introducing a `zone_id` alias for the same identifier. It is a **logical**
reference to `zone_master.location_id` only — `zone_master` is local
Parquet, not PostgreSQL, so no DB-enforced FK is created (confirmed with the
project owner; see the note under `zone_master` above).

**Storage policy:** UPSERT only, keyed by `location_id` — every 15-minute
collection run overwrites each zone's single row with the latest Open-Meteo
observation (migration 0007). There is no history table; anything that needs
"did the weather change" must read this row's existing `impact_signature`
**before** the UPSERT overwrites it.

A zone Open-Meteo had no data for this run is **not** UPSERTed at all (#222)
— its existing row (if any) is left exactly as it was, still holding
whatever `weather_time`/`impact_signature` it last successfully collected.
Because the row is untouched, the change-detection comparison in
`current_score.py` naturally treats an unchanged-because-failed zone the
same as an unchanged-because-the-weather-didn't-move zone — no recompute
is triggered for it either way, which is the correct outcome.

A **partial** miss (some zones failed, at least one succeeded) does not
fail the Airflow task — only `zone_weather_snapshot` records which zones
failed this run (`fetch_status`). A batch-level Open-Meteo request failure
(HTTP error, or a response whose location count doesn't match the request)
is caught per batch in `jobs.weather.fetch_open_meteo`, so one bad batch
does not stop the other batches from being collected. If **every** zone
fails, the task raises after writing the snapshot — `latest_zone_weather`
is still untouched, but Airflow retries, since a 0-for-N result usually
means Open-Meteo itself is down rather than N unrelated per-zone misses.

| Attribute | Column | Type | Nullable | Key | Description |
| --- | --- | --- | --- | --- | --- |
| Zone ID | `location_id` | INTEGER | N | PK | TLC zone code; logical reference to `zone_master.location_id` (not a DB FK — see above) |
| Weather time | `weather_time` | TIMESTAMP | N |  | Observation time of the currently-stored reading (15-minute-aligned); overwritten on every UPSERT, so it always reflects only the latest run, never a history |
| Latitude | `latitude` | DOUBLE | N |  | Zone query-point latitude actually sent to Open-Meteo; copied from `zone_master.representative_latitude` at fetch time, not queried ad hoc |
| Longitude | `longitude` | DOUBLE | N |  | Zone query-point longitude actually sent to Open-Meteo; copied from `zone_master.representative_longitude` at fetch time |
| Temperature | `temperature_2m_c` | DOUBLE | Y |  | 2m air temperature, degrees Celsius |
| Precipitation | `precipitation_mm` | DOUBLE | Y |  | Total precipitation, mm |
| Rain | `rain_mm` | DOUBLE | Y |  | Liquid rain portion, mm |
| Snowfall | `snowfall_cm` | DOUBLE | Y |  | Snowfall, cm |
| Visibility | `visibility_m` | DOUBLE | Y |  | Visibility, meters |
| Wind speed | `wind_speed_10m_mps` | DOUBLE | Y |  | 10m wind speed, m/s |
| Wind gusts | `wind_gusts_10m_mps` | DOUBLE | Y |  | 10m wind gusts, m/s |
| Weather code | `weather_code` | INTEGER | Y |  | Open-Meteo WMO weather code |
| Weather state | `weather_state` | STRING | N |  | Human-readable condition bucket derived from the fields above (dry, rain, snow, fog, or high-wind, in that priority order — see `jobs.weather.classify_weather_state`). Single-valued and for display/debug only; it is **not** the scoring input, and it is classified separately from `impact_signature`'s buckets |
| Impact signature | `impact_signature` | STRING | N |  | The set of active weather conditions for this observation, not a hash of the raw values, so it changes only when a condition turns on or off (`jobs.weather_rules.build_impact_signature`). Rule-version-tagged and sorted: `1.0.0\|ice,snow`, or `1.0.0\|clear` when none apply. `parse_impact_signature` reads it back, so the current-score job applies deductions without reclassifying these columns. It is also the change trigger — the recompute job compares it against `current_segment_comfort_score.weather_impact_signature`, the value it last acted on, since this table keeps no history (`context/comfort-score.md`, Step A) |
| Fetched time | `fetched_at` | TIMESTAMP | N |  | Time this row was last retrieved from Open-Meteo |

`weather_state` and `impact_signature` are both derived from the same raw
fields: `weather_state` is for human/debug readability, `impact_signature` is
the exact equality key a future current-score job compares against this
row's pre-UPSERT value to decide whether to recompute. Nullable measurement
columns reflect fields Open-Meteo may omit on a partial response; a wholly
failed fetch does not write a row at all (see comfort-score.md's Open-Meteo
failure policy).

## `current_segment_comfort_score`

**Status:** proposed (issue #193). **Layer:** Gold/Serving. **Storage:**
PostgreSQL.

**Grain:** one LION segment x vehicle profile's current (latest) state — the
standard score with the latest applicable weather adjustment applied.

**Primary key:** `(segment_id, vehicle_profile_id)`. Unlike
`standard_segment_comfort_score`, there is exactly one row per segment x
vehicle profile at all times; no period is part of the key.

**Storage policy:** UPSERT only, keyed by `(segment_id, vehicle_profile_id)`.
A segment with no zone (`road_segment.location_id` is null) gets no row, because
`location_id` here is `NOT NULL` — such segments exist only in
`standard_segment_comfort_score` (issue #216). `location_id` is indexed, since
the 15-minute path selects by zone while the primary key is segment-first.

- On the hourly standard run (top of the hour): every row is recomputed and
  upserted from the new `standard_segment_comfort_score` snapshot and the
  zone's current `latest_zone_weather` row.
- On each 15-minute weather refresh: only the segments belonging to zones
  whose `impact_signature` differs from the value `latest_zone_weather` held
  just before this refresh are recomputed and upserted; segments in
  unaffected zones are left as-is.

| Attribute | Column | Type | Nullable | Key | Description |
| --- | --- | --- | --- | --- | --- |
| Road segment | `segment_id` | STRING | N | PK, FK | References `road_segment.segment_id` and `standard_segment_comfort_score.segment_id` |
| Vehicle profile | `vehicle_profile_id` | INTEGER | N | PK, FK | References `vehicle_profile.vehicle_profile_id` and `standard_segment_comfort_score.vehicle_profile_id` |
| Zone | `location_id` | INTEGER | N |  | The segment's TLC zone, cached here to join `latest_zone_weather` without going through `road_segment`. **Logical reference only, no FK** (#209) — see "Relationships" below for why |
| Standard score as-of | `standard_score_as_of` | TIMESTAMP | N | FK | The `standard_segment_comfort_score.score_as_of` this row was computed from; together with `segment_id`/`vehicle_profile_id` identifies the exact standard snapshot used |
| Weather time | `weather_time` | TIMESTAMP | Y |  | The `latest_zone_weather.weather_time` this row's weather adjustment was actually computed from, frozen at calculation time. **Logical reference only, no FK** (#209) — `latest_zone_weather` keeps mutating after this row is written, and this column must not follow it; null only before the zone's first weather snapshot has been fetched, in which case the row reflects the standard score unadjusted |
| Data-period start | `data_period_start` | TIMESTAMP | Y |  | Copied from the referenced `standard_segment_comfort_score.data_period_start`, for display without a join. The referenced standard row is never itself null here (issue #198 made those columns NOT NULL), so in practice this column is always populated; the column stays nullable because tightening it is part of the current-score work, not #198 |
| Vertical comfort score | `vertical_score` | DOUBLE | N |  | Weather-adjusted vertical directional score |
| Longitudinal comfort score | `longitudinal_score` | DOUBLE | N |  | Weather-adjusted longitudinal directional score |
| Lateral comfort score | `lateral_score` | DOUBLE | N |  | Weather-adjusted lateral directional score |
| Comfort score | `comfort_score` | DOUBLE | N |  | Weather-adjusted final score, 0-100, always recomputed from the standard score (`context/comfort-score.md`) |
| Sensor sample count | `sample_count` | BIGINT | N |  | Copied from the referenced `standard_segment_comfort_score.sample_count` |
| Confidence score | `confidence_score` | DOUBLE | N |  | Copied from the referenced `standard_segment_comfort_score.confidence_score`; weather adjustment does not change confidence |
| Standard score version | `standard_score_version` | STRING | N |  | Copied from the referenced `standard_segment_comfort_score.score_version` |
| Weather impact signature | `weather_impact_signature` | STRING | Y |  | The `latest_zone_weather.impact_signature` this row was computed from, so the recompute job can tell whether a zone's weather changed without a weather history table (migration `0009`, issue #216). `NULL` exactly when `weather_time` is `NULL`, enforced by `CHECK ((weather_time IS NULL) = (weather_impact_signature IS NULL))` |
| Weather rule version | `weather_rule_version` | STRING | Y |  | Version of the weather-adjustment rule set (thresholds and score deductions); versioned independently of `standard_score_version` since it changes on its own schedule (finalized in a follow-up issue). `NULL` exactly when `weather_time` is `NULL` (before the zone's first weather snapshot) — enforced by a `CHECK ((weather_time IS NULL) = (weather_rule_version IS NULL))` |
| Calculated time | `calculated_at` | TIMESTAMP | N |  | Time this row was last upserted |

### Relationships

- `current_segment_comfort_score.standard_score_as_of` (together with
  `segment_id`, `vehicle_profile_id`) -> `standard_segment_comfort_score.score_as_of`
  (together with `segment_id`, `vehicle_profile_id`).
- `current_segment_comfort_score.(location_id, weather_time)` ->
  `latest_zone_weather.(location_id, weather_time)` — **logical only, no FK**
  (#209, reversing the earlier #196 design where this was a real composite
  FK). `latest_zone_weather` has no history: it is overwritten in place on
  every 15-minute collection run, so a FK would force
  `current_segment_comfort_score.weather_time` to silently become stale-or-
  invalid the moment a *different* zone-wide UPSERT changes the row it once
  pointed at — even though this row's score was never recomputed. Keeping
  the reference logical lets `weather_time` stay a frozen record of "what
  weather this score was actually computed from."

## `current_segment_comfort_score_quarantine`

**Status:** implemented (issue #251, ADR-0008 — see
`docs/adr/0008-current-score-row-level-quarantine-and-circuit-breaker.md`).
**Layer:** Gold/Serving reject log. **Storage:** PostgreSQL, migration
`services/batch-jobs/src/batch_jobs/resources/migrations/0011_create_current_score_quarantine.sql`.

**Grain:** one row-level GX validation rejection from a `current_score` run —
not the serving table's "current state" grain. `run_current_score_job`
(`jobs/current_score.py`) validates each batch in-memory via
`orchestration.jobs.current_score_quarantine` right before the
`current_segment_comfort_score` UPSERT (see `quality-rules.md`, "Row-level
in-flight quarantine"); rows that fail are INSERTed here instead, in the same
transaction as the UPSERT.

**Primary key:** `id` (`BIGSERIAL`). Deliberately **append-only** — no unique
constraint on `(segment_id, vehicle_profile_id)`, since the same key can be
quarantined again across multiple runs and each rejection is its own record,
unlike the UPSERT-only main table. Reprocessing/recovery workflows for
quarantined rows are explicitly out of scope for #251, so this schema covers
only audit and query needs, not replay.

| Attribute | Column | Type | Nullable | Key | Description |
| --- | --- | --- | --- | --- | --- |
| Quarantine record ID | `id` | BIGSERIAL | N | PK | Auto-incrementing rejection identifier |
| Road segment | `segment_id` | TEXT | N |  | The rejected row's segment ID (indexed with `vehicle_profile_id`) |
| Vehicle profile | `vehicle_profile_id` | INTEGER | N |  | The rejected row's vehicle profile ID (indexed with `segment_id`) |
| Calculated time | `calculated_at` | TIMESTAMPTZ | N |  | Indexed; the rejected row's own `calculated_at` value, for time-range audit queries |
| Reject reason | `reject_reason` | TEXT | N |  | Comma-joined, sorted set of the column names whose GX expectations the row violated |
| Reject detail | `reject_detail` | JSONB | N |  | Per-violation detail (expectation type, column, observed value) for every failed expectation on the row; JSONB so new rules don't require a schema migration |
| Raw row | `raw_row` | JSONB | N |  | The full rejected row as it was about to be UPSERTed, for audit/debugging |
| Rejected time | `rejected_at` | TIMESTAMPTZ | N |  | Defaults to `now()`; when the quarantine INSERT itself happened |

## `zone_master`

**Layer:** Reference dimension. **Storage:** local Parquet at
`data/reference/tlc_zone/zone_master.parquet`. **Built by:**
`services/sensor-producer/src/zone_profile/build_tlc_zone_base.py`. **Grain:**
one TLC taxi zone.

**Primary key:** `location_id`.

| Attribute | Column | Source column | Type | Nullable | Key | Description |
| --- | --- | --- | --- | --- | --- | --- |
| Zone ID | `location_id` | `LocationID` | INTEGER | N | PK | TLC zone code, 1-265 |
| Borough | `borough` | `Borough` | STRING | Y | | Same category set as `taxi_zone_lookup.borough` |
| Zone name | `zone` | `Zone` | STRING | Y | | Display name; not unique. Note the column is `zone`, not `zone_name` as in `taxi_zone_lookup` |
| Service zone | `service_zone` | `service_zone` | STRING | Y | | Same category set as `taxi_zone_lookup.service_zone` |
| Geometry | `geometry` | `taxi_zones.shp` polygon | GEOMETRY (WKB) | Y | | Zone polygon in `EPSG:4326`; null only for `location_id` 264 and 265 |
| Representative latitude | `representative_latitude` | Derived from `geometry` | DOUBLE | Y | | `ST_PointOnSurface`/`representative_point()` of `geometry`, guaranteed to fall inside the polygon (unlike a naive centroid); null wherever `geometry` is null (`location_id` 264 and 265) |
| Representative longitude | `representative_longitude` | Derived from `geometry` | DOUBLE | Y | | Paired with `representative_latitude`; same nullability |

`zone_profile_features` and `zone_scores` are 1:1 extensions of this table,
keyed by the same `location_id`. `zone_master` overlaps with the
already-catalogued `taxi_zone_lookup` but is a separate physical table.
**Resolved (OQ-029, accepted 2026-08-19 —
`docs/adr/0005-zone-master-canonical-geometry-reference.md`):** `zone_master`
is the canonical geometry-bearing zone reference — it, not
`taxi_zone_lookup`, is what other contracts should join to for zone geometry
or a representative point. `taxi_zone_lookup` remains the raw TLC
name/borough lookup; unifying the two physical tables is a separate,
still-unplanned implementation question.

`representative_latitude`/`representative_longitude` were added for issue
#193's weather collection: the Open-Meteo collector job (a `batch-jobs`
component, cross-service from `zone_master`'s owning `sensor-producer`
zone-profile pipeline — flagged and confirmed with the project owner) reads
`zone_master.parquet` directly to get each zone's query point, then writes
the coordinates it actually used into
`latest_zone_weather.latitude`/`.longitude` (see below) rather than
querying `zone_master` at read time.

## `zone_profile_features`

**Layer:** Silver (zone-profile feature aggregate). **Storage:** local Parquet
at `data/processed/zone_profile_features.parquet`. **Built by:**
`services/sensor-producer/src/zone_profile/build_zone_profile_features.py`.
**Grain:** one row per `zone_master.location_id`.

**Primary key:** `location_id`. **Foreign key:** `location_id` references
`zone_master.location_id`.

Aggregates MapPLUTO, ACS, LODES WAC, OSM POI, MTA, NYC Facilities, NYC Parks,
and NYS DOH data via spatial join onto zone polygons; category scores are
computed downstream in `zone_scores`, not here. Column names follow
`<source>_<measure>`: `_count` is a raw joined count, `_density` is count /
zone area (km²), `_ratio` is normalized by a same-source total (for example
jobs or building area).

| Attribute | Column | Type | Nullable | Key | Description |
| --- | --- | --- | --- | --- | --- |
| Zone ID | `location_id` | INTEGER | N | PK, FK | References `zone_master.location_id` |
| Residential area ratio | `residential_area_ratio` | DOUBLE | Y |  | MapPLUTO residential floor area / total building floor area |
| Office area ratio | `office_area_ratio` | DOUBLE | Y |  | MapPLUTO office floor area / total building floor area |
| Commercial area ratio | `commercial_area_ratio` | DOUBLE | Y |  | MapPLUTO commercial floor area / total building floor area |
| Retail area ratio | `retail_area_ratio` | DOUBLE | Y |  | MapPLUTO retail floor area / total building floor area |
| Residential unit density | `residential_unit_density` | DOUBLE | Y |  | MapPLUTO residential units / zone area (km²) |
| Population | `population` | DOUBLE | Y |  | ACS block-group population, area-weighted into the zone |
| Household count | `household_count` | DOUBLE | Y |  | ACS households, area-weighted into the zone |
| Family household ratio | `family_household_ratio` | DOUBLE | Y |  | Family households / all households |
| Children household ratio | `children_household_ratio` | DOUBLE | Y |  | Households with children / all households |
| Senior ratio | `senior_ratio` | DOUBLE | Y |  | Population age 65+ / total population |
| Median household income | `median_household_income` | DOUBLE | Y |  | Household-weighted approximation across intersecting block groups |
| Median home value | `median_home_value` | DOUBLE | Y |  | Household-weighted approximation across intersecting block groups |
| Median gross rent | `median_gross_rent` | DOUBLE | Y |  | Household-weighted approximation across intersecting block groups |
| Total jobs | `total_jobs` | DOUBLE | Y |  | LODES WAC total jobs (`C000`) located in the zone |
| Job density | `job_density` | DOUBLE | Y |  | `total_jobs` / zone area (km²) |
| Retail job ratio | `retail_job_ratio` | DOUBLE | Y |  | Retail jobs (`CNS07`) / `total_jobs` |
| Information job ratio | `information_job_ratio` | DOUBLE | Y |  | Information jobs (`CNS09`) / `total_jobs` |
| Finance job ratio | `finance_job_ratio` | DOUBLE | Y |  | Finance jobs (`CNS10`) / `total_jobs` |
| Real estate job ratio | `real_estate_job_ratio` | DOUBLE | Y |  | Real-estate jobs (`CNS11`) / `total_jobs` |
| Professional job ratio | `professional_job_ratio` | DOUBLE | Y |  | Professional-services + management jobs (`CNS12`+`CNS13`) / `total_jobs` |
| Education job ratio | `education_job_ratio` | DOUBLE | Y |  | Education jobs (`CNS15`) / `total_jobs` |
| Healthcare job ratio | `healthcare_job_ratio` | DOUBLE | Y |  | Healthcare jobs (`CNS16`) / `total_jobs` |
| Arts/recreation job ratio | `arts_recreation_job_ratio` | DOUBLE | Y |  | Arts/recreation jobs (`CNS17`) / `total_jobs` |
| Accommodation/food job ratio | `accommodation_food_job_ratio` | DOUBLE | Y |  | Accommodation/food-service jobs (`CNS18`) / `total_jobs` |
| Public admin job ratio | `public_admin_job_ratio` | DOUBLE | Y |  | Public-administration jobs (`CNS20`) / `total_jobs` |
| Shop POI count | `poi_shop_count` | DOUBLE | Y |  | OSM nodes tagged `shop=*` inside the zone |
| Restaurant POI count | `poi_restaurant_count` | DOUBLE | Y |  | OSM `amenity` in (`restaurant`, `cafe`) inside the zone |
| Nightlife POI count | `poi_nightlife_count` | DOUBLE | Y |  | OSM `amenity` in (`bar`, `pub`, `nightclub`) inside the zone |
| Hotel POI count | `poi_hotel_count` | DOUBLE | Y |  | OSM `tourism=hotel` inside the zone |
| Museum POI count | `poi_museum_count` | DOUBLE | Y |  | OSM `tourism=museum` inside the zone |
| Attraction POI count | `poi_attraction_count` | DOUBLE | Y |  | OSM `tourism=attraction` inside the zone |
| Shop POI density | `poi_shop_density` | DOUBLE | Y |  | `poi_shop_count` / zone area (km²) |
| Restaurant POI density | `poi_restaurant_density` | DOUBLE | Y |  | `poi_restaurant_count` / zone area (km²) |
| Nightlife POI density | `poi_nightlife_density` | DOUBLE | Y |  | `poi_nightlife_count` / zone area (km²) |
| Hotel POI density | `poi_hotel_density` | DOUBLE | Y |  | `poi_hotel_count` / zone area (km²) |
| Museum POI density | `poi_museum_density` | DOUBLE | Y |  | `poi_museum_count` / zone area (km²) |
| Attraction POI density | `poi_attraction_density` | DOUBLE | Y |  | `poi_attraction_count` / zone area (km²) |
| Subway complex count | `subway_complex_count` | DOUBLE | Y |  | Distinct MTA station complexes inside the zone |
| Subway station count | `subway_station_count` | DOUBLE | Y |  | Sum of stations per complex inside the zone |
| Education facility count | `facility_education_count` | DOUBLE | Y |  | NYC Facilities classified `education` inside the zone |
| Medical facility count | `facility_medical_count` | DOUBLE | Y |  | NYC Facilities classified `medical` inside the zone |
| Government facility count | `facility_government_count` | DOUBLE | Y |  | NYC Facilities classified `government` inside the zone |
| Cultural facility count | `facility_cultural_count` | DOUBLE | Y |  | NYC Facilities classified `cultural` inside the zone |
| Park area | `park_area_km2` | DOUBLE | Y |  | NYC Parks polygon area intersecting the zone, in km² |
| Park area ratio | `park_area_ratio` | DOUBLE | Y |  | Park intersection area / zone area |
| DOH hospital bed count | `doh_hospital_bed_count` | DOUBLE | Y |  | NYS DOH permanent hospital bed count, summed over hospitals located in the zone |

## `zone_scores`

**Layer:** Gold (zone score/tag). **Storage:** local Parquet at
`data/processed/zone_scores.parquet`. **Built by:**
`services/sensor-producer/src/zone_profile/generate_zone_scores.py`.
**Grain:** one row per `zone_profile_features.location_id`.

**Primary key:** `location_id`. **Foreign key:** `location_id` references
`zone_profile_features.location_id`.

Each category score and `comfort_relevance_score` is a weighted average of
percentile-normalized `zone_profile_features` columns (weights in
`CATEGORY_WEIGHTS`/`COMFORT_WEIGHTS`); a score is `NULL` if less than 70%
(`MIN_FEATURE_COVERAGE`) of its weight has non-null input. Scoring excludes
`location_id` 264, 265, and 1 (Newark Airport/EWR), which get
`zone_tag = "excluded"`.

| Attribute | Column | Type | Nullable | Key | Description |
| --- | --- | --- | --- | --- | --- |
| Zone ID | `location_id` | INTEGER | N | PK, FK | References `zone_profile_features.location_id` |
| Business score | `business_score` | DOUBLE | Y | | Office area, job density, and finance/professional/information/real-estate job ratios |
| Residential score | `residential_score` | DOUBLE | Y | | Residential building-area ratio and residential unit density |
| Shopping score | `shopping_score` | DOUBLE | Y | | Retail area ratio, retail job ratio, and shop-POI density |
| Nightlife score | `nightlife_score` | DOUBLE | Y | | Restaurant/nightlife POI density and accommodation-food job ratio |
| Tourism score | `tourism_score` | DOUBLE | Y | | Hotel/museum/attraction POI density and arts-recreation job ratio |
| Transit score | `transit_score` | DOUBLE | Y | | Subway station count and subway complex count |
| Public service score | `public_service_score` | DOUBLE | Y | | Healthcare/education/public-admin job ratios, medical/education/government facility counts, and DOH hospital-bed count |
| Park score | `park_score` | DOUBLE | Y | | Park area ratio, park area, and arts-recreation job ratio |
| Zone tag | `zone_tag` | STRING | N | | English zone-character label; see value table below |
| Zone tag (Korean) | `zone_tag_ko` | STRING | N | | Korean label paired 1:1 with `zone_tag` |
| Comfort relevance score | `comfort_relevance_score` | DOUBLE | Y | | Income, home value, family/children-household ratio, senior ratio, and medical-capacity weighted score; a candidate proxy for comfort-improvement demand, **not** the sensor-based `comfort_score` in `segment_comfort_score` |

All score columns are validated to fall within `[0, 1]` when non-null.
`zone_tag` is assigned from `TAG_RULES` in `generate_zone_scores.py`; an
unmatched zone falls back to its single highest category score. Possible
values:

| `zone_tag` | `zone_tag_ko` | Origin |
| --- | --- | --- |
| `luxury_residential` | 고급주거 | Tag rule |
| `finance_business` | 금융·업무 | Tag rule |
| `residential_medical` | 주거·의료 | Tag rule |
| `education_residential` | 교육·주거 | Tag rule |
| `shopping_tourism` | 쇼핑·관광 | Tag rule |
| `transit_business` | 교통·업무 | Tag rule |
| `dining_nightlife` | 외식·야간 | Tag rule, also the `nightlife_score` fallback label |
| `business` | 업무·비즈니스 | `business_score` fallback label |
| `residential` | 주거 | `residential_score` fallback label |
| `shopping` | 쇼핑 | `shopping_score` fallback label |
| `tourism_culture` | 관광·문화 | `tourism_score` fallback label |
| `transit` | 교통·환승 | `transit_score` fallback label |
| `public_service` | 행정·의료·교육 | `public_service_score` fallback label |
| `park_leisure` | 공원·레저 | `park_score` fallback label |
| `excluded` | 분석 제외 | Zone excluded from scoring |

## `od_priority`

**Layer:** Gold (OD demand/priority dataset). **Storage:** local Parquet at
`data/processed/od_priority.parquet`. **Built by:**
`services/sensor-producer/src/zone_profile/generate_od_priority.py`.
**Grain:** one row per selected `(pu_location_id, do_location_id)` pair — the
top 1000 by `priority_score` among the valid OD universe.

**Primary key:** (`pu_location_id`, `do_location_id`). **Foreign keys:**
`pu_location_id` and `do_location_id` each reference `zone_scores.location_id`.

Built from 2021-01–2025-12 NYC TLC HVFHV trip records (`hvfhv_trip_input`,
remote Parquet; only `PULocationID`/`DOLocationID` are read), aggregated one
month at a time with DuckDB `read_parquet()` and summed into a single 5-year
`trip_count` per OD pair — raw trip rows are never materialized in bulk.
Before scoring, the OD universe is filtered: pairs with a null
`pu_location_id`/`do_location_id`, or with either endpoint in `{1, 264, 265}`
(EWR and the TLC-lookup-only placeholders — the same exclusion `zone_scores`
applies to scoring), are dropped; pairs where either endpoint's
`zone_scores.comfort_relevance_score` is `NULL` (excluded zone, or scored but
below `MIN_FEATURE_COVERAGE`) are also dropped. Same-zone pairs
(`pu_location_id == do_location_id`) are kept — the router can select distinct
LION nodes within one zone, so they remain valid routing candidates.

| Attribute | Column | Type | Nullable | Key | Description |
| --- | --- | --- | --- | --- | --- |
| Pickup zone | `pu_location_id` | INTEGER | N | PK, FK | References `zone_scores.location_id` |
| Dropoff zone | `do_location_id` | INTEGER | N | PK, FK | References `zone_scores.location_id` |
| Trip count | `trip_count` | BIGINT | N | | 2021-01–2025-12 cumulative HVFHV trip count for this OD pair |
| Demand score | `demand_score` | DOUBLE | N | | Percentile rank of `trip_count` (`rank(method="average", pct=True)`) computed over the full valid OD universe, not just the selected top 1000 |
| PU comfort relevance | `pu_comfort_relevance_score` | DOUBLE | N | | `zone_scores.comfort_relevance_score` joined on `pu_location_id` |
| DO comfort relevance | `do_comfort_relevance_score` | DOUBLE | N | | `zone_scores.comfort_relevance_score` joined on `do_location_id` |
| OD relevance score | `od_relevance_score` | DOUBLE | N | | `(pu_comfort_relevance_score + do_comfort_relevance_score) / 2` |
| Priority score | `priority_score` | DOUBLE | N | | `demand_score * od_relevance_score` |
| Priority rank | `priority_rank` | INTEGER | N | | 1–1000, assigned after sorting by `priority_score` DESC, `trip_count` DESC, `pu_location_id` ASC, `do_location_id` ASC (deterministic tie-break) |

All score columns (`demand_score`, `pu_comfort_relevance_score`,
`do_comfort_relevance_score`, `od_relevance_score`, `priority_score`) are
validated to fall within `[0, 1]`. The table always has exactly 1000 rows with
no duplicate `(pu_location_id, do_location_id)` pairs. `zone_scores` category
scores (e.g. `business_score`) and tags are intentionally not carried into
this table.

## Required but missing contracts

The following referenced entities need schemas before dependent contracts can be
implemented completely:

- taxi-zone geometry keyed by `location_id`
- source snapshot and pipeline-run metadata
- rejection and quarantine records
- API request, response, and error models
