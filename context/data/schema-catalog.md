---
owner: data-engineering
status: draft-contract
last_reviewed: 2026-08-11
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
| `taxi_zone_lookup` | `dim_taxi_zone` | Reference dimension | One TLC taxi zone |
| `sensor_event` | `sensor_event` | Bronze on S3 | One vehicle sensor measurement |
| `road_segment` | `road_segment` | Normalized reference | One LION segment per snapshot date |
| `enriched_segment_reference` | `enriched_segment_reference` | Enriched reference | One segment per integrated reference date |
| `sensor_events_matched` | `sensor_events_matched` | Silver | One row per Bronze sensor event |
| `segment_comfort_score` | TBD | Gold | One segment x vehicle profile x score month |

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
| Pickup zone | `pu_location_id` | `PULocationID` | INTEGER | N | FK to `dim_taxi_zone.location_id` |
| Drop-off zone | `do_location_id` | `DOLocationID` | INTEGER | N | FK to `dim_taxi_zone.location_id` |
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

## `dim_taxi_zone`

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

## `sensor_event`

**Layer:** Bronze. **Storage:** S3. **Grain:** one sensor measurement.

`segment_id` is intentionally absent. GPS-to-LION matching occurs in Spark and
is stored in `sensor_events_matched`. The earlier draft without `trip_seq` is
superseded by this version.

| Attribute | Column | Source | Type | Nullable | Key | Description |
| --- | --- | --- | --- | --- | --- | --- |
| Sensor event ID | `event_id` | Generated | STRING | N | PK | Deterministic UUID-formatted event identifier |
| Vehicle ID | `vehicle_id` | Generated | STRING | N |  | Vehicle instance identifier |
| Vehicle profile | `vehicle_profile_id` | Generated | INTEGER | N | FK | References `vehicle_profile` |
| Trip ID | `trip_id` | Generated | STRING | N |  | Simulated journey identifier |
| Trip sequence | `trip_seq` | Generated | BIGINT | N |  | Zero-based sample order within a trip |
| Event time | `event_time` | Generated | TIMESTAMP | N |  | Sensor measurement time |
| Latitude | `latitude` | Generated | DOUBLE | N |  | GPS latitude |
| Longitude | `longitude` | Generated | DOUBLE | N |  | GPS longitude |
| Speed | `speed_mps` | Generated | DOUBLE | N |  | Meters per second |
| Heading | `heading` | Generated | DOUBLE | Y |  | Direction from 0 through 360 degrees |
| Longitudinal acceleration | `accel_x` | Generated | DOUBLE | Y |  | Forward/backward acceleration in m/s² |
| Lateral acceleration | `accel_y` | Generated | DOUBLE | Y |  | Side-to-side acceleration in m/s² |
| Vertical acceleration | `accel_z` | Generated | DOUBLE | N |  | Vertical vibration or impact in m/s² |
| Legacy jerk | `jerk` | Generated | DOUBLE | N |  | Compatibility alias of `jerk_x` in m/s³ |
| Longitudinal jerk | `jerk_x` | Generated | DOUBLE | N |  | Change in `accel_x` per second in m/s³ |
| Lateral jerk | `jerk_y` | Generated | DOUBLE | N |  | Change in `accel_y` per second in m/s³ |
| Vertical jerk | `jerk_z` | Generated | DOUBLE | N |  | Change in `accel_z` per second in m/s³ |
| Ingested time | `_ingested_at` | Derived | TIMESTAMP | N |  | Bronze load time |
| Run ID | `_run_id` | Derived | STRING | N |  | Simulation and ingestion run identifier |

`event_id` is the declared primary key. `(trip_id, trip_seq)` is the proposed
deterministic idempotency and ordering key, subject to the multi-vehicle trip-ID
decision in `open-questions.md`.

## `road_segment`

**Grain:** one LION segment per snapshot date.

**Primary key:** `(segment_id, snapshot_date)`.

| Attribute | Column | Source column | Type | Nullable | Key | Description |
| --- | --- | --- | --- | --- | --- | --- |
| LION segment ID | `segment_id` | `SegmentID` | STRING | N | PK | Canonical road segment ID |
| Snapshot date | `snapshot_date` | Derived | DATE | N | PK | LION snapshot date |
| Street name | `street_name` | `Street` | STRING | N |  | Road name |
| From node | `from_node_id` | `NodeIDFrom` | BIGINT | N |  | Starting node |
| To node | `to_node_id` | `NodeIDTo` | BIGINT | N |  | Ending node |
| Traffic direction | `traffic_direction` | `TrafDir` | STRING | Y |  | Permitted vehicle direction |
| Segment type | `segment_type` | `SegmentTyp` | STRING | N |  | LION segment type |
| Feature type | `feature_type` | `FeatureTyp` | STRING | N |  | LION feature type |
| Roadbed layer | `roadbed_layer` | `RB_Layer` | STRING | N |  | Grade-separated roadbed discriminator |
| From-node level | `from_node_level` | `NodeLevelF` | STRING | N |  | Starting grade level |
| To-node level | `to_node_level` | `NodeLevelT` | STRING | N |  | Ending grade level |
| Posted speed | `posted_speed_mph` | `POSTED_SPEED` | INTEGER | Y |  | Miles per hour |
| Curve flag | `curve_flag` | `CurveFlag` | STRING | Y |  | Whether the segment is curved |
| Curve radius | `curve_radius` | `Radius` | DOUBLE | Y |  | Radius used for curve effects; unit must be confirmed |
| Length | `length_m` | Converted from `SHAPE_Length` | DOUBLE | N |  | Meters |
| Taxi zone | `location_id` | Spatial mapping | INTEGER | Y | FK | TLC taxi zone containing the segment |
| Geometry | `geometry` | `SHAPE` | GEOMETRY | N |  | LineString or MultiLineString |
| Ingested time | `_ingested_at` | Derived | TIMESTAMP | N |  | Pipeline load time |

## `enriched_segment_reference`

**Grain:** one road segment per integrated `reference_date`.

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

## `sensor_events_matched`

**Layer:** Silver. **Grain:** exactly one row for every Bronze `sensor_event` row.

It performs GPS-to-LION map matching and versioned threshold-based event
classification. Unmatched and ambiguous observations are retained.

| Attribute | Column | Source | Type | Nullable | Key | Description |
| --- | --- | --- | --- | --- | --- | --- |
| Sensor event ID | `event_id` | `sensor_event.event_id` | STRING | N | PK | UUID-formatted Bronze event identifier |
| Vehicle ID | `vehicle_id` | `vehicle_id` | STRING | N |  | Vehicle instance |
| Vehicle profile | `vehicle_profile_id` | `vehicle_profile_id` | INTEGER | N | FK | References `vehicle_profile` |
| Trip ID | `trip_id` | `trip_id` | STRING | N |  | Simulated journey |
| Trip sequence | `trip_seq` | `trip_seq` | BIGINT | N |  | Sample order within the trip |
| Event time | `event_time` | `event_time` | TIMESTAMP | N |  | Sensor measurement time |
| Latitude | `latitude` | `latitude` | DOUBLE | N |  | Validated latitude |
| Longitude | `longitude` | `longitude` | DOUBLE | N |  | Validated longitude |
| Speed | `speed_mps` | `speed_mps` | DOUBLE | N |  | Validated meters per second |
| Heading | `heading` | `heading` | DOUBLE | Y |  | Direction in degrees |
| Longitudinal acceleration | `accel_x` | `accel_x` | DOUBLE | Y |  | Forward/backward acceleration |
| Lateral acceleration | `accel_y` | `accel_y` | DOUBLE | Y |  | Side-to-side acceleration |
| Vertical acceleration | `accel_z` | `accel_z` | DOUBLE | N |  | Vertical impact or vibration |
| Legacy jerk | `jerk` | `jerk` | DOUBLE | N |  | Compatibility alias of `jerk_x`; nullability conflict is open |
| Longitudinal jerk | `jerk_x` | `jerk_x` | DOUBLE | N |  | Change in `accel_x` per second; nullability conflict is open |
| Lateral jerk | `jerk_y` | `jerk_y` | DOUBLE | N |  | Change in `accel_y` per second; nullability conflict is open |
| Vertical jerk | `jerk_z` | `jerk_z` | DOUBLE | N |  | Change in `accel_z` per second; nullability conflict is open |
| Road segment | `segment_id` | GPS-LION match | STRING | Y | FK | Matched canonical LION segment |
| LION snapshot date | `road_snapshot_date` | Matching reference | DATE | N | FK | `road_segment` version used |
| Match status | `match_status` | Derived | STRING | N |  | For example `MATCHED`, `UNMATCHED`, or `AMBIGUOUS` |
| Match distance | `match_distance_m` | Derived | DOUBLE | Y |  | Distance from point to matched segment |
| Match method | `match_method` | Derived | STRING | Y |  | For example nearest or distance+heading |
| Heading difference | `heading_diff_deg` | Derived | DOUBLE | Y |  | Difference between vehicle and road direction |
| Candidate count | `candidate_count` | Derived | INTEGER | N |  | Segments inside the matching radius |
| Hard-brake flag | `brake_flag` | `accel_x`, `jerk_x` | BOOLEAN | N |  | Versioned threshold result |
| Hard-acceleration flag | `accel_flag` | `accel_x`, `jerk_x` | BOOLEAN | N |  | Versioned threshold result |
| Discomfort flag | `discomfort_flag` | Sensor values | BOOLEAN | N |  | Versioned discomfort classification |
| Scoring version | `scoring_version` | Derived | STRING | N |  | Threshold and classification rule version |
| Processed time | `_processed_at` | Derived | TIMESTAMP | N |  | Silver completion time |
| Run ID | `_run_id` | Derived | STRING | N |  | Spark/Airflow ETL run identifier |

## `segment_comfort_score`

The physical table name is not yet confirmed.

**Grain:** one LION segment x vehicle profile x score month.

**Primary key:** `(segment_id, vehicle_profile_id, score_month)`.

| Attribute | Column | Type | Nullable | Key | Description |
| --- | --- | --- | --- | --- | --- |
| Road segment | `segment_id` | STRING | N | PK, FK | Canonical LION segment |
| Vehicle profile | `vehicle_profile_id` | INTEGER | N | PK, FK | References `vehicle_profile` |
| Score month | `score_month` | DATE | N | PK | Month to which the score applies, stored as the first day |
| Data-period start | `data_period_start` | DATE | N |  | First included driving-data date |
| Data-period end | `data_period_end` | DATE | N |  | Last included driving-data date |
| Reference date | `reference_date` | DATE | N | FK | `enriched_segment_reference` version used |
| Comfort score | `comfort_score` | DOUBLE | N |  | Final score from 0 through 100 |
| Vertical score | `vertical_score` | DOUBLE | N |  | Based on `accel_z`, pavement, and humps |
| Longitudinal score | `longitudinal_score` | DOUBLE | N |  | Based on acceleration, braking, and jerk |
| Lateral score | `lateral_score` | DOUBLE | Y |  | Based on lateral acceleration and curve features |
| Pavement score | `pavement_score` | DOUBLE | Y |  | Pavement contribution to comfort |
| Average speed | `avg_speed_mps` | DOUBLE | Y |  | Average segment speed in the driving-data period |
| Vertical acceleration P95 | `p95_accel_z` | DOUBLE | Y |  | 95th percentile of vertical acceleration |
| Jerk P95 | `p95_jerk` | DOUBLE | Y |  | 95th percentile of jerk |
| Hard-brake count | `hard_brake_count` | INTEGER | N |  | Hard-braking events in the driving-data period |
| Hard-acceleration count | `hard_accel_count` | INTEGER | N |  | Hard-acceleration events in the driving-data period |
| Discomfort-event count | `discomfort_event_count` | INTEGER | N |  | All discomfort events in the driving-data period |
| Sensor sample count | `sample_count` | BIGINT | N |  | Sensor events used by the score |
| Confidence score | `confidence_score` | DOUBLE | N |  | Coverage and data-quality confidence |
| Score version | `score_version` | STRING | N |  | Comfort formula version |
| Calculated time | `calculated_at` | TIMESTAMP | N |  | Gold calculation time |

## Required but missing contracts

The following referenced entities need schemas before dependent contracts can be
implemented completely:

- `vehicle_profile`
- taxi-zone geometry keyed by `location_id`
- source snapshot and pipeline-run metadata
- rejection and quarantine records
- API request, response, and error models
