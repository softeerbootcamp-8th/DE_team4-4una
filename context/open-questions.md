---
owner: project-team
status: active
last_reviewed: 2026-08-10
---

# Open Questions and Decision Register

Agents must not silently resolve these questions in implementation. A task may
propose an answer with evidence, but acceptance should be recorded here and in an
ADR when the choice affects multiple components.

## Blocking architecture and contract decisions

| ID | Question | Why it matters | Status |
| --- | --- | --- | --- |
| OQ-001 | Are LION IDs or Street Pavement Rating IDs canonical? | Resolved: LION source `SegmentID`, stored as `segment_id`, is canonical | Accepted 2026-08-10 |
| OQ-002 | Which S3 file/table format and local development access pattern will be used? | S3 is confirmed for Bronze, but Spark read/write and local execution still depend on these choices | Open |
| OQ-003 | Which Kafka implementation, topics, partitions, and serialization format will be used locally? | Prototype uses Apache Kafka 4.3.1, configurable topic, trip-ID key, and JSON values; production partition count remains open | Prototype accepted 2026-08-10 |
| OQ-004 | Which database will serve latest scores? | Controls gold loading, indexing, and API implementation | Open |
| OQ-005 | Which routing engine or algorithm will operate on the canonical segment network? | Prototype uses deterministic Dijkstra routing over directed LION nodes and SegmentIDs | Prototype accepted 2026-08-10 |
| OQ-006 | What is the comfort-score direction, formula, component weights, and minimum coverage? | The 0-100 range is confirmed; remaining semantics control Gold and API behavior | Open |

## Data decisions

| ID | Question | Why it matters | Status |
| --- | --- | --- | --- |
| OQ-007 | What exact source URLs, releases, licenses, and schemas are approved? | Smoke-test endpoints and checksums are recorded in `context/runs/2026-08-10-nyc-sensor-smoke.md`; long-term snapshot policy remains open | Partially accepted 2026-08-10 |
| OQ-008 | Which HVFHV source day should be demonstrated? | Prototype smoke test uses 2024-02-01; final showcase day may still change | Prototype accepted 2026-08-10 |
| OQ-009 | How are eligible rows filtered and stably identified? | Prototype uses valid same-zone trips in stable timestamp/base order and derives a content-based trip ID | Prototype accepted 2026-08-10 |
| OQ-010 | How should endpoints be selected when a taxi zone has no valid canonical road point? | Controls rejects and route coverage | Open |
| OQ-011 | How are pavement observations and speed humps spatially assigned near ambiguous segment boundaries? | Prototype uses normalized street name plus nearest geometry within approximately 39 m; exact production policy remains open | Prototype accepted 2026-08-10 |
| OQ-012 | What does the monthly score period represent: trip month, replay month, reference snapshot month, or publication month? | Controls grouping and API freshness | Open |

## Simulation decisions

| ID | Question | Why it matters | Status |
| --- | --- | --- | --- |
| OQ-013 | What exact vehicle models, model years, trims, and parameter sources are used? | Required for credible stable vehicle profiles | Open |
| OQ-014 | Does "Genesis" mean the brand, Genesis G80, or another model? | Current vehicle identifier is ambiguous | Open |
| OQ-015 | Does "EV5" refer to Kia EV5, and should it remain in a Hyundai-focused set? | Current manufacturer/model description is ambiguous | Open |
| OQ-016 | Is each sampled trip simulated for one vehicle type or all supported vehicle types, and how does that affect `trip_id`? | Changes event volume and determines whether `(trip_id, trip_seq)` is unique | Open |
| OQ-017 | Are overlapping trips replayed concurrently, and are long idle gaps preserved or capped? | Prototype interleaves overlapping trips and preserves gaps; explicit `time_scale=0` removes waits in tests | Prototype accepted 2026-08-10 |
| OQ-018 | How is speed determined along each route: source average, road class, synthetic profile, or another source? | Prototype uses a smoothstep profile over route length and source passenger duration; calibration remains open | Prototype accepted 2026-08-10 |

## Product decisions

| ID | Question | Why it matters | Status |
| --- | --- | --- | --- |
| OQ-019 | What API path, authentication model, error format, and missing-score response are required? | Needed before an OpenAPI contract can be accepted | Open |
| OQ-020 | Must the API expose history or only the latest score? | Controls serving schema and indexes | Open |
| OQ-021 | Is the dashboard part of the first required demonstration? | Controls milestone scope | Open |
| OQ-022 | What latency, availability, and coverage targets define success? | Needed for testing and architecture tradeoffs | Open |

## Schema decisions

| ID | Question | Why it matters | Status |
| --- | --- | --- | --- |
| OQ-023 | What sensor sampling frequency is used for the demonstration? | 10 Hz, producing one sample per 100 ms; configurable for tests | Accepted 2026-08-10 |
| OQ-024 | Should Silver `jerk` be nullable when `trip_seq` has a gap, or should such rows be rejected/quarantined? | Supplied `Nullable=N` conflicts with the stated rule to set invalid jerk to `NULL` | Open |
| OQ-025 | Is `event_id` serialized as a STRING containing a UUID or stored using a native UUID type? | Kafka JSON and the executable Bronze contract use a UUID-formatted STRING | Accepted 2026-08-10 |
| OQ-026 | What are the exact units and axis semantics for `accel_x`, `accel_y`, `accel_z`, and `jerk`? | Prototype uses longitudinal x, lateral y, vertical z in m/s² and longitudinal jerk in m/s³ | Accepted 2026-08-10 |
| OQ-027 | What are the physical names of the unnamed source, reference, and Gold tables? | Required for storage layout, SQL, and catalog registration | Open |
| OQ-028 | What is the `vehicle_profile` schema? | Producer has versioned synthetic response profiles for four named vehicles; a persisted shared dimension remains open | Partially accepted 2026-08-10 |
| OQ-029 | What is the taxi-zone geometry schema and source? | `dim_taxi_zone` alone cannot choose pickup/drop-off road points | Open |
| OQ-030 | What are the accepted enums for map-match status and the three reference quality flags? | Required for shared contracts and data-quality metrics | Open |
| OQ-031 | Must corrections to an existing `enriched_segment_reference` row retain prior versions, or is an in-place upsert sufficient? | `updated_at` exposes the latest rebuild but does not itself preserve audit history | Open |
| OQ-032 | Is jerk's mentioned PDI weight of `0.25` accepted, and what is the complete PDI formula? | One isolated weight is insufficient to reproduce the Gold score | Open |
| OQ-033 | Is a cross-region interruption and lossless-recovery demonstration part of the required prototype? | It is mentioned in the sequence rationale but not in the consolidated project scope | Open |
| OQ-034 | Does idempotency deduplication occur before Bronze persistence or during Bronze-to-Silver processing? | At-least-once duplicates can conflict with the strict one-Bronze-row-to-one-Silver-row invariant | Open |
| OQ-035 | Is `jerk` generated by the producer, recalculated in Silver from acceleration, or both with separate column names? | Producer emits longitudinal jerk in Bronze; Silver should validate/recalculate it for scoring and gap handling | Accepted 2026-08-10 |

## Proposed decision workflow

1. Assign an owner and target milestone to the question.
2. Record alternatives and evidence in an issue or ADR draft.
3. Mark the chosen answer as accepted with a date and link.
4. Update every affected context document and executable contract in the same
   pull request.
5. Keep superseded decisions discoverable through ADR history.
