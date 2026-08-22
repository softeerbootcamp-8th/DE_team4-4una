---
owner: project-team
status: draft
last_reviewed: 2026-08-10
---

# Glossary

| Term | Meaning in this project |
| --- | --- |
| Canonical road segment | A LION road segment keyed by source `SegmentID`, represented as `segment_id` |
| Comfort score | A versioned monthly 0-100 summary of simulated ride-comfort features for a LION segment and vehicle profile; direction is not yet defined |
| Dispatch event | A replay event created from a completed HVFHV row at its source request time; passenger motion begins at pickup |
| Gold dataset | Published monthly comfort-score records ready to load into the serving store |
| HVFHV | NYC TLC High Volume For-Hire Vehicle trip records used as simulation inputs |
| LION | NYC's street and address base map, considered as one source for road topology and segment identity |
| Occupied journey | The passenger portion from simulated pickup to simulated drop-off; excludes travel to pickup |
| Reference data | Monthly road, pavement, hump, and taxi-zone inputs used to build the simulation environment |
| Road environment | A versioned routable network enriched with road-condition and speed-hump attributes |
| Score period | The calendar or source month represented by a gold score; exact period semantics remain open |
| Serving store | The database or index read by the API; technology is not yet selected |
| Simulation offset | Deterministic elapsed simulated time from a trip or run origin, independent of wall-clock execution time |
| Source snapshot | Immutable copy plus metadata for one downloaded source version |
| Taxi zone | TLC geographic zone used to constrain deterministic pickup and drop-off road-point selection |
| Trip sequence | `trip_seq`, the zero-based deterministic sample order within one simulated trip |
| Vehicle profile | Versioned set of synthetic physical-response parameters for a supported vehicle type |

## Naming guidance

- Use `segment_id` for the canonical LION `SegmentID` after map matching.
- Do not add `segment_id` to Bronze `sensor_event`; the authoritative match first
  appears in Silver `sensor_events_matched`.
- Distinguish source event time, simulation time, ingestion time, and processing
  time in names and contracts.
- Use `score_period` for the data period and `calculated_at` for calculation time.
- Do not use `realtime` to describe the monthly score. It applies only to the
  wall-clock replay and streaming ingestion path.
