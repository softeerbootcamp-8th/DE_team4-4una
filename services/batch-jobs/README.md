# Monthly road-environment batch

`batch-jobs` owns acquisition, immutable source retention, normalization, spatial
enrichment, quality validation, and publication of the road environment consumed
by `sensor-producer`. The Producer never maps raw pavement or hump records.

## Local Spark execution requires JDK 21

pyspark 실행/테스트를 컨테이너 밖(호스트)에서 직접 돌리려면 `JAVA_HOME`이 **JDK 21**
(배포 이미지가 쓰는 버전, `Dockerfile` 참고)을 가리켜야 한다. JDK 24부터 Security
Manager가 완전히 제거되면서(JEP 486), pyspark가 번들한 Hadoop 클라이언트의
`UserGroupInformation`/`Subject.getSubject()` 호출이 `UnsupportedOperationException`으로
깨진다 — `great-expectations[spark]`가 강제하는 `pyspark<4.2`(ADR-0004)에서 특히
쉽게 재현된다(`pyspark==4.2.0`은 이 경로를 안 타서 JDK 25에서도 동작했지만, GX
때문에 다운그레이드된 `4.1.3`은 깨진다).

```bash
brew install openjdk@21
export JAVA_HOME=/opt/homebrew/opt/openjdk@21   # pytest/batch-jobs 실행 셸에서
```

## Local data-lake rehearsal

Fetch a bounded real-NYC snapshot for development:

```bash
uv run --package batch-jobs batch-jobs fetch-reference-data \
  --output-dir .local/reference-source \
  --snapshot-date 2026-08-11 \
  --bbox -74.02 40.63 -73.94 40.70
```

Build all layers and promote the validated environment:

```bash
uv run --package batch-jobs batch-jobs build-road-environment \
  --source-dir .local/reference-source \
  --data-lake-uri "file://$PWD/.local/data-lake" \
  --reference-date 2026-08-01 \
  --road-snapshot-date 2026-08-11 \
  --build-id local-20260811-v1 \
  --activate
```

`run-monthly` combines those two steps and is the command intended for an
EventBridge-scheduled container. Omit `--bbox` to request the complete sources:

```bash
uv run --package batch-jobs batch-jobs run-monthly \
  --data-lake-uri s3://de4-reference-us \
  --reference-date 2026-08-01 \
  --road-snapshot-date 2026-08-11 \
  --build-id 20260811T030000Z \
  --activate
```

The S3 form uses the task or instance IAM role through the standard boto3
credential chain. Local execution does not initialize an AWS client.

## Published datasets

The monthly input bundle uses these fixed local names so acquisition and build
steps share one explicit contract:

| Dataset | Local filename | Format |
| --- | --- | --- |
| NYC LION | `lion.geojson` | GeoJSON |
| NYC Street Pavement Ratings | `pavement.geojson` | GeoJSON |
| NYC Speed Humps | `speed_humps.geojson` | GeoJSON |
| TLC Taxi Zones | `taxi_zones.zip` | ZIP containing an ESRI Shapefile |

Each build retains:

- the unmodified LION, pavement, hump, and taxi-zone source objects;
- normalized `road_segment` Parquet;
- `enriched_segment_reference` Parquet following the catalogued field names;
- a compiled `simulation_road_environment` Parquet artifact;
- a taxi-zone geometry Parquet artifact;
- a versioned environment manifest with checksums, source lineage, schema
  fingerprints, row counts, quality metrics, and algorithm version.

The build writes its manifest only after every artifact succeeds. `--activate`
then updates `prepared/simulation_environment/active.json`. A failed or rejected
build cannot replace the previously active environment.

## Comfort scoring calibration (one-off, not part of the pipeline)

`analyze-hourly-feature-distribution` reads real `hourly_segment_features`
(Silver2) and prints count/mean/min/max/P10/P25/P50/P75/P90/P95 for every
feature `hourly_comfort.yaml`'s `normalizers` actually score, plus
`avg_speed_mps`. Rate features (`hard_brake_rate` etc.) are derived the same
way `hourly_comfort.py` derives them (`count / trip_count`), not read as-is,
so the numbers match what scoring actually sees.

No Airflow DAG or production command calls this — it exists so a human can
look at the real distribution before changing `hourly_comfort.yaml`'s
comfortable/uncomfortable anchors or `event_features.yaml`/`steering_features.yaml`
thresholds (#544). Run it against a representative period, decide the new
values from the output, then hard-code them in the YAML configs and bump
`scoring_version`/`feature_version` — this command never writes config itself.

```bash
uv run --package batch-jobs batch-jobs analyze-hourly-feature-distribution \
  --input-path s3://<bucket>/silver/hourly_segment_features \
  --start 2026-08-01T00:00:00+00:00 \
  --end 2026-08-08T00:00:00+00:00
```

`--input-path` defaults to `HOURLY_COMFORT_INPUT_PATH` (same env var the
scoring job reads). `--start`/`--end` are optional; omit both to scan every
partition under the path.

## Quality controls

Structural checks reject empty road/zone data, duplicate LION IDs, invalid
pavement ratings, and incomplete source bundles. Optional deployment thresholds
can require minimum pavement-segment and hump-source mapping rates:

```text
--min-pavement-match-rate 0.70
--min-hump-match-rate 0.90
```

Thresholds should be calibrated separately for a bounded smoke extent and a
complete NYC build.
