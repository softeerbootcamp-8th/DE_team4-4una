# 승차감 점수 파이프라인 성능 베이스라인

`pipeline-perf collect`가 모은 원시 JSON을 `pipeline-perf render`가 옮긴 것이다.
숫자는 모두 실제 실행에서 측정한 값이고, 8절은 관찰된 사실만 담는다 —
최적화 방안은 이 리포트의 범위가 아니다(#460, #462).

| 항목 | 값 |
| --- | --- |
| 수집 시각 | 2026-08-25T16:09:20.121411+09:00 |
| EMR Serverless Application | 00g85ljahc0svj2p |
| DAG run 수 | 9 |
| Spark 릴리스 | 4.0.2-amzn-0 |

수집 중 남은 기록:

- scheduled__2026-08-25T07:00:00+00:00: 아직 끝나지 않아 제외했다(state=running).

## 1. 대상 실행과 데이터량

라벨은 아래 절들이 이 실행을 가리킬 때 쓰는 이름이다.

| 라벨 | DAG | run id | 상태 | 시작(UTC) | 소요 | Bronze 파일 | Bronze 크기 | 평균 파일 | 처리 행 수 |
| --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | --- |
| csp 08-24 23:42 | current_score_pipeline | asset_triggered__2026-08-24T23:42:17.632479+00:00_kLwKGfZj | success | 2026-08-24 23:42 | 7:15 | - | - | - | - |
| csp 08-25 00:42 | current_score_pipeline | asset_triggered__2026-08-25T00:42:17.372134+00:00_mG01ljs4 | success | 2026-08-25 00:42 | 8:03 | - | - | - | - |
| csp 08-25 01:41 | current_score_pipeline | asset_triggered__2026-08-25T01:41:17.750225+00:00_DhWKpVWX | success | 2026-08-25 01:41 | 7:09 | - | - | - | - |
| csp 08-25 02:43 | current_score_pipeline | asset_triggered__2026-08-25T02:43:16.434954+00:00_jff1cXMb | success | 2026-08-25 02:43 | 7:19 | - | - | - | - |
| csp 08-25 06:42 | current_score_pipeline | asset_triggered__2026-08-25T06:42:18.027973+00:00_8NbHPMkZ | success | 2026-08-25 06:42 | 16:16 | - | - | - | - |
| ssp 08-25 03:00 | standard_score_pipeline | scheduled__2026-08-25T03:00:00+00:00 | failed | 2026-08-25 03:00 | 1:09:46 | 14 | 1.7 GiB | 123.8 MiB | - |
| ssp 08-25 04:00 | standard_score_pipeline | scheduled__2026-08-25T04:00:00+00:00 | failed | 2026-08-25 04:00 | 9:43 | 12 | 1.4 GiB | 118.2 MiB | - |
| ssp 08-25 05:00 | standard_score_pipeline | scheduled__2026-08-25T05:00:00+00:00 | failed | 2026-08-25 05:00 | 1:11:47 | 13 | 862.0 MiB | 66.3 MiB | - |
| ssp 08-25 06:00 | standard_score_pipeline | scheduled__2026-08-25T06:00:00+00:00 | success | 2026-08-25 06:00 | 45:29 | 13 | 412.3 MiB | 31.7 MiB | feature_count=13,621, hourly_comfort_score_count=733,942, quarantine_count=36, standard_segment_comfort_score_count=997,332 |

## 2. 타임라인과 오버헤드 대 실제 계산 시간

`spark_app`(Spark 애플리케이션 시작~종료)만 실제 계산이고 나머지는 오버헤드다.
`unaccounted`는 DAG run 총시간에서 아래 구간의 합을 뺀 나머지 — 스케줄러가
task를 집어들기 전 대기처럼 어느 구간에도 안 잡히는 시간이다. 음수면 구간이
서로 겹쳤다는 뜻이다: task가 재시도되면 Airflow가 보고하는 task 소요는 마지막
시도 것인데 Job Run 구간은 그와 겹치는 다른 시도의 것일 수 있다.
계산 비율의 `-`는 그 실행에 EMR Job Run이 없어 Spark 구간을 재지 않았다는 뜻이다
(`current_score_pipeline`은 Spark를 쓰지 않는다).

| run | 총시간 | 프로비저닝 | Spark 부팅 | Spark 계산 | 정리·커밋 | Airflow gap | 기타 task | 미계상 | 계산 비율 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| csp 08-24 23:42 | 7:15 | 0:00 | 0:00 | 0:00 | 0:00 | 0:00 | 7:15 | 0:01 | - |
| csp 08-25 00:42 | 8:03 | 0:00 | 0:00 | 0:00 | 0:00 | 0:00 | 8:03 | 0:01 | - |
| csp 08-25 01:41 | 7:09 | 0:00 | 0:00 | 0:00 | 0:00 | 0:00 | 7:08 | 0:00 | - |
| csp 08-25 02:43 | 7:19 | 0:00 | 0:00 | 0:00 | 0:00 | 0:00 | 7:18 | 0:00 | - |
| csp 08-25 06:42 | 16:16 | 0:00 | 0:00 | 0:00 | 0:00 | 0:00 | 7:53 | 8:23 | - |
| ssp 08-25 03:00 | 1:09:46 | 8:43 | 1:03 | 27:16 | 0:36 | 27:16 | 0:02 | 4:50 | 39.1% |
| ssp 08-25 04:00 | 9:43 | 1:27 | 0:11 | 3:39 | 0:00 | 3:09 | 0:19 | 0:57 | 37.6% |
| ssp 08-25 05:00 | 1:11:47 | 8:38 | 1:01 | 29:04 | 0:35 | 27:31 | 0:01 | 4:56 | 40.5% |
| ssp 08-25 06:00 | 45:29 | 8:50 | 1:01 | 28:33 | 0:38 | 0:07 | 1:41 | 4:39 | 62.8% |

Asset 트리거로 시작한 실행의 대기:

| run | 트리거 | 트리거 시각 | 시작까지 |
| --- | --- | --- | ---: |
| csp 08-24 23:42 | standard_score_pipeline.standard_score.validate_standard_score | 2026-08-24 23:42 | 0:01 |
| csp 08-25 00:42 | standard_score_pipeline.standard_score.validate_standard_score | 2026-08-25 00:42 | 0:01 |
| csp 08-25 01:41 | standard_score_pipeline.standard_score.validate_standard_score | 2026-08-25 01:41 | 0:01 |
| csp 08-25 02:43 | standard_score_pipeline.standard_score.validate_standard_score | 2026-08-25 02:43 | 0:01 |
| csp 08-25 06:42 | standard_score_pipeline.standard_score.validate_standard_score | 2026-08-25 06:42 | 0:01 |

## 3. task별 상세

| run | task | 상태 | 시도 | 소요 | 프로비저닝 | Job Run 실행 | stage | task(Spark) | vCPU-h | mem GB-h | storage GB-h |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| csp 08-24 23:42 | run_current_score | success | 1 | 7:15 | - | - | - | - | - | - | - |
| csp 08-25 00:42 | run_current_score | success | 1 | 8:03 | - | - | - | - | - | - | - |
| csp 08-25 01:41 | run_current_score | success | 1 | 7:08 | - | - | - | - | - | - | - |
| csp 08-25 02:43 | run_current_score | success | 1 | 7:18 | - | - | - | - | - | - | - |
| csp 08-25 06:42 | run_current_score | success | 2 | 7:53 | - | - | - | - | - | - | - |
| ssp 08-25 03:00 | sensor_processing.resolve_road_snapshot_date | success | 1 | 0:02 | - | - | - | - | - | - | - |
| ssp 08-25 03:00 | sensor_processing.run_sensor_processing | success | 1 | 18:02 | 1:24 | 15:49 | 60 | 2,953 | 0.783 | 5.743 | 10.378 |
| ssp 08-25 03:00 | sensor_processing.validate_sensor_processing | success | 1 | 3:01 | 1:24 | 1:00 | 50 | 80 | 0.050 | 0.369 | 0.667 |
| ssp 08-25 03:00 | hourly_scoring.run_hourly_scoring | success | 1 | 4:02 | 1:27 | 1:39 | 13 | 141 | 0.073 | 0.535 | 0.900 |
| ssp 08-25 03:00 | hourly_scoring.validate_hourly_scoring | success | 1 | 3:01 | 1:31 | 0:53 | 25 | 42 | 0.050 | 0.367 | 0.667 |
| ssp 08-25 03:00 | standard_score.run_standard_score | success | 1 | 8:02 | 1:29 | 5:41 | 44 | 324 | 0.275 | 2.018 | 3.600 |
| ssp 08-25 03:00 | standard_score.validate_standard_score | failed | 3 | 6:17 | 1:28 | 3:53 | 0 | 0 | 0.065 | 0.520 | 0.000 |
| ssp 08-25 03:00 | report_processing_counts | upstream_failed | 0 | 0:00 | - | - | - | - | - | - | - |
| ssp 08-25 03:00 | check_emr_serverless_idle | upstream_failed | 0 | 0:00 | - | - | - | - | - | - | - |
| ssp 08-25 03:00 | stop_emr_serverless_application | upstream_failed | 0 | 0:00 | - | - | - | - | - | - | - |
| ssp 08-25 04:00 | sensor_processing.resolve_road_snapshot_date | success | 1 | 0:19 | - | - | - | - | - | - | - |
| ssp 08-25 04:00 | sensor_processing.run_sensor_processing | failed | 2 | 6:12 | 1:27 | 3:48 | 1 | 0 | 0.063 | 0.507 | 0.000 |
| ssp 08-25 04:00 | sensor_processing.validate_sensor_processing | upstream_failed | 0 | 0:00 | - | - | - | - | - | - | - |
| ssp 08-25 04:00 | hourly_scoring.run_hourly_scoring | upstream_failed | 0 | 0:00 | - | - | - | - | - | - | - |
| ssp 08-25 04:00 | hourly_scoring.validate_hourly_scoring | upstream_failed | 0 | 0:00 | - | - | - | - | - | - | - |
| ssp 08-25 04:00 | standard_score.run_standard_score | upstream_failed | 0 | 0:00 | - | - | - | - | - | - | - |
| ssp 08-25 04:00 | standard_score.validate_standard_score | upstream_failed | 0 | 0:00 | - | - | - | - | - | - | - |
| ssp 08-25 04:00 | report_processing_counts | upstream_failed | 0 | 0:00 | - | - | - | - | - | - | - |
| ssp 08-25 04:00 | check_emr_serverless_idle | upstream_failed | 0 | 0:00 | - | - | - | - | - | - | - |
| ssp 08-25 04:00 | stop_emr_serverless_application | upstream_failed | 0 | 0:00 | - | - | - | - | - | - | - |
| ssp 08-25 05:00 | sensor_processing.resolve_road_snapshot_date | success | 1 | 0:01 | - | - | - | - | - | - | - |
| ssp 08-25 05:00 | sensor_processing.run_sensor_processing | success | 1 | 11:03 | 1:29 | 8:18 | 60 | 1,647 | 0.405 | 2.976 | 5.333 |
| ssp 08-25 05:00 | sensor_processing.validate_sensor_processing | success | 1 | 3:01 | 1:25 | 1:01 | 50 | 80 | 0.050 | 0.369 | 0.667 |
| ssp 08-25 05:00 | hourly_scoring.run_hourly_scoring | success | 1 | 4:02 | 1:24 | 1:39 | 13 | 149 | 0.074 | 0.547 | 0.933 |
| ssp 08-25 05:00 | hourly_scoring.validate_hourly_scoring | success | 1 | 3:01 | 1:23 | 0:57 | 25 | 42 | 0.050 | 0.367 | 0.667 |
| ssp 08-25 05:00 | standard_score.run_standard_score | success | 1 | 8:02 | 1:25 | 6:24 | 44 | 324 | 0.313 | 2.294 | 4.111 |
| ssp 08-25 05:00 | standard_score.validate_standard_score | failed | 2 | 13:29 | 1:31 | 12:21 | 0 | 0 | 0.206 | 1.647 | 0.000 |
| ssp 08-25 05:00 | report_processing_counts | skipped | 0 | 0:00 | - | - | - | - | - | - | - |
| ssp 08-25 05:00 | check_emr_serverless_idle | skipped | 0 | 0:00 | - | - | - | - | - | - | - |
| ssp 08-25 05:00 | stop_emr_serverless_application | skipped | 0 | 0:00 | - | - | - | - | - | - | - |
| ssp 08-25 06:00 | sensor_processing.resolve_road_snapshot_date | success | 1 | 0:01 | - | - | - | - | - | - | - |
| ssp 08-25 06:00 | sensor_processing.run_sensor_processing | success | 1 | 16:02 | 1:26 | 13:40 | 60 | 971 | 0.448 | 3.366 | 4.411 |
| ssp 08-25 06:00 | sensor_processing.validate_sensor_processing | success | 1 | 3:02 | 1:29 | 1:06 | 50 | 130 | 0.085 | 0.616 | 1.333 |
| ssp 08-25 06:00 | hourly_scoring.run_hourly_scoring | success | 1 | 3:01 | 1:31 | 1:19 | 13 | 157 | 0.092 | 0.668 | 1.400 |
| ssp 08-25 06:00 | hourly_scoring.validate_hourly_scoring | success | 1 | 3:02 | 1:27 | 0:58 | 25 | 68 | 0.083 | 0.600 | 1.333 |
| ssp 08-25 06:00 | standard_score.run_standard_score | success | 1 | 10:02 | 1:26 | 7:45 | 44 | 393 | 0.626 | 4.514 | 9.944 |
| ssp 08-25 06:00 | standard_score.validate_standard_score | success | 1 | 7:02 | 1:31 | 5:24 | 0 | 0 | 0.090 | 0.720 | 0.000 |
| ssp 08-25 06:00 | report_processing_counts | success | 1 | 1:20 | - | - | - | - | - | - | - |
| ssp 08-25 06:00 | check_emr_serverless_idle | success | 1 | 0:03 | - | - | - | - | - | - | - |
| ssp 08-25 06:00 | stop_emr_serverless_application | success | 1 | 0:17 | - | - | - | - | - | - | - |

## 4. 느린 스테이지 top 10

`GC 비율`은 `jvmGcTime / executorRunTime`이다. JVM GC 시간에는 같은 executor를
쓰는 다른 태스크가 유발한 GC도 잡히므로 100%를 넘을 수 있다 — 짧은 태스크에서
특히 그렇다.

`skew`는 스테이지 안 task duration의 `max / p50`이다. `task 합`은 그 스테이지
task duration의 총합으로, `wall`과 크게 벌어지면 그 차이는 계산이 아니라
슬롯을 기다린 시간이다.

| run/task | stage | 이름 | wall | tasks | task 합 | p50 | p95 | max | skew | shuffle R/W | spill(mem/disk) | GC 비율 |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: |
| ssp 08-25 06:00/run_sensor_processing | 0 | parquet at NativeMethodAccessorImpl.java:0 | 9:40 | 1 | 2.2s | 2.2s | 2.2s | 2.2s | 1.00 | 0 B / 0 B | 0 B / 0 B | 5.0% |
| ssp 08-25 04:00/run_sensor_processing | 0 | parquet at NativeMethodAccessorImpl.java:0 | 3:01 | 0 | 0.0s | - | - | - | - | 0 B / 0 B | 0 B / 0 B | - |
| ssp 08-25 03:00/run_sensor_processing | 30 | $anonfun$withThreadLocalCaptured$2 at Complet… | 2:32 | 100 | 9.5s | 0.1s | 0.1s | 0.2s | 2.74 | 0 B / 300.8 MiB | 0 B / 0 B | 1.8% |
| ssp 08-25 03:00/run_sensor_processing | 29 | $anonfun$withThreadLocalCaptured$2 at Complet… | 2:27 | 100 | 0:27 | 0.3s | 0.3s | 0.5s | 1.75 | 0 B / 2.7 GiB | 0 B / 0 B | 0.9% |
| ssp 08-25 03:00/run_sensor_processing | 28 | $anonfun$withThreadLocalCaptured$2 at Complet… | 2:14 | 100 | 4:28 | 2.5s | 2.8s | 8.3s | 3.26 | 0 B / 2.1 GiB | 0 B / 0 B | 1.1% |
| ssp 08-25 03:00/run_sensor_processing | 1 | count at NativeMethodAccessorImpl.java:0 | 1:34 | 14 | 3:06 | 0:13 | 0:16 | 0:16 | 1.24 | 0 B / 0 B | 0 B / 0 B | 2.8% |
| ssp 08-25 03:00/run_sensor_processing | 47 | count at NativeMethodAccessorImpl.java:0 | 1:33 | 130 | 3:06 | 1.4s | 1.8s | 5.2s | 3.65 | 2.6 GiB / 0 B | 0 B / 0 B | 2.2% |
| ssp 08-25 05:00/run_sensor_processing | 30 | $anonfun$withThreadLocalCaptured$2 at Complet… | 1:20 | 53 | 0:14 | 0.3s | 0.3s | 0.6s | 2.16 | 0 B / 1.4 GiB | 0 B / 0 B | 2.1% |
| ssp 08-25 05:00/run_sensor_processing | 29 | $anonfun$withThreadLocalCaptured$2 at Complet… | 1:15 | 53 | 2:22 | 2.5s | 2.7s | 6.9s | 2.76 | 0 B / 1.1 GiB | 0 B / 0 B | 1.0% |
| ssp 08-25 03:00/run_sensor_processing | 34 | $anonfun$withThreadLocalCaptured$2 at Complet… | 1:10 | 112 | 2:19 | 1.2s | 1.4s | 2.2s | 1.82 | 2.4 GiB / 1.4 GiB | 0 B / 0 B | 1.3% |

## 5. 느린 SQL execution top 10

| run/task | execution | 소요 | description |
| --- | ---: | ---: | --- |
| ssp 08-25 03:00/run_sensor_processing | 5 | 6:14 | count at NativeMethodAccessorImpl.java:0 |
| ssp 08-25 05:00/run_sensor_processing | 5 | 3:16 | count at NativeMethodAccessorImpl.java:0 |
| ssp 08-25 03:00/run_sensor_processing | 0 | 3:03 | count at NativeMethodAccessorImpl.java:0 |
| ssp 08-25 05:00/run_sensor_processing | 0 | 1:30 | count at NativeMethodAccessorImpl.java:0 |
| ssp 08-25 03:00/run_sensor_processing | 2 | 1:27 | count at NativeMethodAccessorImpl.java:0 |
| ssp 08-25 03:00/run_sensor_processing | 8 | 1:11 | parquet at NativeMethodAccessorImpl.java:0 |
| ssp 08-25 03:00/run_sensor_processing | 7 | 1:03 | count at NativeMethodAccessorImpl.java:0 |
| ssp 08-25 06:00/run_sensor_processing | 5 | 1:02 | count at NativeMethodAccessorImpl.java:0 |
| ssp 08-25 03:00/run_sensor_processing | 10 | 0:40 | first at /usr/local/lib/python3.12/site-packages/batch_jobs… |
| ssp 08-25 05:00/run_standard_score | 7 | 0:35 | save at NativeMethodAccessorImpl.java:0 |

## 6. Spark 밖 구간 (PERF 로그)

`de4_core.perf_phase`가 남긴 구간이다(#461). Spark event log에 흔적이 없는
psycopg2 직접 실행 구간이 여기 잡힌다. EMR Job Run은 driver의 stdout에서,
Spark를 쓰지 않는 task는 Airflow task 로그에서 읽는다.

| run/task | phase | 소요 | 성공 | 부가 필드 |
| --- | --- | ---: | --- | --- |
| csp 08-25 06:42/run_current_score | current_score.load_segment_zones | 0:01 | 예 | rows=165675 |
| csp 08-25 06:42/run_current_score | current_score.upsert_loop | 7:46 | 예 | rows=994050 |
| ssp 08-25 05:00/run_sensor_processing | sensor_processing.spark_session | 0:16 | 예 | - |
| ssp 08-25 05:00/run_sensor_processing | sensor_processing.features | 5:31 | 예 | - |
| ssp 08-25 05:00/run_sensor_processing | sensor_processing.job | 7:46 | 예 | - |
| ssp 08-25 05:00/validate_sensor_processing | validate_sensor_processing.spark_session | 0:13 | 예 | - |
| ssp 08-25 05:00/validate_sensor_processing | validate_sensor_processing.job | 0:24 | 예 | - |
| ssp 08-25 05:00/run_hourly_scoring | hourly_scoring.spark_session | 0:14 | 예 | - |
| ssp 08-25 05:00/run_hourly_scoring | hourly_scoring.job | 1:10 | 예 | - |
| ssp 08-25 05:00/validate_hourly_scoring | validate_hourly_scoring.spark_session | 0:13 | 예 | - |
| ssp 08-25 05:00/validate_hourly_scoring | validate_hourly_scoring.job | 0:22 | 예 | - |
| ssp 08-25 05:00/run_standard_score | standard_score.spark_session | 0:13 | 예 | - |
| ssp 08-25 05:00/run_standard_score | standard_score.postgres_connect | 0:00 | 예 | - |
| ssp 08-25 05:00/run_standard_score | standard_score.staging_lock | 0:00 | 예 | - |
| ssp 08-25 05:00/run_standard_score | standard_score.staging_validate | 0:10 | 예 | rows=997332 |
| ssp 08-25 05:00/run_standard_score | standard_score.postgres_merge | 4:17 | 예 | rows=997332 |
| ssp 08-25 05:00/run_standard_score | standard_score.staging_truncate | 0:00 | 예 | - |
| ssp 08-25 05:00/run_standard_score | standard_score.job | 5:56 | 예 | - |
| ssp 08-25 05:00/validate_standard_score | validate_standard_score.postgres_connect | 0:00 | 예 | - |
| ssp 08-25 06:00/run_sensor_processing | sensor_processing.spark_session | 0:36 | 예 | - |
| ssp 08-25 06:00/run_sensor_processing | sensor_processing.features | 2:14 | 예 | - |
| ssp 08-25 06:00/run_sensor_processing | sensor_processing.job | 12:47 | 예 | - |
| ssp 08-25 06:00/validate_sensor_processing | validate_sensor_processing.spark_session | 0:14 | 예 | - |
| ssp 08-25 06:00/validate_sensor_processing | validate_sensor_processing.job | 0:29 | 예 | - |
| ssp 08-25 06:00/run_hourly_scoring | hourly_scoring.spark_session | 0:13 | 예 | - |
| ssp 08-25 06:00/run_hourly_scoring | hourly_scoring.job | 0:50 | 예 | - |
| ssp 08-25 06:00/validate_hourly_scoring | validate_hourly_scoring.spark_session | 0:13 | 예 | - |
| ssp 08-25 06:00/validate_hourly_scoring | validate_hourly_scoring.job | 0:22 | 예 | - |
| ssp 08-25 06:00/run_standard_score | standard_score.spark_session | 0:14 | 예 | - |
| ssp 08-25 06:00/run_standard_score | standard_score.postgres_connect | 0:00 | 예 | - |
| ssp 08-25 06:00/run_standard_score | standard_score.staging_lock | 0:00 | 예 | - |
| ssp 08-25 06:00/run_standard_score | standard_score.staging_validate | 0:09 | 예 | rows=997332 |
| ssp 08-25 06:00/run_standard_score | standard_score.postgres_merge | 5:43 | 예 | rows=997332 |
| ssp 08-25 06:00/run_standard_score | standard_score.staging_truncate | 0:00 | 예 | - |
| ssp 08-25 06:00/run_standard_score | standard_score.job | 7:14 | 예 | - |
| ssp 08-25 06:00/validate_standard_score | validate_standard_score.postgres_connect | 0:00 | 예 | - |
| ssp 08-25 06:00/validate_standard_score | validate_standard_score.job | 5:04 | 예 | - |

## 7. 정규화 지표와 DAG run당 원가

`vCPU-초/100만건`은 billed vCPU-hour를 Spark 입력 레코드 수로 정규화한 값이다.
금액은 요금표를 리포트에 박아 두지 않기 위해 싣지 않는다 — 자원 사용량만 남긴다.

| run | Spark 입력 레코드 | 입력 바이트 | 레코드/초 | vCPU-h | mem GB-h | storage GB-h | vCPU-초/100만건 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| csp 08-24 23:42 | - | - | - | - | - | - | - |
| csp 08-25 00:42 | - | - | - | - | - | - | - |
| csp 08-25 01:41 | - | - | - | - | - | - | - |
| csp 08-25 02:43 | - | - | - | - | - | - | - |
| csp 08-25 06:42 | - | - | - | - | - | - | - |
| ssp 08-25 03:00 | 14,953,814 | 91.4 GiB | 9,140.8 | 1.296 | 9.552 | 16.212 | 312.0 |
| ssp 08-25 04:00 | 0 | 0 B | - | 0.063 | 0.507 | - | - |
| ssp 08-25 05:00 | 11,779,311 | 52.1 GiB | 6,753.2 | 1.098 | 8.200 | 11.711 | 335.6 |
| ssp 08-25 06:00 | 10,020,557 | 26.3 GiB | 5,850.4 | 1.424 | 10.484 | 18.421 | 511.6 |

## 8. 관찰된 병목 후보

사실만 적는다. 원인 진단과 대응은 후속 이슈에서 다룬다.

- 수집한 DAG run 9건의 상태는 failed 3건, success 6건이다.
- current_score_pipeline 5건의 총시간 합은 46:02이고, Spark를 쓰지 않아 계산 구간을 따로 재지 않았다.
- standard_score_pipeline 4건의 총시간 합은 3:16:45이고, 그중 Spark 계산 구간은 1:28:32(45.0%), task 사이 Airflow gap은 58:04(29.5%)이다.
- Job Run 19건의 프로비저닝 대기 합은 27:39이고, 건당 평균은 1:27이다.
- task duration의 max/p50이 2.0 이상인 스테이지가 142개이고, 최대는 ssp 08-25 03:00/run_sensor_processing stage 99의 29.34배다.
- GC 시간이 executor 실행시간의 10.0% 이상인 스테이지가 4개이고, 최대는 ssp 08-25 03:00/run_standard_score stage 8의 18.7%다.
- 가장 오래 걸린 스테이지는 ssp 08-25 06:00/run_sensor_processing stage 0 (parquet at NativeMethodAccessorImpl.jav…)로 9:40이 걸렸다.
- Job Run 15건의 태스크 점유 시간은 가용 슬롯 시간의 17.6%~92.7%이고, 중앙값은 52.8%다.
- Bronze 입력 파티션 4개의 파일 수는 52개, 합계는 4.3 GiB이고, 파일당 평균은 85.1 MiB이다.
