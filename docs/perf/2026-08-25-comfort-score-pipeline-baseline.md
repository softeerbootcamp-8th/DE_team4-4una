# 승차감 점수 파이프라인 성능 베이스라인

> `pipeline-perf collect`가 모은 원시 JSON을 `pipeline-perf render`가 옮긴 것이다.
> 숫자는 모두 실제 실행에서 측정한 값이고, 8절은 관찰된 사실만 담는다 —
> 최적화 방안은 이 리포트의 범위가 아니다(#460, #462).

## 한눈에 보기

Spark를 쓰는 standard_score_pipeline 5건 기준이다.

| | |
| --- | --- |
| DAG run 총시간 합 | 3:49:23 (평균 45:53) |
| 그중 Spark 계산 | 1:44:33 (45.6%) |
| Job Run 프로비저닝 대기 | 36:23 (15.9%) |
| task 사이 Airflow gap | 58:14 (25.4%) |
| 가장 긴 Spark 밖 구간 | `standard_score.postgres_merge` 5:43 |

## 수집 범위

| 항목 | 값 |
| --- | --- |
| 수집 시각 | 2026-08-25T16:45:31.525686+09:00 |
| EMR Serverless Application | 00g85ljahc0svj2p |
| DAG run 수 | 10 |
| Spark 릴리스 | 4.0.2-amzn-0 |

## 1. 대상 실행과 데이터량

라벨은 아래 절들이 이 실행을 가리킬 때 쓰는 이름이다.

| 라벨 | DAG | run id | 상태 | 시작(UTC) | 소요 | Bronze 파일 | Bronze 크기 | 평균 파일 | 처리 행 수 |
| --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | --- |
| csp 08-25 00:42 | current_score_pipeline | asset_triggered__2026-08-25T00:42:17.372134+00:00_mG01ljs4 | success | 2026-08-25 00:42 | 8:03 | - | - | - | - |
| csp 08-25 01:41 | current_score_pipeline | asset_triggered__2026-08-25T01:41:17.750225+00:00_DhWKpVWX | success | 2026-08-25 01:41 | 7:09 | - | - | - | - |
| csp 08-25 02:43 | current_score_pipeline | asset_triggered__2026-08-25T02:43:16.434954+00:00_jff1cXMb | success | 2026-08-25 02:43 | 7:19 | - | - | - | - |
| csp 08-25 06:42 | current_score_pipeline | asset_triggered__2026-08-25T06:42:18.027973+00:00_8NbHPMkZ | success | 2026-08-25 06:42 | 16:16 | - | - | - | - |
| csp 08-25 07:31 | current_score_pipeline | asset_triggered__2026-08-25T07:31:18.492709+00:00_sNOQ3Brf | success | 2026-08-25 07:31 | 8:52 | - | - | - | - |
| ssp 08-25 03:00 | standard_score_pipeline | scheduled__2026-08-25T03:00:00+00:00 | failed | 2026-08-25 03:00 | 1:09:46 | 14 | 1.7 GiB | 123.8 MiB | - |
| ssp 08-25 04:00 | standard_score_pipeline | scheduled__2026-08-25T04:00:00+00:00 | failed | 2026-08-25 04:00 | 9:43 | 12 | 1.4 GiB | 118.2 MiB | - |
| ssp 08-25 05:00 | standard_score_pipeline | scheduled__2026-08-25T05:00:00+00:00 | failed | 2026-08-25 05:00 | 1:11:47 | 13 | 862.0 MiB | 66.3 MiB | - |
| ssp 08-25 06:00 | standard_score_pipeline | scheduled__2026-08-25T06:00:00+00:00 | success | 2026-08-25 06:00 | 45:29 | 13 | 412.3 MiB | 31.7 MiB | feature_count=13,621, hourly_comfort_score_count=733,942, quarantine_count=36, standard_segment_comfort_score_count=997,332 |
| ssp 08-25 07:00 | standard_score_pipeline | scheduled__2026-08-25T07:00:00+00:00 | success | 2026-08-25 07:00 | 32:38 | 13 | 257.0 MiB | 19.8 MiB | feature_count=10,229, hourly_comfort_score_count=744,171, quarantine_count=16, standard_segment_comfort_score_count=997,332 |

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
| csp 08-25 00:42 | 8:03 | 0:00 | 0:00 | 0:00 | 0:00 | 0:00 | 8:03 | 0:01 | - |
| csp 08-25 01:41 | 7:09 | 0:00 | 0:00 | 0:00 | 0:00 | 0:00 | 7:08 | 0:00 | - |
| csp 08-25 02:43 | 7:19 | 0:00 | 0:00 | 0:00 | 0:00 | 0:00 | 7:18 | 0:00 | - |
| csp 08-25 06:42 | 16:16 | 0:00 | 0:00 | 0:00 | 0:00 | 0:00 | 7:53 | 8:23 | - |
| csp 08-25 07:31 | 8:52 | 0:00 | 0:00 | 0:00 | 0:00 | 0:00 | 8:48 | 0:05 | - |
| ssp 08-25 03:00 | 1:09:46 | 8:43 | 1:03 | 27:16 | 0:36 | 27:16 | 0:02 | 4:50 | 39.1% |
| ssp 08-25 04:00 | 9:43 | 1:27 | 0:11 | 3:39 | 0:00 | 3:09 | 0:19 | 0:57 | 37.6% |
| ssp 08-25 05:00 | 1:11:47 | 8:38 | 1:01 | 29:04 | 0:35 | 27:31 | 0:01 | 4:56 | 40.5% |
| ssp 08-25 06:00 | 45:29 | 8:50 | 1:01 | 28:33 | 0:38 | 0:07 | 1:41 | 4:39 | 62.8% |
| ssp 08-25 07:00 | 32:38 | 8:44 | 1:10 | 16:01 | 0:38 | 0:10 | 1:15 | 4:40 | 49.0% |

Asset 트리거로 시작한 실행의 대기:

| run | 트리거 | 트리거 시각 | 시작까지 |
| --- | --- | --- | ---: |
| csp 08-25 00:42 | standard_score_pipeline.standard_score.validate_standard_score | 2026-08-25 00:42 | 0:01 |
| csp 08-25 01:41 | standard_score_pipeline.standard_score.validate_standard_score | 2026-08-25 01:41 | 0:01 |
| csp 08-25 02:43 | standard_score_pipeline.standard_score.validate_standard_score | 2026-08-25 02:43 | 0:01 |
| csp 08-25 06:42 | standard_score_pipeline.standard_score.validate_standard_score | 2026-08-25 06:42 | 0:01 |
| csp 08-25 07:31 | standard_score_pipeline.standard_score.validate_standard_score | 2026-08-25 07:31 | 0:01 |

## 3. task별 상세

| run | task | 상태 | 시도 | 소요 | 프로비저닝 | Job Run 실행 | stage | task(Spark) | vCPU-h | mem GB-h | storage GB-h |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| csp 08-25 00:42 | run_current_score | success | 1 | 8:03 | - | - | - | - | - | - | - |
| csp 08-25 01:41 | run_current_score | success | 1 | 7:08 | - | - | - | - | - | - | - |
| csp 08-25 02:43 | run_current_score | success | 1 | 7:18 | - | - | - | - | - | - | - |
| csp 08-25 06:42 | run_current_score | success | 2 | 7:53 | - | - | - | - | - | - | - |
| csp 08-25 07:31 | run_current_score | success | 1 | 8:48 | - | - | - | - | - | - | - |
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
| ssp 08-25 07:00 | sensor_processing.resolve_road_snapshot_date | success | 1 | 0:03 | - | - | - | - | - | - | - |
| ssp 08-25 07:00 | sensor_processing.run_sensor_processing | success | 1 | 6:03 | 1:24 | 2:59 | 55 | 830 | 0.232 | 1.676 | 3.644 |
| ssp 08-25 07:00 | sensor_processing.validate_sensor_processing | success | 1 | 4:02 | 1:38 | 1:21 | 50 | 127 | 0.089 | 0.649 | 1.333 |
| ssp 08-25 07:00 | hourly_scoring.run_hourly_scoring | success | 1 | 3:01 | 1:25 | 1:21 | 13 | 157 | 0.094 | 0.682 | 1.433 |
| ssp 08-25 07:00 | hourly_scoring.validate_hourly_scoring | success | 1 | 3:01 | 1:29 | 0:59 | 25 | 68 | 0.083 | 0.600 | 1.333 |
| ssp 08-25 07:00 | standard_score.run_standard_score | success | 1 | 8:02 | 1:24 | 5:57 | 44 | 393 | 0.479 | 3.452 | 7.589 |
| ssp 08-25 07:00 | standard_score.validate_standard_score | success | 1 | 7:02 | 1:24 | 5:12 | 0 | 0 | 0.087 | 0.693 | 0.000 |
| ssp 08-25 07:00 | report_processing_counts | success | 1 | 0:55 | - | - | - | - | - | - | - |
| ssp 08-25 07:00 | check_emr_serverless_idle | success | 1 | 0:01 | - | - | - | - | - | - | - |
| ssp 08-25 07:00 | stop_emr_serverless_application | success | 1 | 0:16 | - | - | - | - | - | - | - |

## 4. 느린 스테이지 top 10

`작업`은 그 스테이지가 속한 SQL execution의 실행 계획에서 뽑은 주요 연산이다.
스테이지 이름(`$anonfun$withThreadLocalCaptured$2 at ...`)은 Spark 내부 호출
지점이라 그대로는 뜻이 없어 이렇게 바꿔 적는다. 어느 execution에도 안 붙는
스테이지는 원래 이름을 그대로 둔다.

`GC 비율`은 `jvmGcTime / executorRunTime`이다. JVM GC 시간에는 같은 executor를
쓰는 다른 태스크가 유발한 GC도 잡히므로 100%를 넘을 수 있다 — 짧은 태스크에서
특히 그렇다.

`skew`는 스테이지 안 task duration의 `max / p50`이다. `task 합`은 그 스테이지
task duration의 총합으로, `wall`과 크게 벌어지면 그 차이는 계산이 아니라
슬롯을 기다린 시간이다.

| run/task | stage | 작업 (exec) | wall | tasks | task 합 | p50 | p95 | max | skew | shuffle R/W | spill(mem/disk) | GC 비율 |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: |
| ssp 08-25 06:00/run_sensor_processing | 0 | parquet at NativeMethodAccessorImpl.java:0 | 9:40 | 1 | 2.2s | 2.2s | 2.2s | 2.2s | 1.00 | 0 B / 0 B | 0 B / 0 B | 5.0% |
| ssp 08-25 04:00/run_sensor_processing | 0 | parquet at NativeMethodAccessorImpl.java:0 | 3:01 | 0 | 0.0s | - | - | - | - | 0 B / 0 B | 0 B / 0 B | - |
| ssp 08-25 03:00/run_sensor_processing | 30 | 파일 읽기 → 캐시 읽기 → 셔플 해시 조인 → 정렬 병합 조인 → 윈도우 → 정렬 → 셔플 → 집계 → Python UDF (exec 5) | 2:32 | 100 | 9.5s | 0.1s | 0.1s | 0.2s | 2.74 | 0 B / 300.8 MiB | 0 B / 0 B | 1.8% |
| ssp 08-25 03:00/run_sensor_processing | 29 | 파일 읽기 → 캐시 읽기 → 셔플 해시 조인 → 정렬 병합 조인 → 윈도우 → 정렬 → 셔플 → 집계 → Python UDF (exec 5) | 2:27 | 100 | 0:27 | 0.3s | 0.3s | 0.5s | 1.75 | 0 B / 2.7 GiB | 0 B / 0 B | 0.9% |
| ssp 08-25 03:00/run_sensor_processing | 28 | 파일 읽기 → 캐시 읽기 → 셔플 해시 조인 → 정렬 병합 조인 → 윈도우 → 정렬 → 셔플 → 집계 → Python UDF (exec 5) | 2:14 | 100 | 4:28 | 2.5s | 2.8s | 8.3s | 3.26 | 0 B / 2.1 GiB | 0 B / 0 B | 1.1% |
| ssp 08-25 03:00/run_sensor_processing | 1 | 파일 읽기 → 캐시 읽기 → 윈도우 → 정렬 → 셔플 → 집계 (exec 0) | 1:34 | 14 | 3:06 | 0:13 | 0:16 | 0:16 | 1.24 | 0 B / 0 B | 0 B / 0 B | 2.8% |
| ssp 08-25 03:00/run_sensor_processing | 47 | 파일 읽기 → 캐시 읽기 → 셔플 해시 조인 → 정렬 병합 조인 → 윈도우 → 정렬 → 셔플 → 집계 → Python UDF (exec 5) | 1:33 | 130 | 3:06 | 1.4s | 1.8s | 5.2s | 3.65 | 2.6 GiB / 0 B | 0 B / 0 B | 2.2% |
| ssp 08-25 05:00/run_sensor_processing | 30 | 파일 읽기 → 캐시 읽기 → 셔플 해시 조인 → 정렬 병합 조인 → 윈도우 → 정렬 → 셔플 → 집계 → Python UDF (exec 5) | 1:20 | 53 | 0:14 | 0.3s | 0.3s | 0.6s | 2.16 | 0 B / 1.4 GiB | 0 B / 0 B | 2.1% |
| ssp 08-25 05:00/run_sensor_processing | 29 | 파일 읽기 → 캐시 읽기 → 셔플 해시 조인 → 정렬 병합 조인 → 윈도우 → 정렬 → 셔플 → 집계 → Python UDF (exec 5) | 1:15 | 53 | 2:22 | 2.5s | 2.7s | 6.9s | 2.76 | 0 B / 1.1 GiB | 0 B / 0 B | 1.0% |
| ssp 08-25 03:00/run_sensor_processing | 34 | 파일 읽기 → 캐시 읽기 → 셔플 해시 조인 → 정렬 병합 조인 → 윈도우 → 정렬 → 셔플 → 집계 → Python UDF (exec 5) | 1:10 | 112 | 2:19 | 1.2s | 1.4s | 2.2s | 1.82 | 2.4 GiB / 1.4 GiB | 0 B / 0 B | 1.3% |

## 5. 느린 SQL execution top 10

Spark가 붙이는 `description`은 `count at NativeMethodAccessorImpl.java:0`처럼
py4j 경계에서 끊긴 JVM 호출 지점이라 무슨 작업인지 알려주지 않는다. 그래서
실행 계획에서 뽑은 **읽은 데이터**와 **주요 연산**을 앞에 둔다. 각 실행의 계획
앞부분은 [부록 A](#부록-a-느린-sql-execution의-실행-계획)에 있다.

캐시된 DataFrame(`InMemoryTableScan`)을 읽는 실행은 그 캐시를 만든 계보의
연산까지 계획에 함께 담기므로, 연산 목록이 파이프라인 전체처럼 보일 수 있다.
그 실행이 그 순간 모두 계산했다는 뜻은 아니다.

| run/task | exec | 소요 | 주요 연산 | 읽고 쓴 데이터 | Spark 호출 지점 |
| --- | ---: | ---: | --- | --- | --- |
| ssp 08-25 03:00/run_sensor_processing | 5 | 6:14 | 파일 읽기 → 캐시 읽기 → 셔플 해시 조인 → 정렬 병합 조인 → 윈도우 → 정렬 외 3개 | bronze/sensor-events | count at NativeMethodAccessorImpl.java:0 |
| ssp 08-25 05:00/run_sensor_processing | 5 | 3:16 | 파일 읽기 → 캐시 읽기 → 셔플 해시 조인 → 정렬 병합 조인 → 윈도우 → 정렬 외 3개 | bronze/sensor-events | count at NativeMethodAccessorImpl.java:0 |
| ssp 08-25 03:00/run_sensor_processing | 0 | 3:03 | 파일 읽기 → 캐시 읽기 → 윈도우 → 정렬 → 셔플 → 집계 | bronze/sensor-events | count at NativeMethodAccessorImpl.java:0 |
| ssp 08-25 05:00/run_sensor_processing | 0 | 1:30 | 파일 읽기 → 캐시 읽기 → 윈도우 → 정렬 → 셔플 → 집계 | bronze/sensor-events | count at NativeMethodAccessorImpl.java:0 |
| ssp 08-25 03:00/run_sensor_processing | 2 | 1:27 | 파일 읽기 → 캐시 읽기 → 윈도우 → 정렬 → 셔플 → 집계 | bronze/sensor-events | count at NativeMethodAccessorImpl.java:0 |
| ssp 08-25 03:00/run_sensor_processing | 8 | 1:11 | 파일 읽기 → 캐시 읽기 → 셔플 해시 조인 → 정렬 병합 조인 → 윈도우 → 정렬 외 2개 | silver/sensor_event_quarantine/_staging/sched…<br>bronze/sensor-events | parquet at NativeMethodAccessorImpl.java:0 |
| ssp 08-25 03:00/run_sensor_processing | 7 | 1:03 | 파일 읽기 → 캐시 읽기 → 셔플 해시 조인 → 정렬 병합 조인 → 윈도우 → 정렬 외 3개 | bronze/sensor-events | count at NativeMethodAccessorImpl.java:0 |
| ssp 08-25 06:00/run_sensor_processing | 5 | 1:02 | 파일 읽기 → 캐시 읽기 → 셔플 해시 조인 → 정렬 병합 조인 → 윈도우 → 정렬 외 3개 | bronze/sensor-events | count at NativeMethodAccessorImpl.java:0 |
| ssp 08-25 03:00/run_sensor_processing | 10 | 0:40 | 파일 읽기 → 캐시 읽기 → 셔플 해시 조인 → 정렬 병합 조인 → 윈도우 → 정렬 외 3개 | bronze/sensor-events | first at /usr/local/lib/python3.12/site-pac… |
| ssp 08-25 05:00/run_standard_score | 7 | 0:35 | - | - | save at NativeMethodAccessorImpl.java:0 |

## 6. Spark 밖 구간 (PERF 로그)

`de4_core.perf_phase`가 남긴 구간이다(#461). Spark event log에 흔적이 없는
psycopg2 직접 실행 구간이 여기 잡힌다. EMR Job Run은 driver의 stdout에서,
Spark를 쓰지 않는 task는 Airflow task 로그에서 읽는다.

| run/task | phase | 소요 | 성공 | 부가 필드 |
| --- | --- | ---: | --- | --- |
| csp 08-25 06:42/run_current_score | current_score.load_segment_zones | 0:01 | 예 | rows=165675 |
| csp 08-25 06:42/run_current_score | current_score.upsert_loop | 7:46 | 예 | rows=994050 |
| csp 08-25 07:31/run_current_score | current_score.load_segment_zones | 0:01 | 예 | rows=165675 |
| csp 08-25 07:31/run_current_score | current_score.upsert_loop | 8:40 | 예 | rows=994050 |
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
| ssp 08-25 07:00/run_sensor_processing | sensor_processing.spark_session | 0:12 | 예 | - |
| ssp 08-25 07:00/run_sensor_processing | sensor_processing.features | 1:47 | 예 | - |
| ssp 08-25 07:00/run_sensor_processing | sensor_processing.job | 2:30 | 예 | - |
| ssp 08-25 07:00/validate_sensor_processing | validate_sensor_processing.spark_session | 0:16 | 예 | - |
| ssp 08-25 07:00/validate_sensor_processing | validate_sensor_processing.job | 0:33 | 예 | - |
| ssp 08-25 07:00/run_hourly_scoring | hourly_scoring.spark_session | 0:14 | 예 | - |
| ssp 08-25 07:00/run_hourly_scoring | hourly_scoring.job | 0:51 | 예 | - |
| ssp 08-25 07:00/validate_hourly_scoring | validate_hourly_scoring.spark_session | 0:13 | 예 | - |
| ssp 08-25 07:00/validate_hourly_scoring | validate_hourly_scoring.job | 0:22 | 예 | - |
| ssp 08-25 07:00/run_standard_score | standard_score.spark_session | 0:14 | 예 | - |
| ssp 08-25 07:00/run_standard_score | standard_score.postgres_connect | 0:00 | 예 | - |
| ssp 08-25 07:00/run_standard_score | standard_score.staging_lock | 0:00 | 예 | - |
| ssp 08-25 07:00/run_standard_score | standard_score.staging_validate | 0:10 | 예 | rows=997332 |
| ssp 08-25 07:00/run_standard_score | standard_score.postgres_merge | 4:09 | 예 | rows=997332 |
| ssp 08-25 07:00/run_standard_score | standard_score.staging_truncate | 0:00 | 예 | - |
| ssp 08-25 07:00/run_standard_score | standard_score.job | 5:28 | 예 | - |
| ssp 08-25 07:00/validate_standard_score | validate_standard_score.postgres_connect | 0:00 | 예 | - |
| ssp 08-25 07:00/validate_standard_score | validate_standard_score.job | 4:50 | 예 | - |

## 7. 정규화 지표와 DAG run당 원가

`vCPU-초/100만건`은 billed vCPU-hour를 Spark 입력 레코드 수로 정규화한 값이다.
금액은 요금표를 리포트에 박아 두지 않기 위해 싣지 않는다 — 자원 사용량만 남긴다.

| run | Spark 입력 레코드 | 입력 바이트 | 레코드/초 | vCPU-h | mem GB-h | storage GB-h | vCPU-초/100만건 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| csp 08-25 00:42 | - | - | - | - | - | - | - |
| csp 08-25 01:41 | - | - | - | - | - | - | - |
| csp 08-25 02:43 | - | - | - | - | - | - | - |
| csp 08-25 06:42 | - | - | - | - | - | - | - |
| csp 08-25 07:31 | - | - | - | - | - | - | - |
| ssp 08-25 03:00 | 14,953,814 | 91.4 GiB | 9,140.8 | 1.296 | 9.552 | 16.212 | 312.0 |
| ssp 08-25 04:00 | 0 | 0 B | - | 0.063 | 0.507 | - | - |
| ssp 08-25 05:00 | 11,779,311 | 52.1 GiB | 6,753.2 | 1.098 | 8.200 | 11.711 | 335.6 |
| ssp 08-25 06:00 | 10,020,557 | 26.3 GiB | 5,850.4 | 1.424 | 10.484 | 18.421 | 511.6 |
| ssp 08-25 07:00 | 9,417,552 | 16.3 GiB | 9,804.6 | 1.064 | 7.752 | 15.332 | 406.7 |

## 8. 관찰된 병목 후보

사실만 적는다. 원인 진단과 대응은 후속 이슈에서 다룬다.

- 수집한 DAG run 10건의 상태는 failed 3건, success 7건이다.
- current_score_pipeline 5건의 총시간 합은 47:39이고, Spark를 쓰지 않아 계산 구간을 따로 재지 않았다.
- standard_score_pipeline 5건의 총시간 합은 3:49:23이고, 그중 Spark 계산 구간은 1:44:33(45.6%), task 사이 Airflow gap은 58:14(25.4%)이다.
- Job Run 25건의 프로비저닝 대기 합은 36:23이고, 건당 평균은 1:27이다.
- task duration의 max/p50이 2.0 이상인 스테이지가 186개이고, 최대는 ssp 08-25 03:00/run_sensor_processing stage 99의 29.34배다.
- GC 시간이 executor 실행시간의 10.0% 이상인 스테이지가 5개이고, 최대는 ssp 08-25 03:00/run_standard_score stage 8의 18.7%다.
- 가장 오래 걸린 스테이지는 ssp 08-25 06:00/run_sensor_processing stage 0 (parquet at NativeMethodAccessorImpl.jav…)로 9:40이 걸렸다.
- Job Run 20건의 태스크 점유 시간은 가용 슬롯 시간의 17.6%~92.7%이고, 중앙값은 52.6%다.
- Bronze 입력 파티션 5개의 파일 수는 65개, 합계는 4.6 GiB이고, 파일당 평균은 72.1 MiB이다.

## 부록 A. 느린 SQL execution의 실행 계획

5절 표의 각 행이 실제로 무엇을 실행했는지 보여주는 계획 앞부분이다. 긴 줄과
뒷부분은 잘려 있다 — 전체 계획은 실행 하나가 1MB를 넘어 리포트에 실을 수 없다.

### ssp 08-25 03:00/run_sensor_processing · exec 5 (6:14)

읽고 쓴 데이터:

- `bronze/sensor-events`

```text
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[], functions=[count(1)], output=[count#6332L])
   +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=1272]
      +- HashAggregate(keys=[], functions=[partial_count(1)], output=[count#7013L])
         +- InMemoryTableScan
               +- InMemoryRelation [event_id#272, vehicle_profile_id#273, trip_id#274, trip_seq#275L, event_time#276, latitude#277, longitude#278, spe
                     +- AdaptiveSparkPlan isFinalPlan=false
                        +- Project [event_id#272, vehicle_profile_id#273, trip_id#274, trip_seq#275L, event_time#276, latitude#277, longitude#278, spe
                           +- Window [max(_w0#4957L) windowspecdefinition(trip_id#274, _episode_run_id#4951L, specifiedwindowframe(RowFrame, unbounded
                              +- Sort [trip_id#274 ASC NULLS FIRST, _episode_run_id#4951L ASC NULLS FIRST], false, 0
                                 +- Project [event_id#272, vehicle_profile_id#273, trip_id#274, trip_seq#275L, event_time#276, latitude#277, longitude
                                    +- Window [sum(_w0#4953) windowspecdefinition(trip_id#274, trip_seq#275L ASC NULLS FIRST, event_time#276 ASC NULLS
                                       +- Project [event_id#272, vehicle_profile_id#273, trip_id#274, trip_seq#275L, event_time#276, latitude#277, lon
                                          +- Project [event_id#272, vehicle_profile_id#273, trip_id#274, trip_seq#275L, event_time#276, latitude#277, 
```

### ssp 08-25 05:00/run_sensor_processing · exec 5 (3:16)

읽고 쓴 데이터:

- `bronze/sensor-events`

```text
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[], functions=[count(1)], output=[count#6332L])
   +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=1223]
      +- HashAggregate(keys=[], functions=[partial_count(1)], output=[count#7013L])
         +- InMemoryTableScan
               +- InMemoryRelation [event_id#272, vehicle_profile_id#273, trip_id#274, trip_seq#275L, event_time#276, latitude#277, longitude#278, spe
                     +- AdaptiveSparkPlan isFinalPlan=false
                        +- Project [event_id#272, vehicle_profile_id#273, trip_id#274, trip_seq#275L, event_time#276, latitude#277, longitude#278, spe
                           +- Window [max(_w0#4957L) windowspecdefinition(trip_id#274, _episode_run_id#4951L, specifiedwindowframe(RowFrame, unbounded
                              +- Sort [trip_id#274 ASC NULLS FIRST, _episode_run_id#4951L ASC NULLS FIRST], false, 0
                                 +- Project [event_id#272, vehicle_profile_id#273, trip_id#274, trip_seq#275L, event_time#276, latitude#277, longitude
                                    +- Window [sum(_w0#4953) windowspecdefinition(trip_id#274, trip_seq#275L ASC NULLS FIRST, event_time#276 ASC NULLS
                                       +- Project [event_id#272, vehicle_profile_id#273, trip_id#274, trip_seq#275L, event_time#276, latitude#277, lon
                                          +- Project [event_id#272, vehicle_profile_id#273, trip_id#274, trip_seq#275L, event_time#276, latitude#277, 
```

### ssp 08-25 03:00/run_sensor_processing · exec 0 (3:03)

읽고 쓴 데이터:

- `bronze/sensor-events`

```text
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[], functions=[count(1)], output=[count#836L])
   +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=82]
      +- HashAggregate(keys=[], functions=[partial_count(1)], output=[count#1261L])
         +- InMemoryTableScan
               +- InMemoryRelation [event_id#272, vehicle_profile_id#273, trip_id#274, trip_seq#275L, event_time#276, latitude#277, longitude#278, spe
                     +- AdaptiveSparkPlan isFinalPlan=false
                        +- Project [event_id#6, vehicle_profile_id#7, trip_id#8, trip_seq#9L, cast(event_time#10 as timestamp) AS event_time#276, lati
                           +- Filter (_duplicate_rank#200 = 1)
                              +- Window [row_number() windowspecdefinition(event_id#6, _ingested_at#23 DESC NULLS LAST, specifiedwindowframe(RowFrame,
                                 +- WindowGroupLimit [event_id#6], [_ingested_at#23 DESC NULLS LAST], row_number(), 1, Final
                                    +- Sort [event_id#6 ASC NULLS FIRST, _ingested_at#23 DESC NULLS LAST], false, 0
                                       +- Exchange hashpartitioning(event_id#6, 1000), ENSURE_REQUIREMENTS, [plan_id=44]
                                          +- WindowGroupLimit [event_id#6], [_ingested_at#23 DESC NULLS LAST], row_number(), 1, Partial
```

### ssp 08-25 05:00/run_sensor_processing · exec 0 (1:30)

읽고 쓴 데이터:

- `bronze/sensor-events`

```text
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[], functions=[count(1)], output=[count#836L])
   +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=82]
      +- HashAggregate(keys=[], functions=[partial_count(1)], output=[count#1261L])
         +- InMemoryTableScan
               +- InMemoryRelation [event_id#272, vehicle_profile_id#273, trip_id#274, trip_seq#275L, event_time#276, latitude#277, longitude#278, spe
                     +- AdaptiveSparkPlan isFinalPlan=false
                        +- Project [event_id#6, vehicle_profile_id#7, trip_id#8, trip_seq#9L, cast(event_time#10 as timestamp) AS event_time#276, lati
                           +- Filter (_duplicate_rank#200 = 1)
                              +- Window [row_number() windowspecdefinition(event_id#6, _ingested_at#23 DESC NULLS LAST, specifiedwindowframe(RowFrame,
                                 +- WindowGroupLimit [event_id#6], [_ingested_at#23 DESC NULLS LAST], row_number(), 1, Final
                                    +- Sort [event_id#6 ASC NULLS FIRST, _ingested_at#23 DESC NULLS LAST], false, 0
                                       +- Exchange hashpartitioning(event_id#6, 1000), ENSURE_REQUIREMENTS, [plan_id=44]
                                          +- WindowGroupLimit [event_id#6], [_ingested_at#23 DESC NULLS LAST], row_number(), 1, Partial
```

### ssp 08-25 03:00/run_sensor_processing · exec 2 (1:27)

읽고 쓴 데이터:

- `bronze/sensor-events`

```text
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[], functions=[count(1)], output=[count#4017L])
   +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=372]
      +- HashAggregate(keys=[], functions=[partial_count(1)], output=[count#4216L])
         +- InMemoryTableScan
               +- InMemoryRelation [event_id#6, trip_id#8, event_date#139, reject_reason#140, reject_detail#141, raw_record#142, _run_id#143, _rejecte
                     +- AdaptiveSparkPlan isFinalPlan=false
                        +- Union
                           :- Project [event_id#6, trip_id#8, cast(event_time#10 as date) AS event_date#139, MALFORMED_JSON AS reject_reason#140, null
                           :  +- Filter _parse_failed#26: boolean
                           :     +- InMemoryTableScan [_parse_failed#26, _raw_record#25, event_id#6, event_time#10, trip_id#8], [_parse_failed#26]
                           :           +- InMemoryRelation [event_id#6, vehicle_profile_id#7, trip_id#8, trip_seq#9L, event_time#10, latitude#11, long
                           :                 +- Project [from_json(StructField(event_id,StringType,false), StructField(vehicle_profile_id,IntegerType,
                           :                    +- Filter ((coalesce(cast(from_json(StructField(event_id,StringType,false), StructField(vehicle_profil
```

### ssp 08-25 03:00/run_sensor_processing · exec 8 (1:11)

읽고 쓴 데이터:

- `silver/sensor_event_quarantine/_staging/scheduled__2026-08-25T03:00:00+00:00`
- `bronze/sensor-events`

```text
AdaptiveSparkPlan isFinalPlan=false
+- Execute InsertIntoHadoopFsRelationCommand s3://de4-data-lake-473551908409-ap-northeast-2-an/silver/sensor_event_quarantine/_staging/scheduled__2026
   +- WriteFiles
      +- Sort [target_date#11705 ASC NULLS FIRST, target_hour#11706 ASC NULLS FIRST], false, 0
         +- Union
            :- Project [event_id#6, trip_id#8, cast(event_time#10 as date) AS event_date#139, MALFORMED_JSON AS reject_reason#140, null AS reject_deta
            :  +- Filter _parse_failed#26: boolean
            :     +- InMemoryTableScan [_parse_failed#26, _raw_record#25, event_id#6, event_time#10, trip_id#8], [_parse_failed#26]
            :           +- InMemoryRelation [event_id#6, vehicle_profile_id#7, trip_id#8, trip_seq#9L, event_time#10, latitude#11, longitude#12, speed
            :                 +- Project [from_json(StructField(event_id,StringType,false), StructField(vehicle_profile_id,IntegerType,false), StructF
            :                    +- Filter ((coalesce(cast(from_json(StructField(event_id,StringType,false), StructField(vehicle_profile_id,IntegerTyp
            :                       +- *(1) ColumnarToRow
            :                          +- FileScan parquet [value#1,timestamp#5] Batched: true, DataFilters: [(coalesce(cast(from_json(StructField(eve
            :- Project [event_id#162, trip_id#164, cast(event_time#166 as date) AS event_date#147, MISSING_REQUIRED_FIELD AS reject_reason#148, concat
```

### ssp 08-25 03:00/run_sensor_processing · exec 7 (1:03)

읽고 쓴 데이터:

- `bronze/sensor-events`

```text
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[], functions=[count(1)], output=[count#8814L])
   +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=4586]
      +- HashAggregate(keys=[], functions=[partial_count(1)], output=[count#10779L])
         +- Union
            :- Project
            :  +- Filter _parse_failed#26: boolean
            :     +- InMemoryTableScan [_parse_failed#26], [_parse_failed#26]
            :           +- InMemoryRelation [event_id#6, vehicle_profile_id#7, trip_id#8, trip_seq#9L, event_time#10, latitude#11, longitude#12, speed
            :                 +- Project [from_json(StructField(event_id,StringType,false), StructField(vehicle_profile_id,IntegerType,false), StructF
            :                    +- Filter ((coalesce(cast(from_json(StructField(event_id,StringType,false), StructField(vehicle_profile_id,IntegerTyp
            :                       +- *(1) ColumnarToRow
            :                          +- FileScan parquet [value#1,timestamp#5] Batched: true, DataFilters: [(coalesce(cast(from_json(StructField(eve
            :- Project
```

### ssp 08-25 06:00/run_sensor_processing · exec 5 (1:02)

읽고 쓴 데이터:

- `bronze/sensor-events`

```text
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[], functions=[count(1)], output=[count#6332L])
   +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=1222]
      +- HashAggregate(keys=[], functions=[partial_count(1)], output=[count#7013L])
         +- InMemoryTableScan
               +- InMemoryRelation [event_id#272, vehicle_profile_id#273, trip_id#274, trip_seq#275L, event_time#276, latitude#277, longitude#278, spe
                     +- AdaptiveSparkPlan isFinalPlan=false
                        +- Project [event_id#272, vehicle_profile_id#273, trip_id#274, trip_seq#275L, event_time#276, latitude#277, longitude#278, spe
                           +- Window [max(_w0#4957L) windowspecdefinition(trip_id#274, _episode_run_id#4951L, specifiedwindowframe(RowFrame, unbounded
                              +- Sort [trip_id#274 ASC NULLS FIRST, _episode_run_id#4951L ASC NULLS FIRST], false, 0
                                 +- Project [event_id#272, vehicle_profile_id#273, trip_id#274, trip_seq#275L, event_time#276, latitude#277, longitude
                                    +- Window [sum(_w0#4953) windowspecdefinition(trip_id#274, trip_seq#275L ASC NULLS FIRST, event_time#276 ASC NULLS
                                       +- Project [event_id#272, vehicle_profile_id#273, trip_id#274, trip_seq#275L, event_time#276, latitude#277, lon
                                          +- Project [event_id#272, vehicle_profile_id#273, trip_id#274, trip_seq#275L, event_time#276, latitude#277, 
```

### ssp 08-25 03:00/run_sensor_processing · exec 10 (0:40)

읽고 쓴 데이터:

- `bronze/sensor-events`

```text
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[], functions=[max(cast(((((((((isnull(segment_id#14511) OR isnull(vehicle_profile_id#14512)) OR isnull(data_period_start#14513)
   +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=5561]
      +- HashAggregate(keys=[], functions=[partial_max(cast(((((((((isnull(segment_id#14511) OR isnull(vehicle_profile_id#14512)) OR isnull(data_perio
         +- InMemoryTableScan [segment_id#14511, vehicle_profile_id#14512, data_period_start#14513, data_period_end#14514, road_snapshot_date#14515, h
               +- InMemoryRelation [segment_id#14511, vehicle_profile_id#14512, data_period_start#14513, data_period_end#14514, road_snapshot_date#145
                     +- AdaptiveSparkPlan isFinalPlan=false
                        +- Project [segment_id#4851, vehicle_profile_id#273, data_period_start#14175, data_period_end#14176, road_snapshot_date#14393,
                           +- ShuffledHashJoin (HybridHash) [data_period_start#14175, data_period_end#14176, segment_id#4851, vehicle_profile_id#273],
                              :- ObjectHashAggregate(keys=[data_period_start#14175, data_period_end#14176, segment_id#4851, vehicle_profile_id#273], f
                              :  +- Exchange hashpartitioning(data_period_start#14175, data_period_end#14176, segment_id#4851, vehicle_profile_id#273,
                              :     +- ObjectHashAggregate(keys=[data_period_start#14175, data_period_end#14176, segment_id#4851, vehicle_profile_id#2
                              :        +- Project [vehicle_profile_id#273, speed_mps#279, accel_x#281, accel_y#282, accel_z#283, jerk_x#284, jerk_y#28
                              :           +- Project [vehicle_profile_id#273, speed_mps#279, accel_x#281, accel_y#282, accel_z#283, jerk_x#284, jerk_y
```

### ssp 08-25 05:00/run_standard_score · exec 7 (0:35)

```text
Execute SaveIntoDataSourceCommand
   +- SaveIntoDataSourceCommand org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider@6341eeaf, HashMap(url -> *********(redacted), tru
         +- Relation [segment_id#2440,vehicle_profile_id#2441,score_as_of#2442,data_period_start#2443,data_period_end#2444,vertical_score#2445,longitu
```
