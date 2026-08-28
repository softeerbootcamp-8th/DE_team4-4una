<div align="center">

<h1>NYC Road Comfort Score API</h1>

<img width="2400" height="760" alt="banner_panel" src="https://github.com/user-attachments/assets/c088a8f9-f45d-4dd1-bbc4-e7da6aec0a39" />

<h3>"조금 더 걸리더라도, 더 편안한 길을 선택할 수 있도록"</h3>

<p>
  <strong>
    도로별 승차감 데이터를 구축해 내비게이션의<br/>
    ‘편안한 경로 우선’ 기능을 가능하게 하는 API 제공 프로덕트입니다.
  </strong>
</p>

<p>
  <a href="http://43.203.192.129:8501/"><strong>대시보드 </strong></a>
</p>

</div>

---

## 목차

1. [프로젝트 개요](#1-프로젝트-개요)
2. [데이터 파이프라인](#2-데이터-파이프라인)
3. [AWS 인프라 아키텍처](#3-aws-인프라-아키텍처)
4. [기술적 고민과 결정](#4-기술적-고민과-결정)
5. [한계 및 개선 방향](#5-한계-및-개선-방향)
6. [기술 스택](#6-기술-스택)
7. [팀원](#7-팀원)

---

## 1. 프로젝트 개요

### 대상

- 도로별 승차감 정보를 경로 추천에 활용하려는 **내비게이션 시스템 개발자**

### 문제

- 기존 경로 추천은 시간·거리·비용 중심으로 제공되어, 도로별 승차감을 비교할 수 있는 데이터의 부재

### 해결 방법

NYC 택시 운행 기록과 도로 환경 데이터를 기반으로 차량 주행 센서 데이터를 생성하고,<br/>
도로 Segment × 차량 Profile별 **Comfort Score**를 구축한다.<br/>

또한 최신 날씨를 반영해 점수를 갱신하고, <br/>
내비게이션이 전달한 여러 후보 경로의 승차감을 제공하여 <br/>
가장 편안한 경로를 비교할 수 있게 한다.

### 기대 효과

| 관점 | AS-IS | TO-BE |
| --- | --- | --- |
| **경로 추천 기준** | 시간·거리·비용 중심 | **승차감까지 고려한 경로 비교**  |
| **도로 승차감 정보** | 도로별 비교 데이터 없음 | Segment 단위 Comfort Score 제공 |
| **환경 변화 반영** | 현재 환경 반영 어려움 | 최신 날씨를 반영한 Current Score 제공 |

### API 명세서

<img width="2218" height="982" alt="image" src="https://github.com/user-attachments/assets/c3af43fc-4c18-4cd3-9de8-5135bab36149" />

---
## 2. 데이터 파이프라인
<div align="center">
  
  <img width="901" height="338" alt="image" src="https://github.com/user-attachments/assets/33dd132b-5fc6-4e3f-a250-486cf882bfb1" />

</div>

### Input

| 데이터 | 역할 |
| --- | --- |
| NYC TLC HVFHV | 실제 차량 운행 패턴 |
| LION | 도로 Segment 기준정보 |
| Pavement Ratings | 도로 노면 상태 |
| Speed Humps | 도로 시설 정보 |
| Taxi Zone | 공간 단위 및 날씨 매핑 |
| Open-Meteo | 현재 기상 상태 |
| Vehicle Profile | 차량 특성별 승차감 차이 |

### Output

| Output | Grain / 대상 | 주요 내용 |
| --- | --- | --- |
| **Hourly Comfort Score** | `1시간 × Segment × Vehicle Profile` | 센서 Feature를 기반으로 수직·종·횡 방향의 **0~100 Comfort Score** 계산 |
| **Standard Comfort Score** | `Segment × Vehicle Profile` | 최근 **168시간(1주일)** 데이터를 집계해 기준 승차감과 **Confidence** 제공 |
| **Current Comfort Score** | `Segment × Vehicle Profile` | Standard Score에 **최신 날씨 영향**을 반영한 현재 승차감 점수 |
| **Serving API** | 후보 경로 | 후보 경로의 Segment 목록을 입력받아 **경로별 Comfort Score를 계산하고 추천 경로 제공** |

### 각 파이프라인 단계별 Input/Output

<details>
<summary><strong>① Source — 원천 데이터와 시뮬레이션 환경</strong></summary>

| 구분 | 내용 |
| --- | --- |
| Input | TLC HVFHV 월간 Parquet, LION, Pavement Ratings, Speed Humps, Taxi Zone |
| 규모 | TLC 2024-02 **19,359,148행 / 441MB**, LION **166,222 segments**, Taxi Zone **263개** |
| 처리 | 원천 스냅샷과 checksum 보존 → 좌표계·geometry 표준화 → 노면·방지턱·zone을 LION segment에 공간 매핑 |
| Output | 버전형 <code>road_segment</code>, <code>enriched_segment_reference</code>, 시뮬레이션용 road environment |

</details>

### 
<a id="stage-simulation"></a>

<details>
<summary><strong>② Simulation — TLC 운행을 10Hz 센서 이벤트로 재생</strong></summary>

| 구분 | 내용 |
| --- | --- |
| Input | TLC 월간 trip, road environment, 차량 프로필 5종 |
| 규모 | 전체 trip을 request 시각·원천 row 순서로 batch fetch, 동시에 활성화되는 차량 수 제한 없음 |
| 처리 | Zone 안 승·하차 지점 선택 → LION 경로 계산 → 노면·방지턱·차량 반응을 반영해 속도·가속도·jerk·조향값 생성 |
| Output | Kafka <code>sensor-events</code> JSON, 순서·멱등 키 <code>(trip_id, trip_seq)</code> |

</details>

<a id="stage-bronze"></a>

<details>
<summary><strong>③ Bronze — Kafka 원본을 S3에 보존</strong></summary>

| 구분 | 내용 |
| --- | --- |
| Input | Kafka key·value·topic·partition·offset·timestamp |
| 규모 | 1시간 실측 **12,058,018행 / 2.6GB / 3,545 files** |
| 처리 | Structured Streaming checkpoint로 offset 복구, <code>_ingested_at</code> 추가, event time 기준 파티셔닝 |
| Output | S3 <code>bronze/sensor-events/event_date=YYYY-MM-DD/hour=HH</code> Parquet |

</details>

<a id="stage-features"></a>

<details>
<summary><strong>④ Silver Features — 정제·중복 제거·GPS 맵매칭</strong></summary>

| 구분 | 내용 |
| --- | --- |
| Input | 대상 시간 Bronze와 인접 경계 이벤트, 버전 고정 LION geometry |
| 규모 | 약 1,205만 센서 이벤트/시간 |
| 처리 | 계약·범위 검증 → <code>(trip_id, trip_seq)</code> 중복 제거 → GPS·heading 기반 LION 맵매칭 → RMS·P95·급가감속·조향 특징 집계 |
| Output | <code>hourly_segment_features</code> **38,049행**, 실패 원문과 사유를 담은 quarantine |

</details>

<a id="stage-hourly"></a>

<details>
<summary><strong>⑤ Hourly Score — 수직·종·횡 승차감 계산</strong></summary>

| 구분 | 내용 |
| --- | --- |
| Input | 시간 × segment × 차량 프로필별 features **38,049행** |
| 규모 | Bronze 대비 약 **317:1 축약** |
| 처리 | 가속도·jerk·조향 특징을 수직·종·횡 0~100 점수로 변환 |
| Output | <code>hourly_comfort_score</code> **38,049행**, 표본 수·trip 수·산식 버전 |

</details>

<a id="stage-standard"></a>

<details>
<summary><strong>⑥ Standard Gold — 최근 168시간 기준 점수</strong></summary>

| 구분 | 내용 |
| --- | --- |
| Input | 최근 168시간 Hourly Score, 전체 segment와 차량 프로필 universe |
| 규모 | full-NYC 기준 실행당 최대 **997,332행**¹ |
| 처리 | 최소 통행량 필터 → 방향별 평균 → 표본 부족 구간을 모집단 평균으로 shrinkage → 신뢰도 계산 |
| Output | 버전형 S3 snapshot과 PostgreSQL <code>standard_segment_comfort_score</code> |

¹ 166,222 segments × 차량 프로필 5종 및 차량 무관 대표 프로필 1종

</details>

<a id="stage-current"></a>

<details>
<summary><strong>⑦ Current Gold — 최신 날씨 반영</strong></summary>

| 구분 | 내용 |
| --- | --- |
| Input | 최신 Standard Score, 최대 263개 zone의 Open-Meteo 날씨 |
| 규모 | 날씨는 15분마다 수집하며 영향 등급이 바뀐 zone만 재계산 |
| 처리 | 비·눈·결빙·강풍·저시야 영향을 방향별 점수에 적용, 오류 행은 quarantine |
| Output | PostgreSQL <code>current_segment_comfort_score</code> 최신 행 |

</details>

<a id="stage-serving"></a>

<details>
<summary><strong>⑧ Serving API — 최종 Comfort Score 제공</strong></summary>

| 구분 | 내용 |
| --- | --- |
| Input | 차량 프로필과 여러 후보 경로의 LION segment 목록 |
| 규모 | 후보 경로 전체 segment를 PostgreSQL에서 한 번에 조회 |
| 처리 | Current 우선·Standard fallback → 경로 평균과 취약 구간 점수 결합 → 점수순 정렬 |
| Output | 경로별 Comfort Score·신뢰도·버전과 <code>recommended_route_id</code> |

</details>

---

## 3. AWS 인프라 아키텍처

<div align="center">

<img width="486" height="379" alt="image" src="https://github.com/user-attachments/assets/094c5863-0d41-4d59-ad31-cdd2f560aa3c" />

</div>

---

## 4. 기술적 고민과 결정

> **지속적으로 유입되는 주행 데이터와 변화하는 날씨를 빠르게 반영하면서도,  
> 잘못된 데이터가 결과를 오염시키지 않고, 일부 시스템에 문제가 생겨도 서비스를 계속 제공하려면 어떻게 해야 할까?**

파이프라인의 핵심 가치를  
**최신성(Freshness)** · **신뢰성(Reliability)** · **가용성(Availability)** 으로 정의한다.

특정 기술을 먼저 선택하기보다,
데이터가 생성되고 API로 제공되는 과정에서 발생하는  
**지연·데이터 오류·시스템 장애를 어떻게 줄이고 격리할 것인가**를 기준으로 기술과 구조를 선택한다.

| 핵심 가치 | 핵심 질문 | 설계 방향 |
| --- | --- | --- |
| **최신성 Freshness** | 변화한 데이터를 필요한 시점 안에 반영할 수 있는가? | Streaming 수집, Spark 최적화, 필요한 범위만 재계산, Lag 관측 |
| **신뢰성 Reliability** | 생성된 데이터와 Score를 믿고 사용할 수 있는가? | 계층별 검증, Quarantine, Circuit Breaker, Idempotency, Versioning |
| **가용성 Availability** | 일부 데이터나 시스템이 실패해도 서비스를 계속 제공할 수 있는가? | Serving Fallback, 통합 모니터링, Alert, 자동 복구 및 Escalation |

---

### 최신성 Freshness

시간당 약 **1,200만 건의 Sensor Event**와
15분 주기의 날씨 변화를 필요한 시점 안에 반영해야 한다.

단순히 Spark 실행 자원을 늘리기보다  
**불필요한 Scan·I/O·재계산을 줄이고, 실제 데이터 지연을 직접 관측하는 것**에 우선순위를 둔다.

| 고민 | 결정 | 효과 |
| --- | --- | --- |
| **10Hz Sensor Event를 지속적으로 어떻게 수집할까** | **Kafka + Spark Structured Streaming**을 사용하고 30초 단위 Micro Batch로 S3 Bronze에 지속 적재한다 | Producer와 저장 처리 속도를 분리하고 일시적인 처리 지연을 Kafka가 흡수한다 |
| **Streaming Process가 중단되면 어디서부터 다시 처리할까** | Spark Structured Streaming **Checkpoint**를 통해 처리 Offset을 보존한다 | 재시작 시 처음부터 읽지 않고 마지막 처리 지점부터 이어서 처리한다 |
| **Streaming Process가 살아 있어도 데이터가 밀리고 있는지 어떻게 알까** | Structured Streaming Progress에서 **Event Time Lag, Kafka Offset Lag, Input/Processed Rows, Batch Duration, Query Status**를 Prometheus Metric으로 수집한다 | Process 생존 여부와 **실제 데이터 최신성**을 분리해 관측한다 |
| **Bronze에 작은 Parquet File이 계속 쌓인다** | Offset 기반 Trigger와 **출력 Partition 수 제어**를 적용하고, 잔여 소파일은 별도 **Compaction DAG**로 정리한다 | 시간 단위 Batch의 파일 탐색·읽기 비용을 줄이고 지속적인 소파일 누적을 방지한다 |
| **매시간 Bronze 전체를 Scan한다** | `event_date/hour` 기준 **Target Hour Partition만 읽도록 Partition Pruning**을 적용한다 | Parquet Scan **3회 → 1회**로 감소한다 |
| **동일 데이터를 여러 Spark Action이 반복 계산한다** | 불필요한 Action을 제거하고 필요한 DataFrame을 재사용한다 | 반복 Scan과 연산을 줄인다 |
| **T1 결과를 S3에 저장한 뒤 T2에서 다시 읽어야 할까** | T1과 T2를 동일 Spark Application에서 실행하고 DataFrame을 **In-memory로 전달**한다 | 중간 `processed_sensor_event` S3 Write / Read를 제거한다 |
| **GPS Point마다 전체 LION Segment를 탐색해야 할까** | LION Reference를 **Broadcast**하고 Partition 내부에서 **STRtree + `mapInPandas`** 기반으로 주변 후보 Segment만 탐색한다 | 전체 Sensor × Segment Shuffle을 피하고 Map Matching 탐색 범위를 줄인다 |
| **Spark Job마다 동일한 Resource를 할당해야 할까** | Job 특성에 따라 **EMR Serverless Resource Profile**을 분리하고 실행 동시성을 제어한다 | 작은 Job의 자원 낭비와 큰 Job의 자원 경합을 줄인다 |
| **Standard와 Weather의 갱신 주기가 다르다** | Standard / Weather / Current를 **3개 DAG로 분리하고 Airflow Asset으로 연결**한다 | 날씨 변경 때문에 168시간 Standard 집계를 다시 수행하지 않는다 |
| **15분마다 Weather를 수집할 때마다 Current를 전부 다시 계산해야 할까** | 날씨 값 자체가 아니라 **승차감 영향 등급이 변경된 Zone만 재계산**한다 | 실제 Score 변화가 없는 Zone의 불필요한 계산을 막는다 |
| **Current 계산에는 최신 날씨만 필요한데 이력을 계속 조회해야 할까** | Serving 계산에는 **Zone별 최신 Weather 1건**을 사용한다 | Current 계산의 조회 범위와 데이터 관리 복잡도를 줄인다 |
| **Kafka Broker 디스크 사용량이 계속 증가한다** | Batch 크기 증가 가설을 측정으로 기각하고 **zstd Compression**을 적용한다 | Broker 저장량을 약 **42% 감소**시킨다 |

<img width="864" height="235" alt="image" src="https://github.com/user-attachments/assets/7f79cced-2d60-454a-8a9b-d1de1048cc04" />
<img width="2048" height="489" alt="image" src="https://github.com/user-attachments/assets/274d3260-e39d-4ea2-a711-19dfd46dd02a" />


---

### 가용성 Availability

데이터와 시스템의 일부가 항상 정상이라고 가정하지 않는다.

일부 최신 데이터가 없거나 Component 장애가 발생하더라도  
**사용 가능한 데이터와 시스템을 활용해 서비스 제공을 지속하는 것**을 목표로 한다.

| 고민 | 결정 | 효과 |
| --- | --- | --- |
| **Current Score가 없는 Segment 때문에 API 요청 전체를 실패시켜야 할까** | **Current Score를 우선 사용하고 없으면 Standard Score로 Fallback**한다 | 날씨 미수집 또는 Zone 외 Segment가 포함되어도 사용 가능한 기준 점수로 응답한다 |
| **후보 경로마다 DB를 여러 번 조회해야 할까** | 여러 후보 경로의 Segment를 모아 PostgreSQL에서 **Bulk 조회**한다 | 경로·Segment 수 증가에 따른 DB Round Trip을 줄인다 |
| **여러 Component의 장애를 Application Log만으로 확인할 수 있을까** | **Prometheus + Grafana**에서 EC2 / Container / Kafka / Spark Streaming / Airflow / Serving API 상태를 통합 관측한다 | 장애 지점과 영향 범위를 한 화면에서 파악한다 |
| **EMR Serverless도 Prometheus로 직접 Scrape해야 할까** | AWS Managed Service인 EMR Serverless는 **CloudWatch Metric을 Grafana에서 조회**한다 | Prometheus 대상이 아닌 Managed Service까지 같은 Monitoring 화면에서 확인한다 |
| **Monitoring System 자체가 죽으면 장애를 어떻게 알까** | Grafana / Prometheus / Ops Agent 등 핵심 Monitoring Component는 **외부 Probe 방식으로 Self-health를 확인**한다 | 모니터링 시스템 자신의 장애를 내부 Metric에만 의존하지 않는다 |
| **Ops Agent가 죽으면 장애 알림까지 사라지지 않을까** | Grafana Alert를 **Slack과 Ops Agent에 병렬 전달**한다 | Ops Agent 장애와 관계없이 최초 장애 알림은 Slack으로 전달된다 |
| **Grafana Alert를 받자마자 자동 복구를 실행할까** | Ops Agent가 **Prometheus에서 상태를 재검증하고 진단한 뒤** 조치 여부를 결정한다 | 일시적인 Alert나 이미 복구된 상태에 불필요한 조치를 수행하지 않는다 |
| **모든 장애를 자동 복구해야 할까** | Container 재시작처럼 **영향 범위가 작고 되돌릴 수 있는 작업만 Allowlist**로 자동화한다 | 자동 복구로 인한 2차 장애 가능성을 제한한다 |
| **Kafka Offset Reset·데이터 삭제·DB Schema 변경도 자동화할까** | 데이터와 인프라 상태를 직접 변경하는 작업은 **자동화하지 않고 담당자에게 Escalation**한다 | 복구 속도보다 데이터 정합성과 운영 안전성을 우선한다 |
| **자동 조치 직후 바로 복구 성공으로 판단할까** | 일정 시간 상태를 Polling해 **실제 복구 여부를 다시 확인**한다 | Container 기동·Metric Scrape 지연을 복구 실패로 오판하지 않는다 |

---
### 신뢰성 Reliability

빠르게 계산된 결과라도
잘못된 데이터에서 만들어졌거나 어떤 기준으로 계산됐는지 추적할 수 없다면
Serving 데이터로 사용할 수 없다.

따라서 **잘못된 데이터는 조기에 격리하고,
정상 데이터는 보존하며,
같은 입력을 다시 처리했을 때 결과의 기준을 추적할 수 있도록 하는 것**을 목표로 한다.

| 고민 | 결정 | 효과 |
| --- | --- | --- |
| **일부 Row의 품질 검증 실패 때문에 전체 갱신을 멈춰야 할까** | 실패 Row만 **Quarantine**하고 정상 Row는 계속 처리한다 | 일부 데이터 오류 때문에 정상 데이터까지 버리는 것을 방지한다 |
| **상류 데이터 전체가 크게 오염된 경우에도 계속 처리해야 할까** | Quarantine 비율이 임계치를 넘으면 **Failure-rate Circuit Breaker**로 전체 반영을 중단한다 | 부분 오류는 격리하되 데이터 전체의 신뢰성이 무너지는 상황은 차단한다 |
| **모든 품질 검증을 하나의 방식으로 처리해야 할까** | Schema·PK·필수값 같은 **Hard Invariant는 Code / DB에서 검증**하고, 범위·분포·품질 조건은 GX 또는 실행 단계 Validation으로 분리한다 | 검증 성격에 맞는 위치에서 오류를 가장 빠르게 발견한다 |
| **Sensor Event가 처리 과정에서 조용히 유실되면 어떻게 알까** | Bronze Event가 **정상 Feature의 `sample_count` 또는 Quarantine 중 하나에 반드시 포함되도록 Conservation Rule**을 검증한다 | Silent Data Loss를 탐지한다 |
| **중복 이벤트나 Replay 데이터를 어떻게 구분할까** | `(trip_id, trip_seq)`를 **Ordering / Replay / Deduplication Key**로 사용한다 | 재처리 시 동일 Sensor Event가 중복 반영되는 것을 방지한다 |
| **원본 데이터를 정제 과정에서 변경해도 될까** | S3 Bronze Sensor Event는 **Immutable / Append-only**로 보존한다 | 산식이나 처리 로직이 변경되어도 원본부터 다시 계산할 수 있다 |
| **도로 기준정보가 변경되면 과거 Map Matching 결과를 어떻게 설명할까** | 사용한 LION 기준정보의 **`road_snapshot_date`**를 결과와 함께 저장한다 | 어떤 도로 Snapshot을 기준으로 계산했는지 추적할 수 있다 |
| **Score 산식이 변경되면 이전 결과와 어떻게 구분할까** | `feature_version`, `scoring_version`, `score_version` 등 **Algorithm Version을 데이터와 함께 관리**한다 | 산식 변경 전후 결과를 구분하고 재현할 수 있다 |
| **결과가 어떤 실행에서 만들어졌는지 어떻게 추적할까** | Run ID, Data Period, 계산 시각, Snapshot / Version 정보를 결과에 함께 기록한다 | 장애 분석·Backfill·재처리 시 데이터 Lineage를 추적할 수 있다 |
| **시간 단위 결과를 교체하다 실패하면 기존 정상 결과가 사라지지 않을까** | 결과를 **Staging에서 검증한 뒤 교체하고 Backup을 통해 실패 시 복구**할 수 있도록 구성한다 | 부분적으로 생성된 결과가 정상 데이터처럼 사용되는 것을 방지한다 |
| **Serving 직전 검증만으로 충분할까** | 실행 중 검증과 별도로 Gold 데이터를 주기적으로 확인하는 **At-rest Audit**을 수행한다 | 이미 적재된 데이터의 Range·Freshness·Reference 이상도 지속적으로 탐지한다 |

<img width="2933" height="1890" alt="image" src="https://github.com/user-attachments/assets/2ce683bc-3902-49d4-8a7b-bb01726b9c36" />
<img width="3016" height="1492" alt="image" src="https://github.com/user-attachments/assets/5085de61-4c8b-4e11-b418-97915bc9c2c2" />


---

### 그 외 엔지니어링 결정

세 가지 핵심 가치 외에도
데이터가 장기간 운영되고 변경될 수 있다는 점을 고려해
모델링·비용·문서화 측면의 기준을 함께 관리한다.

| 영역 | 고민 | 결정 |
| --- | --- | --- |
| **데이터 모델링** | API 요청 시 필요한 조건과 Pipeline의 저장 Grain이 다르면 Serving이 복잡해지지 않을까 | Gold를 `Segment × Vehicle Profile` 중심으로 설계하고 Serving에서 사용하는 조회 Key와 맞춘다 |
| **데이터 모델링** | 최신값만 필요한 테이블에 이력을 계속 누적해야 할까 | Standard / Current / Weather 등 사용 목적에 따라 **Snapshot과 Latest-state 저장 방식을 구분**한다 |
| **버전 관리** | 산식·Reference·데이터 세대 변경을 어떻게 추적할까 | Version / Snapshot Date / 계산 시각을 결과와 함께 저장해 변경 이력을 추적한다 |
| **비용 최적화** | Spark를 위해 Cluster를 항상 실행해둘 필요가 있을까 | Batch Compute는 **EMR Serverless**를 사용해 필요한 시점에만 실행한다 |
| **저장 비용** | 오래된 Image와 Object가 계속 누적되어도 될까 | **S3 / ECR Lifecycle Policy**를 적용해 불필요한 저장 데이터를 정리한다 |
| **문서화** | 빠르게 변경되는 설계를 팀 전체가 동일하게 이해하려면 | 주요 기술 결정은 `docs/decisions/`, ADR은 `docs/adr/`, Schema·Architecture·DQ 규칙은 `context/`에 지속적으로 기록한다 |
| **AI 협업** | AI Agent마다 서로 다른 프로젝트 전제를 사용하면 구현이 어긋나지 않을까 | Architecture / Schema / Data Quality / Open Question 등을 Context로 관리해 **팀원과 AI Agent가 동일한 기준을 참조**하도록 한다 |

---

각 의사결정의 측정값·대안 비교·검증 과정은 [`docs/decisions/`](docs/decisions/)에 기록한다.
---

## 5. 한계 및 개선 방향

현재 프로토타입을 운영하며 확인한 한계와, 이후 개선 방향이다.

### 데이터와 점수

* Comfort Score는 **시뮬레이션 센서 데이터**를 기반으로 산출한다. 파이프라인 내부의 데이터 정합성과 점수 분포는 검증했지만, 실제 차량에서 느끼는 승차감과의 일치 여부는 아직 검증하지 않았다.

  * 향후 실제 주행 데이터를 수집해 **실차 캘리브레이션 및 점수 검증**을 진행할 필요가 있다.

* 현재 점수 산식과 임계치는 수집된 시뮬레이션 데이터의 분포를 기준으로 조정되어 있다.

  * 다양한 차량·도로 환경의 실제 데이터를 확보해 **임계치와 가중치를 지속적으로 보정**할 수 있다.

### 파이프라인 및 처리 성능

* Kafka로 유입되는 데이터량은 **시간대별 택시 운행량에 따라 일정하지 않는다.** 예를 들어 새벽에는 운행 차량이 줄어들고, 출·퇴근 시간대에는 입력량이 증가한다.

  * 이처럼 시간별 입력 건수가 달라지면서 동일한 파이프라인에서도 **전체 처리 시간에 약 1~2분의 편차**가 발생한다.
  * 현재는 정해진 리소스로 처리하고 있어, 향후 **입력량에 따른 EMR Executor 조정, 파티션 최적화, Auto Scaling** 등을 통해 처리 시간을 안정화할 수 있다.

* Standard Comfort Score는 매시간 **최근 168시간 데이터를 다시 읽어 계산**한다. 현재 규모에서는 처리 가능하지만 데이터 규모가 커질수록 계산량도 함께 증가한다.

  * 향후 이전 집계 결과를 활용하는 **증분 집계 방식**으로 전환해 반복 연산을 줄일 수 있다.

* Bronze 데이터의 Small File은 주기적인 Compaction으로 정리하고 있지만, 실시간 적재와 별도의 배치 작업으로 수행된다.

  * 대규모 backlog가 발생하는 상황까지 고려해 **입력량 기반 Compaction 주기 및 파일 크기 최적화**가 필요하다.

* 처리 과정에서 문제가 발생한 데이터는 Quarantine으로 격리하지만, 현재는 **자동 재처리 경로가 없다.**

  * 실패 원인 수정 후 Quarantine 데이터를 다시 투입할 수 있는 **Replay/Reprocessing 파이프라인**을 추가할 수 있다.

### 운영 및 모니터링

* Airflow는 현재 **단일 EC2에서 LocalExecutor로 운영**하고 있어 해당 인스턴스가 장애를 일으키면 orchestration 전체가 영향을 받는다.

  * 운영 규모가 커질 경우 Airflow 구성 요소 분리 및 **고가용성 구조**로 확장할 수 있다.

* AWS 인프라는 대부분 수동으로 구성하고 있어 환경 재생성 및 변경 이력 관리에 한계가 있다.

  * 향후 Terraform 등의 **IaC(Infrastructure as Code)** 를 적용해 인프라 구성을 코드로 관리할 수 있다.

---

## 6. 기술 스택

<div align="center">

### Data Engineering

![Python](https://img.shields.io/badge/Python_3.12-3776AB?style=flat-square&logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![Great Expectations](https://img.shields.io/badge/Great_Expectations-FF6310?style=flat-square)

### Storage / Serving

![Amazon S3](https://img.shields.io/badge/Amazon_S3-569A31?style=flat-square&logo=amazons3&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=flat-square&logo=postgresql&logoColor=white)
![Amazon RDS](https://img.shields.io/badge/Amazon_RDS-527FFF?style=flat-square&logo=amazonrds&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=flat-square&logo=fastapi&logoColor=white)
![React](https://img.shields.io/badge/React_19-61DAFB?style=flat-square&logo=react&logoColor=black)
![Leaflet](https://img.shields.io/badge/Leaflet-199900?style=flat-square&logo=leaflet&logoColor=white)
![Vite](https://img.shields.io/badge/Vite-646CFF?style=flat-square&logo=vite&logoColor=white)

### Infrastructure / Compute

![AWS](https://img.shields.io/badge/AWS-232F3E?style=flat-square&logo=amazonwebservices&logoColor=white)
![Amazon EC2](https://img.shields.io/badge/Amazon_EC2-FF9900?style=flat-square&logo=amazonec2&logoColor=white)
![Amazon EMR](https://img.shields.io/badge/EMR_Serverless-FF9900?style=flat-square&logo=amazonwebservices&logoColor=white)
![Amazon ECR](https://img.shields.io/badge/Amazon_ECR-FF9900?style=flat-square&logo=amazonwebservices&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)

### Observability

![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=flat-square&logo=prometheus&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F46800?style=flat-square&logo=grafana&logoColor=white)
![Slack](https://img.shields.io/badge/Slack-4A154B?style=flat-square&logo=slack&logoColor=white)

### CI / CD

![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=flat-square&logo=githubactions&logoColor=white)
![GitHub](https://img.shields.io/badge/GitHub-181717?style=flat-square&logo=github&logoColor=white)

</div>

---

## 7. 팀원

<table align="center">
  <tr>
    <td align="center" width="180">
      <a href="https://github.com/Kijoonj">
        <img src="https://github.com/Kijoonj.png" width="120" height="120" alt="정기준" /><br/>
        <strong>정기준</strong>
      </a><br/>
      <sub><a href="https://github.com/Kijoonj">@Kijoonj</a></sub>
    </td>
    <td align="center" width="180">
      <a href="https://github.com/codrae">
        <img src="https://github.com/codrae.png" width="120" height="120" alt="김용진" /><br/>
        <strong>김용진</strong>
      </a><br/>
      <sub><a href="https://github.com/codrae">@codrae</a></sub>
    </td>
    <td align="center" width="180">
      <a href="https://github.com/jiyoon-ryu">
        <img src="https://github.com/jiyoon-ryu.png" width="120" height="120" alt="류지윤" /><br/>
        <strong>류지윤</strong>
      </a><br/>
      <sub><a href="https://github.com/jiyoon-ryu">@jiyoon-ryu</a></sub>
    </td>
    <td align="center" width="180">
      <a href="https://github.com/Lmmhhhh">
        <img src="https://github.com/Lmmhhhh.png" width="120" height="120" alt="이민하" /><br/>
        <strong>이민하</strong>
      </a><br/>
      <sub><a href="https://github.com/Lmmhhhh">@Lmmhhhh</a></sub>
    </td>
  </tr>
  <tr>
    <td align="center"><sub>Data Engineer</sub></td>
    <td align="center"><sub>Data Engineer</sub></td>
    <td align="center"><sub>Data Engineer</sub></td>
    <td align="center"><sub>Data Engineer</sub></td>
  </tr>
</table>

<p align="center">
  <sub>소프티어 부트캠프 8기 · Data Engineering 트랙 4팀(4una)</sub>
</p>
