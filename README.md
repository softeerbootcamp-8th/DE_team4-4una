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
2. [데이터 프로덕트](#2-데이터-프로덕트)
3. [데이터 파이프라인](#3-데이터-파이프라인)
4. [데이터 아키텍처](#4-데이터-아키텍처)
5. [기술적 고민과 결정](#5-기술적-고민과-결정)
6. [한계와 향후 개선](#6-한계와-향후-개선)
7. [기술 스택](#7-기술-스택)
8. [팀원](#8-팀원)

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
  
<img width="739" height="271" alt="image" src="https://github.com/user-attachments/assets/93370e3a-bdad-4671-9e4e-7cb0acac15b7" />

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

> **실시간으로 유입되는 주행 데이터와 변화하는 날씨를 빠르게 반영하면서도,  
> 일부 데이터나 시스템에 문제가 생겼을 때 서비스가 함께 멈추지 않게 하려면 어떻게 해야 할까?**

저희는 이 문제를 해결하기 위해 데이터 파이프라인의 핵심 가치를  
**최신성(Freshness)** 과 **가용성(Availability)** 으로 정의했습니다.

기술을 먼저 정한 뒤 구조를 맞추기보다,  
각 단계에서 두 핵심 가치를 지키기 위해 필요한 기술과 처리 방식을 선택했습니다.

| 핵심 가치 | 목표 | 주요 설계 |
| --- | --- | --- |
| **최신성** | 센서와 날씨 변화를 필요한 갱신 주기 안에 반영 | Kafka, Spark Structured Streaming, Spark/EMR 최적화, Airflow |
| **가용성** | 일부 데이터·컴포넌트에 문제가 생겨도 가능한 범위에서 계속 제공 | Serving Fallback, Data Quality, Observability, Ops Agent |

<p align="center"><sub>고민을 클릭하면 측정값·기각한 대안·검증 기준을 담은 상세 페이지로 이동합니다</sub></p>

| 고민 | 결정 |
| --- | --- |
| **[Spark 배치를 어디서 실행할까 →](docs/decisions/01-spark-execution-environment.md)** | 부트스트랩 5~10분이 연산 시간보다 긴 워크로드라 **EMR Serverless** 선택. |
| **[Bronze에 소파일이 쌓인다 →](docs/decisions/02-bronze-small-files.md)** | 트리거 기준을 시간에서 **오프셋**으로 전환. 1시간 **3,545개 → 20개 이하** |
| **[매시간 Bronze 전체를 스캔한다 →](docs/decisions/03-bronze-partition-pruning.md)** | 쓰기 측 파티션에 맞춰 읽기 프루닝. parquet scan **3회 → 1회** |
| **[갱신 주기가 다른 데이터를 어떻게 묶을까 →](docs/decisions/05-pipeline-split-and-assets.md)** | 168시간 집계와 15분 보정을 **3개 DAG로 분리**하고 Airflow Asset으로 연결 |
| **[품질 검증 실패를 어떻게 처리할까 →](docs/decisions/06-row-quarantine-and-circuit-breaker.md)** | 행 단위 **quarantine** + 실패율 **circuit breaker**. 정상 행은 계속 서빙 |
| **[중간 산출물을 저장해야 할까 →](docs/decisions/07-in-memory-intermediate.md)** | T1·T2를 같은 Spark session으로 연결해 S3 write/read 제거 |
| **[날씨를 이력으로 쌓아야 할까 →](docs/decisions/08-latest-zone-weather.md)** | 15분 스냅샷 누적을 **존별 최신 1건**으로 재설계, 적용 시각만 점수에 고정 |

<p align="center">
  <a href="docs/decisions/"><strong>의사결정 기록 템플릿과 전체 목록</strong></a>
  &nbsp;·&nbsp;
  <a href="docs/adr/"><strong>ADR</strong></a>
</p>

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
