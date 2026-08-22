# DE_team4-4una

> **세그먼트별 승차감(Ride Comfort) 표준 데이터셋 구축 프로젝트**
>
> 소프티어 부트캠프 8기 Data Engineering 트랙 4팀(4una) 최종 프로젝트입니다.

> 🚧 **현재 기획 단계입니다.** 아래 내용은 개발 진행 상황에 따라 계속 업데이트됩니다.

## 목차
0. [로컬 개발 환경](#로컬-개발-환경)
1. [프로젝트 개요](#1-프로젝트-개요)
2. [문제 정의](#2-문제-정의)
3. [데이터](#3-데이터)
4. [아키텍처](#4-아키텍처)
5. [처리 과정](#5-처리-과정)
6. [결과](#6-결과)
7. [팀원 소개](#7-팀원-소개)

## 로컬 개발 환경

이 프로젝트는 Python 3.12와 [uv](https://docs.astral.sh/uv/)를 사용합니다.

```bash
uv sync --all-packages
uv run --package sensor-producer sensor-producer --help
```

전체 워크스페이스는 하나의 `uv.lock`을 공유합니다. 특정 패키지 명령은 저장소 루트에서
`uv run --package <패키지명> ...` 형식으로 실행합니다.

실제 NYC 데이터로 센서 스트림을 생성하는 방법은
[`services/sensor-producer/README.md`](services/sensor-producer/README.md)를
참조하세요.

### 저장소 구조

```text
libs/de4-core/         서비스 간 공유 계약과 공통 코드
services/              독립적으로 실행·배포되는 워크스페이스 패키지
db/                    마이그레이션과 시드
infra/                 로컬 및 공용 인프라 구성
terraform/envs/        리전별 Terraform 환경
config/                공통·개발·운영 환경 설정
tests/                  워크스페이스 통합 테스트와 fixture
```

## 1. 프로젝트 개요

팀별로 메인 데이터셋 1개와 추가 데이터셋 1개 이상을 조합하여, 수집부터 서빙까지 실제로 동작하는 End-to-End 데이터 파이프라인과 데이터 프로덕트를 구축하는 프로젝트입니다.

프로젝트의 주제와 해결할 문제는 팀이 직접 정의하며, 본 팀은 **차량 주행 데이터를 활용한 세그먼트별 승차감 표준 데이터셋 구축**을 주제로 선정했습니다.


## 2. 문제 정의

| 구분 | 내용 |
| --- | --- |
| **누구의 문제?** | 현대자동차의 네비게이션 경로 추천 시스템 개발자 |
| **어떤 문제?** | 경로 알고리즘 개발 시 필요한, 세그먼트별 승차감(Ride Comfort)을 나타내는 표준 데이터셋이 없음 |
| **핵심 지표 (Index)** | 승차감 반영 경로의 선택률 |

> 승차감 민감 승객(임산부, 노약자, 유아 동반 등)이 탑승한 주행을 집계한 결과, 최단 경로가 아닌 **편안한 경로**를 선택한 비율이 **70%**를 차지합니다. 이것이 본 프로젝트가 풀고자 하는 문제입니다.

### 핵심 가치

> 🚧 개발 예정


## 3. 데이터

### Main Dataset

| 데이터셋 | 설명 |
| --- | --- |
| **NYC TLC Trip Record Data** | 승차감 분석의 중심이 되는 시간 기반 이력(trip) 데이터 |

### Sub Dataset

| 데이터셋 | 설명 |
| --- | --- |
| **Zone Lookup** | 🚧 추가 예정 |
| **LION** | 🚧 추가 예정 |
| **Street Pavement Ratings** | 🚧 추가 예정 |
| **Speed Humps** | 🚧 추가 예정 |

> 각 데이터셋의 상세 스키마 및 활용 방식은 데이터 모델링 진행 후 추가될 예정입니다.


## 4. 아키텍처

> 🚧 인프라/파이프라인 아키텍처 다이어그램은 설계 확정 후 추가될 예정입니다.

### 주요 기술 선택 근거

<details>
<summary><b>Spark</b></summary>

🚧 설계 예정

</details>

<details>
<summary><b>Kafka + Spark Streaming</b></summary>

🚧 설계 예정

</details>

<details>
<summary><b>Airflow</b></summary>

🚧 설계 예정

</details>

<details>
<summary><b>Great Expectations</b></summary>

🚧 설계 예정

</details>

<details>
<summary><b>Prometheus + Grafana</b></summary>

🚧 설계 예정

</details>

### 설계 결정 근거

> 🚧 개발 예정

### 기술 스택

<div align="center">

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![Great Expectations](https://img.shields.io/badge/Great_Expectations-FF6B35?style=for-the-badge&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&logo=prometheus&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-232F3E?style=for-the-badge&logo=amazonaws&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)

</div>


## 5. 처리 과정

> 🚧 개발 예정

### 문제 해결 과정

⚠️ 각 구현 기능 및 내용에 대한 우선순위 결정 필요.

### 기술적 내용

> 🚧 개발 예정


## 6. 결과

### 구현 결과

> 🚧 개발 예정

### 향후 개선 방향

> 🚧 개발 예정


## 7. 팀원 소개

<table align="center">
  <tr>
    <td align="center"><a href="https://github.com/Kijoonj"><b>정기준</b></a></td>
    <td align="center"><a href="https://github.com/codrae"><b>김용진</b></a></td>
    <td align="center"><a href="https://github.com/jiyoon-ryu"><b>류지윤</b></a></td>
    <td align="center"><a href="https://github.com/Lmmhhhh"><b>이민하</b></a></td>
  </tr>
  <tr>
    <td align="center"><img src="https://github.com/Kijoonj.png" width="150px;" alt="정기준"/></td>
    <td align="center"><img src="https://github.com/codrae.png" width="150px;" alt="김용진"/></td>
    <td align="center"><img src="https://github.com/jiyoon-ryu.png" width="150px;" alt="류지윤"/></td>
    <td align="center"><img src="https://github.com/Lmmhhhh.png" width="150px;" alt="이민하"/></td>
  </tr>
  <tr>
    <td align="center"><b>DE</b></td>
    <td align="center"><b>DE</b></td>
    <td align="center"><b>DE</b></td>
    <td align="center"><b>DE</b></td>
  </tr>
</table>
