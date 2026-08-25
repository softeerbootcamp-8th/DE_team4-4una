# 09. Kafka 저장량은 batch 크기가 아니라 압축 codec이 결정한다

> 가설을 세워 측정했고, 측정이 그 가설을 기각한 사례입니다.

← [의사결정 목록](README.md)

## 트리거

Kafka 레이어를 튜닝하려 했으나 현재 부하는 3,349 events/sec(약 1.5 MB/s)로 단일 broker 용량의 1~2%에 불과해, throughput·latency 튜닝은 측정 가능한 차이를 만들 수 없었다. 부하를 올리는 것도 불가능하다 — producer가 로컬 머신에서 공용 인터넷을 거쳐 EC2로 붙는 구조라 업링크와 시뮬레이션 CPU가 broker보다 먼저 한계에 닿는다.

실제 제약은 처리량이 아니라 **디스크**다. Kafka volume을 Airflow·serving-api·dashboard와 공유하며 `no space left on device`를 두 번 겪었다.

최초 가설은 "producer batch(`linger.ms`)를 키우면 압축률이 올라 저장량이 줄어든다"였다.

## 관측된 사실

격리된 broker(`apache/kafka:4.3.1`, 전용 volume, 3 partition)에 `data/mzlake` Bronze에서 추출한 실제 payload 100,000건(572 B/record)을 `KafkaPublisher` 설정 그대로 흘려 측정했다. **배포 이벤트 레이트(3,349/s)로 페이싱**했다 — 전속으로 보내면 `batch.size`가 먼저 차서 `linger.ms`가 무효과로 보인다.

`linger.ms` (lz4 고정)

| linger.ms | msgs/batch | disk B/record |
| --- | --- | --- |
| 0 | 1.0 | 564.2 |
| 5 | 9.6 | 268.8 |
| **20 (당시 설정)** | 33.0 | 226.7 |
| 100 | 138.4 | 210.2 |
| 500 | 209.5 | 208.1 |
| 1000 | 213.4 | 208.0 |

codec (linger 20 고정)

| codec | disk B/record | 압축률 | µs/event | 무제한 처리량 |
| --- | --- | --- | --- | --- |
| none | 607.0 | 0.94x | 3.0 | 107,015 ev/s |
| snappy | 226.4 | 2.53x | — | — |
| lz4 | 226.2 | 2.53x | 2.8 | 76,257 ev/s |
| gzip | 142.9 | 4.00x | 14.8 | 40,581 ev/s |
| **zstd** | **132.1** | **4.33x** | 3.5 | 72,544 ev/s |

`zstd` + `linger 500`은 125.3 B/record(4.57x)였다.

레코드 유실이 없음을 별도로 확인했다 — produce 100,000건 = broker 잔존 100,000건(end offset 합), `cleanup.policy=delete`. 이 payload의 distinct `trip_id`는 116개뿐이라, 만약 log compaction이 돌았다면 116건(0.12%)만 남아 자릿수가 달라진다.

## 근본 원인

압축률의 상한을 정하는 것은 batch 크기가 아니라 **codec**이다.

`linger=0`에서 batch당 1건이면 압축률이 1.01x다. Kafka가 record batch 단위로 압축한다는 사실이 그대로 드러난다. 그래서 batch를 키우면 압축률이 오르는 것은 맞다 — 다만 lz4는 2.75x에서 천장에 닿고, 당시 설정인 `linger=20`이 이미 그 중 2.52x를 확보한 상태였다.

lz4·snappy 같은 약한 압축기는 반복을 찾으려면 충분한 윈도우가 필요해 batch 크기에 민감하다. zstd는 사전과 엔트로피 코딩으로 33건짜리 batch에서도 한계 근처까지 짜낸다. **batch를 키워 약한 압축기를 돕는 것보다, 작은 batch에서도 잘 하는 압축기로 바꾸는 것이 압도적으로 효과적이다.**

## 선택지

| 대안 | 장점 | 단점 | 기각 이유 |
| --- | --- | --- | --- |
| `linger.ms` 20 -> 500 (최초 가설) | codec 변경 없음 | 개선폭 8%, publish 지연과 미전송 버퍼 노출 증가 | **측정으로 기각** — 전체 개선폭 45% 중 3pp에 불과 |
| gzip | 4.00x | CPU 14.8 µs/event (zstd의 4배), 압축률은 zstd보다 낮음 | 압축률·CPU 양쪽에서 zstd에 열등 |
| snappy | — | lz4와 동일한 2.53x | 현재 대비 개선 없음 |
| 압축 해제(none) | CPU 최소 | 607 B/record로 원본 572 B보다 큼 | key(24 B)와 record·batch 헤더 때문에 손해만 남음 |
| Bronze Parquet codec 변경 | 누적 절감은 더 큼 | 계층이 다르고 배치 성능까지 영향 | 별건 — 후속 이슈로 분리 |
| **zstd, linger 유지** | 저장량 -42%, 지연 변화 없음 | producer CPU +0.7 µs/event | **채택** |

## 결정

producer `compression.type`을 `lz4` -> `zstd`로 변경하고 `linger.ms=20`은 유지한다.

broker 쪽은 변경하지 않는다 — `infra/compose/kafka.yaml`에 `compression.type`이 없어 기본값 `producer`(pass-through)이므로 재압축이 일어나지 않고 broker CPU 비용도 없다.

## 최적화 대상과 포기한 것

**얻은 것**: broker 저장량 226 -> 132 B/record(-42%). 시간당 2.73 GB -> 1.59 GB.

**내놓은 것**: producer CPU event당 +0.7 µs. 현재 부하 환산 시 코어의 약 1.2%다. zstd는 무제한 전송에서도 72,544 ev/s를 견뎌 현재 부하의 21배 여유가 있다.

**의도적으로 남긴 것**: `linger=500`으로 얻을 수 있는 추가 5%(132.1 -> 125.3). 지연과 미전송 버퍼 노출, 그리고 설명해야 할 변수가 하나 늘어나는 대가로는 3pp가 아깝다.

## 검증 방법

- `KafkaPublisher`의 `compression.type`이 `zstd`이고 나머지 producer 설정은 불변 (`test_publisher.py`)
- producer -> Kafka -> stream-processor 경로에서 소비가 정상이고 이벤트 수가 일치 (Kafka client는 2.1부터 zstd 지원, broker는 4.3.1)
- 동일 harness 재실행 시 disk B/record가 132 근방 (`.local/kafka-tuning/`, run 간 변동 0.2%)

## 결과

측정이 가설을 뒤집었다. `linger.ms`를 키우려고 시작한 작업이 codec 변경으로 끝났고, 최초 가설은 전체 개선폭의 7%만 설명했다.

부수적으로 **partition 수와 압축률이 상충한다**는 사실이 드러났다. batch 충전 속도는 partition 수로 나뉜다(3,349/s ÷ 3 = partition당 1,116/s, linger 20에서 33건). 읽기 병렬성을 위해 partition을 12개로 늘리면 batch가 약 6건으로 줄어 압축률이 떨어진다. 이 지점에서 zstd 선택이 보험으로도 작동한다 — 작은 batch에서 lz4는 2.13x 수준으로 무너지지만 zstd는 견고하다.

## 재검토 조건

- 부하가 현재의 20배(약 67,000 ev/s)를 넘어 producer CPU가 병목이 될 때 — lz4 재검토
- partition을 3에서 늘릴 때 — partition당 batch 건수를 유지하도록 `linger.ms`를 함께 재산정
- payload 스키마가 크게 바뀔 때 — 4.33x는 현재 필드 구성에 대한 측정값
- `batch.size`(128 KB)를 키울 때 — `linger` 500과 1000의 결과가 동일한 것은 batch가 이 한계에 닿았다는 뜻이므로, 그 이상은 `linger`가 아니라 이 값이 좌우한다

## 근거

- 이슈 #476
- `services/sensor-producer/src/sensor_producer/publisher.py`
- 측정 harness와 16개 run 원본: `.local/kafka-tuning/` (untracked)
- 총량 상한(`log.retention.bytes`)은 별건이다. zstd는 디스크가 차는 속도를 늦출 뿐 상한을 걸지 않으며, 현재 유일한 제한인 7일 기본값은 1.59 GB/hour 기준 267 GB라 실질적으로 무제한이다.
