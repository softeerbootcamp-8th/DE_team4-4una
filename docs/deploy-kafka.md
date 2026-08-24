# Kafka 배포

Kafka는 Project EC2에서 상시 실행하고, 로컬에서 실행하는 Sensor Producer와
Project EC2에서 실행하는 Stream Processor가 함께 사용한다. 자동 배포는
[deploy-kafka.yml](../.github/workflows/deploy-kafka.yml)이 담당하고 실행 설정은
[kafka.yaml](../infra/compose/kafka.yaml)이 단일 기준이다.

## 연결 경계

| 클라이언트 | 주소 | 용도 |
| --- | --- | --- |
| EC2의 Stream Processor와 kafka-exporter | `localhost:9092` | 내부 listener |
| 개발자의 로컬 Sensor Producer | `<KAFKA_EXTERNAL_HOST>:9094` | 외부 listener |

두 listener 모두 현재 PLAINTEXT다. 따라서 EC2 Security Group의 TCP 9094는
Sensor Producer를 실행하는 공인 IP의 `/32`에만 허용해야 한다. 9092는 호스트의
`127.0.0.1`에만 bind하며 외부에 공개하지 않는다.

## 자동 배포 흐름

```text
main push (경로 감지) → kafka.yaml 구문 검증
  → SSH로 EC2 고정 경로에 kafka.yaml 복사
  → docker compose up -d --wait
  → sensor-events topic 생성 또는 partition 수 증가
  → topic 상태 출력
```

독립 워크플로다. `main` push 중 아래 경로가 바뀌었을 때만 실행되고, Actions 탭에서
`Run workflow`로 수동 실행할 수도 있다. 고정 이미지(`apache/kafka`)를 쓰므로 배포에
영향을 주는 것은 compose 설정뿐이다.

```
infra/compose/kafka.yaml   .github/workflows/deploy-kafka.yml
```

**다른 서비스와 달리 `main` 기준이다.** broker를 건드리면 스트림 전체가 멈추므로
`develop` 병합만으로는 반영하지 않는다. 검증된 `develop`을 `main`에 병합할 때만
배포된다.

CI 게이트는 branch protection이 맡는다. 전에는 이 배포가 `ci.yml` 안에서
`needs: ci-passed`로 CI 완료를 기다렸지만, 지금은 독립 워크플로라 그럴 수 없다.
**`main`의 Require status checks에 `CI Passed`가 걸려 있어야** 검증되지 않은
커밋이 `main`에 들어가는 것을 막을 수 있다. `ci.yml`은 `main`으로 향하는 PR에서도
실행되므로 이 설정이 가능하다.

## GitHub 설정

### 필수

| 구분 | 이름 | 설명 |
| --- | --- | --- |
| Variable | `EC2_HOST` | SSH 접속 가능한 Project EC2의 공개 IP 또는 DNS |
| Secret | `EC2_SSH_PRIVATE_KEY` | EC2 SSH 개인키 전문 |

### 선택

| Variable | 기본값 | 설명 |
| --- | --- | --- |
| `EC2_USER` | `ec2-user` | SSH 사용자 |
| `ORCHESTRATION_REPO_DIR` | `/home/ec2-user/DE_team4-4una` | Compose 파일을 동기화할 기존 저장소 경로 |
| `KAFKA_EXTERNAL_HOST` | `EC2_HOST` | Kafka가 외부 client에게 advertise할 공개 IP 또는 DNS |
| `KAFKA_SENSOR_TOPIC` | `sensor-events` | producer와 consumer가 사용할 topic |
| `KAFKA_SENSOR_TOPIC_PARTITIONS` | `3` | topic의 최소 partition 수 |

Kafka 이미지는 Docker Hub에서 받으므로 ECR 저장소나 GitHub OIDC 배포 Role은 필요하지
않다. EC2에는 Docker, Docker Compose plugin과 GitHub Actions runner에서 접속 가능한
SSH 설정이 필요하다.

## 이미 실행 중인 broker 처리

배포는 `docker compose down`이나 volume 삭제를 하지 않고 다음 명령을 사용한다.

```bash
KAFKA_EXTERNAL_HOST=<public-ip-or-dns> \
docker compose -p de4-kafka -f infra/compose/kafka.yaml \
  up -d --wait --wait-timeout 120
```

- 실행 설정이 같으면 기존 broker를 재시작하지 않는다
- 이미지나 Compose 설정이 바뀌면 해당 컨테이너만 recreate한다
- recreate 중에는 짧은 broker 중단이 발생하지만 `de4-kafka-data` named volume은 유지된다
- 기존 `sensor-events` topic도 유지하며 partition 수가 설정값보다 작을 때만 늘린다
- Kafka partition은 줄일 수 없으므로 이미 더 많다면 그대로 둔다

기존 broker도 Compose project 이름이 `de4-kafka`이고 같은 named volume을 사용하는
경우에 위 동작이 적용된다. 다른 project 이름이나 `docker run`으로 띄운 broker는 자동으로
인수하지 않으며, 같은 port를 점유하고 있다면 배포가 명확하게 실패한다. 현재 EC2의
`de4-kafka-kafka-1`은 이 조건을 만족한다.

`docker compose down -v`와 `docker volume rm de4-kafka-data`는 topic, message, consumer
offset을 삭제하므로 사용하지 않는다. 단일 broker와 replication factor 1 구성이라
broker 장애 중 무중단 처리는 보장하지 않으며 현재 PoC 범위의 제약이다.

## 수동 확인

EC2에서 내부 listener와 topic을 확인한다.

```bash
cd /home/ec2-user/DE_team4-4una
docker compose -p de4-kafka -f infra/compose/kafka.yaml ps
docker compose -p de4-kafka -f infra/compose/kafka.yaml exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic sensor-events
```

로컬 Sensor Producer는 다음 주소를 사용한다.

```bash
export KAFKA_BOOTSTRAP_SERVERS=<KAFKA_EXTERNAL_HOST>:9094
```
