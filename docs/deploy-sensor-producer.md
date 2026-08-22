# Sensor Producer 배포

`main`에 머지된 커밋을 EC2의 sensor-producer 컨테이너로 배포하는 절차와 사전 조건을
정리한다. 파이프라인은
[.github/workflows/deploy-sensor-producer.yml](../.github/workflows/deploy-sensor-producer.yml)
하나로 구성된다.

## sensor-producer는 상시 서비스가 아니다

stream-processor(상시 Spark Streaming job), serving-api(상시 HTTP 서버)와 달리
sensor-producer는 **유한한 리플레이 배치 작업**이다. `sensor-producer run`은 주어진
trip 목록을 처음부터 끝까지 재생해 Kafka로 발행하고, 다 끝나면 프로세스가 정상
종료(`exit 0`)한다. 재생 시간은 `--time-scale`에 따라 다르지만 실시간 배율(`1.0`,
기본값)이면 실제 운행 시간만큼 걸린다.

그래서 이 워크플로는 컨테이너를 띄운 뒤 **끝까지 기다리지 않는다.** 10초만 대기한 뒤
`docker inspect`로 "죽지 않고 떠 있는지"만 확인하고 끝낸다(stream-processor의 기동
확인 패턴과 동일). 리플레이 자체는 워크플로가 끝난 뒤에도 컨테이너 안에서 계속
진행되고, 스스로 끝나면 컨테이너는 재시작 없이 그대로 종료 상태로 남는다. 다음
`main` push가 배포를 다시 트리거하면 컨테이너를 교체하면서 리플레이가 처음부터 다시
시작된다.

## 흐름

```
main에 머지 → CI 통과 → ci.yml이 배포 워크플로 호출
  → repository variables 확인
  → 이미지 빌드, ECR push (태그 = commit SHA)
  → SSH로 EC2에 배포 스크립트 전송
       인스턴스: pull → 기존 컨테이너 교체 → 10초 대기 후 기동 확인
                 실패하면 로그를 남기고 워크플로 실패
  → job summary에 commit, image, host 기록
```

`ci.yml`이 `workflow_call`로 호출하는 reusable workflow이고 OIDC/`sub` 클레임 이유는
[deploy-serving-api.md](deploy-serving-api.md#흐름)와 동일하다.

## GitHub 설정

### Variables — 필수

`AWS_REGION`, `AWS_DEPLOY_ROLE_ARN`, `EC2_HOST`은 다른 서비스 배포와 공유하는
값이라 이미 등록되어 있다면 새로 만들 필요가 없다.

| 변수 | 예시 | 비고 |
| --- | --- | --- |
| `AWS_REGION` | `ap-northeast-2` | 공유 |
| `AWS_DEPLOY_ROLE_ARN` | `arn:aws:iam::123456789012:role/github-actions-deploy` | 공유 |
| `SENSOR_PRODUCER_ECR_REPOSITORY` | `de4/sensor-producer` | sensor-producer 전용 |
| `EC2_HOST` | EC2의 퍼블릭 IP 또는 DNS | 공유 — stream-processor/orchestration과 같은 인스턴스를 쓴다면 동일 값 |

`SENSOR_PRODUCER_ECR_REPOSITORY`에는 리포지토리 이름만 넣는다. 이유는
[deploy-serving-api.md](deploy-serving-api.md#variables--필수)와 동일하다.

### Secrets — 필수

| 이름 | 내용 |
| --- | --- |
| `EC2_SSH_PRIVATE_KEY` | EC2 키페어의 개인키 전문 (공유) |

### Variables — 선택

| 변수 | 기본값 |
| --- | --- |
| `EC2_USER` | `ec2-user` |
| `SENSOR_PRODUCER_ENV_FILE` | `/etc/sensor-producer/sensor-producer.env` |

## AWS 사전 준비

OIDC provider, 배포 Role, ECR 리포지토리(lifecycle policy 포함) 설정은
[deploy-serving-api.md](deploy-serving-api.md#aws-사전-준비)를 그대로 따른다.
`ECR_REPOSITORY` 자리에 `SENSOR_PRODUCER_ECR_REPOSITORY` 값을 쓴다.

**EC2 인스턴스 프로파일에 S3 read 권한이 추가로 필요하다.** `SENSOR_PRODUCER_TRIPS_URI`나
`SENSOR_ENVIRONMENT_POINTER_URI`/`SENSOR_ENVIRONMENT_MANIFEST_URI`에 `s3://` URI를
쓸 경우, 인스턴스가 그 버킷에서 직접 읽는다(README의
[S3 road environment](../services/sensor-producer/README.md#s3-road-environment)
참고). 기존 ECR pull 권한에 아래를 더한다.

```json
{
  "Effect": "Allow",
  "Action": ["s3:GetObject"],
  "Resource": "arn:aws:s3:::<BUCKET>/*"
}
```

## EC2 사전 조건

docker, AWS CLI, docker 그룹 등록은
[deploy-serving-api.md](deploy-serving-api.md#ec2-사전-조건)와 동일하다. sensor-producer는
HTTP 포트를 열지 않으므로 보안그룹에 추가로 열 인바운드는 없다.

## env 파일

기본 경로는 `/etc/sensor-producer/sensor-producer.env`이고, 사람이 인스턴스에 직접
만든다. 값은 저장소에 기록하지 않는다.

```bash
sudo install -d -m 700 /etc/sensor-producer
sudo chown root:root /etc/sensor-producer/sensor-producer.env
sudo chmod 600 /etc/sensor-producer/sensor-producer.env
```

이 파일은 두 가지 용도로 쓰인다. ① `docker run --env-file`로 컨테이너에 그대로
주입되어 `sensor-producer` CLI가 환경변수로 읽는 값, ② 배포 스크립트가 셸로
`source`해서 `docker run` 명령 뒤에 CLI 옵션으로 조립하는 값. 어느 쪽이든 파일
형식은 동일한 `KEY=값`이다.

### 필수 — 트립 원본 (둘 중 하나)

| 키 | 용도 |
| --- | --- |
| `SENSOR_PRODUCER_TRIPS_URI` | `--trips-uri`로 전달. S3의 월별 HVFHV Parquet 하나 (예: `s3://de4-lake/raw/tlc/fhvhv_tripdata_2024-02.parquet`). **같이 쓸 때 `SENSOR_PRODUCER_SOURCE_DATE`도 필수** |
| `SENSOR_PRODUCER_TRIPS_PATH` | `--trips-path`로 전달. 로컬 fixture용 (`trips.json`) — 운영 배포에는 보통 안 씀 |

두 값이 모두 비어 있으면 배포 스크립트가 즉시 실패한다. 실제 값(어떤 월/구간의
데이터를 반복 재생할지)은 아직 정해지지 않았으므로, 이 문서 작성 시점에는 EC2에
채워 넣어야 할 항목으로만 표시해 둔다.

### 조건부 필수

| 키 | 용도 |
| --- | --- |
| `SENSOR_PRODUCER_SOURCE_DATE` | `--source-date`로 전달 (`YYYY-MM-DD`). `SENSOR_PRODUCER_TRIPS_URI`를 쓸 때 필수 |

### 선택 — Kafka / 환경 (CLI가 환경변수로 직접 읽음)

| 키 | 기본값 |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` — 컨테이너를 `--network host`로 띄우므로 같은 EC2에서 `docker compose`로 뜬 Kafka에 이 기본값 그대로 붙는다. 보통 채울 필요 없다 |
| `KAFKA_SENSOR_TOPIC` | `sensor-events` |
| `SENSOR_ENVIRONMENT_POINTER_URI` | 없음 — road environment를 S3 활성 포인터로 따라갈 때 사용 |
| `SENSOR_ENVIRONMENT_MANIFEST_URI` | 없음 — 특정 빌드를 고정할 때 사용 (포인터와 동시 사용 불가) |
| `SENSOR_CACHE_DIR` | 컨테이너 내부 `/var/lib/de4/cache` — 호스트에 마운트하지 않으므로 컨테이너를 교체하면 캐시도 비워진다 |

### 선택 — 리플레이 파라미터 (배포 스크립트가 CLI 옵션으로 조립)

| 키 | 전달되는 옵션 |
| --- | --- |
| `SENSOR_PRODUCER_ROAD_SEGMENT_PATH` | `--road-segment-path` (로컬 road_segment Parquet를 쓸 때만) |
| `SENSOR_PRODUCER_RUN_ID` | `--run-id` (기본값은 CLI의 `nyc-smoke-v1`) |
| `SENSOR_PRODUCER_MAX_TRIPS` | `--max-trips` |
| `SENSOR_PRODUCER_VEHICLE_MIX` | `--vehicle-mix` |
| `SENSOR_PRODUCER_VEHICLE_PROFILE_ID` | `--vehicle-profile-id` (`--vehicle-mix`와 동시 사용 불가) |

## 배포 동작

컨테이너를 지우고 새로 띄우는 recreate 방식이다.

```
docker pull <새 이미지>
docker rm --force <기존 컨테이너>   (없으면 무시)
docker run -d --network host <새 이미지> run ...   (리플레이 시작, 백그라운드)
10초 대기
docker inspect Running == true     통과 — 워크플로 성공, 리플레이는 계속 진행
docker inspect Running != true     실패 — 컨테이너 로그를 남기고 워크플로 실패
```

serving-api처럼 이전 이미지로 자동 롤백하지는 않는다. 10초 안에 죽었다는 것은
env 파일 설정 오류나 이미지 자체 문제일 가능성이 높아, 로그를 보고 원인을 고친
뒤 다시 push하는 편이 낫다고 판단했다.

배포된 컨테이너에는 `org.opencontainers.image.revision` 라벨로 commit SHA가 남는다.

## 실패했을 때

| 증상 | 원인 |
| --- | --- |
| `repository variables가 비어 있습니다` | 필수 variables 미설정 |
| SSH 연결 시간 초과 / `Permission denied` | [deploy-serving-api.md](deploy-serving-api.md#실패했을-때)와 동일 |
| `Sensor Producer env file not found` | 인스턴스에 env 파일 미생성, 또는 `SENSOR_PRODUCER_ENV_FILE` 경로 불일치 |
| `SENSOR_PRODUCER_TRIPS_URI 또는 SENSOR_PRODUCER_TRIPS_PATH 중 하나가 필요합니다` | env 파일에 트립 원본 미설정 |
| `SENSOR_PRODUCER_TRIPS_URI를 쓰려면 SENSOR_PRODUCER_SOURCE_DATE도 필요합니다` | `--trips-uri`만 넣고 `--source-date`를 안 넣음 |
| `docker pull` 실패 | 인스턴스 프로파일의 ECR 권한 누락 |
| 10초 뒤 `Sensor Producer failed to start.` | 컨테이너가 시작하자마자 죽음 — 같이 출력되는 `docker logs`를 본다. `choose only one environment pointer or manifest URI`, `--source-date is required with --trips-uri` 같은 CLI 자체 검증 에러가 흔하다 |
| `exec format error` (컨테이너 로그) | 이미지가 arm64로 빌드되지 않았다 |
