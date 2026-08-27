# Stream Processor

Kafka의 `sensor-events`를 Spark Structured Streaming으로 읽어 로컬 또는 S3
Bronze Parquet에 append 방식으로 적재합니다. Kafka 토픽은 실행 전에 생성되어
있어야 합니다.

## 로컬 실행

```bash
docker compose -f infra/compose/kafka.yaml up -d
docker compose -f infra/compose/kafka.yaml exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic sensor-events \
  --partitions 1 \
  --replication-factor 1

export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_SENSOR_TOPIC=sensor-events
export KAFKA_STARTING_OFFSETS=earliest
export STREAM_TRIGGER_INTERVAL_SECONDS=30
export STREAM_BRONZE_OUTPUT_PATH=data/local-lake/bronze/sensor-events
export STREAM_BRONZE_CHECKPOINT_LOCATION=checkpoints/bronze-sensor-events
# Kafka에 이만큼(전체 partition 합계) 모일 때까지 배치를 미뤄 Bronze 파일을 크게 만든다.
# 기본값 600000은 parquet 약 128MB다(우리 Bronze 실측으로 디스크에서 행당 223B).
# 데이터가 적은 스모크 테스트에서는 0으로 꺼라. 켜 둔 채로 몇백 건만 넣으면
# STREAM_MAX_TRIGGER_DELAY만큼(기본 30초) 기다린 뒤에야 파일이 생겨서, 스트림이
# 멈춘 것처럼 보인다.
export STREAM_MIN_OFFSETS_PER_TRIGGER=600000
# 위 조건과 항상 같이 걸린다. 양이 모자라도 이 시간이 지나면 배치를 실행한다.
# 이게 없으면 한산할 때 배치가 아예 돌지 않아 멈춘 것처럼 보인다.
export STREAM_MAX_TRIGGER_DELAY=30s
# 복구 backlog가 한 번에 커지지 않도록 30초 batch를 120만 건으로 제한한다.
export STREAM_MAX_OFFSETS_PER_TRIGGER=1200000
# 배치 한 번이 남길 파일 수. 2 vCPU Spark 런타임에서는 두 writer task가
# coalesce(1)의 단일 S3 writer 병목을 피하면서 파일 수를 제한한다.
export STREAM_BRONZE_OUTPUT_PARTITIONS=2

SPARK_LOCAL_IP=127.0.0.1 uv run --package stream-processor stream-processor
```

Spark는 실제 Bronze 적재 시각인 `_ingested_at`을 센서 필드와 같은 `value` JSON
안에 추가합니다. Kafka 메타데이터에는 별도 적재 시각 컬럼을 추가하지 않습니다.
동일한 checkpoint 경로로 재시작하면 마지막으로 처리한 Kafka offset 이후부터
이어서 적재합니다.

하나의 `STREAM_BRONZE_OUTPUT_PATH`에는 하나의 고정 checkpoint만 사용해야 합니다.
동일한 출력 경로에 새 checkpoint를 연결하면 File Sink의 `_spark_metadata` batch ID와
충돌해 Kafka offset은 전진하지만 Parquet가 기록되지 않을 수 있습니다.

Bronze 데이터는 센서의 UTC `event_time`을 기준으로 다음 경로에 저장됩니다.

```text
${STREAM_BRONZE_OUTPUT_PATH}/event_date=2026-08-14/hour=05/part-*.parquet
```

파싱할 수 없는 JSON도 유실하지 않으며 이 경우 Kafka record timestamp를 파티션
시각으로 사용합니다. `event_date`와 `hour`는 물리 파티션 컬럼이며 센서 `value`
JSON에는 추가하지 않습니다.

## EC2에서 S3 적재

이미지에는 Java와 Kafka/S3A 커넥터가 포함됩니다. AWS 자격증명은 이미지나
환경변수에 넣지 않고 EC2 Instance Role에서 가져옵니다. Spark 경로에는
`s3://`가 아니라 `s3a://`를 사용합니다.

CD 배포 전 다음 GitHub Repository Variable을 등록해야 합니다. 둘 중 하나라도
비어 있거나 `s3a://` URI가 아니면 기존 컨테이너를 교체하기 전에 배포가 실패합니다.

| 변수 | 예시 |
| --- | --- |
| `STREAM_BRONZE_OUTPUT_PATH` | `s3a://de4-data-lake/bronze/sensor-events` |
| `STREAM_BRONZE_CHECKPOINT_LOCATION` | `s3a://de4-data-lake/checkpoints/stream-processor/sensor-events` |

EC2 Instance Role에는 두 경로를 조회하고 쓸 수 있는 S3 권한이 필요합니다. 동일한
출력 경로에는 배포를 거쳐도 같은 checkpoint URI를 계속 사용해야 합니다.

```bash
docker build -f services/stream-processor/Dockerfile -t de4-stream-processor .

docker run --rm --network host \
  -e AWS_REGION=ap-northeast-2 \
  -e KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
  -e KAFKA_SENSOR_TOPIC=sensor-events \
  -e KAFKA_STARTING_OFFSETS=earliest \
  -e STREAM_SPARK_MASTER='local[2]' \
  -e STREAM_MIN_OFFSETS_PER_TRIGGER=0 \
  -e STREAM_BRONZE_OUTPUT_PATH=s3a://de4-data-lake/bronze/sensor-events \
  -e STREAM_BRONZE_CHECKPOINT_LOCATION=s3a://de4-data-lake/checkpoints/stream-processor/sensor-events \
  de4-stream-processor
```

스모크 테스트에서는 `STREAM_MIN_OFFSETS_PER_TRIGGER=0`을 사용합니다. 운영값은
실측 이벤트 크기와 허용 지연시간을 기준으로 조정합니다.

## 30초 Bronze freshness canary

기본값은 `processingTime=30s`, `maxTriggerDelay=30s`,
`maxOffsetsPerTrigger=1200000`이다. 센서 `event_time`이 04:00인 이벤트는
다음 batch에서 읽혀도 `event_date=.../hour=04`에 기록된다.

실제 EC2 canary에서는 Spark Streaming dashboard로 연속 15분 이상 다음을 확인한다.

- p95 `stream_processor_batch_duration_seconds` < 30초
- `processedRowsPerSecond` > `inputRowsPerSecond`
- Kafka lag가 장기적으로 커지지 않음
- 시간 경계 이벤트가 올바른 `event_date/hour` 파티션에 기록됨

## Bronze writer canary

`STREAM_BRONZE_OUTPUT_PARTITIONS=2` 는 2 vCPU Spark Streaming 런타임의
초기 canary 값이다. 적용 전후에 다음을 같은 Kafka offset 범위에서 비교한다.

- `stream_processor_processed_rows_per_second`와
  `stream_processor_batch_duration_seconds`
- batch당 Parquet 파일 수와 평균 파일 크기
- Bronze의 `(topic, partition, offset)`과 value 내 `event_id` 중복/누락

이 값이 2 vCPU에서 가장 빠르다는 결론을 미리 뜻하지는 않는다. batch duration이
trigger interval을 넘거나 Kafka lag가 지속적으로 커지면 1개와 3개도 같은
방법으로 재검증한다.
