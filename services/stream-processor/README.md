# Stream Processor

Kafka의 `sensor-events`를 Spark Structured Streaming으로 읽어 로컬 Bronze
Parquet에 append 방식으로 적재합니다. Kafka 토픽은 실행 전에 생성되어 있어야
합니다.

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
export STREAM_TRIGGER_INTERVAL_SECONDS=5
export STREAM_BRONZE_OUTPUT_PATH=data/local-lake/bronze/sensor-events
export STREAM_BRONZE_CHECKPOINT_LOCATION=checkpoints/bronze-sensor-events
# Kafka에 이만큼(전체 partition 합계) 모일 때까지 배치를 미뤄 Bronze 파일을 크게 만든다.
# 기본값 600000은 parquet 약 128MB다(우리 Bronze 실측으로 디스크에서 행당 223B).
# 데이터가 적은 스모크 테스트에서는 0으로 꺼라. 켜 둔 채로 몇백 건만 넣으면
# STREAM_MAX_TRIGGER_DELAY만큼(기본 5분) 기다린 뒤에야 파일이 생겨서, 스트림이
# 멈춘 것처럼 보인다.
export STREAM_MIN_OFFSETS_PER_TRIGGER=600000
# 위 조건과 항상 같이 걸린다. 양이 모자라도 이 시간이 지나면 배치를 실행한다.
# 이게 없으면 한산할 때 배치가 아예 돌지 않아 멈춘 것처럼 보인다.
export STREAM_MAX_TRIGGER_DELAY=5m
# 배치 한 번이 남길 파일 수. Kafka partition마다 task가 하나씩 생기므로
# 합치지 않으면 배치마다 partition 수만큼 잔파일이 쌓인다.
export STREAM_BRONZE_OUTPUT_PARTITIONS=1

SPARK_LOCAL_IP=127.0.0.1 uv run --package stream-processor stream-processor
```

Spark는 실제 Bronze 적재 시각인 `_ingested_at`을 센서 필드와 같은 `value` JSON
안에 추가합니다. Kafka 메타데이터에는 별도 적재 시각 컬럼을 추가하지 않습니다.
동일한 checkpoint 경로로 재시작하면 마지막으로 처리한 Kafka offset 이후부터
이어서 적재합니다.
