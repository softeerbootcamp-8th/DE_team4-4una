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

SPARK_LOCAL_IP=127.0.0.1 uv run --package stream-processor stream-processor
```

Spark는 실제 Bronze 적재 시각인 `_ingested_at`을 센서 필드와 같은 `value` JSON
안에 추가합니다. Kafka 메타데이터에는 별도 적재 시각 컬럼을 추가하지 않습니다.
동일한 checkpoint 경로로 재시작하면 마지막으로 처리한 Kafka offset 이후부터
이어서 적재합니다.
