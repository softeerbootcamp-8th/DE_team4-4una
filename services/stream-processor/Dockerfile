FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        openjdk-21-jre-headless \
        tini \
    && rm -rf /var/lib/apt/lists/* \
    && JAVA_REAL_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" \
    && ln -s "$JAVA_REAL_HOME" /opt/java

ENV JAVA_HOME=/opt/java \
    PATH="/opt/java/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    STREAM_SPARK_MASTER="local[*]"

COPY --from=ghcr.io/astral-sh/uv:0.12.3 /uv /uvx /bin/
WORKDIR /app
COPY . .

RUN uv sync --locked --package stream-processor --no-dev --no-editable

# 운영 EC2가 Maven Central에 연결되지 않아도 시작하도록 Kafka/S3A JAR를 미리 받는다
RUN SPARK_LOCAL_IP=127.0.0.1 uv run --no-sync --package stream-processor python -c \
    "from pyspark.sql import SparkSession; from stream_processor.cli import SPARK_PACKAGES; session = SparkSession.builder.master('local[1]').config('spark.ui.enabled', 'false').config('spark.jars.packages', SPARK_PACKAGES).getOrCreate(); session.stop()"

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["uv", "run", "--no-sync", "--package", "stream-processor", "stream-processor"]
