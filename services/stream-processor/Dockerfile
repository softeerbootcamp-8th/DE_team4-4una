FROM python:3.12-slim

# PySpark 실행과 컨테이너 종료 처리에 필요한 런타임
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

# pyspark은 PyPI에 wheel이 없고 434MB sdist만 있어서, 받아둔 sdist와 빌드 산출물이
# uv 캐시에 1.8GB 쌓인다. 런타임 CMD는 --no-sync라 캐시를 읽을 일이 없다.
# 반드시 같은 RUN에서 지운다 -- 다음 레이어에서 지우면 삭제 전 레이어가 그대로 남는다.
RUN uv sync --locked --package stream-processor --no-dev --no-editable \
    && rm -rf /root/.cache/uv

# 운영 EC2가 Maven Central에 연결되지 않아도 시작하도록 Kafka/S3A JAR를 미리 받는다
RUN SPARK_LOCAL_IP=127.0.0.1 uv run --no-sync --package stream-processor python -c \
    "from pyspark.sql import SparkSession; from stream_processor.cli import SPARK_PACKAGES; session = SparkSession.builder.master('local[1]').config('spark.ui.enabled', 'false').config('spark.jars.packages', SPARK_PACKAGES).getOrCreate(); session.stop()"

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["uv", "run", "--no-sync", "--package", "stream-processor", "stream-processor"]
