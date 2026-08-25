FROM public.ecr.aws/emr-serverless/spark4/emr-spark-8.0.0:latest

USER root

# uv 설치
COPY --from=ghcr.io/astral-sh/uv:0.12.3 /uv /uvx /usr/local/bin/

WORKDIR /app
COPY . .

# Batch Jobs의 외부 의존성 설치
# pyspark/py4j는 EMR Serverless 기본 제공 버전을 사용한다.
RUN uv export \
    --frozen \
    --package batch-jobs \
    --no-dev \
    --no-emit-project \
    --no-emit-package de4-core \
    --no-emit-package pyspark \
    --no-emit-package py4j \
    -o /tmp/batch-jobs-requirements.txt \
    && uv pip install \
        --system \
        --python /usr/bin/python3.12 \
        -r /tmp/batch-jobs-requirements.txt \
    && uv pip install \
        --system \
        --python /usr/bin/python3.12 \
        --no-deps \
        ./libs/de4-core \
        ./services/batch-jobs \
    && rm -f /tmp/batch-jobs-requirements.txt \
    && rm -rf /root/.cache/uv

# PostgreSQL JDBC Driver
RUN curl -fL \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar \
    -o "${SPARK_HOME}/jars/postgresql-42.7.4.jar"

# batch_jobs는 python3.12 전용 경로에만 설치되는데 base 이미지 기본 python3는
# 3.9라, PYSPARK_PYTHON을 명시하지 않으면 Spark driver/executor가 모듈을 못 찾는다(#360).
# Job Run 제출 쪽 --conf(emr_serverless.py)와 병행해 이미지 레벨에서도 강제한다.
ENV PYSPARK_PYTHON=/usr/bin/python3.12

# EMR Serverless는 hadoop 사용자로 실행
USER hadoop:hadoop